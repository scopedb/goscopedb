/*
 * Copyright 2024 ScopeDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package scopedb

import (
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestOversizedResponsesBoundReadsAndRetainedErrors(t *testing.T) {
	for _, test := range []struct {
		name        string
		status      int
		code        string
		appendState AppendState
	}{
		{name: "statement", status: 503, code: "statement_submission_outcome_unknown"},
		{name: "catalog", status: 502, code: "catalog_commit_outcome_unknown"},
		{name: "append acknowledgement", status: 200, appendState: AppendStateUnknown},
		{name: "append rate limit", status: 429, appendState: AppendStateRejected},
		{name: "append stream", status: 503, appendState: AppendStateUnknown},
	} {
		t.Run(test.name, func(t *testing.T) {
			// The prefix remains valid JSON if the oversized whitespace is cut,
			// but must not be trusted as a complete acknowledgement.
			prefix := `{"code":"rejected","message":"server rejected","retryable":true,"details":{"status":"rejected","row_errors":[]}}`
			if test.status == http.StatusOK {
				prefix = `{"status":"committed","num_rows_inserted":1}`
			}
			data := prefix + strings.Repeat(" ", 2*maxErrorResponseBodyBytes)
			if test.name == "append stream" {
				data = strings.Repeat("x", 2*maxErrorResponseBodyBytes)
			}
			body := &observedResponseBody{Reader: strings.NewReader(data)}
			client, err := NewClient(Config{
				Endpoint: "https://example.com", Compression: CompressionGzip,
				HTTPClient: &http.Client{Transport: clientRoundTripFunc(func(*http.Request) (*http.Response, error) {
					return &http.Response{StatusCode: test.status, Header: http.Header{"X-Request-Id": {"bounded-response"}, "Retry-After": {"2"}}, Body: body, ContentLength: -1}, nil
				})},
			})
			require.NoError(t, err)
			t.Cleanup(client.Close)
			var stream *AppendStream
			switch test.name {
			case "statement":
				_, err = client.Query(t.Context(), "SELECT 1")
			case "catalog":
				_, err = client.Catalog().Apply(t.Context(), &CatalogDocument{})
			case "append stream":
				stream, err = client.Table("events").AppendStream(AppendStreamOptions{MaxBufferedBytes: 1024})
				require.NoError(t, err)
				require.NoError(t, stream.Send(t.Context(), map[string]int{"id": 1}))
				report, shutdownErr := stream.Shutdown(t.Context())
				err = shutdownErr
				require.EqualValues(t, 1, report.UnknownRows)
			default:
				_, err = client.Table("events").AppendNDJSON(t.Context(), []byte(`{"id":1}`))
			}
			require.LessOrEqual(t, body.Size()-int64(body.Len()), int64(maxErrorResponseBodyBytes+1))
			require.True(t, body.closed)
			require.ErrorIs(t, err, ErrResponseTooLarge)
			var failure *Error
			require.ErrorAs(t, err, &failure)
			require.Equal(t, test.status, failure.HTTPStatus)
			require.Equal(t, "bounded-response", failure.RequestID)
			require.Equal(t, 2*time.Second, failure.RetryAfter)
			require.Equal(t, test.code, failure.Code)
			require.Equal(t, test.appendState == AppendStateRejected, failure.Retryable)
			require.LessOrEqual(t, len(failure.Message), maxErrorResponseBodyBytes)
			require.Empty(t, failure.Details)
			if test.appendState != "" {
				require.Equal(t, test.appendState, failure.AppendDetails.Status)
			}
			if stream != nil {
				last := stream.Stats().LastFailure
				require.LessOrEqual(t, len(last.Message), maxErrorResponseBodyBytes)
				require.Empty(t, last.Details)
				require.Empty(t, last.RowErrors)
			}
		})
	}
}

func TestResponseLimitPreservesCompleteDiagnosticsAndLargeResults(t *testing.T) {
	id := uuid.New()
	value := strings.Repeat("x", maxErrorResponseBodyBytes+1)
	prefix := `{"code":"schema_mismatch","message":"invalid column","details":{"status":"rejected","row_errors":[]}}`
	for _, operation := range []string{"error at limit", "query result", "catalog definition"} {
		t.Run(operation, func(t *testing.T) {
			client, err := NewClient(Config{
				Endpoint: "https://example.com", Compression: CompressionGzip,
				HTTPClient: &http.Client{Transport: clientRoundTripFunc(func(*http.Request) (*http.Response, error) {
					status := http.StatusOK
					data := ""
					switch operation {
					case "error at limit":
						status = http.StatusUnprocessableEntity
						data = prefix + strings.Repeat(" ", maxErrorResponseBodyBytes-len(prefix))
					case "query result":
						data = `{"statement_id":"` + id.String() + `","created_at":"2026-10-27T00:00:00Z","status":"finished","result_set":{"format":"json","metadata":{"fields":[{"name":"text","data_type":"string"}],"num_rows":1},"rows":[["` + value + `"]]}}`
					case "catalog definition":
						data = `{"name":"events","comment":"` + value + `"}`
					}
					return &http.Response{StatusCode: status, Header: http.Header{"Etag": {`"1"`}}, Body: &observedResponseBody{Reader: strings.NewReader(data)}}, nil
				})},
			})
			require.NoError(t, err)
			t.Cleanup(client.Close)
			switch operation {
			case "error at limit":
				_, err = client.Table("events").AppendNDJSON(t.Context(), []byte(`{"id":1}`))
				require.NotErrorIs(t, err, ErrResponseTooLarge)
				var failure *Error
				require.ErrorAs(t, err, &failure)
				require.Equal(t, "schema_mismatch", failure.Code)
				require.Equal(t, "invalid column", failure.Message)
				require.Equal(t, AppendStateRejected, failure.AppendDetails.Status)
			case "query result":
				result, err := client.Query(t.Context(), "SELECT 'text'")
				require.NoError(t, err)
				row, found, err := result.First()
				require.NoError(t, err)
				require.True(t, found)
				require.Equal(t, value, row["text"])
			case "catalog definition":
				snapshot, err := client.Table("events").Catalog().Get(t.Context())
				require.NoError(t, err)
				require.Equal(t, value, *snapshot.Definition.Comment)
			}
		})
	}
}

type observedResponseBody struct {
	*strings.Reader
	closed bool
}

func (b *observedResponseBody) Close() error { b.closed = true; return nil }
