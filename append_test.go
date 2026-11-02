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
	"bytes"
	"errors"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAppendAcknowledgementMustConfirmEveryRow(t *testing.T) {
	for _, tc := range []struct {
		name            string
		status          int
		payload         string
		acknowledgement string
		committed       bool
	}{
		{"modern", 200, `{"id":1}`, `{"num_rows_inserted":1}`, true},
		{"legacy", 200, `{"id":1}`, `{"status":"committed","num_rows_inserted":1}`, true},
		{"explicit zero", 200, "\n \n", `{"status":"committed","num_rows_inserted":0}`, true},
		{"missing zero", 200, "\n \n", `{"status":"committed"}`, false},
		{"contradictory state", 200, `{}`, `{"status":"rejected","num_rows_inserted":1}`, false},
		{"null status", 200, `{"id":1}`, `{"status":null,"num_rows_inserted":1}`, false},
		{"empty status", 200, `{"id":1}`, `{"status":"","num_rows_inserted":1}`, false},
		{"wrong count", 200, `{}`, `{"status":"committed","num_rows_inserted":2}`, false},
		{"wrong HTTP status", 201, `{"id":1}`, `{"num_rows_inserted":1}`, false},
		{"malformed", 200, `{}`, `not JSON`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			calls := 0
			client := newTestClient(t, func(*http.Request) (*http.Response, error) {
				calls++
				return httpResponse(tc.status, tc.acknowledgement), nil
			})
			result, err := client.Table("events").AppendNDJSON(t.Context(), []byte(tc.payload))
			if tc.committed {
				require.NoError(t, err)
				require.Equal(t, AppendOutcomeSucceeded, result.Status)
			} else {
				failure := requireError(t, err)
				require.Equal(t, AppendOutcomeAmbiguous, failure.AppendDetails.Status)
				require.False(t, failure.Retryable)
			}
			require.Equal(t, 1, calls, "direct appends make a single SDK attempt")
		})
	}
}

func TestAppendFailureClassification(t *testing.T) {
	for _, tc := range []struct {
		name      string
		status    int
		body      string
		readError bool
		state     AppendOutcome
		retryable bool
	}{
		{"schema rejection", 422, `{"code":"invalid_schema","message":"invalid integer","details":{"status":"rejected","row_errors":[{"row_index":0,"column":"id","message":"expected int"}]}}`, false, AppendOutcomeFailed, false},
		{"explicit unknown", 503, `{"retryable":true,"details":{"status":"unknown"}}`, false, AppendOutcomeAmbiguous, false},
		{"rate limit without details", 429, "too many requests", false, AppendOutcomeAmbiguous, false},
		{"interrupted rejection", 401, "", true, AppendOutcomeAmbiguous, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := newTestClient(t, func(*http.Request) (*http.Response, error) {
				resp := httpResponse(tc.status, tc.body)
				resp.Header.Set("X-Request-ID", "append-request")
				resp.Header.Set("Retry-After", "2")
				if tc.readError {
					resp.Body = errorReadCloser{io.ErrUnexpectedEOF}
				}
				return resp, nil
			})
			_, err := client.Table("events").AppendNDJSON(t.Context(), []byte(`{}`))
			failure := requireError(t, err)
			require.Equal(t, tc.state, failure.AppendDetails.Status)
			require.Equal(t, tc.retryable, failure.Retryable)
			require.Equal(t, tc.status, failure.HTTPStatus)
			require.Equal(t, "append-request", failure.RequestID)
			require.Equal(t, 2*time.Second, failure.RetryAfter)
			if tc.readError {
				require.ErrorIs(t, err, io.ErrUnexpectedEOF)
			}
			if tc.name == "schema rejection" {
				require.Equal(t, "invalid_schema", failure.Code)
				require.Equal(t, []AppendRowError{{RowIndex: 0, Column: "id", Message: "expected int"}}, failure.AppendDetails.RowErrors)
			}
		})
	}
}

func TestAppendLimitsRejectBeforeDispatch(t *testing.T) {
	client := newTestClient(t, func(*http.Request) (*http.Response, error) {
		t.Error("oversized append reached transport")
		return nil, errors.New("unexpected request")
	})
	for _, payload := range [][]byte{
		bytes.Repeat([]byte{'x'}, 8*1024*1024+1),
		bytes.Repeat([]byte("{}\n"), 200001),
	} {
		_, err := client.Table("events").AppendNDJSON(t.Context(), payload)
		failure := requireError(t, err)
		require.Equal(t, AppendOutcomeFailed, failure.AppendDetails.Status)
	}
}
