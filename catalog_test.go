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
	"io"
	"net/http"
	"testing"

	"github.com/stretchr/testify/require"
)

func TestCatalogConditionalWorkflow(t *testing.T) {
	calls := 0
	client := newTestClient(t, func(r *http.Request) (*http.Response, error) {
		calls++
		require.Equal(t, "/v1/catalog/databases/scopedb/schemas/public/tables/events.a%2Fb", r.URL.EscapedPath())
		var body string
		switch calls {
		case 1:
			require.Equal(t, http.MethodGet, r.Method)
			body = `{"name":"events.a/b","columns":[{"name":"id","data_type":"int"}]}`
		case 2, 3:
			require.Equal(t, http.MethodPut, r.Method)
			require.Equal(t, `"7"`, r.Header.Get("If-Match"))
			require.Equal(t, calls == 2, r.URL.Query().Get("dry_run") == "true")
			require.Empty(t, r.Header.Get("Content-Encoding"))
			data, err := io.ReadAll(r.Body)
			require.NoError(t, err)
			require.JSONEq(t, `{"name":"events.a/b","comment":"edited","columns":[{"name":"id","data_type":"int"}],"distinct":{}}`, string(data))
			body = `{"before":{"name":"events.a/b"},"after":{"name":"events.a/b","comment":"edited"},"destructive":false}`
		case 4:
			require.Equal(t, http.MethodDelete, r.Method)
			require.Equal(t, `"8"`, r.Header.Get("If-Match"))
			require.Equal(t, "true", r.URL.Query().Get("allow_destructive"))
			body = `{"before":{"name":"events.a/b"},"after":null,"destructive":true}`
		default:
			t.Errorf("unexpected request %d", calls)
		}
		resp := httpResponse(200, body)
		resp.Header.Set("ETag", `"7"`)
		if calls >= 3 {
			resp.Header.Set("ETag", `"8"`)
		}
		return resp, nil
	})
	resource := client.Table("events.a/b").Catalog()
	snapshot, err := resource.Get(t.Context())
	require.NoError(t, err)
	snapshot.Definition.Comment = new("edited")
	plan, err := resource.Apply(t.Context(), snapshot.Definition, CatalogOptions{DryRun: true, ExpectedVersion: snapshot.Version})
	require.NoError(t, err)
	require.Equal(t, snapshot.Version, plan.Version)
	result, err := resource.Apply(t.Context(), snapshot.Definition, CatalogOptions{ExpectedVersion: plan.Version})
	require.NoError(t, err)
	require.Equal(t, "edited", *result.After.Comment)
	deleted, err := resource.Delete(t.Context(), CatalogOptions{ExpectedVersion: result.Version, AllowDestructive: true})
	require.NoError(t, err)
	require.True(t, deleted.Destructive)
	require.Nil(t, deleted.After)
	require.Equal(t, 4, calls)
}

func TestCatalogRawPreservesUnknownNestedFields(t *testing.T) {
	const definition = `{"name":"events","columns":[{"name":"id","data_type":"int","future":{"precise":9007199254740993}}]}`
	client := newTestClient(t, func(r *http.Request) (*http.Response, error) {
		body := definition
		if r.Method == http.MethodPut {
			data, err := io.ReadAll(r.Body)
			require.NoError(t, err)
			require.JSONEq(t, definition, string(data))
			body = `{"before":` + definition + `,"after":` + definition + `,"destructive":false}`
		}
		resp := httpResponse(200, body)
		resp.Header.Set("ETag", `"1"`)
		return resp, nil
	})
	resource := client.Table("events").Catalog()
	_, err := resource.Get(t.Context())
	require.ErrorContains(t, err, "use Raw")
	snapshot, err := resource.Raw().Get(t.Context())
	require.NoError(t, err)
	result, err := resource.Raw().Apply(t.Context(), snapshot.Definition, CatalogOptions{ExpectedVersion: snapshot.Version})
	require.NoError(t, err)
	require.JSONEq(t, definition, string(result.After))
	require.Contains(t, string(result.After), "9007199254740993")
	_, err = client.Catalog().Raw().Delete(t.Context())
	require.Equal(t, ErrorKindConfigInvalid, requireError(t, err).Kind)
}

func TestCatalogMutationOutcome(t *testing.T) {
	for _, tc := range []struct {
		name      string
		status    int
		body      string
		readError bool
		dryRun    bool
		code      string
		outcome   CatalogMutationOutcome
	}{
		{"version conflict", 412, `{"code":"precondition_failed","message":"stale"}`, false, false, "precondition_failed", CatalogMutationRejected},
		{"empty error code", 412, `{"error":{"code":"","message":"stale"}}`, false, false, "", CatalogMutationUnknown},
		{"mismatched error code", 412, `{"error":{"code":"internal_server_error","message":"stale"}}`, false, false, "internal_server_error", CatalogMutationUnknown},
		{"missing error message", 412, `{"code":"precondition_failed"}`, false, false, "precondition_failed", CatalogMutationUnknown},
		{"gateway", 502, "response lost", false, false, "", CatalogMutationUnknown},
		{"truncated acknowledgement", 200, "", true, false, "", CatalogMutationUnknown},
		{"missing acknowledgement", 200, `{}`, false, false, "", CatalogMutationUnknown},
		{"truncated preview", 200, "", true, true, "", ""},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := newTestClient(t, func(*http.Request) (*http.Response, error) {
				resp := httpResponse(tc.status, tc.body)
				resp.Header.Set("X-Request-ID", "catalog-request")
				if tc.readError {
					resp.Body = errorReadCloser{io.ErrUnexpectedEOF}
				}
				return resp, nil
			})
			_, err := client.Catalog().Apply(t.Context(), &CatalogDocument{}, CatalogOptions{DryRun: tc.dryRun})
			failure := requireError(t, err)
			require.Equal(t, tc.code, failure.Code)
			require.Equal(t, tc.outcome, failure.CatalogOutcome)
			require.False(t, failure.Retryable)
			require.Equal(t, "catalog-request", failure.RequestID)
			if tc.readError {
				require.ErrorIs(t, err, io.ErrUnexpectedEOF)
			}
		})
	}
}

func TestTableIdentifierEscapesConfiguredParts(t *testing.T) {
	table := &Table{Database: "analytics`archive", Schema: "event\nlogs", Name: "request\\completed"}
	require.Equal(t, "`analytics\\`archive`.`event\\nlogs`.`request\\\\completed`", table.Identifier())
	table.Schema = ""
	require.Equal(t, "`analytics\\`archive`.`public`.`request\\\\completed`", table.Identifier())
}

func TestCatalogErrorCertaintyPreservesDiagnostics(t *testing.T) {
	for _, tc := range []struct {
		mode    string
		status  int
		code    string
		outcome CatalogMutationOutcome
	}{
		{"write", 412, "precondition_failed", CatalogMutationRejected},
		{"write", 408, "request_timeout", CatalogMutationUnknown},
		{"write", 503, "service_unavailable", CatalogMutationUnknown},
		{"read", 412, "precondition_failed", ""},
		{"preview", 412, "precondition_failed", ""},
	} {
		t.Run(tc.code+"/"+tc.mode, func(t *testing.T) {
			client := newTestClient(t, func(*http.Request) (*http.Response, error) {
				return httpResponse(tc.status, `{"error":{"code":"`+tc.code+`","message":"failed","details":{"reason":"test"}},"request_id":"catalog-request"}`), nil
			})
			resource := client.Table("events").Catalog()
			var err error
			if tc.mode == "read" {
				_, err = resource.Get(t.Context())
			} else {
				_, err = resource.Apply(t.Context(), &CatalogTable{Name: "events"}, CatalogOptions{DryRun: tc.mode == "preview"})
			}
			failure := requireError(t, err)
			require.Equal(t, tc.code, failure.Code)
			require.Equal(t, "catalog-request", failure.RequestID)
			require.JSONEq(t, `{"reason":"test"}`, string(failure.Details))
			require.Equal(t, tc.outcome, failure.CatalogOutcome)
			require.False(t, failure.Retryable)
		})
	}
}
