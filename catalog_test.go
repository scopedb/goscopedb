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
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"testing/iotest"
	"time"

	"github.com/stretchr/testify/require"
)

func TestCatalogWorkflow(t *testing.T) {
	t.Parallel()
	calls := 0
	table := CatalogTable{Name: "events.a/b", Columns: []CatalogColumn{{Name: "message", DataType: StringDataType, Indexes: []CatalogColumnIndex{{Type: "search"}}}}}
	expected, err := json.Marshal(table)
	require.NoError(t, err)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		require.Equal(t, "Bearer secret", r.Header.Get("Authorization"))
		require.Equal(t, "/proxy/v1/databases/db/schemas/public/tables/events.a%2Fb", r.URL.EscapedPath())
		w.Header().Set("ETag", `"7"`)
		switch calls {
		case 1:
			require.Equal(t, http.MethodGet, r.Method)
			writeJSONValue(t, w, table)
		case 2, 3:
			require.Equal(t, http.MethodPut, r.Method)
			require.Equal(t, "application/json", r.Header.Get("Content-Type"))
			data, err := io.ReadAll(r.Body)
			require.NoError(t, err)
			require.JSONEq(t, string(expected), string(data))
			require.Equal(t, `"7"`, r.Header.Get("If-Match"))
			if calls == 2 {
				require.Equal(t, "true", r.URL.Query().Get("dry_run"))
				require.Empty(t, r.URL.Query().Get("allow_destructive"))
			} else {
				require.Empty(t, r.URL.Query().Get("dry_run"))
				require.Equal(t, "true", r.URL.Query().Get("allow_destructive"))
				w.Header().Set("ETag", `"8"`)
			}
			writeJSONValue(t, w, CatalogResult{Before: expected, After: expected})
		case 4:
			require.Equal(t, http.MethodDelete, r.Method)
			require.Equal(t, `"8"`, r.Header.Get("If-Match"))
			require.Equal(t, "true", r.URL.Query().Get("allow_destructive"))
			writeJSONValue(t, w, CatalogResult{Before: expected, After: json.RawMessage(`null`), Destructive: true})
		default:
			t.Errorf("unexpected request %d", calls)
		}
	}))
	defer server.Close()
	client, err := NewClient(Config{Endpoint: server.URL + "/proxy", APIKey: "secret"})
	require.NoError(t, err)
	defer client.Close()
	resource := client.Catalog("databases", "db", "schemas", "public", "tables", table.Name)
	snapshot, err := resource.Get(context.Background())
	require.NoError(t, err)
	var exported CatalogTable
	require.NoError(t, json.Unmarshal(snapshot.Definition, &exported))
	require.Equal(t, table, exported)
	plan, err := resource.Apply(context.Background(), table, CatalogOptions{DryRun: true, IfMatch: snapshot.ETag})
	require.NoError(t, err)
	result, err := resource.Apply(context.Background(), snapshot.Definition, CatalogOptions{IfMatch: plan.ETag, AllowDestructive: true})
	require.NoError(t, err)
	deleted, err := resource.Delete(context.Background(), CatalogOptions{IfMatch: result.ETag, AllowDestructive: true})
	require.NoError(t, err)
	require.True(t, deleted.Destructive)
	require.JSONEq(t, "null", string(deleted.After))
	require.Equal(t, 4, calls)
}

func TestCatalogFailureOutcomes(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		dryRun  bool
		status  int
		body    string
		cause   error
		partial bool
		code    string
	}{
		{name: "conflict", status: 412, body: `{"code":"catalog_version_mismatch","message":"stale"}`, code: "catalog_version_mismatch"},
		{name: "destructive", status: 409, body: `{"code":"catalog_destructive_change","message":"secret"}`, code: "catalog_destructive_change"},
		{name: "commit", status: 500, body: `{"code":"catalog_commit_outcome_unknown","message":"unknown"}`, code: "catalog_commit_outcome_unknown"},
		{name: "transport", cause: context.Canceled, code: "catalog_commit_outcome_unknown"},
		{name: "body", status: 200, partial: true, cause: io.ErrUnexpectedEOF, code: "catalog_commit_outcome_unknown"},
		{name: "invalid", status: 200, body: `{}`, code: "catalog_commit_outcome_unknown"},
		{name: "preview", dryRun: true, cause: context.DeadlineExceeded},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			calls := 0
			client, err := NewClient(Config{Endpoint: "https://example.com", APIKey: "secret", HTTPClient: &http.Client{Transport: clientRoundTripFunc(func(req *http.Request) (*http.Response, error) {
				calls++
				if tc.cause != nil && !tc.partial {
					return nil, tc.cause
				}
				body := io.NopCloser(strings.NewReader(tc.body))
				if tc.partial {
					body = io.NopCloser(iotest.ErrReader(tc.cause))
				}
				return &http.Response{StatusCode: tc.status, Header: http.Header{"X-Request-Id": []string{"request"}, "Etag": []string{`"1"`}}, Body: body, Request: req}, nil
			})}})
			require.NoError(t, err)
			defer client.Close()
			_, err = client.Catalog().Apply(context.Background(), CatalogDocument{}, CatalogOptions{DryRun: tc.dryRun})
			var failure *Error
			require.ErrorAs(t, err, &failure)
			require.Equal(t, ErrorKindCatalogFailed, failure.Kind)
			require.Equal(t, tc.code, failure.Code)
			require.Equal(t, tc.status, failure.HTTPStatus)
			require.NotContains(t, failure.Message, "secret")
			if tc.status != 0 {
				require.Equal(t, "request", failure.RequestID)
			}
			if tc.cause != nil {
				require.ErrorIs(t, err, tc.cause)
			}
			require.False(t, failure.Retryable)
			require.Equal(t, 1, calls)
		})
	}
}

func TestParseRetryAfterHTTPDate(t *testing.T) {
	t.Parallel()

	now := time.Date(2026, time.August, 8, 12, 0, 0, 0, time.UTC)
	retryAt := now.Add(7 * time.Second).Format(http.TimeFormat)
	require.Equal(t, 7*time.Second, parseRetryAfter(retryAt, now))
}

func writeJSONValue(t *testing.T, w http.ResponseWriter, value any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	require.NoError(t, json.NewEncoder(w).Encode(value))
}
