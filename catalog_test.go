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
	original := &CatalogTable{Name: "events.a/b", Columns: []CatalogColumn{{Name: "message", DataType: StringDataType, Indexes: []CatalogColumnIndex{{Type: "search"}}}}}
	updated := *original
	comment := "edited"
	updated.Comment = &comment
	expected, err := json.Marshal(&updated)
	require.NoError(t, err)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		require.Equal(t, "Bearer secret", r.Header.Get("Authorization"))
		require.Equal(t, "/proxy/v1/catalog/databases/db/schemas/public/tables/events.a%2Fb", r.URL.EscapedPath())
		w.Header().Set("ETag", `"7"`)
		switch calls {
		case 1:
			require.Equal(t, http.MethodGet, r.Method)
			writeJSONValue(t, w, original)
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
				w.Header().Set("ETag", `"8"`)
			}
			writeJSONValue(t, w, CatalogResult[*CatalogTable]{Before: original, After: &updated})
		case 4:
			require.Equal(t, http.MethodDelete, r.Method)
			require.Equal(t, `"8"`, r.Header.Get("If-Match"))
			require.Equal(t, "true", r.URL.Query().Get("allow_destructive"))
			w.Header().Set("ETag", `"9"`)
			writeJSONValue(t, w, CatalogResult[*CatalogTable]{Before: &updated, Destructive: true})
		case 5:
			require.Equal(t, http.MethodPut, r.Method)
			require.Empty(t, r.Header.Get("If-Match"))
			require.Empty(t, r.URL.RawQuery)
			writeJSONValue(t, w, CatalogResult[*CatalogTable]{After: original})
		default:
			t.Errorf("unexpected request %d", calls)
		}
	}))
	defer server.Close()
	client, err := NewClient(Config{Endpoint: server.URL + "/proxy", APIKey: "secret"})
	require.NoError(t, err)
	defer client.Close()
	resource := client.Catalog().Table("db", "public", original.Name)
	snapshot, err := resource.Get(t.Context())
	require.NoError(t, err)
	require.Equal(t, original, snapshot.Definition)
	snapshot.Definition.Comment = &comment
	plan, err := resource.Apply(t.Context(), snapshot.Definition, CatalogOptions{DryRun: true, IfMatch: snapshot.ETag})
	require.NoError(t, err)
	require.Equal(t, original, plan.Before)
	require.Equal(t, &updated, plan.After)
	result, err := resource.Apply(t.Context(), snapshot.Definition, CatalogOptions{IfMatch: plan.ETag})
	require.NoError(t, err)
	require.Equal(t, &updated, result.After)
	deleted, err := resource.Delete(t.Context(), CatalogOptions{IfMatch: result.ETag, AllowDestructive: true})
	require.NoError(t, err)
	require.True(t, deleted.Destructive)
	require.Equal(t, &updated, deleted.Before)
	require.Nil(t, deleted.After)
	table := client.Table(original.Name)
	table.Database = "db"
	created, err := table.Apply(t.Context(), original)
	require.NoError(t, err)
	require.Nil(t, created.Before)
	require.Equal(t, original, created.After)
	require.Equal(t, 5, calls)
}

func TestCatalogRawRoundTrip(t *testing.T) {
	t.Parallel()
	definition := `{"name":"events","columns":[{"name":"value","data_type":"future_type"}],"future":{"precise":9007199254740993}}`
	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls++
		require.Equal(t, "/v1/catalog/databases/db/schemas/public/tables/events", r.URL.Path)
		w.Header().Set("ETag", `"3"`)
		if r.Method == http.MethodGet {
			_, err := io.WriteString(w, definition)
			require.NoError(t, err)
			return
		}
		require.Equal(t, http.MethodPut, r.Method)
		require.Equal(t, `"3"`, r.Header.Get("If-Match"))
		data, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.Equal(t, definition, string(data))
		_, err = io.WriteString(w, `{"before":`+definition+`,"after":`+definition+`,"destructive":false}`)
		require.NoError(t, err)
	}))
	defer server.Close()
	client, err := NewClient(Config{Endpoint: server.URL})
	require.NoError(t, err)
	defer client.Close()
	resource := client.Catalog().Table("db", "public", "events").Raw()
	snapshot, err := resource.Get(t.Context())
	require.NoError(t, err)
	require.Equal(t, definition, string(snapshot.Definition))
	result, err := resource.Apply(t.Context(), snapshot.Definition, CatalogOptions{IfMatch: snapshot.ETag})
	require.NoError(t, err)
	require.Equal(t, definition, string(result.Before))
	require.Equal(t, definition, string(result.After))
	require.Equal(t, snapshot.ETag, result.ETag)
	require.Equal(t, 2, calls)
}

func TestCatalogFailureOutcomes(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name    string
		dryRun  bool
		get     bool
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
		{name: "typed_read", get: true, status: 200, body: `{"databases":"invalid"}`},
		{name: "typed_preview", dryRun: true, status: 200, body: `{"before":null,"after":{"databases":"invalid"}}`},
		{name: "typed_write", status: 200, body: `{"before":null,"after":{"databases":"invalid"}}`, code: "catalog_commit_outcome_unknown"},
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
			if tc.get {
				_, err = client.Catalog().Get(context.Background())
			} else if tc.dryRun {
				_, err = client.Catalog().Apply(context.Background(), &CatalogDocument{}, CatalogOptions{DryRun: true})
			} else {
				_, err = client.Catalog().Apply(context.Background(), &CatalogDocument{})
			}
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

func TestCatalogRejectsPathTraversalBeforeMutation(t *testing.T) {
	client, err := NewClient(Config{Endpoint: "https://example.invalid"})
	require.NoError(t, err)
	defer client.Close()
	for _, name := range []string{"", ".", ".."} {
		_, err := client.Catalog().Table("scopedb", "public", name).Delete(t.Context(), CatalogOptions{AllowDestructive: true})
		var failure *Error
		require.ErrorAs(t, err, &failure)
		require.Equal(t, ErrorKindConfigInvalid, failure.Kind)
	}
}
