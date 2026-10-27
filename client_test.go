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
	"errors"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestClientWireRequests(t *testing.T) {
	for _, codec := range []Compression{"", CompressionGzip} {
		t.Run(string(codec), func(t *testing.T) {
			id := uuid.New()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				require.Equal(t, "Bearer test-key", r.Header.Get("Authorization"))
				require.Equal(t, http.MethodPost, r.Method)
				wantCodec := codec
				if wantCodec == "" {
					wantCodec = CompressionZstd
				}
				require.Equal(t, string(wantCodec), r.Header.Get("Content-Encoding"))
				body, err := decodeCompressedRequestBody(r)
				require.NoError(t, err)
				if strings.HasSuffix(r.URL.Path, "/statements") {
					require.Equal(t, "/proxy/v1/statements", r.URL.Path)
					require.Equal(t, strconv.Itoa(len(body)), r.Header.Get("X-ScopeDB-Uncompressed-Content-Length"))
					require.JSONEq(t, `{"statement":"SELECT 42","format":"json"}`, string(body))
					_, _ = w.Write([]byte(statementJSON(id, "finished", finishedResult)))
				} else {
					require.Equal(t, "/proxy/v1/catalog/databases/db%2Farchive/schemas/public/tables/events%3F%23%2Fraw/ingest", r.URL.EscapedPath())
					require.Equal(t, "application/x-ndjson", r.Header.Get("Content-Type"))
					require.Equal(t, "{}\n\n  \n{}", string(body))
					_, _ = w.Write([]byte(`{"status":"committed","num_rows_inserted":2}`))
				}
			}))
			t.Cleanup(server.Close)
			client, err := NewClient(Config{Endpoint: server.URL + "/proxy", APIKey: "test-key", Compression: codec})
			require.NoError(t, err)
			t.Cleanup(client.Close)
			result, err := client.Query(t.Context(), "SELECT 42")
			require.NoError(t, err)
			row, found, err := result.First()
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, int64(42), row["n"])
			table := client.Table("events?#/raw")
			table.Database = "db/archive"
			appended, err := table.AppendNDJSON(t.Context(), []byte("{}\n\n  \n{}"))
			require.NoError(t, err)
			require.EqualValues(t, 2, appended.NumRowsInserted)
		})
	}
}

func TestClientRedirectPolicyAndOwnership(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/destination" {
			http.Redirect(w, r, "/destination", http.StatusTemporaryRedirect)
			return
		}
		body, err := decodeCompressedRequestBody(r)
		require.NoError(t, err)
		require.Equal(t, `{}`, string(body))
		_, _ = w.Write([]byte(`{"status":"committed","num_rows_inserted":1}`))
	}))
	t.Cleanup(server.Close)
	owned, err := NewClient(Config{Endpoint: server.URL})
	require.NoError(t, err)
	t.Cleanup(owned.Close)
	_, err = owned.Table("events").AppendNDJSON(t.Context(), []byte(`{}`))
	require.NoError(t, err)

	transport := &trackedTransport{Transport: http.DefaultTransport.(*http.Transport).Clone()}
	t.Cleanup(transport.Transport.CloseIdleConnections)
	injected, err := NewClient(Config{Endpoint: server.URL, HTTPClient: &http.Client{
		Transport:     transport,
		CheckRedirect: func(*http.Request, []*http.Request) error { return http.ErrUseLastResponse },
	}})
	require.NoError(t, err)
	_, err = injected.Table("events").AppendNDJSON(t.Context(), []byte(`{}`))
	require.Equal(t, http.StatusTemporaryRedirect, apiError(t, err).HTTPStatus)
	injected.Close()
	require.False(t, transport.closed, "the injected transport belongs to its caller")
}

type trackedTransport struct {
	*http.Transport
	closed bool
}

func (t *trackedTransport) CloseIdleConnections() {
	t.closed = true
	t.Transport.CloseIdleConnections()
}

func TestClientRejectsUnsafeDestinations(t *testing.T) {
	for _, endpoint := range []string{"", "localhost:6543", "ftp://example.com", "https://user:password@example.com", "https://example.com?query=true", "https://example.com#fragment"} {
		_, err := NewClient(Config{Endpoint: endpoint})
		require.Equal(t, ErrorKindConfigInvalid, apiError(t, err).Kind, endpoint)
	}
	client := testClient(t, func(*http.Request) (*http.Response, error) {
		t.Error("unsafe destination reached transport")
		return nil, errors.New("unexpected request")
	})
	for _, name := range []string{"", ".", ".."} {
		_, err := client.Table(name).Catalog().Delete(t.Context(), CatalogOptions{AllowDestructive: true})
		require.Equal(t, ErrorKindConfigInvalid, apiError(t, err).Kind)
		_, err = client.Table(name).AppendNDJSON(t.Context(), []byte(`{}`))
		require.Error(t, err)
	}
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	_, err := client.Query(ctx, "SELECT 1")
	require.ErrorIs(t, err, context.Canceled)
	_, err = client.Table("events").AppendNDJSON(ctx, []byte(`{}`))
	require.ErrorIs(t, err, context.Canceled)
}
