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
	"compress/gzip"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"sync/atomic"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
)

func TestHTTPClientDoPostUsesZstdByDefault(t *testing.T) {
	t.Parallel()

	expected := []byte(`{"statement":"select 1"}`)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Helper()
		require.Equal(t, http.MethodPost, r.Method)
		require.Equal(t, string(CompressionZstd), r.Header.Get("Content-Encoding"))
		require.Equal(t, strconv.Itoa(len(expected)), r.Header.Get("X-ScopeDB-Uncompressed-Content-Length"))

		actual, err := decodeCompressedRequestBody(r)
		require.NoError(t, err)
		require.Equal(t, expected, actual)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client, err := NewClient(Config{Endpoint: server.URL})
	require.NoError(t, err)
	reqURL, err := url.Parse(server.URL)
	require.NoError(t, err)

	resp, err := client.http.doPost(context.Background(), reqURL, expected)
	require.NoError(t, err)
	require.Equal(t, http.StatusNoContent, resp.StatusCode)
	require.NoError(t, resp.Body.Close())
}

func TestHTTPClientDoPostSupportsGzip(t *testing.T) {
	t.Parallel()

	expected := []byte(`{"statement":"select 1"}`)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Helper()
		require.Equal(t, string(CompressionGzip), r.Header.Get("Content-Encoding"))

		actual, err := decodeCompressedRequestBody(r)
		require.NoError(t, err)
		require.Equal(t, expected, actual)
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client, err := NewClient(Config{
		Endpoint:    server.URL,
		Compression: CompressionGzip,
	})
	require.NoError(t, err)
	reqURL, err := url.Parse(server.URL)
	require.NoError(t, err)

	resp, err := client.http.doPost(context.Background(), reqURL, expected)
	require.NoError(t, err)
	require.Equal(t, http.StatusNoContent, resp.StatusCode)
	require.NoError(t, resp.Body.Close())
}

func TestHTTPClientDoPostRejectsUnsupportedCompression(t *testing.T) {
	t.Parallel()

	_, err := NewClient(Config{
		Endpoint:    "http://example.com",
		Compression: Compression("brotli"),
	})
	require.ErrorContains(t, err, `unsupported compression: "brotli"`)
}

func TestNewClientRejectsInvalidEndpoint(t *testing.T) {
	t.Parallel()

	for _, endpoint := range []string{
		"",
		"localhost:6543",
		"ftp://example.com",
		"https://user:fixture-password@example.com",
		"https://example.com/base?query=true",
		"https://example.com/base#fragment",
	} {
		t.Run(endpoint, func(t *testing.T) {
			t.Parallel()

			_, err := NewClient(Config{Endpoint: endpoint})
			var scopeErr *Error
			require.ErrorAs(t, err, &scopeErr)
			require.Equal(t, ErrorKindConfigInvalid, scopeErr.Kind)
		})
	}
}

func TestNewClientUsesIndependentHTTPClient(t *testing.T) {
	t.Parallel()

	client, err := NewClient(Config{Endpoint: "https://example.com"})
	require.NoError(t, err)
	t.Cleanup(client.Close)

	require.NotSame(t, http.DefaultClient, client.http.client)
	require.NotSame(t, http.DefaultTransport, client.http.client.Transport)
}

func TestNewClientHandlesReplacedDefaultTransport(t *testing.T) {
	originalTransport := http.DefaultTransport
	replacementCalls := 0
	http.DefaultTransport = clientRoundTripFunc(func(*http.Request) (*http.Response, error) {
		replacementCalls++
		return nil, errors.New("application default transport must not be reused")
	})
	t.Cleanup(func() { http.DefaultTransport = originalTransport })

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	t.Cleanup(server.Close)

	client, err := NewClient(Config{Endpoint: server.URL})
	require.NoError(t, err)
	t.Cleanup(client.Close)
	require.IsType(t, &http.Transport{}, client.http.client.Transport)

	requestURL, err := url.Parse(server.URL)
	require.NoError(t, err)
	resp, err := client.http.doGet(context.Background(), requestURL)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
	require.Zero(t, replacementCalls)
}

func TestClientCloseDoesNotCloseInjectedHTTPClient(t *testing.T) {
	t.Parallel()

	transport := &closeTrackingTransport{}
	standardClient := &http.Client{Transport: transport}
	client, err := NewClient(Config{
		Endpoint:   "https://example.com",
		HTTPClient: standardClient,
	})
	require.NoError(t, err)

	client.Close()
	require.False(t, transport.closed)
}

func TestAPIKeyOverridesAnExistingAuthorizationHeader(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, "Bearer scope-key", r.Header.Get("Authorization"))
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client, err := NewClient(Config{
		Endpoint:   server.URL,
		APIKey:     "scope-key",
		HTTPClient: server.Client(),
	})
	require.NoError(t, err)

	req, err := http.NewRequestWithContext(context.Background(), http.MethodGet, server.URL, nil)
	require.NoError(t, err)
	req.Header.Set("Authorization", "Basic old-credentials")
	resp, err := client.http.do(req)
	require.NoError(t, err)
	require.NoError(t, resp.Body.Close())
}

type closeTrackingTransport struct {
	closed bool
}

type clientRoundTripFunc func(*http.Request) (*http.Response, error)

func (f clientRoundTripFunc) RoundTrip(request *http.Request) (*http.Response, error) {
	return f(request)
}

func (t *closeTrackingTransport) RoundTrip(*http.Request) (*http.Response, error) {
	return nil, errors.New("unexpected request")
}

func (t *closeTrackingTransport) CloseIdleConnections() {
	t.closed = true
}

func decodeCompressedRequestBody(r *http.Request) ([]byte, error) {
	compressedBody, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}

	switch Compression(r.Header.Get("Content-Encoding")) {
	case CompressionZstd:
		zr, err := zstd.NewReader(bytes.NewReader(compressedBody))
		if err != nil {
			return nil, err
		}
		defer zr.Close()
		return io.ReadAll(zr)
	case CompressionGzip:
		gr, err := gzip.NewReader(bytes.NewReader(compressedBody))
		if err != nil {
			return nil, err
		}
		decoded, readErr := io.ReadAll(gr)
		return decoded, errors.Join(readErr, gr.Close())
	default:
		return nil, io.ErrUnexpectedEOF
	}
}

func TestOwnedClientFollowsRedirectedWrite(t *testing.T) {
	for _, tc := range []struct {
		status int
		method string
	}{
		{http.StatusMovedPermanently, http.MethodGet},
		{http.StatusFound, http.MethodGet},
		{http.StatusSeeOther, http.MethodGet},
		{http.StatusTemporaryRedirect, http.MethodPost},
		{http.StatusPermanentRedirect, http.MethodPost},
	} {
		t.Run(strconv.Itoa(tc.status), func(t *testing.T) {
			payload := []byte(`{"id":1}`)
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				require.Equal(t, "Bearer redirect-key", r.Header.Get("Authorization"))
				if r.URL.Path != "/redirected" {
					http.Redirect(w, r, "/redirected", tc.status)
					return
				}
				require.Equal(t, tc.method, r.Method)
				if r.Method == http.MethodPost {
					body, err := decodeCompressedRequestBody(r)
					require.NoError(t, err)
					require.Equal(t, payload, body)
				} else {
					body, err := io.ReadAll(r.Body)
					require.NoError(t, err)
					require.Empty(t, body)
				}
				writeJSONValue(t, w, AppendRowsResult{Status: AppendStateCommitted, NumRowsInserted: 1})
			}))
			t.Cleanup(server.Close)
			client, err := NewClient(Config{Endpoint: server.URL, APIKey: "redirect-key"})
			require.NoError(t, err)
			t.Cleanup(client.Close)
			result, err := client.Table("events").AppendNDJSON(t.Context(), payload)
			require.NoError(t, err)
			require.Equal(t, AppendStateCommitted, result.Status)
			require.EqualValues(t, 1, result.NumRowsInserted)
			require.EqualValues(t, 2, requests.Load())
		})
	}
}

func TestClientPreservesCallerRedirectPolicy(t *testing.T) {
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		http.Redirect(w, r, "/redirected", http.StatusTemporaryRedirect)
	}))
	t.Cleanup(server.Close)
	standardClient := server.Client()
	standardClient.CheckRedirect = func(_ *http.Request, _ []*http.Request) error {
		return http.ErrUseLastResponse
	}
	client, err := NewClient(Config{Endpoint: server.URL, HTTPClient: standardClient})
	require.NoError(t, err)
	t.Cleanup(standardClient.CloseIdleConnections)
	t.Cleanup(client.Close)
	_, err = client.Table("events").AppendNDJSON(t.Context(), []byte(`{"id":1}`))
	var failure *Error
	require.ErrorAs(t, err, &failure)
	require.Equal(t, http.StatusTemporaryRedirect, failure.HTTPStatus)
	require.NotNil(t, failure.AppendDetails)
	require.Equal(t, AppendStateUnknown, failure.AppendDetails.Status)
	require.EqualValues(t, 1, requests.Load())
}
