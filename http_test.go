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
	"errors"
	"io"
	"net/http"
	"strings"
	"testing"

	"github.com/klauspost/compress/zstd"
	"github.com/stretchr/testify/require"
)

type roundTripFunc func(*http.Request) (*http.Response, error)

func (f roundTripFunc) RoundTrip(r *http.Request) (*http.Response, error) { return f(r) }

func newTestClient(t *testing.T, transport roundTripFunc) *Client {
	t.Helper()
	c, err := NewClient(Config{Endpoint: "https://example.com", Compression: CompressionGzip, HTTPClient: &http.Client{Transport: transport}})
	require.NoError(t, err)
	t.Cleanup(c.Close)
	return c
}

func httpResponse(status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}
}

func requireError(t *testing.T, err error) *Error {
	t.Helper()
	var failure *Error
	require.ErrorAs(t, err, &failure)
	return failure
}

func decodeCompressedRequestBody(r *http.Request) ([]byte, error) {
	data, err := io.ReadAll(r.Body)
	if err != nil {
		return nil, err
	}
	switch Compression(r.Header.Get("Content-Encoding")) {
	case CompressionZstd:
		reader, err := zstd.NewReader(bytes.NewReader(data))
		if err != nil {
			return nil, err
		}
		defer reader.Close()
		return io.ReadAll(reader)
	case CompressionGzip:
		reader, err := gzip.NewReader(bytes.NewReader(data))
		if err != nil {
			return nil, err
		}
		data, readErr := io.ReadAll(reader)
		return data, errors.Join(readErr, reader.Close())
	default:
		return nil, errors.New("missing or unsupported request compression")
	}
}

type errorReadCloser struct{ err error }

func (r errorReadCloser) Read([]byte) (int, error) { return 0, r.err }
func (errorReadCloser) Close() error               { return nil }
