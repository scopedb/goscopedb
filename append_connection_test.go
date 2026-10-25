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
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"syscall"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestAppendConnectionFailuresAreRejected(t *testing.T) {
	for _, test := range []struct {
		name      string
		cause     error
		retryable bool
	}{
		{"dns", &net.DNSError{Err: "no such host", Name: "scopedb.invalid", IsNotFound: true}, true},
		{"connection refused", &net.OpError{Op: "dial", Net: "tcp", Err: syscall.ECONNREFUSED}, true},
		{"connection timeout", context.DeadlineExceeded, true},
		{"cancelled connection", context.Canceled, false},
	} {
		t.Run(test.name, func(t *testing.T) {
			transport := &http.Transport{DialContext: func(context.Context, string, string) (net.Conn, error) {
				return nil, test.cause
			}}
			t.Cleanup(transport.CloseIdleConnections)
			client, err := NewClient(Config{Endpoint: "http://scopedb.invalid", HTTPClient: &http.Client{Transport: transport}})
			require.NoError(t, err)
			_, err = client.Table("events").AppendNDJSON(t.Context(), []byte("{}\n"))
			var failure *Error
			require.ErrorAs(t, err, &failure)
			require.ErrorIs(t, err, test.cause)
			require.Equal(t, AppendStateRejected, failure.AppendDetails.Status)
			require.Equal(t, test.retryable, failure.Retryable)
			if failure.WriteDetails != nil {
				require.Equal(t, AppendStateRejected, failure.WriteDetails.Outcome)
			}
		})
	}
}

func TestAppendTLSAndRedirectFailures(t *testing.T) {
	t.Run("invalid certificate is not retried", func(t *testing.T) {
		server := httptest.NewTLSServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) {
			t.Error("append reached an untrusted server")
		}))
		defer server.Close()
		client, err := NewClient(Config{Endpoint: server.URL})
		require.NoError(t, err)
		t.Cleanup(client.Close)
		_, err = client.Table("events").AppendNDJSON(t.Context(), []byte("{}\n"))
		var failure *Error
		require.ErrorAs(t, err, &failure)
		require.Equal(t, AppendStateRejected, failure.AppendDetails.Status)
		require.False(t, failure.Retryable)
	})
	t.Run("redirect followed by dial failure stays unknown", func(t *testing.T) {
		server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			_, err := io.Copy(io.Discard, r.Body)
			require.NoError(t, err)
			http.Redirect(w, r, "http://scopedb.invalid/ingest", http.StatusTemporaryRedirect)
		}))
		defer server.Close()
		transport := &http.Transport{DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
			if strings.HasPrefix(addr, "scopedb.invalid:") {
				return nil, &net.DNSError{Err: "no such host", Name: "scopedb.invalid"}
			}
			return (&net.Dialer{}).DialContext(ctx, network, addr)
		}}
		t.Cleanup(transport.CloseIdleConnections)
		client, err := NewClient(Config{Endpoint: server.URL, HTTPClient: &http.Client{Transport: transport}})
		require.NoError(t, err)
		_, err = client.Table("events").AppendNDJSON(t.Context(), []byte("{}\n"))
		var failure *Error
		require.ErrorAs(t, err, &failure)
		require.Equal(t, AppendStateUnknown, failure.AppendDetails.Status)
		require.False(t, failure.Retryable)
	})
}

func TestAppendRateLimitWithoutDetails(t *testing.T) {
	for _, test := range []struct {
		name   string
		status int
		body   func() io.ReadCloser
		state  AppendState
	}{
		{"text", 429, func() io.ReadCloser { return io.NopCloser(strings.NewReader("too many requests")) }, AppendStateRejected},
		{"empty", 429, func() io.ReadCloser { return io.NopCloser(strings.NewReader("")) }, AppendStateRejected},
		{"unreadable", 429, func() io.ReadCloser { return errorReadCloser{err: io.ErrUnexpectedEOF} }, AppendStateRejected},
		{"unavailable", 503, func() io.ReadCloser { return io.NopCloser(strings.NewReader("unavailable")) }, AppendStateUnknown},
	} {
		t.Run(test.name, func(t *testing.T) {
			client, err := NewClient(Config{
				Endpoint: "http://scopedb.invalid",
				HTTPClient: &http.Client{Transport: appendRoundTripFunc(func(*http.Request) (*http.Response, error) {
					return &http.Response{StatusCode: test.status, Header: http.Header{"Retry-After": {"2"}}, Body: test.body()}, nil
				})},
			})
			require.NoError(t, err)
			_, err = client.Table("events").AppendNDJSON(t.Context(), []byte("{}\n"))
			var failure *Error
			require.ErrorAs(t, err, &failure)
			require.Equal(t, test.state, failure.AppendDetails.Status)
			require.Equal(t, test.state == AppendStateRejected, failure.Retryable)
			require.Equal(t, 2*time.Second, failure.RetryAfter)
			if test.name == "unreadable" {
				require.ErrorIs(t, err, io.ErrUnexpectedEOF)
			}
		})
	}
}

func TestAppendStreamRecoversFromConnectionFailure(t *testing.T) {
	var attempts atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeAppendStreamSuccess(t, w, 1)
	}))
	defer server.Close()
	transport := &http.Transport{DialContext: func(ctx context.Context, network, addr string) (net.Conn, error) {
		if attempts.Add(1) == 1 {
			return nil, &net.OpError{Op: "dial", Net: network, Err: syscall.ECONNREFUSED}
		}
		return (&net.Dialer{}).DialContext(ctx, network, addr)
	}}
	t.Cleanup(transport.CloseIdleConnections)
	client, err := NewClient(Config{Endpoint: server.URL, HTTPClient: &http.Client{Transport: transport}})
	require.NoError(t, err)
	stream, err := client.Table("events").AppendStream(AppendStreamOptions{TargetBatchBytes: 1})
	require.NoError(t, err)
	require.NoError(t, stream.Send(t.Context(), map[string]int{"id": 1}))
	report, err := stream.Shutdown(t.Context())
	require.NoError(t, err)
	require.Equal(t, uint64(1), report.Retries)
	require.Equal(t, uint64(1), report.CommittedRows)
	require.Zero(t, report.UnknownRows)
	require.Equal(t, int32(2), attempts.Load())
}
