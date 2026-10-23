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
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWriteTimeoutDefaultAndRequestScope(t *testing.T) {
	t.Parallel()

	var requestCtx context.Context
	standardClient := &http.Client{Transport: clientRoundTripFunc(func(req *http.Request) (*http.Response, error) {
		requestCtx = req.Context()
		deadline, hasDeadline := requestCtx.Deadline()
		body := `{"status":"committed","num_rows_inserted":1}`
		if req.Method == http.MethodGet {
			body = `{"name":"scopedb"}`
			require.False(t, hasDeadline, "catalog reads must not inherit the write timeout")
		} else {
			require.True(t, hasDeadline)
			require.InDelta(t, 30, time.Until(deadline).Seconds(), 1)
		}
		return &http.Response{
			StatusCode: http.StatusOK,
			Header:     http.Header{"Etag": []string{`"1"`}},
			Body:       io.NopCloser(strings.NewReader(body)),
		}, nil
	})}
	client, err := NewClient(Config{Endpoint: "https://example.com", HTTPClient: standardClient})
	require.NoError(t, err)

	_, err = client.Table("events").AppendNDJSON(context.Background(), []byte(`{"id":1}`))
	require.NoError(t, err)
	require.ErrorIs(t, requestCtx.Err(), context.Canceled, "completed requests must release their timers")
	_, err = client.Catalog().Database("scopedb").Get(context.Background())
	require.NoError(t, err)
	require.Zero(t, standardClient.Timeout, "the caller's HTTP client must not be changed")
}

func TestWriteTimeoutRejectsNegativeConfig(t *testing.T) {
	t.Parallel()
	_, err := NewClient(Config{Endpoint: "https://example.com", WriteTimeout: -time.Second})
	var scopeErr *Error
	require.ErrorAs(t, err, &scopeErr)
	require.Equal(t, ErrorKindConfigInvalid, scopeErr.Kind)
}

func TestWriteTimeoutBoundsHeadersAndResponseBody(t *testing.T) {
	t.Parallel()
	for _, operation := range []string{"append", "catalog apply", "catalog delete"} {
		for _, headers := range []bool{false, true} {
			name := operation
			if headers {
				name += "/response body"
			} else {
				name += "/response headers"
			}
			t.Run(name, func(t *testing.T) {
				t.Parallel()
				var requests atomic.Int32
				server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
					_, err := io.Copy(io.Discard, r.Body)
					require.NoError(t, err)
					requests.Add(1)
					if headers {
						w.Header().Set("X-Request-ID", "stalled-write")
						w.WriteHeader(http.StatusOK)
						w.(http.Flusher).Flush()
					}
					<-r.Context().Done()
				}))
				defer server.Close()
				client, err := NewClient(Config{Endpoint: server.URL, WriteTimeout: 50 * time.Millisecond})
				require.NoError(t, err)
				defer client.Close()
				switch operation {
				case "append":
					_, err = client.Table("events").AppendNDJSON(context.Background(), []byte(`{"id":1}`))
				case "catalog apply":
					_, err = client.Catalog().Table("scopedb", "public", "events").Apply(context.Background(), &CatalogTable{Name: "events"})
				case "catalog delete":
					_, err = client.Catalog().Table("scopedb", "public", "events").Delete(context.Background())
				}
				require.ErrorIs(t, err, context.DeadlineExceeded)
				var scopeErr *Error
				require.ErrorAs(t, err, &scopeErr)
				require.False(t, scopeErr.Retryable)
				require.Equal(t, &WriteErrorDetails{
					Reason: WriteErrorReasonTimeout, Outcome: AppendStateUnknown,
				}, scopeErr.WriteDetails)
				if operation == "append" {
					require.Equal(t, AppendStateUnknown, scopeErr.AppendDetails.Status)
				} else {
					require.Equal(t, ErrorKindCatalogFailed, scopeErr.Kind)
					require.Equal(t, "catalog_commit_outcome_unknown", scopeErr.Code)
				}
				if headers {
					require.Equal(t, http.StatusOK, scopeErr.HTTPStatus)
					require.Equal(t, "stalled-write", scopeErr.RequestID)
				}
				require.EqualValues(t, 1, requests.Load())
			})
		}
	}
}

func TestWriteTimeoutHonorsEarlierCallerDeadline(t *testing.T) {
	t.Parallel()
	client, err := NewClient(Config{
		Endpoint:     "https://example.com",
		WriteTimeout: time.Second,
		HTTPClient: &http.Client{Transport: clientRoundTripFunc(func(req *http.Request) (*http.Response, error) {
			<-req.Context().Done()
			return nil, req.Context().Err()
		})},
	})
	require.NoError(t, err)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	_, err = client.Table("events").AppendNDJSON(ctx, []byte(`{"id":1}`))
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.ErrorIs(t, ctx.Err(), context.DeadlineExceeded)
	var scopeErr *Error
	require.ErrorAs(t, err, &scopeErr)
	require.Equal(t, WriteErrorReasonTimeout, scopeErr.WriteDetails.Reason)
}

func TestWriteTimeoutStreamInheritanceAndOverride(t *testing.T) {
	t.Parallel()
	for _, override := range []bool{false, true} {
		name := "append"
		if override {
			name += "/override"
		} else {
			name += "/inherit"
		}
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				_, err := io.Copy(io.Discard, r.Body)
				require.NoError(t, err)
				requests.Add(1)
				select {
				case <-time.After(100 * time.Millisecond):
					writeTestJSON(t, w, `{"status":"committed","num_rows_inserted":1}`)
				case <-r.Context().Done():
				}
			}))
			defer server.Close()
			client, err := NewClient(Config{Endpoint: server.URL, WriteTimeout: 50 * time.Millisecond})
			require.NoError(t, err)
			defer client.Close()
			var attemptTimeout time.Duration
			if override {
				attemptTimeout = time.Second
			}
			stream, buildErr := client.Table("events").AppendStream(AppendStreamOptions{
				AttemptTimeout: attemptTimeout,
			})
			require.NoError(t, buildErr)
			require.NoError(t, stream.Send(context.Background(), map[string]any{"id": 1}))
			_, err = stream.Shutdown(context.Background())
			if override {
				require.NoError(t, err)
			} else {
				require.ErrorIs(t, err, context.DeadlineExceeded)
				var scopeErr *Error
				require.ErrorAs(t, err, &scopeErr)
				require.Equal(t, &WriteErrorDetails{
					Reason: WriteErrorReasonTimeout, Outcome: AppendStateUnknown,
				}, scopeErr.WriteDetails)
			}
			require.EqualValues(t, 1, requests.Load(), "timeouts must not be retried")
		})
	}
}

func TestWriteTimeoutStartsAfterRequestPreparation(t *testing.T) {
	const timeout = time.Second
	payload := []byte(`{"data":"` + strings.Repeat("x", 6*1024*1024) + `"}`)
	var started time.Time
	client, err := NewClient(Config{
		Endpoint: "https://example.com", Compression: CompressionGzip, WriteTimeout: timeout,
		HTTPClient: &http.Client{Transport: clientRoundTripFunc(func(req *http.Request) (*http.Response, error) {
			reachedTransport := time.Now()
			deadline, ok := req.Context().Deadline()
			require.True(t, ok)
			budgetStarted := deadline.Add(-timeout)
			// Compression dominates preparation for this payload. The deadline must
			// start near transport handoff, rather than before that work.
			require.GreaterOrEqual(t, budgetStarted.Sub(started), reachedTransport.Sub(started)*3/4)
			require.NoError(t, req.Context().Err())
			return &http.Response{
				StatusCode: http.StatusOK, Header: make(http.Header),
				Body: io.NopCloser(strings.NewReader(`{"status":"committed","num_rows_inserted":1}`)),
			}, nil
		})},
	})
	require.NoError(t, err)
	started = time.Now()
	_, err = client.Table("events").AppendNDJSON(context.Background(), payload)
	require.NoError(t, err)
}

func TestWriteInterruptionUsesCurrentContextReason(t *testing.T) {
	t.Parallel()
	ctx, cancel := context.WithCancelCause(context.Background())
	client, err := NewClient(Config{
		Endpoint: "https://example.com",
		HTTPClient: &http.Client{Transport: clientRoundTripFunc(func(req *http.Request) (*http.Response, error) {
			cancel(context.DeadlineExceeded)
			return nil, context.Cause(req.Context())
		})},
	})
	require.NoError(t, err)
	_, err = client.Table("events").AppendNDJSON(ctx, []byte(`{"id":1}`))
	var scopeErr *Error
	require.ErrorAs(t, err, &scopeErr)
	require.Equal(t, &WriteErrorDetails{
		Reason: WriteErrorReasonCancelled, Outcome: AppendStateUnknown,
	}, scopeErr.WriteDetails)
	require.False(t, scopeErr.Retryable)
	require.ErrorIs(t, err, context.DeadlineExceeded, "the original cause remains available")
}

func TestWriteTimeoutHonorsShorterHTTPClientTimeout(t *testing.T) {
	t.Parallel()
	client, err := NewClient(Config{
		Endpoint: "https://example.com", WriteTimeout: time.Second,
		HTTPClient: &http.Client{
			Timeout: 20 * time.Millisecond,
			Transport: clientRoundTripFunc(func(req *http.Request) (*http.Response, error) {
				<-req.Context().Done()
				return nil, req.Context().Err()
			}),
		},
	})
	require.NoError(t, err)
	_, err = client.Table("events").AppendNDJSON(context.Background(), []byte(`{"id":1}`))
	var scopeErr *Error
	require.ErrorAs(t, err, &scopeErr)
	require.Equal(t, &WriteErrorDetails{
		Reason: WriteErrorReasonTimeout, Outcome: AppendStateUnknown,
	}, scopeErr.WriteDetails)
	require.False(t, scopeErr.Retryable)
}

func TestServerTimeoutRejectionRemainsSafelyRetryable(t *testing.T) {
	t.Parallel()
	client, err := NewClient(Config{
		Endpoint: "https://example.com",
		HTTPClient: &http.Client{Transport: clientRoundTripFunc(func(*http.Request) (*http.Response, error) {
			return &http.Response{
				StatusCode: http.StatusRequestTimeout, Header: make(http.Header),
				Body: io.NopCloser(strings.NewReader(`{"message":"admission timed out","retryable":true,"details":{"status":"rejected"},"code":"invalid_schema"}`)),
			}, nil
		})},
	})
	require.NoError(t, err)
	_, err = client.Table("events").AppendNDJSON(context.Background(), []byte(`{"id":1}`))
	var scopeErr *Error
	require.ErrorAs(t, err, &scopeErr)
	require.Nil(t, scopeErr.WriteDetails)
	require.Equal(t, AppendStateRejected, scopeErr.AppendDetails.Status)
	require.True(t, scopeErr.Retryable)
}
