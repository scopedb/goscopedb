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
	"net/http"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
)

func TestWriteTimeoutCoversHeadersAndBody(t *testing.T) {
	for _, tc := range []struct {
		operation string
		headers   bool
	}{
		{"append", false}, {"append", true}, {"catalog", false}, {"catalog", true},
	} {
		t.Run(tc.operation+map[bool]string{true: "/body", false: "/headers"}[tc.headers], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				var body *deadlineBody
				client, err := NewClient(Config{Endpoint: "https://example.com", Compression: CompressionGzip, WriteTimeout: new(time.Second), HTTPClient: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
					if !tc.headers {
						<-r.Context().Done()
						return nil, r.Context().Err()
					}
					body = &deadlineBody{ctx: r.Context()}
					return &http.Response{StatusCode: 200, Header: http.Header{"X-Request-Id": {"stalled-write"}}, Body: body}, nil
				})}})
				require.NoError(t, err)
				if tc.operation == "append" {
					_, err = client.Table("events").AppendNDJSON(t.Context(), []byte(`{}`))
				} else {
					_, err = client.Table("events").Catalog().Delete(t.Context())
				}
				require.ErrorIs(t, err, context.DeadlineExceeded)
				failure := requireError(t, err)
				require.Equal(t, &WriteErrorDetails{Reason: WriteErrorReasonTimeout, Outcome: AppendStateUnknown}, failure.WriteDetails)
				require.False(t, failure.Retryable)
				if tc.operation == "catalog" {
					require.Equal(t, "catalog_commit_outcome_unknown", failure.Code)
				}
				if tc.headers {
					require.True(t, body.closed)
					require.Equal(t, "stalled-write", failure.RequestID)
				}
			})
		})
	}
}

type deadlineBody struct {
	ctx    context.Context
	closed bool
}

func (b *deadlineBody) Read([]byte) (int, error) { <-b.ctx.Done(); return 0, b.ctx.Err() }
func (b *deadlineBody) Close() error             { b.closed = true; return nil }

func TestWriteDeadlinePrecedence(t *testing.T) {
	for _, tc := range []struct {
		name                                       string
		writeTimeout, callerTimeout, clientTimeout time.Duration
	}{
		{"caller", time.Minute, time.Second, 0},
		{"HTTP client", time.Minute, 0, time.Second},
		{"disabled write timeout", 0, time.Second, 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				client, err := NewClient(Config{Endpoint: "https://example.com", Compression: CompressionGzip, WriteTimeout: &tc.writeTimeout, HTTPClient: &http.Client{Timeout: tc.clientTimeout, Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
					<-r.Context().Done()
					return nil, r.Context().Err()
				})}})
				require.NoError(t, err)
				ctx := t.Context()
				if tc.callerTimeout > 0 {
					var cancel context.CancelFunc
					ctx, cancel = context.WithTimeout(ctx, tc.callerTimeout)
					defer cancel()
				}
				started := time.Now()
				_, err = client.Table("events").AppendNDJSON(ctx, []byte(`{}`))
				require.ErrorIs(t, err, context.DeadlineExceeded)
				require.Equal(t, time.Second, time.Since(started))
				require.Equal(t, WriteErrorReasonTimeout, requireError(t, err).WriteDetails.Reason)
			})
		})
	}
}

func TestAppendStreamAttemptTimeoutOverride(t *testing.T) {
	for _, tc := range []struct {
		name    string
		attempt *time.Duration
		want    time.Duration
	}{
		{"inherited", nil, time.Minute}, {"overridden", new(time.Second), time.Second}, {"disabled", new(time.Duration(0)), 2 * time.Minute},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				client, err := NewClient(Config{Endpoint: "https://example.com", Compression: CompressionGzip, WriteTimeout: new(time.Minute), HTTPClient: &http.Client{Transport: roundTripFunc(func(r *http.Request) (*http.Response, error) {
					deadline, ok := r.Context().Deadline()
					require.True(t, ok)
					require.Equal(t, tc.want, time.Until(deadline))
					return appendSuccess(1), nil
				})}})
				require.NoError(t, err)
				stream, err := client.Table("events").AppendStream(AppendStreamOptions{AttemptTimeout: tc.attempt, Retry: &AppendRetryOptions{MaxElapsedTime: 2 * time.Minute}})
				require.NoError(t, err)
				if tc.attempt != nil {
					*tc.attempt = time.Hour
				}
				require.NoError(t, stream.Send(t.Context(), map[string]int{"id": 1}))
				_, err = stream.Shutdown(t.Context())
				require.NoError(t, err)
			})
		})
	}
}
