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
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
)

func TestStreamRetryRequiresSafeRejectionOrReplayOptIn(t *testing.T) {
	for _, tc := range []struct {
		name                               string
		status                             int
		state                              AppendState
		replay, readError, permanentSecond bool
		calls                              int
		outcome                            AppendDeliveryOutcome
	}{
		{"rate limit", 429, AppendStateRejected, false, false, false, 2, AppendDeliveryOK},
		{"unknown default", 503, AppendStateUnknown, false, false, false, 1, AppendDeliveryUnknown},
		{"replay opt in", 503, AppendStateUnknown, true, false, false, 2, AppendDeliveryOK},
		{"unknown then rejected", 503, AppendStateUnknown, true, false, true, 2, AppendDeliveryUnknown},
		{"interrupted authentication response", 401, AppendStateUnknown, true, true, false, 1, AppendDeliveryUnknown},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				calls := 0
				started := time.Now()
				stream := testStream(t, AppendStreamOptions{Retry: &AppendRetryOptions{
					MaxRetries: 2, RetryUnknown: tc.replay, InitialBackoff: time.Millisecond, MaxBackoff: time.Millisecond,
				}}, func(r *http.Request) (*http.Response, error) {
					body, err := decodeCompressedRequestBody(r)
					require.NoError(t, err)
					require.JSONEq(t, `{"id":1}`, string(body))
					calls++
					if calls > 1 {
						if tc.permanentSecond {
							return appendFailure(422, AppendStateRejected, false), nil
						}
						if tc.status == 429 {
							require.GreaterOrEqual(t, time.Since(started), time.Second)
						}
						return appendSuccess(1), nil
					}
					resp := appendFailure(tc.status, tc.state, true)
					if tc.status == 429 {
						resp.Header.Set("Retry-After", time.Now().Add(time.Second).UTC().Format(http.TimeFormat))
					}
					if tc.readError {
						resp.Body = errorReadCloser{io.ErrUnexpectedEOF}
					}
					return resp, nil
				})
				require.NoError(t, stream.Send(t.Context(), map[string]int{"id": 1}))
				report, err := stream.Shutdown(t.Context())
				require.Equal(t, tc.calls, calls, "delivery error: %v", err)
				require.Equal(t, tc.outcome, report.Outcome)
				require.EqualValues(t, tc.calls-1, report.Retries)
				if tc.outcome == AppendDeliveryOK {
					require.NoError(t, err)
					require.EqualValues(t, 1, report.CommittedRows)
				} else {
					require.Error(t, err)
					require.EqualValues(t, 1, report.UnknownRows)
					require.Zero(t, report.FailedRows)
					if tc.readError {
						require.ErrorIs(t, err, io.ErrUnexpectedEOF)
					}
				}
				require.Zero(t, stream.Stats().PendingBytes)
			})
		})
	}
}

func TestStreamRetryCountIsBounded(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		calls := 0
		stream := testStream(t, AppendStreamOptions{Retry: &AppendRetryOptions{MaxRetries: 2, RetryUnknown: true}}, func(*http.Request) (*http.Response, error) {
			calls++
			return appendFailure(503, AppendStateUnknown, true), nil
		})
		require.NoError(t, stream.Send(t.Context(), map[string]int{"id": 1}))
		report, err := stream.Shutdown(t.Context())
		require.ErrorIs(t, err, ErrAppendRetryExhausted)
		require.Equal(t, 3, calls)
		require.EqualValues(t, 2, report.Retries)
		require.EqualValues(t, 1, report.UnknownRows)
	})
}

func TestStreamDeliveryBudgetBoundsAttemptsAndBackoff(t *testing.T) {
	for _, slowAttempt := range []bool{true, false} {
		t.Run(map[bool]string{true: "attempt", false: "retry delay"}[slowAttempt], func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				calls := 0
				stream := testStream(t, AppendStreamOptions{
					AttemptTimeout: new(25 * time.Millisecond),
					Retry:          &AppendRetryOptions{MaxRetries: 10, RetryUnknown: true, MaxElapsedTime: 40 * time.Millisecond, InitialBackoff: time.Millisecond},
				}, func(r *http.Request) (*http.Response, error) {
					calls++
					if slowAttempt {
						<-r.Context().Done()
						return nil, r.Context().Err()
					}
					resp := appendFailure(429, AppendStateRejected, true)
					resp.Header.Set("Retry-After", "3600")
					return resp, nil
				})
				require.NoError(t, stream.Send(t.Context(), map[string]int{"id": 1}))
				report, err := stream.Shutdown(t.Context())
				require.ErrorIs(t, err, ErrAppendRetryExhausted)
				if slowAttempt {
					require.ErrorIs(t, err, context.DeadlineExceeded)
					require.Equal(t, 2, calls)
					require.EqualValues(t, 1, report.UnknownRows)
				} else {
					require.Equal(t, 1, calls)
					require.EqualValues(t, 1, report.FailedRows)
				}
			})
		})
	}
}

func TestStreamFailureInterruptsOtherRetryWaits(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		calls := 0
		stream := testStream(t, AppendStreamOptions{
			MaxBatchRows: 1, MaxConcurrentBatches: 2,
			Retry: &AppendRetryOptions{MaxRetries: 2, MaxElapsedTime: time.Hour},
		}, func(*http.Request) (*http.Response, error) {
			calls++
			if calls == 1 {
				resp := appendFailure(429, AppendStateRejected, true)
				resp.Header.Set("Retry-After", "3600")
				return resp, nil
			}
			return appendFailure(422, AppendStateRejected, false), nil
		})
		require.NoError(t, stream.Send(t.Context(), map[string]int{"id": 1}))
		synctest.Wait()
		started := time.Now()
		require.NoError(t, stream.Send(t.Context(), map[string]int{"id": 2}))
		report, err := stream.Shutdown(t.Context())
		require.Error(t, err)
		require.Zero(t, time.Since(started), "a terminal failure must wake a batch waiting an hour to retry")
		require.EqualValues(t, 2, report.FailedRows)
		require.Zero(t, report.Retries)
	})
}
