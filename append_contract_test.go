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
	"net/http"
	"testing"
	"testing/synctest"

	"github.com/stretchr/testify/require"
)

func TestAppendModernContract(t *testing.T) {
	for _, tc := range []struct {
		name       string
		status     int
		body       string
		outcome    AppendOutcome
		retry      bool
		testStream bool
	}{
		{"rate limit", 429, `{"error":{"code":"too_many_requests","message":"busy"}}`, AppendOutcomeFailed, true, true},
		{"refusal", 429, `{"error":{"code":"too_many_requests","message":"busy","retryable":false}}`, AppendOutcomeFailed, false, true},
		{"outer refusal", 429, `{"retryable":false,"error":{"code":"too_many_requests","message":"busy"}}`, AppendOutcomeFailed, false, false},
		{"HTTP timeout", 408, `{"error":{"code":"request_timeout","message":"late","retryable":true}}`, AppendOutcomeFailed, false, false},
		{"server error", 503, `{"error":{"code":"service_unavailable","message":"busy","details":{"status":"rejected"}}}`, AppendOutcomeAmbiguous, false, false},
		{"future code", 409, `{"error":{"code":"future_conflict","message":"conflict"}}`, AppendOutcomeFailed, false, false},
		{"mismatched code", 429, `{"error":{"code":"bad_request","message":"bad"}}`, AppendOutcomeAmbiguous, false, false},
		{"statement code", 400, `{"error":{"code":"invalid_request","message":"bad"}}`, AppendOutcomeAmbiguous, false, false},
		{"empty code", 429, `{"error":{"code":"","message":"busy"}}`, AppendOutcomeAmbiguous, false, false},
		{"missing code", 429, `{"error":{"message":"busy"}}`, AppendOutcomeAmbiguous, false, false},
		{"null message", 429, `{"error":{"code":"too_many_requests","message":null}}`, AppendOutcomeAmbiguous, false, false},
		{"invalid retry hint", 429, `{"error":{"code":"too_many_requests","message":"busy","retryable":"true"}}`, AppendOutcomeAmbiguous, false, false},
		{"malformed outer retry hint", 429, `{"error":{"code":"too_many_requests","message":"busy"},"retryable":"false"}`, AppendOutcomeAmbiguous, false, false},
		{"legacy 5xx rejection", 503, `{"code":"ingest_failure","message":"busy","details":{"status":"rejected"}}`, AppendOutcomeAmbiguous, false, false},
		{"legacy mismatched known code", 429, `{"code":"internal_server_error","message":"failed","details":{"status":"rejected"}}`, AppendOutcomeAmbiguous, false, false},
		{"legacy missing outcome", 429, `{"code":"ingest_failure","message":"busy"}`, AppendOutcomeAmbiguous, false, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			directClient := newTestClient(t, func(*http.Request) (*http.Response, error) {
				return httpResponse(tc.status, tc.body), nil
			})
			_, directErr := directClient.Table("events").AppendNDJSON(t.Context(), []byte(`{"id":1}`))
			directFailure := requireError(t, directErr)
			require.Equal(t, tc.outcome, directFailure.AppendDetails.Status)
			require.Equal(t, tc.retry, directFailure.Retryable)
			if !tc.testStream {
				return
			}
			synctest.Test(t, func(t *testing.T) {
				calls := 0
				stream := newTestAppendStream(t, AppendStreamOptions{FailurePolicy: AppendFailureContinue, Retry: &AppendRetryOptions{MaxRetries: 1}}, func(*http.Request) (*http.Response, error) {
					calls++
					if calls == 2 {
						return httpResponse(200, `{"num_rows_inserted":1}`), nil
					}
					resp := httpResponse(tc.status, tc.body)
					resp.Header.Set("X-Request-ID", "request-header")
					return resp, nil
				})
				require.NoError(t, stream.Send(t.Context(), map[string]int{"id": 1}))
				report, err := stream.Shutdown(t.Context())
				require.NoError(t, err)
				if tc.retry {
					require.Equal(t, 2, calls)
					require.Equal(t, AppendDeliverySucceeded, report.Outcome)
				} else {
					require.Equal(t, 1, calls)
					last := stream.Stats().LastFailure
					require.Equal(t, tc.outcome, last.Status)
					require.Equal(t, tc.status, last.HTTPStatus)
					require.Equal(t, "request-header", last.RequestID)
				}
			})
		})
	}
}

func TestAppendAmbiguousRetryHonorsServerRefusal(t *testing.T) {
	for _, tc := range []struct {
		name      string
		body      string
		retryable bool
	}{
		{"explicit refusal", `{"error":{"code":"service_unavailable","message":"busy","retryable":false}}`, false},
		{"inner refusal with malformed outer field", `{"error":{"code":"service_unavailable","message":"busy","retryable":false},"request_id":123}`, false},
		{"outer refusal with malformed outer field", `{"error":{"code":"service_unavailable","message":"busy"},"retryable":false,"request_id":123}`, false},
		{"explicit retryable", `{"error":{"code":"service_unavailable","message":"busy","retryable":true}}`, true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				calls := 0
				stream := newTestAppendStream(t, AppendStreamOptions{FailurePolicy: AppendFailureContinue, Retry: &AppendRetryOptions{MaxRetries: 1, RetryAmbiguous: true}}, func(*http.Request) (*http.Response, error) {
					calls++
					if calls == 2 {
						return httpResponse(200, `{"num_rows_inserted":1}`), nil
					}
					return httpResponse(503, tc.body), nil
				})
				require.NoError(t, stream.Send(t.Context(), map[string]int{"id": 1}))
				report, err := stream.Shutdown(t.Context())
				require.NoError(t, err)
				if tc.retryable {
					require.Equal(t, 2, calls)
					require.Equal(t, AppendDeliverySucceeded, report.Outcome)
				} else {
					require.Equal(t, 1, calls)
					require.Equal(t, AppendDeliveryAmbiguous, report.Outcome)
				}
			})
		})
	}
}
