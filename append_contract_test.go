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
		name    string
		status  int
		body    string
		outcome AppendOutcome
		retry   bool
	}{
		{"rate limit", 429, `{"error":{"code":"too_many_requests","message":"busy"}}`, AppendOutcomeFailed, true},
		{"refusal", 429, `{"error":{"code":"too_many_requests","message":"busy","retryable":false}}`, AppendOutcomeFailed, false},
		{"HTTP timeout", 408, `{"error":{"code":"request_timeout","message":"late","retryable":true}}`, AppendOutcomeFailed, false},
		{"server error", 503, `{"error":{"code":"service_unavailable","message":"busy","details":{"status":"rejected"}}}`, AppendOutcomeAmbiguous, false},
		{"future code", 409, `{"error":{"code":"future_conflict","message":"conflict"}}`, AppendOutcomeFailed, false},
		{"mismatched code", 429, `{"error":{"code":"bad_request","message":"bad"}}`, AppendOutcomeAmbiguous, false},
		{"statement code", 400, `{"error":{"code":"invalid_request","message":"bad"}}`, AppendOutcomeAmbiguous, false},
		{"empty code", 429, `{"error":{"code":"","message":"busy"}}`, AppendOutcomeAmbiguous, false},
		{"missing code", 429, `{"error":{"message":"busy"}}`, AppendOutcomeAmbiguous, false},
		{"null message", 429, `{"error":{"code":"too_many_requests","message":null}}`, AppendOutcomeAmbiguous, false},
		{"invalid retry hint", 429, `{"error":{"code":"too_many_requests","message":"busy","retryable":"true"}}`, AppendOutcomeAmbiguous, false},
		{"legacy 5xx rejection", 503, `{"code":"ingest_failure","message":"busy","details":{"status":"rejected"}}`, AppendOutcomeAmbiguous, false},
		{"legacy missing outcome", 429, `{"code":"ingest_failure","message":"busy"}`, AppendOutcomeAmbiguous, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
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

func TestAppendAcknowledgementContract(t *testing.T) {
	for _, tc := range []struct {
		name      string
		status    int
		body      string
		succeeded bool
	}{
		{"modern", 200, `{"num_rows_inserted":1}`, true},
		{"legacy", 200, `{"status":"committed","num_rows_inserted":1}`, true},
		{"count mismatch", 200, `{"num_rows_inserted":2}`, false},
		{"wrong HTTP status", 201, `{"num_rows_inserted":1}`, false},
		{"null status", 200, `{"status":null,"num_rows_inserted":1}`, false},
		{"empty status", 200, `{"status":"","num_rows_inserted":1}`, false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := newTestClient(t, func(*http.Request) (*http.Response, error) { return httpResponse(tc.status, tc.body), nil })
			result, err := client.Table("events").AppendNDJSON(t.Context(), []byte(`{"id":1}`))
			if tc.succeeded {
				require.NoError(t, err)
				require.Equal(t, AppendOutcomeSucceeded, result.Status)
			} else {
				require.Equal(t, AppendOutcomeAmbiguous, requireError(t, err).AppendDetails.Status)
			}
		})
	}
}

func TestAppendAmbiguousRetryHonorsServerRefusal(t *testing.T) {
	for _, retryable := range []bool{false, true} {
		synctest.Test(t, func(t *testing.T) {
			calls := 0
			stream := newTestAppendStream(t, AppendStreamOptions{FailurePolicy: AppendFailureContinue, Retry: &AppendRetryOptions{MaxRetries: 1, RetryAmbiguous: true}}, func(*http.Request) (*http.Response, error) {
				calls++
				if calls == 2 {
					return httpResponse(200, `{"num_rows_inserted":1}`), nil
				}
				hint := "false"
				if retryable {
					hint = "true"
				}
				return httpResponse(503, `{"error":{"code":"service_unavailable","message":"busy","retryable":`+hint+`}}`), nil
			})
			require.NoError(t, stream.Send(t.Context(), map[string]int{"id": 1}))
			report, err := stream.Shutdown(t.Context())
			require.NoError(t, err)
			if retryable {
				require.Equal(t, 2, calls)
				require.Equal(t, AppendDeliverySucceeded, report.Outcome)
			} else {
				require.Equal(t, 1, calls)
				require.Equal(t, AppendDeliveryAmbiguous, report.Outcome)
			}
		})
	}
}
