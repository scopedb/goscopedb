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

// TODO(legacy): Remove the dual-protocol cases after ScopeDB 0.5 support ends.

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestStatementTimeoutCompatibility(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls.Add(1)
		body, err := decodeCompressedRequestBody(r)
		require.NoError(t, err)
		require.JSONEq(t, `{"statement":"values (1)","format":"json","max_elapsed_seconds":0.25,"max_estimated_seconds":0.125}`, string(body))
		writeTestJSON(t, w, fmt.Sprintf(`{"statement_id":%q,"created_at":"2026-08-08T00:00:00Z","status":"running","progress":{}}`, uuid.New()))
	}))
	defer server.Close()
	client := newTestClient(t, server.URL)
	statement := client.Statement("values (1)")
	statement.MaxElapsed = 250 * time.Millisecond
	statement.MaxEstimated = 125 * time.Millisecond
	_, err := statement.Submit(context.Background())
	require.NoError(t, err)
	statement.ExecTimeout = "1s"
	_, err = statement.Submit(context.Background())
	require.ErrorContains(t, err, "cannot be specified together")
	statement.ExecTimeout = ""
	for _, limits := range [][2]time.Duration{{-1, 0}, {0, -1}} {
		statement.MaxElapsed, statement.MaxEstimated = limits[0], limits[1]
		_, err = statement.Submit(context.Background())
		require.ErrorContains(t, err, "must not be negative")
	}
	require.Equal(t, int32(1), calls.Load())
}

func TestStatementResponseCompatibility(t *testing.T) {
	t.Parallel()
	for _, tc := range []struct {
		name, fields, code, message string
		httpStatus                  int
	}{
		{"legacy message", `"message":"legacy failure"`, "", "legacy failure", 200},
		{"legacy structured", `"message":"outer","error":{"code":"execute_error","message":"nested","details":{"extra":1}}`, "execute_error", "nested", 200},
		{"current", `"error":{"code":"statement_failed","message":"nested","details":{"extra":1}}`, "statement_failed", "nested", 200},
		{"unknown code", `"error":{"code":"future_failure","message":"future","details":{"extra":1}}`, "future_failure", "future", 200},
		{"http failure", `"code":"not_found","message":"missing statement"`, "not_found", "missing statement", 404},
	} {
		t.Run(tc.name, func(t *testing.T) {
			id := uuid.New()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				w.WriteHeader(tc.httpStatus)
				writeTestJSON(t, w, fmt.Sprintf(`{"statement_id":%q,"created_at":"2026-08-08T00:00:00Z","status":"failed","progress":{},%s}`, id, tc.fields))
			}))
			defer server.Close()
			_, err := newTestClient(t, server.URL).StatementHandle(id).Wait(context.Background())
			var failure *Error
			require.ErrorAs(t, err, &failure)
			require.Equal(t, tc.code, failure.Code)
			require.Equal(t, tc.message, failure.Message)
			if tc.code != "" && tc.httpStatus == 200 {
				require.Equal(t, StatementErrorCode(tc.code), failure.StatementDetails.Code)
				require.JSONEq(t, `{"extra":1}`, string(failure.StatementDetails.Details))
			}
		})
	}
}

func TestStatementCancelCompatibility(t *testing.T) {
	t.Parallel()
	for _, status := range []StatementStatus{StatementStatusFinished, StatementStatusFailed, StatementStatusCancelled} {
		t.Run(string(status), func(t *testing.T) {
			id := uuid.New()
			var requests atomic.Int32
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				requests.Add(1)
				require.Equal(t, http.MethodPost, r.Method)
				require.Equal(t, "json", r.URL.Query().Get("format"))
				response := map[string]any{
					"statement_id": id, "created_at": "2026-08-08T00:00:00Z", "status": status,
					"progress": map[string]int{"scanned_rows": 3},
				}
				if status == StatementStatusFinished {
					response["result_set"] = map[string]any{"metadata": map[string]any{"fields": []any{}, "num_rows": 0}, "format": "json", "rows": []any{}}
				}
				if status == StatementStatusFailed {
					response["error"] = map[string]string{"code": "statement_failed", "message": "failed during cancellation"}
				}
				require.NoError(t, json.NewEncoder(w).Encode(response))
			}))
			defer server.Close()
			handle := newTestClient(t, server.URL).StatementHandle(id)
			cancelled, err := handle.Cancel(context.Background())
			require.NoError(t, err)
			require.Equal(t, status, cancelled.Status)
			require.Equal(t, int64(3), handle.Progress().ScannedRows)
			result, err := handle.Wait(context.Background())
			if status == StatementStatusFinished {
				require.NoError(t, err)
				require.NotNil(t, result)
			} else {
				var failure *Error
				require.ErrorAs(t, err, &failure)
				if status == StatementStatusFailed {
					require.Equal(t, "statement_failed", failure.Code)
					require.Equal(t, "failed during cancellation", failure.Message)
				}
			}
			_, err = handle.Cancel(context.Background())
			require.NoError(t, err)
			require.Equal(t, int32(1), requests.Load(), "full cancellation results need no fetch")
		})
	}
}
