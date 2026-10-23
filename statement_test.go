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
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestClientQueryExecutesAndReturnsRows(t *testing.T) {
	t.Parallel()

	statementID := uuid.New()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		require.Equal(t, http.MethodPost, r.Method)
		require.Equal(t, "/v1/statements", r.URL.Path)
		var request struct {
			Format string `json:"format"`
		}
		body, err := decodeCompressedRequestBody(r)
		require.NoError(t, err)
		require.NoError(t, json.Unmarshal(body, &request))
		require.Equal(t, "json", request.Format)
		writeTestJSON(t, w, `{
			"statement_id":"`+statementID.String()+`",
			"status":"finished",
			"created_at":"2026-08-08T00:00:00Z",
			"progress":{},
			"result_set":{
				"metadata":{"fields":[{"name":"ready","data_type":"uint"}],"num_rows":1},
				"format":"json",
				"rows":[["1"]]
			}
		}`)
	}))
	defer server.Close()

	client := newTestClient(t, server.URL)
	result, err := client.Query(context.Background(), "SELECT 1 AS ready")
	require.NoError(t, err)
	row, ok, err := result.First()
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, uint64(1), row["ready"])
}

func TestStatementSubmitSendsOnlySupportedConfiguration(t *testing.T) {
	t.Parallel()

	providedID := uuid.MustParse("01989a4e-4ee2-7e63-87a5-65ac3b5161dc")
	tests := []struct {
		name         string
		statementID  *uuid.UUID
		maxElapsed   time.Duration
		maxEstimated time.Duration
		wantBody     string
	}{
		{
			name:     "default",
			wantBody: `{"statement":"FROM events","format":"json"}`,
		},
		{
			name:         "configured",
			statementID:  &providedID,
			maxElapsed:   time.Minute,
			maxEstimated: 30 * time.Second,
			wantBody:     `{"statement_id":"01989a4e-4ee2-7e63-87a5-65ac3b5161dc","statement":"FROM events","max_elapsed_seconds":60,"max_estimated_seconds":30,"format":"json"}`,
		},
		{
			name:         "sub-second limits",
			maxElapsed:   time.Nanosecond,
			maxEstimated: 250 * time.Millisecond,
			wantBody:     `{"statement":"FROM events","max_elapsed_seconds":0.000000001,"max_estimated_seconds":0.25,"format":"json"}`,
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			responseID := uuid.New()
			if test.statementID != nil {
				responseID = *test.statementID
			}
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body, err := decodeCompressedRequestBody(r)
				require.NoError(t, err)
				require.JSONEq(t, test.wantBody, string(body))
				writeTestJSON(t, w, `{
					"statement_id":"`+responseID.String()+`",
					"status":"running",
					"created_at":"2026-08-08T00:00:00Z",
					"progress":{}
				}`)
			}))
			defer server.Close()

			client := newTestClient(t, server.URL)
			statement := client.Statement("FROM events")
			statement.ID = test.statementID
			statement.MaxElapsed = test.maxElapsed
			statement.MaxEstimated = test.maxEstimated
			handle, err := statement.Submit(context.Background())
			require.NoError(t, err)
			require.Equal(t, responseID, handle.ID())
		})
	}
}

func TestStatementHTTPError(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("X-Request-ID", "statement-request")
		w.WriteHeader(http.StatusBadRequest)
		writeTestJSON(t, w, `{"code":"invalid_request","message":"invalid statement request"}`)
	}))
	defer server.Close()

	client := newTestClient(t, server.URL)
	_, err := client.Statement("SELECT 1").Submit(t.Context())
	var failure *Error
	require.ErrorAs(t, err, &failure)
	require.Equal(t, "invalid_request", failure.Code)
	require.Equal(t, "invalid statement request", failure.Message)
	require.Equal(t, http.StatusBadRequest, failure.HTTPStatus)
	require.Equal(t, "statement-request", failure.RequestID)
}

func TestStatementHandleStatusFetchesOnceAndCachesTerminalStatus(t *testing.T) {
	t.Parallel()

	statementID := uuid.New()
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		require.Equal(t, http.MethodGet, r.Method)
		require.Equal(t, "/v1/statements/"+statementID.String(), r.URL.Path)
		require.Equal(t, "json", r.URL.Query().Get("format"))
		writeTestJSON(t, w, `{
			"statement_id":"`+statementID.String()+`",
			"status":"finished",
			"created_at":"2026-08-08T00:00:00Z",
			"progress":{},
			"result_set":{"metadata":{"fields":[],"num_rows":0},"format":"json","rows":[]}
		}`)
	}))
	defer server.Close()

	client := newTestClient(t, server.URL)
	handle := client.StatementHandle(statementID)
	require.Equal(t, statementID, handle.ID())
	require.Nil(t, handle.Snapshot())
	require.Zero(t, requests.Load(), "Snapshot must not make a request")

	status, err := handle.Refresh(context.Background())
	require.NoError(t, err)
	require.Equal(t, StatementStatusFinished, status.Status)
	require.Equal(t, int32(1), requests.Load())

	status, err = handle.Refresh(context.Background())
	require.NoError(t, err)
	require.Equal(t, StatementStatusFinished, status.Status)

	cancelled, err := handle.Cancel(context.Background())
	require.NoError(t, err)
	require.Equal(t, StatementStatusFinished, cancelled.Status)
	require.Equal(t, int32(1), requests.Load(), "terminal status must be cached")
	require.Equal(t, StatementStatusFinished, handle.Snapshot().Status)
}

func TestStatementHandleWaitPollsToFinished(t *testing.T) {
	t.Parallel()

	statementID := uuid.New()
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		request := requests.Add(1)
		if request == 1 {
			writeTestJSON(t, w, `{
				"statement_id":"`+statementID.String()+`",
				"status":"running",
				"created_at":"2026-08-08T00:00:00Z",
				"progress":{}
			}`)
			return
		}
		writeTestJSON(t, w, `{
			"statement_id":"`+statementID.String()+`",
			"status":"finished",
			"created_at":"2026-08-08T00:00:00Z",
			"progress":{},
			"result_set":{
				"metadata":{"fields":[{"name":"value","data_type":"string"}],"num_rows":1},
				"format":"json",
				"rows":[["done"]]
			}
		}`)
	}))
	defer server.Close()

	client := newTestClient(t, server.URL)
	result, err := client.StatementHandle(statementID).Wait(context.Background())
	require.NoError(t, err)
	require.Equal(t, int32(2), requests.Load())
	row, ok, err := result.First()
	require.NoError(t, err)
	require.True(t, ok)
	require.Equal(t, "done", row["value"])
}

func TestStatementHandleWaitReturnsStatementFailure(t *testing.T) {
	t.Parallel()

	statementID := uuid.New()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeTestJSON(t, w, `{
			"statement_id":"`+statementID.String()+`",
			"status":"failed",
			"created_at":"2026-08-08T00:00:00Z",
			"progress":{},
			"error":{
				"code":"statement_estimate_limit_exceeded",
				"message":"structured server message",
				"details":{"estimated_seconds":42,"max_estimated_seconds":30}
			}
		}`)
	}))
	defer server.Close()

	client := newTestClient(t, server.URL)
	_, err := client.StatementHandle(statementID).Wait(context.Background())
	var scopeDBError *Error
	require.ErrorAs(t, err, &scopeDBError)
	require.Equal(t, ErrorKindStatementFailed, scopeDBError.Kind)
	require.Equal(t, "structured server message", scopeDBError.Message)
	require.Equal(t, "statement_estimate_limit_exceeded", scopeDBError.Code)
	require.Equal(t, &StatementErrorDetails{
		Code:    StatementErrorCodeEstimateLimitExceeded,
		Message: "structured server message",
		Details: json.RawMessage(`{"estimated_seconds":42,"max_estimated_seconds":30}`),
	}, scopeDBError.StatementDetails)
}

func TestStatementHandleCancelResumesWithoutSnapshot(t *testing.T) {
	t.Parallel()

	statementID := uuid.New()
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		require.Equal(t, http.MethodPost, r.Method)
		require.Equal(t, "/v1/statements/"+statementID.String()+"/cancel", r.URL.Path)
		require.Equal(t, "json", r.URL.Query().Get("format"))
		writeTestJSON(t, w, `{
			"statement_id":"`+statementID.String()+`",
			"created_at":"2026-08-08T00:00:00Z",
			"status":"cancelled",
			"progress":{"scanned_rows":6}
		}`)
	}))
	defer server.Close()

	client := newTestClient(t, server.URL)
	handle := client.StatementHandle(statementID)
	cancelled, err := handle.Cancel(context.Background())
	require.NoError(t, err)
	require.Equal(t, &StatementSnapshot{
		ID:        statementID,
		CreatedAt: time.Date(2026, 8, 8, 0, 0, 0, 0, time.UTC),
		Status:    StatementStatusCancelled,
		Progress:  StatementProgress{ScannedRows: 6},
	}, cancelled)
	require.Equal(t, cancelled.Status, handle.Snapshot().Status)
	require.Equal(t, int64(6), handle.Snapshot().Progress.ScannedRows)

	again, err := handle.Cancel(context.Background())
	require.NoError(t, err)
	require.Equal(t, cancelled, again)

	_, err = handle.Wait(context.Background())
	require.ErrorContains(t, err, "cancelled")
	require.ErrorContains(t, err, statementID.String())
	require.NotErrorIs(t, err, context.Canceled)
	require.Equal(t, int32(1), requests.Load(), "terminal cancel result must be cached")
}

func TestStatementHandleCancelCachesFinishedResult(t *testing.T) {
	t.Parallel()

	statementID := uuid.New()
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		require.Equal(t, http.MethodPost, r.Method)
		require.Equal(t, "/v1/statements/"+statementID.String()+"/cancel", r.URL.Path)
		writeTestJSON(t, w, `{
			"statement_id":"`+statementID.String()+`",
			"status":"finished",
			"created_at":"2026-08-08T00:00:00Z",
			"progress":{},
			"result_set":{
				"metadata":{"fields":[{"name":"value","data_type":"string"}],"num_rows":1},
				"format":"json",
				"rows":[["done"]]
			}
		}`)
	}))
	defer server.Close()

	client := newTestClient(t, server.URL)
	handle := client.StatementHandle(statementID)
	cancelled, err := handle.Cancel(context.Background())
	require.NoError(t, err)
	require.Equal(t, StatementStatusFinished, cancelled.Status)
	require.NotNil(t, handle.Snapshot().ResultSet)

	result, err := handle.Wait(context.Background())
	require.NoError(t, err)
	row, found, err := result.First()
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, "done", row["value"])

	status, err := handle.Refresh(context.Background())
	require.NoError(t, err)
	require.Equal(t, StatementStatusFinished, status.Status)
	require.Equal(t, int32(1), requests.Load(), "cancel provides the complete result")
}

func TestStatementHandleCancelCachesFailure(t *testing.T) {
	t.Parallel()

	statementID := uuid.New()
	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests.Add(1)
		switch {
		case r.Method == http.MethodGet && r.URL.Path == "/v1/statements/"+statementID.String():
			writeTestJSON(t, w, `{
				"statement_id":"`+statementID.String()+`",
				"status":"running",
				"created_at":"2026-08-08T00:00:00Z",
				"progress":{"scanned_rows":11}
			}`)
		case r.Method == http.MethodPost && r.URL.Path == "/v1/statements/"+statementID.String()+"/cancel":
			writeTestJSON(t, w, `{
				"statement_id":"`+statementID.String()+`",
				"status":"failed",
				"created_at":"2026-08-08T00:00:00Z",
				"progress":{"scanned_rows":12,"skipped_rows":3},
				"error":{
					"code":"statement_estimate_limit_exceeded",
					"message":"estimated duration exceeds limit",
					"details":{"estimated_seconds":12,"max_estimated_seconds":10}
				}
			}`)
		default:
			http.Error(w, "unexpected request", http.StatusNotFound)
		}
	}))
	defer server.Close()

	client := newTestClient(t, server.URL)
	handle := client.StatementHandle(statementID)
	status, err := handle.Refresh(context.Background())
	require.NoError(t, err)
	require.Equal(t, StatementStatusRunning, status.Status)
	require.Equal(t, int64(11), handle.Snapshot().Progress.ScannedRows)

	cancelled, err := handle.Cancel(context.Background())
	require.NoError(t, err)
	require.Equal(t, StatementStatusFailed, cancelled.Status)
	require.Equal(t, "estimated duration exceeds limit", cancelled.Error.Message)
	require.Equal(t, cancelled.Status, handle.Snapshot().Status)
	require.Equal(t, int64(12), handle.Snapshot().Progress.ScannedRows)
	require.Equal(t, int64(3), handle.Snapshot().Progress.SkippedRows)

	_, err = handle.Wait(context.Background())
	var scopeDBError *Error
	require.ErrorAs(t, err, &scopeDBError)
	require.Equal(t, ErrorKindStatementFailed, scopeDBError.Kind)
	require.Equal(t, "statement_estimate_limit_exceeded", scopeDBError.Code)
	require.Equal(t, "estimated duration exceeds limit", scopeDBError.Message)
	require.Equal(t, &StatementErrorDetails{
		Code:    StatementErrorCodeEstimateLimitExceeded,
		Message: "estimated duration exceeds limit",
		Details: json.RawMessage(`{"estimated_seconds":12,"max_estimated_seconds":10}`),
	}, scopeDBError.StatementDetails)
	require.Equal(t, int32(2), requests.Load(), "cancel provides the complete failure")
}

func TestStatementProgressIncludesComputeAndSkippedWork(t *testing.T) {
	t.Parallel()

	statementID := uuid.New()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeTestJSON(t, w, `{
			"statement_id":"`+statementID.String()+`",
			"status":"running",
			"created_at":"2026-08-08T00:00:00Z",
			"progress":{
				"compute_seconds":0.125,
				"skipped_partitions":1,
				"skipped_rows":2,
				"skipped_compressed_bytes":3,
				"skipped_uncompressed_bytes":4
			}
		}`)
	}))
	defer server.Close()

	client := newTestClient(t, server.URL)
	handle := client.StatementHandle(statementID)
	_, err := handle.Refresh(context.Background())
	require.NoError(t, err)
	require.Equal(t, &StatementProgress{
		ComputeSeconds:           0.125,
		SkippedPartitions:        1,
		SkippedRows:              2,
		SkippedCompressedBytes:   3,
		SkippedUncompressedBytes: 4,
	}, &handle.Snapshot().Progress)
}

func TestStatementStatusRejectsMalformedStructuredFailures(t *testing.T) {
	t.Parallel()

	tests := map[string]string{
		"missing error":    `null`,
		"non-object error": `[]`,
		"missing code":     `{"message":"failed"}`,
		"missing message":  `{"code":"statement_failed"}`,
	}
	for name, statementError := range tests {
		name, statementError := name, statementError
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			statementID := uuid.New()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				writeTestJSON(t, w, `{
					"statement_id":"`+statementID.String()+`",
					"status":"failed",
					"created_at":"2026-08-08T00:00:00Z",
					"progress":{},
					"error":`+statementError+`
				}`)
			}))
			defer server.Close()

			client := newTestClient(t, server.URL)
			_, err := client.StatementHandle(statementID).Refresh(context.Background())
			var scopeDBError *Error
			require.ErrorAs(t, err, &scopeDBError)
			require.Equal(t, ErrorKindUnexpected, scopeDBError.Kind)
		})
	}
}

func TestStatementStatusRejectsMalformedFinishedResultsWithoutPanicking(t *testing.T) {
	t.Parallel()

	tests := map[string]string{
		"missing result set": ``,
		"missing metadata":   `,"result_set":{"format":"json","rows":[]}`,
		"unsupported format": `,"result_set":{"metadata":{"fields":[],"num_rows":0},"format":"arrow","rows":[]}`,
		"null field":         `,"result_set":{"metadata":{"fields":[null],"num_rows":0},"format":"json","rows":[]}`,
		"missing data type":  `,"result_set":{"metadata":{"fields":[{"name":"v"}],"num_rows":0},"format":"json","rows":[]}`,
		"unknown data type":  `,"result_set":{"metadata":{"fields":[{"name":"v","data_type":"unsigned"}],"num_rows":0},"format":"json","rows":[]}`,
	}
	for name, resultSet := range tests {
		name, resultSet := name, resultSet
		t.Run(name, func(t *testing.T) {
			t.Parallel()

			statementID := uuid.New()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				writeTestJSON(t, w, `{
					"statement_id":"`+statementID.String()+`",
					"status":"finished",
					"created_at":"2026-08-08T00:00:00Z",
					"progress":{}`+resultSet+`
				}`)
			}))
			defer server.Close()

			client := newTestClient(t, server.URL)
			require.NotPanics(t, func() {
				_, err := client.StatementHandle(statementID).Refresh(context.Background())
				var scopeDBError *Error
				require.ErrorAs(t, err, &scopeDBError)
				require.Equal(t, ErrorKindUnexpected, scopeDBError.Kind)
			})
		})
	}
}

func TestMalformedFinishedRowsReturnResultErrorsWithoutPanicking(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name      string
		metadata  string
		rows      string
		assertErr func(*testing.T, *ResultSet)
	}{
		{
			name:     "malformed rows",
			metadata: `{"fields":[],"num_rows":0}`,
			rows:     `{}`,
			assertErr: func(t *testing.T, result *ResultSet) {
				_, err := result.RawRows()
				require.Error(t, err)
			},
		},
		{
			name:     "null rows",
			metadata: `{"fields":[],"num_rows":0}`,
			rows:     `null`,
			assertErr: func(t *testing.T, result *ResultSet) {
				_, err := result.RawRows()
				require.ErrorContains(t, err, "must be a JSON array")
			},
		},
		{
			name:     "row count mismatch",
			metadata: `{"fields":[],"num_rows":1}`,
			rows:     `[]`,
			assertErr: func(t *testing.T, result *ResultSet) {
				_, err := result.RawRows()
				require.ErrorContains(t, err, "result row count mismatch")
			},
		},
		{
			name:     "field count mismatch",
			metadata: `{"fields":[{"name":"v","data_type":"int"}],"num_rows":1}`,
			rows:     `[[]]`,
			assertErr: func(t *testing.T, result *ResultSet) {
				_, err := result.ToValues()
				require.ErrorContains(t, err, "schema length does not match record length")
			},
		},
	}
	for _, test := range tests {
		test := test
		t.Run(test.name, func(t *testing.T) {
			t.Parallel()

			statementID := uuid.New()
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				writeTestJSON(t, w, `{
					"statement_id":"`+statementID.String()+`",
					"status":"finished",
					"created_at":"2026-08-08T00:00:00Z",
					"progress":{},
					"result_set":{"metadata":`+test.metadata+`,"format":"json","rows":`+test.rows+`}
				}`)
			}))
			defer server.Close()

			client := newTestClient(t, server.URL)
			require.NotPanics(t, func() {
				handle := client.StatementHandle(statementID)
				_, err := handle.Refresh(context.Background())
				require.NoError(t, err)
				result := handle.Snapshot().ResultSet
				require.NotNil(t, result)
				test.assertErr(t, result)
			})
		})
	}
}

func newTestClient(t *testing.T, endpoint string) *Client {
	t.Helper()
	client, err := NewClient(Config{Endpoint: endpoint})
	require.NoError(t, err)
	t.Cleanup(client.Close)
	return client
}

func writeTestJSON(t *testing.T, w http.ResponseWriter, body string) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	_, err := w.Write([]byte(body))
	require.NoError(t, err)
}

func TestStatementCancelWinsOverStaleRefresh(t *testing.T) {
	for _, terminal := range []StatementStatus{StatementStatusCancelled, StatementStatusFinished, StatementStatusFailed} {
		t.Run(string(terminal), func(t *testing.T) {
			id := uuid.New()
			started, release := make(chan struct{}, 4), make(chan struct{})
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				status := terminal
				if r.Method == http.MethodGet {
					started <- struct{}{}
					<-release
					status = StatementStatusRunning
				}
				response := map[string]any{
					"statement_id": id, "created_at": "2026-10-23T00:00:00Z",
					"status": status, "progress": map[string]int{"scanned_rows": 7},
				}
				if status == StatementStatusFinished {
					response["result_set"] = json.RawMessage(`{"metadata":{"fields":[{"name":"n","data_type":"int"}],"num_rows":1},"format":"json","rows":[["9007199254740993"]]}`)
				}
				if status == StatementStatusFailed {
					response["error"] = json.RawMessage(`{"code":"statement_failed","message":"original failure","details":{"n":9007199254740993}}`)
				}
				writeJSONValue(t, w, response)
			}))
			defer server.Close()
			handle := newTestClient(t, server.URL).StatementHandle(id)
			finished := make(chan *StatementSnapshot, 4)
			for range 4 {
				go func() {
					snapshot, err := handle.Refresh(t.Context())
					if err != nil {
						t.Error(err)
					}
					finished <- snapshot
				}()
			}
			for range 4 {
				<-started
			}
			cancelled, err := handle.Cancel(t.Context())
			close(release)
			require.NoError(t, err)
			require.Equal(t, terminal, cancelled.Status)
			for range 4 {
				require.Equal(t, terminal, (<-finished).Status)
			}
			cancelled.Status = StatementStatusPending
			cancelled.Progress.ScannedRows = 999
			if cancelled.ResultSet != nil {
				cancelled.ResultSet.Schema[0].Name = "changed"
			}
			if cancelled.Error != nil {
				cancelled.Error.Message = "changed"
				cancelled.Error.Details[0] = '['
			}
			snapshot := handle.Snapshot()
			require.Equal(t, terminal, snapshot.Status)
			require.EqualValues(t, 7, snapshot.Progress.ScannedRows)
			result, err := handle.Wait(t.Context())
			switch terminal {
			case StatementStatusCancelled:
				require.ErrorIs(t, err, ErrStatementCancelled)
				require.NotErrorIs(t, err, context.Canceled)
			case StatementStatusFinished:
				require.NoError(t, err)
				row, ok, err := result.First()
				require.NoError(t, err)
				require.True(t, ok)
				require.Equal(t, int64(9007199254740993), row["n"])
			case StatementStatusFailed:
				require.EqualError(t, err, "original failure")
				require.JSONEq(t, `{"n":9007199254740993}`, string(snapshot.Error.Details))
			case StatementStatusPending, StatementStatusRunning:
				t.Fatal("expected a terminal snapshot")
			}
		})
	}
}

func TestStatementRejectsMismatchedResponseID(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		writeJSONValue(t, w, map[string]any{
			"statement_id": uuid.New(), "created_at": "2026-10-23T00:00:00Z", "status": "running",
		})
	}))
	defer server.Close()
	client := newTestClient(t, server.URL)
	id := uuid.New()
	statement := client.Statement("SELECT 1")
	statement.ID = &id
	_, err := statement.Submit(t.Context())
	require.ErrorContains(t, err, "ID does not match")
	handle := client.StatementHandle(id)
	_, err = handle.Refresh(t.Context())
	require.ErrorContains(t, err, "ID does not match")
	_, err = handle.Cancel(t.Context())
	require.ErrorContains(t, err, "ID does not match")
	require.Nil(t, handle.Snapshot())
}
