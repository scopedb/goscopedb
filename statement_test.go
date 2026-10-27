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
	"errors"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

const finishedResult = `,"result_set":{"format":"json","metadata":{"fields":[{"name":"n","data_type":"int"}],"num_rows":1},"rows":[["42"]]}`

func statementJSON(id uuid.UUID, status, extra string) string {
	return fmt.Sprintf(`{"statement_id":%q,"created_at":"2026-10-27T00:00:00Z","status":%q%s}`, id.String(), status, extra)
}

func TestStatementResumesWithoutResubmission(t *testing.T) {
	for _, tc := range []struct {
		name    string
		execute bool
	}{{"execute", true}, {"wait", false}} {
		t.Run(tc.name, func(t *testing.T) {
			id := uuid.New()
			submissions, polls := 0, 0
			client := newTestClient(t, func(r *http.Request) (*http.Response, error) {
				if r.Method == http.MethodPost {
					submissions++
					body, err := decodeCompressedRequestBody(r)
					require.NoError(t, err)
					require.JSONEq(t, `{"statement_id":"`+id.String()+`","statement":"FROM staging INSERT INTO events","format":"json","max_elapsed_seconds":0.25}`, string(body))
					return httpResponse(200, statementJSON(id, "running", "")), nil
				}
				require.Equal(t, "/v1/statements/"+id.String(), r.URL.Path)
				require.Equal(t, "json", r.URL.Query().Get("format"))
				polls++
				if polls == 1 {
					resp := httpResponse(503, `{"code":"unavailable","message":"poll failed","retryable":true,"details":{"phase":"poll"}}`)
					resp.Header.Set("X-Request-ID", "poll-request")
					resp.Header.Set("Retry-After", "2")
					return resp, nil
				}
				return httpResponse(200, statementJSON(id, "finished", finishedResult)), nil
			})
			statement := client.Statement("FROM staging INSERT INTO events")
			statement.ID, statement.MaxElapsed = &id, 250*time.Millisecond
			var err error
			if tc.execute {
				_, err = statement.Execute(t.Context())
			} else {
				handle, submitErr := statement.Submit(t.Context())
				require.NoError(t, submitErr)
				_, err = handle.Wait(t.Context())
			}
			failure := requireError(t, err)
			require.Equal(t, !tc.execute, failure.Retryable)
			require.Equal(t, id, *failure.StatementID)
			require.Equal(t, "unavailable", failure.Code)
			require.Equal(t, "poll-request", failure.RequestID)
			require.Equal(t, 2*time.Second, failure.RetryAfter)
			require.JSONEq(t, `{"phase":"poll"}`, string(failure.Details))
			result, err := client.StatementHandle(*failure.StatementID).Wait(t.Context())
			require.NoError(t, err)
			row, found, err := result.First()
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, int64(42), row["n"])
			require.Equal(t, 1, submissions)
		})
	}
}

func TestStatementSubmissionOutcome(t *testing.T) {
	lost := errors.New("acknowledgement lost")
	for _, tc := range []struct {
		name      string
		status    int
		body      string
		cause     error
		knownID   bool
		code      string
		retryable bool
	}{
		{name: "lost acknowledgement", cause: lost, knownID: true, code: "statement_submission_outcome_unknown"},
		{name: "gateway", status: 503, body: "unavailable", code: "statement_submission_outcome_unknown"},
		{name: "invalid acknowledgement", status: 200, body: `{}`, code: "statement_submission_outcome_unknown"},
		{name: "mismatched ID", status: 200, body: statementJSON(uuid.New(), "running", ""), knownID: true},
		{name: "confirmed rejection", status: 503, body: `{"code":"busy","message":"rejected","retryable":true}`, code: "busy", retryable: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := newTestClient(t, func(*http.Request) (*http.Response, error) {
				if tc.cause != nil {
					return nil, tc.cause
				}
				return httpResponse(tc.status, tc.body), nil
			})
			statement := client.Statement("FROM staging INSERT INTO events")
			id := uuid.New()
			if tc.knownID {
				statement.ID = &id
			}
			_, err := statement.Submit(t.Context())
			failure := requireError(t, err)
			require.Equal(t, tc.code, failure.Code)
			require.Equal(t, tc.retryable, failure.Retryable)
			if tc.knownID {
				require.Equal(t, id, *failure.StatementID)
			} else {
				require.Nil(t, failure.StatementID)
			}
			if tc.cause != nil {
				require.ErrorIs(t, err, tc.cause)
			}
		})
	}
}

func TestStatementLocalCancellationPreservesRecoveryID(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	id := uuid.New()
	client := newTestClient(t, func(r *http.Request) (*http.Response, error) {
		if r.Method == http.MethodPost {
			return httpResponse(200, statementJSON(id, "running", "")), nil
		}
		cancel()
		return nil, r.Context().Err()
	})
	_, err := client.Query(ctx, "SELECT 1")
	require.ErrorIs(t, err, context.Canceled)
	failure := requireError(t, err)
	require.Equal(t, id, *failure.StatementID)
	require.False(t, failure.Retryable)
}

func TestStatementTerminalWinsOverInFlightRequest(t *testing.T) {
	for _, tc := range []struct {
		operation, late, terminal, extra string
	}{
		{"wait", "http error", "cancelled", ""},
		{"refresh", "stale success", "finished", finishedResult},
		{"cancel", "transport error", "failed", `,"error":{"code":"statement_failed","message":"execution failed","details":{"phase":"execute"}}`},
	} {
		t.Run(tc.operation+"/"+tc.late, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				id := uuid.New()
				started, release := make(chan struct{}), make(chan struct{})
				calls := 0
				client := newTestClient(t, func(*http.Request) (*http.Response, error) {
					calls++
					if calls == 1 {
						close(started)
						<-release
						switch tc.late {
						case "http error":
							return httpResponse(503, `{"message":"stale failure"}`), nil
						case "transport error":
							return nil, errors.New("connection lost")
						default:
							return httpResponse(200, statementJSON(id, "running", "")), nil
						}
					}
					return httpResponse(200, statementJSON(id, tc.terminal, tc.extra)), nil
				})
				handle := client.StatementHandle(id)
				done := make(chan error, 1)
				go func() {
					var err error
					switch tc.operation {
					case "wait":
						_, err = handle.Wait(t.Context())
					case "refresh":
						_, err = handle.Refresh(t.Context())
					case "cancel":
						_, err = handle.Cancel(t.Context())
					}
					done <- err
				}()
				<-started
				terminal, err := handle.Cancel(t.Context())
				close(release)
				require.NoError(t, err)
				if tc.operation == "wait" {
					require.ErrorIs(t, <-done, ErrStatementCancelled)
				} else {
					require.NoError(t, <-done)
				}
				terminal.Status = StatementStatusPending
				if terminal.Error != nil {
					terminal.Error.Details[0] = '['
				}
				if terminal.ResultSet != nil {
					terminal.ResultSet.Schema[0].Name = "changed"
				}
				result, err := handle.Wait(t.Context())
				switch tc.terminal {
				case "cancelled":
					require.ErrorIs(t, err, ErrStatementCancelled)
					require.NotErrorIs(t, err, context.Canceled)
				case "failed":
					failure := requireError(t, err)
					require.Equal(t, "execution failed", failure.Message)
					require.JSONEq(t, `{"phase":"execute"}`, string(failure.StatementDetails.Details))
				case "finished":
					require.NoError(t, err)
					row, found, err := result.First()
					require.NoError(t, err)
					require.True(t, found)
					require.Equal(t, int64(42), row["n"])
				}
				require.Equal(t, StatementStatus(tc.terminal), handle.Snapshot().Status)
				require.Equal(t, 2, calls, "terminal observations must remain cached")
			})
		})
	}
}

func TestStatementRejectsIncompleteOrMismatchedResponses(t *testing.T) {
	id := uuid.New()
	for _, body := range []string{
		statementJSON(uuid.New(), "running", ""),
		statementJSON(id, "failed", ""),
		statementJSON(id, "finished", ""),
		statementJSON(id, "finished", `,"result_set":{"format":"json","metadata":{"fields":[null],"num_rows":0},"rows":[]}`),
		statementJSON(id, "finished", strings.Replace(finishedResult, `"int"`, `"unsigned"`, 1)),
	} {
		client := newTestClient(t, func(*http.Request) (*http.Response, error) { return httpResponse(200, body), nil })
		handle := client.StatementHandle(id)
		_, err := handle.Refresh(t.Context())
		require.Equal(t, ErrorKindUnexpected, requireError(t, err).Kind)
		require.Nil(t, handle.Snapshot())
	}
}
