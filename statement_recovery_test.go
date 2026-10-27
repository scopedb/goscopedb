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
	"io"
	"net/http"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/stretchr/testify/require"
)

func TestStatementExecuteResumesAfterPollingFailure(t *testing.T) {
	for _, operation := range []string{"query", "execute", "wait"} {
		t.Run(operation, func(t *testing.T) {
			id := uuid.New()
			var submissions, polls atomic.Int32
			client, err := NewClient(Config{
				Endpoint: "https://example.com", Compression: CompressionGzip,
				HTTPClient: &http.Client{Transport: clientRoundTripFunc(func(r *http.Request) (*http.Response, error) {
					if r.Method == http.MethodPost {
						submissions.Add(1)
						return statementRecoveryResponse(http.StatusOK, `{"statement_id":"`+id.String()+`","created_at":"2026-10-27T00:00:00Z","status":"running"}`), nil
					}
					require.Equal(t, "/v1/statements/"+id.String(), r.URL.Path)
					if polls.Add(1) == 1 {
						resp := statementRecoveryResponse(http.StatusServiceUnavailable, `{"code":"unavailable","message":"poll failed","retryable":true,"details":{"phase":"poll"}}`)
						resp.Header.Set("X-Request-ID", "poll-request")
						resp.Header.Set("Retry-After", "2")
						return resp, nil
					}
					return statementRecoveryResponse(http.StatusOK, `{"statement_id":"`+id.String()+`","created_at":"2026-10-27T00:00:00Z","status":"finished","result_set":{"format":"json","metadata":{"fields":[{"name":"n","data_type":"int"}],"num_rows":1},"rows":[["42"]]}}`), nil
				})},
			})
			require.NoError(t, err)
			t.Cleanup(client.Close)
			switch operation {
			case "query":
				_, err = client.Query(t.Context(), "FROM staging INSERT INTO events")
			case "execute":
				_, err = client.Statement("FROM staging INSERT INTO events").Execute(t.Context())
			case "wait":
				var handle *StatementHandle
				handle, err = client.Statement("FROM staging INSERT INTO events").Submit(t.Context())
				require.NoError(t, err)
				_, err = handle.Wait(t.Context())
			}
			var failure *Error
			require.ErrorAs(t, err, &failure)
			require.Equal(t, operation == "wait", failure.Retryable)
			require.Equal(t, "poll failed", failure.Message)
			require.Equal(t, "unavailable", failure.Code)
			require.Equal(t, http.StatusServiceUnavailable, failure.HTTPStatus)
			require.Equal(t, "poll-request", failure.RequestID)
			require.JSONEq(t, `{"phase":"poll"}`, string(failure.Details))
			require.Equal(t, 2*time.Second, failure.RetryAfter)
			require.NotNil(t, failure.StatementID)
			require.Equal(t, id, *failure.StatementID)
			result, err := client.StatementHandle(*failure.StatementID).Wait(t.Context())
			require.NoError(t, err)
			row, found, err := result.First()
			require.NoError(t, err)
			require.True(t, found)
			require.Equal(t, int64(42), row["n"])
			require.EqualValues(t, 1, submissions.Load())
			require.EqualValues(t, 2, polls.Load())
		})
	}
}

func TestStatementTerminalStateWinsOverLateRequestFailure(t *testing.T) {
	for _, lateFailure := range []string{"http", "transport", "malformed response"} {
		for _, operation := range []string{"wait", "refresh", "cancel"} {
			for _, terminal := range []StatementStatus{StatementStatusCancelled, StatementStatusFinished, StatementStatusFailed} {
				t.Run(operation+"/"+string(terminal)+"/"+lateFailure, func(t *testing.T) {
					id := uuid.New()
					started, release := make(chan struct{}), make(chan struct{})
					var requests atomic.Int32
					client, err := NewClient(Config{
						Endpoint: "https://example.com", Compression: CompressionGzip,
						HTTPClient: &http.Client{Transport: clientRoundTripFunc(func(*http.Request) (*http.Response, error) {
							if requests.Add(1) == 1 {
								close(started)
								<-release
								switch lateFailure {
								case "transport":
									return nil, errors.New("late transport failure")
								case "malformed response":
									return statementRecoveryResponse(http.StatusOK, `{}`), nil
								default:
									return statementRecoveryResponse(http.StatusServiceUnavailable, `{"message":"late request failed"}`), nil
								}
							}
							extra := ""
							if terminal == StatementStatusFinished {
								extra = `,"result_set":{"format":"json","metadata":{"fields":[],"num_rows":0},"rows":[]}`
							}
							if terminal == StatementStatusFailed {
								extra = `,"error":{"code":"statement_failed","message":"execution failed","details":{"phase":"execute"}}`
							}
							return statementRecoveryResponse(http.StatusOK, `{"statement_id":"`+id.String()+`","created_at":"2026-10-27T00:00:00Z","status":"`+string(terminal)+`"`+extra+`}`), nil
						})},
					})
					require.NoError(t, err)
					t.Cleanup(client.Close)
					handle := client.StatementHandle(id)
					type outcome struct {
						snapshot *StatementSnapshot
						result   *ResultSet
						err      error
					}
					done := make(chan outcome, 1)
					go func() {
						var got outcome
						switch operation {
						case "wait":
							got.result, got.err = handle.Wait(t.Context())
						case "refresh":
							got.snapshot, got.err = handle.Refresh(t.Context())
						case "cancel":
							got.snapshot, got.err = handle.Cancel(t.Context())
						}
						done <- got
					}()
					<-started
					snapshot, err := handle.Cancel(t.Context())
					close(release)
					require.NoError(t, err)
					require.Equal(t, terminal, snapshot.Status)
					got := <-done
					if operation != "wait" {
						require.NoError(t, got.err)
						require.Equal(t, terminal, got.snapshot.Status)
					} else {
						switch terminal {
						case StatementStatusCancelled:
							require.ErrorIs(t, got.err, ErrStatementCancelled)
						case StatementStatusFailed:
							require.EqualError(t, got.err, "execution failed")
							var failure *Error
							require.ErrorAs(t, got.err, &failure)
							require.Equal(t, StatementErrorCodeFailed, failure.StatementDetails.Code)
							require.JSONEq(t, `{"phase":"execute"}`, string(failure.StatementDetails.Details))
						case StatementStatusFinished:
							require.NoError(t, got.err)
							require.NotNil(t, got.result)
						case StatementStatusPending, StatementStatusRunning:
							t.Fatal("expected terminal state")
						}
					}
					require.Equal(t, terminal, handle.Snapshot().Status)
					require.EqualValues(t, 2, requests.Load())
				})
			}
		}
	}
}

func statementRecoveryResponse(status int, body string) *http.Response {
	return &http.Response{StatusCode: status, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(body))}
}

func TestStatementExecutePreservesCancellationAndRecoveryID(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	defer cancel()
	id := uuid.New()
	client, err := NewClient(Config{
		Endpoint: "https://example.com", Compression: CompressionGzip,
		HTTPClient: &http.Client{Transport: clientRoundTripFunc(func(r *http.Request) (*http.Response, error) {
			if r.Method == http.MethodPost {
				return statementRecoveryResponse(http.StatusOK, `{"statement_id":"`+id.String()+`","created_at":"2026-10-27T00:00:00Z","status":"running"}`), nil
			}
			cancel()
			return nil, r.Context().Err()
		})},
	})
	require.NoError(t, err)
	t.Cleanup(client.Close)
	_, err = client.Query(ctx, "SELECT 1")
	require.ErrorIs(t, err, context.Canceled)
	var failure *Error
	require.ErrorAs(t, err, &failure)
	require.False(t, failure.Retryable)
	require.NotNil(t, failure.StatementID)
	require.Equal(t, id, *failure.StatementID)
}

func TestStatementSubmissionRecovery(t *testing.T) {
	lost := errors.New("connection lost")
	for _, test := range []struct {
		name        string
		status      int
		body        string
		cause       error
		readFailure bool
		retryable   bool
		code        string
	}{
		{name: "lost acknowledgement", cause: lost, code: "statement_submission_outcome_unknown"},
		{name: "gateway error", status: 503, body: "unavailable", code: "statement_submission_outcome_unknown"},
		{name: "rate limit without rejection details", status: 429, body: "rate limited", code: "statement_submission_outcome_unknown"},
		{name: "invalid acknowledgement", status: 200, body: `{}`, code: "statement_submission_outcome_unknown"},
		{name: "truncated acknowledgement", status: 200, cause: io.ErrUnexpectedEOF, readFailure: true, code: "statement_submission_outcome_unknown"},
		{name: "confirmed rejection", status: 503, body: `{"code":"unavailable","message":"busy","retryable":true}`, retryable: true, code: "unavailable"},
	} {
		for _, knownID := range []bool{false, true} {
			name := "generated ID"
			if knownID {
				name = "caller ID"
			}
			t.Run(test.name+"/"+name, func(t *testing.T) {
				id := uuid.New()
				client, err := NewClient(Config{
					Endpoint: "https://example.com", Compression: CompressionGzip,
					HTTPClient: &http.Client{Transport: clientRoundTripFunc(func(*http.Request) (*http.Response, error) {
						if test.cause != nil && !test.readFailure {
							return nil, test.cause
						}
						resp := statementRecoveryResponse(test.status, test.body)
						resp.Header.Set("X-Request-ID", "submission-request")
						if test.readFailure {
							resp.Body = errorReadCloser{err: test.cause}
						}
						return resp, nil
					})},
				})
				require.NoError(t, err)
				t.Cleanup(client.Close)
				statement := client.Statement("FROM staging INSERT INTO events")
				if knownID {
					statement.ID = &id
				}
				_, err = statement.Execute(t.Context())
				var failure *Error
				require.ErrorAs(t, err, &failure)
				require.Equal(t, test.retryable, failure.Retryable)
				require.Equal(t, test.code, failure.Code)
				if knownID {
					require.NotNil(t, failure.StatementID)
					require.Equal(t, id, *failure.StatementID)
					require.NotSame(t, statement.ID, failure.StatementID)
				} else {
					require.Nil(t, failure.StatementID)
				}
				if test.cause != nil {
					require.ErrorIs(t, err, test.cause)
				}
				if test.status != 0 {
					require.Equal(t, test.status, failure.HTTPStatus)
					require.Equal(t, "submission-request", failure.RequestID)
				}
			})
		}
	}
}

func TestStatementPreCancelledSubmissionDoesNotSend(t *testing.T) {
	ctx, cancel := context.WithCancel(t.Context())
	cancel()
	client, err := NewClient(Config{
		Endpoint: "https://example.com",
		HTTPClient: &http.Client{Transport: clientRoundTripFunc(func(*http.Request) (*http.Response, error) {
			t.Error("cancelled submission reached transport")
			return nil, errors.New("unexpected request")
		})},
	})
	require.NoError(t, err)
	t.Cleanup(client.Close)
	_, err = client.Query(ctx, "SELECT 1")
	require.Same(t, context.Canceled, err)
}

func TestStatementFailureContextDoesNotMutateTransportError(t *testing.T) {
	id := uuid.New()
	cause := errors.New("response lost")
	original := &Error{Kind: ErrorKindUnexpected, Code: "read_failed", Message: "poll failed", Retryable: true, cause: cause}
	client, err := NewClient(Config{
		Endpoint: "https://example.com", Compression: CompressionGzip,
		HTTPClient: &http.Client{Transport: clientRoundTripFunc(func(r *http.Request) (*http.Response, error) {
			if r.Method == http.MethodPost {
				return statementRecoveryResponse(http.StatusOK, `{"statement_id":"`+id.String()+`","created_at":"2026-10-27T00:00:00Z","status":"running"}`), nil
			}
			return nil, original
		})},
	})
	require.NoError(t, err)
	t.Cleanup(client.Close)
	_, err = client.Query(t.Context(), "SELECT 1")
	require.ErrorIs(t, err, original)
	require.ErrorIs(t, err, cause)
	var failure *Error
	require.ErrorAs(t, err, &failure)
	require.False(t, failure.Retryable)
	require.Equal(t, "read_failed", failure.Code)
	require.Equal(t, id, *failure.StatementID)
	require.True(t, original.Retryable)
	require.Nil(t, original.StatementID)
}
