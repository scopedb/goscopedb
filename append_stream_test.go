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
	"fmt"
	"net/http"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/stretchr/testify/require"
)

type streamResult struct {
	report AppendDeliveryReport
	err    error
}

func testStream(t *testing.T, options AppendStreamOptions, transport roundTripFunc) *AppendStream {
	t.Helper()
	stream, err := testClient(t, transport).Table("events").AppendStream(options)
	require.NoError(t, err)
	return stream
}

func appendSuccess(rows int) *http.Response {
	return jsonResponse(200, fmt.Sprintf(`{"status":"committed","num_rows_inserted":%d}`, rows))
}

func appendFailure(status int, state AppendState, retryable bool) *http.Response {
	return jsonResponse(status, fmt.Sprintf(`{"code":"append_failed","message":"append failed","retryable":%t,"details":{"status":%q,"row_errors":[{"row_index":0,"column":"id","message":"invalid integer"}]}}`, retryable, state))
}

func TestStreamBatchBoundaries(t *testing.T) {
	for _, tc := range []struct {
		name    string
		options AppendStreamOptions
	}{
		{"rows", AppendStreamOptions{MaxBatchRows: 2}},
		{"bytes", AppendStreamOptions{TargetBatchBytes: 17}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var bodies []string
			tc.options.MaxConcurrentBatches, tc.options.FlushInterval = 1, time.Hour
			stream := testStream(t, tc.options, func(r *http.Request) (*http.Response, error) {
				data, err := decodeCompressedRequestBody(r)
				require.NoError(t, err)
				bodies = append(bodies, string(data))
				return appendSuccess(len(strings.Split(string(data), "\n"))), nil
			})
			for id := range 3 {
				row := map[string]int{"id": id}
				require.NoError(t, stream.Send(t.Context(), row))
				row["id"] = 99 // Admitted data must no longer depend on the caller's value.
			}
			report, err := stream.Shutdown(t.Context())
			require.NoError(t, err)
			require.EqualValues(t, 3, report.CommittedRows)
			require.Equal(t, []string{"{\"id\":0}\n{\"id\":1}", "{\"id\":2}"}, bodies)
		})
	}
}

func TestStreamFlushesPartialBatchOnTimer(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		var calls atomic.Int32
		stream := testStream(t, AppendStreamOptions{FlushInterval: time.Second}, func(*http.Request) (*http.Response, error) {
			calls.Add(1)
			return appendSuccess(1), nil
		})
		require.NoError(t, stream.Send(t.Context(), map[string]int{"id": 1}))
		synctest.Wait()
		require.Zero(t, calls.Load())
		time.Sleep(time.Second)
		synctest.Wait()
		require.EqualValues(t, 1, calls.Load())
		_, err := stream.Shutdown(t.Context())
		require.NoError(t, err)
	})
}

func TestStreamBackpressureAndCanceledSend(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		unblock := sync.OnceFunc(func() { close(release) })
		defer unblock()
		stream := testStream(t, AppendStreamOptions{TargetBatchBytes: 1, MaxBufferedBytes: 9}, func(*http.Request) (*http.Response, error) {
			<-release
			return appendSuccess(1), nil
		})
		require.NoError(t, stream.Send(t.Context(), map[string]int{"id": 1}))
		synctest.Wait()
		require.ErrorIs(t, stream.TrySend(map[string]int{"id": 2}), ErrAppendStreamFull)
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan error, 1)
		go func() { done <- stream.Send(ctx, map[string]int{"id": 2}) }()
		synctest.Wait()
		select {
		case err := <-done:
			t.Fatalf("full stream admitted a row: %v", err)
		default:
		}
		cancel()
		require.ErrorIs(t, <-done, context.Canceled)
		unblock()
		require.NoError(t, stream.Send(t.Context(), map[string]int{"id": 3}))
		report, err := stream.Shutdown(t.Context())
		require.NoError(t, err)
		require.EqualValues(t, 2, report.CommittedRows)
		require.EqualValues(t, 1, report.DroppedRows)
		require.Zero(t, stream.Stats().PendingBytes)
	})
}

func TestStreamFlushAccountsOnlyItsPrefix(t *testing.T) {
	for _, mode := range []string{"success", "stop on failure", "canceled wait"} {
		t.Run(mode, func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				release := make(chan struct{})
				unblock := sync.OnceFunc(func() { close(release) })
				defer unblock()
				calls := 0
				stream := testStream(t, AppendStreamOptions{TargetBatchBytes: 1, MaxConcurrentBatches: 1, FlushInterval: time.Hour}, func(*http.Request) (*http.Response, error) {
					calls++
					<-release
					if mode == "stop on failure" {
						return appendFailure(422, AppendStateRejected, false), nil
					}
					return appendSuccess(1), nil
				})
				require.NoError(t, stream.Send(t.Context(), map[string]int{"id": 1}))
				ctx, cancel := context.WithCancel(t.Context())
				defer cancel()
				done := make(chan streamResult, 1)
				go func() { report, err := stream.Flush(ctx); done <- streamResult{report: report, err: err} }()
				synctest.Wait() // Flush has enqueued its barrier and is waiting for the first batch.
				require.NoError(t, stream.Send(t.Context(), map[string]int{"id": 2}))
				if mode == "canceled wait" {
					cancel()
					result := <-done
					require.ErrorIs(t, result.err, context.Canceled)
					require.Zero(t, result.report)
				}
				unblock()
				synctest.Wait()
				if mode != "canceled wait" {
					result := <-done
					require.Equal(t, mode == "stop on failure", result.err != nil)
					require.EqualValues(t, 1, result.report.AcceptedRows)
				} else {
					require.EqualValues(t, 1, stream.Stats().LastReport.CommittedRows)
				}
				final, err := stream.Shutdown(t.Context())
				require.Equal(t, mode == "stop on failure", err != nil)
				require.EqualValues(t, 1, final.AcceptedRows)
				if mode == "stop on failure" {
					require.EqualValues(t, 1, final.FailedRows)
					require.Equal(t, 1, calls)
				} else {
					require.EqualValues(t, 1, final.CommittedRows)
				}
				require.Zero(t, stream.Stats().PendingRows)
			})
		})
	}
}

func TestStreamFailurePoliciesAndDiagnosticOwnership(t *testing.T) {
	for _, policy := range []AppendFailurePolicy{AppendFailureStop, AppendFailureContinue} {
		t.Run(fmt.Sprint(policy), func(t *testing.T) {
			synctest.Test(t, func(t *testing.T) {
				release := make(chan struct{})
				unblock := sync.OnceFunc(func() { close(release) })
				defer unblock()
				calls := 0
				stream := testStream(t, AppendStreamOptions{FailurePolicy: policy, TargetBatchBytes: 1, MaxConcurrentBatches: 1}, func(*http.Request) (*http.Response, error) {
					<-release
					calls++
					if calls == 1 {
						return appendFailure(422, AppendStateRejected, false), nil
					}
					return appendSuccess(1), nil
				})
				for id := range 3 {
					require.NoError(t, stream.Send(t.Context(), map[string]int{"id": id}))
				}
				unblock()
				report, err := stream.Shutdown(t.Context())
				if policy == AppendFailureStop {
					require.Error(t, err)
					require.Equal(t, 1, calls)
					require.EqualValues(t, 3, report.FailedRows)
				} else {
					require.NoError(t, err)
					require.Equal(t, AppendDeliveryPartial, report.Outcome)
					require.EqualValues(t, 2, report.CommittedRows)
					require.EqualValues(t, 1, report.FailedRows)
				}
				failure := stream.Stats().LastFailure
				require.Equal(t, "append_failed", failure.Code)
				require.Equal(t, "invalid integer", failure.RowErrors[0].Message)
				failure.Details[0], failure.RowErrors[0].Message = '[', "changed"
				require.True(t, json.Valid(stream.Stats().LastFailure.Details))
				require.Equal(t, "invalid integer", stream.Stats().LastFailure.RowErrors[0].Message)
				require.Zero(t, stream.Stats().PendingBytes)
			})
		})
	}
}

func TestStreamConcurrencyAndShutdownSettlement(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		rejected, unknown := make(chan struct{}), make(chan struct{})
		releaseRejected := sync.OnceFunc(func() { close(rejected) })
		releaseUnknown := sync.OnceFunc(func() { close(unknown) })
		defer releaseRejected()
		defer releaseUnknown()
		var calls atomic.Int32
		stream := testStream(t, AppendStreamOptions{TargetBatchBytes: 1, MaxConcurrentBatches: 2}, func(r *http.Request) (*http.Response, error) {
			calls.Add(1)
			body, err := decodeCompressedRequestBody(r)
			require.NoError(t, err)
			if strings.Contains(string(body), `"id":0`) {
				<-rejected
				return appendFailure(422, AppendStateRejected, false), nil
			}
			<-unknown
			return appendFailure(503, AppendStateUnknown, true), nil
		})
		for id := range 3 {
			require.NoError(t, stream.Send(t.Context(), map[string]int{"id": id}))
		}
		synctest.Wait()
		require.EqualValues(t, 2, calls.Load(), "the third batch must remain queued")
		releaseRejected()
		synctest.Wait()
		ctx, cancel := context.WithCancel(t.Context())
		done := make(chan streamResult, 1)
		go func() { report, err := stream.Shutdown(ctx); done <- streamResult{report: report, err: err} }()
		synctest.Wait()
		select {
		case <-done:
			t.Fatal("shutdown did not wait for the in-flight batch")
		default:
		}
		cancel()
		waiting := <-done
		require.ErrorIs(t, waiting.err, context.Canceled)
		require.Zero(t, waiting.report)
		releaseUnknown()
		report, err := stream.Shutdown(t.Context())
		require.Equal(t, AppendStateUnknown, apiError(t, err).AppendDetails.Status)
		require.EqualValues(t, 1, report.UnknownRows)
		require.EqualValues(t, 2, report.FailedRows)
		require.Equal(t, AppendDeliveryUnknown, report.Outcome)
		require.EqualValues(t, 2, calls.Load())
		require.Zero(t, stream.Stats().PendingBytes)
	})
}

func TestStreamConcurrentSendAndShutdown(t *testing.T) {
	var mu sync.Mutex
	seen := make(map[int]bool)
	stream := testStream(t, AppendStreamOptions{MaxBatchRows: 1}, func(r *http.Request) (*http.Response, error) {
		body, err := decodeCompressedRequestBody(r)
		require.NoError(t, err)
		var row map[string]int
		require.NoError(t, json.Unmarshal(body, &row))
		mu.Lock()
		if seen[row["id"]] {
			t.Errorf("duplicate row %d", row["id"])
		}
		seen[row["id"]] = true
		mu.Unlock()
		return appendSuccess(1), nil
	})
	require.NoError(t, stream.Send(t.Context(), map[string]int{"id": 100}))
	start := make(chan struct{})
	results := make(chan error, 32)
	var producers sync.WaitGroup
	for id := range 32 {
		producers.Go(func() { <-start; results <- stream.Send(t.Context(), map[string]int{"id": id}) })
	}
	done := make(chan streamResult, 1)
	go func() {
		<-start
		report, err := stream.Shutdown(t.Context())
		done <- streamResult{report: report, err: err}
	}()
	close(start)
	producers.Wait()
	accepted := 1
	for range 32 {
		if err := <-results; err == nil {
			accepted++
		} else {
			require.ErrorIs(t, err, ErrAppendStreamClosed)
		}
	}
	final := <-done
	require.NoError(t, final.err)
	require.EqualValues(t, accepted, final.report.AcceptedRows)
	require.EqualValues(t, accepted, final.report.CommittedRows)
	mu.Lock()
	require.Len(t, seen, accepted)
	mu.Unlock()
	again, err := stream.Shutdown(t.Context())
	require.NoError(t, err)
	require.Equal(t, final.report, again)
	require.ErrorIs(t, stream.TrySend(map[string]int{"id": 99}), ErrAppendStreamClosed)
}

func TestStreamRejectsUnsendableRows(t *testing.T) {
	stream := testStream(t, AppendStreamOptions{}, func(*http.Request) (*http.Response, error) {
		t.Error("invalid row was dispatched")
		return appendSuccess(0), nil
	})
	require.ErrorIs(t, stream.TrySend([]int{1}), ErrAppendRowInvalid)
	require.ErrorIs(t, stream.Send(t.Context(), nil), ErrAppendRowInvalid)
	require.ErrorIs(t, stream.TrySend(map[string]string{"data": strings.Repeat("x", 8*1024*1024)}), ErrAppendRowTooLarge)
	report, err := stream.Shutdown(t.Context())
	require.NoError(t, err)
	require.Zero(t, report.AcceptedRows)
	require.EqualValues(t, 2, report.DroppedRows, "Send failures do not count as TrySend drops")
}

func TestStreamShutdownDuringRowEncoding(t *testing.T) {
	stream := testStream(t, AppendStreamOptions{}, func(*http.Request) (*http.Response, error) {
		t.Error("a row rejected during shutdown reached transport")
		return appendSuccess(0), nil
	})
	row := blockedRow{started: make(chan struct{}), release: make(chan struct{})}
	done := make(chan error, 1)
	go func() { done <- stream.TrySend(row) }()
	<-row.started
	_, err := stream.Shutdown(t.Context())
	close(row.release)
	require.NoError(t, err)
	require.ErrorIs(t, <-done, ErrAppendStreamClosed)
	require.Zero(t, stream.Stats().AcceptedRows)
	require.Zero(t, stream.Stats().DroppedByReason.BufferFull)
}

type blockedRow struct{ started, release chan struct{} }

func (row blockedRow) MarshalJSON() ([]byte, error) {
	close(row.started)
	<-row.release
	return []byte(`{"id":1}`), nil
}
