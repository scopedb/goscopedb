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
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func quickAppendRetry(n int) *AppendRetryOptions {
	return &AppendRetryOptions{MaxRetries: n, InitialBackoff: time.Millisecond, MaxBackoff: 2 * time.Millisecond, MaxElapsedTime: time.Second}
}

// Simulate a server that commits the first payload, then loses the response.
// The baseline must favor delivery over duplicate avoidance.
func TestAppendAtLeastOnceLostAcknowledgement(t *testing.T) {
	var commits atomic.Int64
	table := newAppendStreamTestTable(t, func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		require.JSONEq(t, `{"id":1}`, string(body))
		if commits.Add(1) == 1 {
			conn, _, err := w.(http.Hijacker).Hijack()
			require.NoError(t, err)
			require.NoError(t, conn.Close())
			return
		}
		writeAppendStreamSuccess(t, w, 1)
	})
	stream, err := table.AppendStream(AppendStreamOptions{Retry: quickAppendRetry(2)})
	require.NoError(t, err)
	require.NoError(t, stream.Send(context.Background(), map[string]int{"id": 1}))
	report, err := stream.Shutdown(context.Background())
	require.NoError(t, err)
	require.Equal(t, int64(2), commits.Load())
	require.Equal(t, uint64(1), report.CommittedRows) // logical rows, not remote copies
	require.Equal(t, uint64(1), report.Retries)
	require.Zero(t, report.UnknownRows)
}

func TestAppendRetryTimeoutThenCommit(t *testing.T) {
	var calls atomic.Int64
	client, err := NewClient(Config{
		Endpoint: "https://example.com", Compression: CompressionGzip,
		HTTPClient: &http.Client{Transport: appendRoundTripFunc(func(r *http.Request) (*http.Response, error) {
			if calls.Add(1) == 1 {
				<-r.Context().Done()
				return nil, r.Context().Err()
			}
			return &http.Response{StatusCode: 200, Header: make(http.Header), Body: io.NopCloser(strings.NewReader(`{"append_state":"committed","num_rows_inserted":1}`))}, nil
		})},
	})
	require.NoError(t, err)
	t.Cleanup(client.Close)
	stream, err := client.Table("events").AppendStream(AppendStreamOptions{AttemptTimeout: 25 * time.Millisecond, Retry: quickAppendRetry(2)})
	require.NoError(t, err)
	require.NoError(t, stream.Send(context.Background(), map[string]int{"id": 1}))
	report, err := stream.Shutdown(context.Background())
	require.NoError(t, err)
	require.Equal(t, uint64(1), report.CommittedRows)
	require.GreaterOrEqual(t, report.Retries, uint64(1))
}

func TestAppendOutageBackpressureAndRecovery(t *testing.T) {
	var healthy atomic.Bool
	var calls atomic.Int64
	table := newAppendStreamTestTable(t, func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		if !healthy.Load() {
			writeAppendStreamFailure(t, w, 503, AppendStateUnknown, true)
			return
		}
		writeAppendStreamSuccess(t, w, 1)
	})
	stream, err := table.AppendStream(AppendStreamOptions{TargetBatchBytes: 1, MaxBufferedBytes: 9, Retry: quickAppendRetry(1000)})
	require.NoError(t, err)
	t.Cleanup(func() { healthy.Store(true); _, _ = stream.Shutdown(context.Background()) })
	require.NoError(t, stream.Send(context.Background(), map[string]int{"id": 1}))
	require.Eventually(t, func() bool { return calls.Load() >= 2 }, time.Second, time.Millisecond)
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	require.ErrorIs(t, stream.Send(ctx, map[string]int{"id": 2}), context.DeadlineExceeded)
	stats := stream.Stats()
	require.Equal(t, 9, stats.PendingBytes)
	require.Greater(t, stats.Retries, uint64(0))
	require.ErrorIs(t, stream.TrySend(map[string]int{"id": 2}), ErrAppendStreamFull)
	healthy.Store(true)
	_, err = stream.Shutdown(context.Background())
	require.NoError(t, err)
	require.Equal(t, uint64(1), stream.Stats().CommittedRows)
	require.Zero(t, stream.Stats().PendingBytes)
}

func TestAppendStopReleasesFailedAndUnsentCapacity(t *testing.T) {
	release := make(chan struct{})
	var calls atomic.Int64
	table := newAppendStreamTestTable(t, func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		<-release
		writeAppendStreamFailure(t, w, 422, AppendStateRejected, false)
	})
	stream, err := table.AppendStream(AppendStreamOptions{TargetBatchBytes: 1, MaxConcurrentBatches: 1})
	require.NoError(t, err)
	for id := 1; id <= 3; id++ {
		require.NoError(t, stream.Send(context.Background(), map[string]int{"id": id}))
	}
	close(release)
	report, err := stream.Shutdown(context.Background())
	require.Error(t, err)
	require.Equal(t, uint64(3), report.FailedRows)
	require.Zero(t, stream.Stats().PendingRows)
	require.Zero(t, stream.Stats().PendingBytes)
	require.Equal(t, int64(1), calls.Load())
	require.Error(t, stream.Send(context.Background(), map[string]int{"id": 4}))
	stream.budget.mu.Lock()
	used := stream.budget.used
	stream.budget.mu.Unlock()
	require.Zero(t, used)
}

func TestAppendUnknownRemainsUnknownAfterRejectedRetry(t *testing.T) {
	var calls atomic.Int64
	table := newAppendStreamTestTable(t, func(w http.ResponseWriter, _ *http.Request) {
		if calls.Add(1) == 1 {
			writeAppendStreamFailure(t, w, 503, AppendStateUnknown, true)
			return
		}
		writeAppendStreamFailure(t, w, 422, AppendStateRejected, false)
	})
	stream, err := table.AppendStream(AppendStreamOptions{Retry: quickAppendRetry(2)})
	require.NoError(t, err)
	require.NoError(t, stream.Send(context.Background(), map[string]int{"id": 1}))
	report, err := stream.Shutdown(context.Background())
	require.Error(t, err)
	require.Equal(t, AppendStateUnknown, appendErrorState(err))
	require.Equal(t, uint64(1), report.UnknownRows)
	require.Zero(t, report.FailedRows)
	require.Equal(t, 422, stream.Stats().LastFailure.HTTPStatus)
}

func TestAppendRetryAfterDoesNotRetryEarly(t *testing.T) {
	var calls atomic.Int64
	table := newAppendStreamTestTable(t, func(w http.ResponseWriter, _ *http.Request) {
		calls.Add(1)
		w.Header().Set("Retry-After", "3600")
		writeAppendStreamFailure(t, w, 429, AppendStateRejected, true)
	})
	retry := quickAppendRetry(3)
	retry.MaxElapsedTime = time.Second
	stream, err := table.AppendStream(AppendStreamOptions{Retry: retry})
	require.NoError(t, err)
	require.NoError(t, stream.Send(context.Background(), map[string]int{"id": 1}))
	report, err := stream.Shutdown(context.Background())
	require.ErrorIs(t, err, ErrAppendRetryExhausted)
	require.Equal(t, int64(1), calls.Load())
	require.Zero(t, report.Retries)
	require.Equal(t, uint64(1), report.FailedRows)
	require.Zero(t, stream.Stats().PendingBytes)
}

func TestAppendShutdownWaitsForInFlightAfterFailure(t *testing.T) {
	release := make(chan struct{})
	started := make(chan struct{})
	table := newAppendStreamTestTable(t, func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		if strings.Contains(string(body), `"id":1`) {
			close(started)
			<-release
			writeAppendStreamSuccess(t, w, 1)
			return
		}
		writeAppendStreamFailure(t, w, 422, AppendStateRejected, false)
	})
	stream, err := table.AppendStream(AppendStreamOptions{TargetBatchBytes: 1, MaxConcurrentBatches: 2})
	require.NoError(t, err)
	require.NoError(t, stream.Send(context.Background(), map[string]int{"id": 1}))
	<-started
	require.NoError(t, stream.Send(context.Background(), map[string]int{"id": 2}))
	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	report, err := stream.Shutdown(ctx)
	require.ErrorIs(t, err, context.DeadlineExceeded)
	require.Zero(t, report)
	close(release)
	report, err = stream.Shutdown(context.Background())
	require.Error(t, err)
	require.Equal(t, uint64(1), report.FailedRows)
	require.Zero(t, stream.Stats().PendingBytes)
	require.Equal(t, uint64(1), stream.Stats().CommittedRows)
}

func TestAppendDeliveryRetryLimitsAndPermanentErrors(t *testing.T) {
	for _, tc := range []struct {
		name         string
		status       int
		state        AppendState
		rejectedOnly bool
		maxRetries   int
		wantCalls    int64
	}{
		{name: "unknown retry budget", status: 503, state: AppendStateUnknown, maxRetries: 2, wantCalls: 3},
		{name: "unknown retries disabled", status: 503, state: AppendStateUnknown, maxRetries: 0, wantCalls: 1},
		{name: "rejected only compatibility", status: 503, state: AppendStateUnknown, rejectedOnly: true, maxRetries: 2, wantCalls: 1},
		{name: "unstructured authentication failure", status: 401, maxRetries: 2, wantCalls: 1},
		{name: "unstructured oversized request", status: 413, maxRetries: 2, wantCalls: 1},
		{name: "permanent schema rejection", status: 422, state: AppendStateRejected, maxRetries: 2, wantCalls: 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var calls atomic.Int64
			table := newAppendStreamTestTable(t, func(w http.ResponseWriter, _ *http.Request) {
				calls.Add(1)
				if tc.state == "" {
					http.Error(w, "gateway rejection", tc.status)
					return
				}
				writeAppendStreamFailure(t, w, tc.status, tc.state, false)
			})
			retry := quickAppendRetry(tc.maxRetries)
			retry.RejectedOnly = tc.rejectedOnly
			stream, err := table.AppendStream(AppendStreamOptions{Retry: retry})
			require.NoError(t, err)
			require.NoError(t, stream.Send(context.Background(), map[string]int{"id": 1}))
			report, err := stream.Shutdown(context.Background())
			require.Error(t, err)
			require.Equal(t, tc.wantCalls, calls.Load())
			if tc.state == AppendStateUnknown && !tc.rejectedOnly {
				require.ErrorIs(t, err, ErrAppendRetryExhausted)
			}
			require.EqualValues(t, tc.wantCalls-1, stream.Stats().Retries)
			require.Equal(t, uint64(1), report.FailedRows+report.UnknownRows)
			require.Zero(t, stream.Stats().PendingBytes)
		})
	}
}

func TestAppendFailureInterruptsOtherBatchRetrySleep(t *testing.T) {
	firstStarted := make(chan struct{})
	table := newAppendStreamTestTable(t, func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		if strings.Contains(string(body), `"id":1`) {
			close(firstStarted)
			w.Header().Set("Retry-After", "3600")
			writeAppendStreamFailure(t, w, 429, AppendStateRejected, true)
			return
		}
		writeAppendStreamFailure(t, w, 422, AppendStateRejected, false)
	})
	stream, err := table.AppendStream(AppendStreamOptions{
		TargetBatchBytes: 1, MaxConcurrentBatches: 2,
		Retry: &AppendRetryOptions{MaxRetries: 2, MaxElapsedTime: time.Hour},
	})
	require.NoError(t, err)
	require.NoError(t, stream.Send(context.Background(), map[string]int{"id": 1}))
	<-firstStarted
	require.NoError(t, stream.Send(context.Background(), map[string]int{"id": 2}))
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	report, err := stream.Shutdown(ctx)
	require.Error(t, err)
	require.NotErrorIs(t, err, context.DeadlineExceeded)
	require.Equal(t, uint64(2), report.FailedRows)
	require.Zero(t, stream.Stats().Retries)
}

func TestAppendSendCanceledBeforeAdmissionDoesNotLeakCapacity(t *testing.T) {
	table := newAppendStreamTestTable(t, func(_ http.ResponseWriter, _ *http.Request) {
		t.Error("canceled row was sent")
	})
	stream, err := table.AppendStream(AppendStreamOptions{})
	require.NoError(t, err)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	stream.admissionMu.Lock()
	go func() { done <- stream.Send(ctx, map[string]int{"id": 1}) }()
	// Wait for the producer to reserve capacity before it can acquire admission.
	reserved := false
	until := time.Now().Add(time.Second)
	for time.Now().Before(until) {
		stream.budget.mu.Lock()
		reserved = stream.budget.used > 0
		stream.budget.mu.Unlock()
		if reserved {
			break
		}
		time.Sleep(time.Millisecond)
	}
	cancel()
	stream.admissionMu.Unlock()
	require.True(t, reserved)
	require.ErrorIs(t, <-done, context.Canceled)
	_, err = stream.Shutdown(context.Background())
	require.NoError(t, err)
	require.Zero(t, stream.Stats().AcceptedRows)
	stream.budget.mu.Lock()
	used := stream.budget.used
	stream.budget.mu.Unlock()
	require.Zero(t, used)
}

func TestAppendReplayUsesCallerOwnedSourceInterval(t *testing.T) {
	var repaired atomic.Bool
	var firstCopies, secondCopies atomic.Int64
	table := newAppendStreamTestTable(t, func(w http.ResponseWriter, r *http.Request) {
		body, err := io.ReadAll(r.Body)
		require.NoError(t, err)
		if strings.Contains(string(body), `"id":1`) {
			firstCopies.Add(1)
		} else if !repaired.Load() {
			writeAppendStreamFailure(t, w, 422, AppendStateRejected, false)
			return
		} else {
			secondCopies.Add(1)
		}
		writeAppendStreamSuccess(t, w, 1)
	})
	// The caller owns the source interval independently of SDK batch boundaries.
	source := []map[string]int{{"id": 1}, {"id": 2}}
	options := AppendStreamOptions{MaxBatchRows: 1, MaxConcurrentBatches: 1}
	stream, err := table.AppendStream(options)
	require.NoError(t, err)
	for _, row := range source {
		require.NoError(t, stream.Send(context.Background(), row))
	}
	report, err := stream.Flush(context.Background())
	require.Error(t, err)
	require.Equal(t, uint64(1), report.CommittedRows)
	_, err = stream.Shutdown(context.Background())
	require.Error(t, err)

	// No checkpoint is advanced on failure. Repair and replay the whole interval.
	repaired.Store(true)
	replay, err := table.AppendStream(options)
	require.NoError(t, err)
	for _, row := range source {
		require.NoError(t, replay.Send(context.Background(), row))
	}
	report, err = replay.Shutdown(context.Background())
	require.NoError(t, err)
	require.Equal(t, uint64(2), report.CommittedRows)
	require.Equal(t, int64(2), firstCopies.Load())
	require.Equal(t, int64(1), secondCopies.Load())
}
