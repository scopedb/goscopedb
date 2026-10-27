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
	"errors"
	"fmt"
	"sync"
	"time"
)

const (
	defaultAppendTargetBatchBytes     = 4 * 1024 * 1024
	defaultAppendMaxBatchRows         = maxAppendRows
	defaultAppendFlushInterval        = time.Second
	defaultAppendCommandCapacity      = 1024
	defaultAppendMaxBufferedBytes     = 64 * 1024 * 1024
	defaultAppendMaxConcurrentBatches = 4
	maxAppendConcurrentBatches        = 1024
	defaultAppendMaxRetries           = 8
	defaultAppendInitialBackoff       = 100 * time.Millisecond
	defaultAppendMaxBackoff           = 5 * time.Second
	defaultAppendRetryElapsedTime     = 5 * time.Minute
)

var (
	// ErrAppendStreamFull means TrySend could not reserve bounded local capacity.
	ErrAppendStreamFull = errors.New("append stream buffer is full")
	// ErrAppendStreamClosed means the stream no longer accepts rows.
	ErrAppendStreamClosed = errors.New("append stream is closed")
	// ErrAppendRetryExhausted means the configured retry count or elapsed budget
	// ended. The wrapped append error still describes the last attempt.
	ErrAppendRetryExhausted = errors.New("append retry budget exhausted")
	// ErrAppendRowInvalid means a row did not encode to one JSON object.
	ErrAppendRowInvalid = errors.New("append row must encode to a JSON object")
	// ErrAppendRowTooLarge means one encoded row exceeds the append stream request limit.
	ErrAppendRowTooLarge = errors.New("append row exceeds the protocol limit")
)

// AppendFailurePolicy controls what a stream does after a remote batch fails.
type AppendFailurePolicy uint8

const (
	// AppendFailureStop stops admission after the first failed batch.
	AppendFailureStop AppendFailurePolicy = iota
	// AppendFailureContinue accounts for the failed batch and processes later rows.
	AppendFailureContinue
)

func (p AppendFailurePolicy) String() string {
	switch p {
	case AppendFailureStop:
		return "stop"
	case AppendFailureContinue:
		return "continue"
	default:
		return fmt.Sprintf("AppendFailurePolicy(%d)", p)
	}
}

// AppendStreamState is the lifecycle state of an AppendStream.
type AppendStreamState uint8

const (
	// AppendStreamOpen means the stream accepts rows.
	AppendStreamOpen AppendStreamState = iota
	// AppendStreamClosing means shutdown has closed admission and is settling rows.
	AppendStreamClosing
	// AppendStreamClosed means shutdown settled all accepted rows without a fatal error.
	AppendStreamClosed
	// AppendStreamFailed means strict delivery stopped after a batch failure.
	AppendStreamFailed
)

func (s AppendStreamState) String() string {
	switch s {
	case AppendStreamOpen:
		return "open"
	case AppendStreamClosing:
		return "closing"
	case AppendStreamClosed:
		return "closed"
	case AppendStreamFailed:
		return "failed"
	default:
		return fmt.Sprintf("AppendStreamState(%d)", s)
	}
}

// AppendDeliveryOutcome summarizes one completed delivery barrier.
type AppendDeliveryOutcome uint8

const (
	// AppendDeliveryOK means the interval settled without failed, unknown, or dropped rows.
	AppendDeliveryOK AppendDeliveryOutcome = iota
	// AppendDeliveryPartial means some rows committed and some did not settle cleanly.
	AppendDeliveryPartial
	// AppendDeliveryFailed means no rows committed and at least one row failed or dropped.
	AppendDeliveryFailed
	// AppendDeliveryUnknown means no rows committed and at least one row may have committed.
	AppendDeliveryUnknown
)

func (o AppendDeliveryOutcome) String() string {
	switch o {
	case AppendDeliveryOK:
		return "ok"
	case AppendDeliveryPartial:
		return "partial"
	case AppendDeliveryFailed:
		return "failed"
	case AppendDeliveryUnknown:
		return "unknown"
	default:
		return fmt.Sprintf("AppendDeliveryOutcome(%d)", o)
	}
}

// AppendRetryOptions configures retries of an exact encoded batch for temporary
// rejections, with optional replay of transient unknown outcomes. A nil Retry
// uses the defaults; a non-nil option with MaxRetries=0 disables retries.
// Caller cancellations are never retried. Timeouts with an unknown commit
// outcome require RetryUnknown; timeouts known to precede dispatch do not.
type AppendRetryOptions struct {
	MaxRetries int
	// RetryUnknown opts into retries after a transient failure that may have
	// committed, including attempt timeouts. This can insert duplicate rows. It
	// defaults to false and does not override cancellation or the delivery budget.
	RetryUnknown bool
	// Zero durations use defaults: 100 ms initial, 5 s maximum backoff, 5 min
	// elapsed budget per dispatched batch (including HTTP attempts).
	InitialBackoff time.Duration
	MaxBackoff     time.Duration
	MaxElapsedTime time.Duration
}

// AppendStreamOptions configures bounded asynchronous table appends.
// Zero-valued fields use SDK defaults.
type AppendStreamOptions struct {
	// FailurePolicy defaults to AppendFailureStop.
	FailurePolicy AppendFailurePolicy
	Retry         *AppendRetryOptions
	// TargetBatchBytes defaults to 4 MiB and cannot exceed the request limit.
	TargetBatchBytes int
	// MaxBatchRows defaults to 200,000 and cannot exceed the request limit.
	MaxBatchRows int
	// FlushInterval defaults to one second.
	FlushInterval time.Duration
	// MaxBufferedBytes bounds accepted encoded rows, including in-flight batches.
	// It defaults to 64 MiB and excludes temporary encoding/compression buffers
	// and rows held by blocked Send callers.
	MaxBufferedBytes int
	// MaxConcurrentBatches defaults to four, cannot exceed 1024, and may be set
	// to one for serial request submission.
	MaxConcurrentBatches int
	// AttemptTimeout bounds each HTTP attempt. Nil inherits Config.WriteTimeout.
	// A pointer to zero disables this limit, but the delivery/retry budget still applies.
	// Negative values are invalid. The value is copied when the stream is created.
	// A timeout after connection acquisition leaves the commit outcome unknown.
	AttemptTimeout *time.Duration
}

// AppendDeliveryReport contains settlement counters since the previous barrier.
type AppendDeliveryReport struct {
	// Outcome summarizes delivery for this barrier interval.
	Outcome AppendDeliveryOutcome
	// AcceptedRows is the number of rows locally admitted in the interval.
	AcceptedRows uint64
	// CommittedRows is the number of rows confirmed committed.
	CommittedRows uint64
	// FailedRows is the number of rejected or unsent accepted rows.
	FailedRows uint64
	// UnknownRows is the number of rows whose commit outcome is unknown.
	UnknownRows uint64
	// DroppedRows is the number of TrySend calls rejected locally in the interval.
	DroppedRows      uint64
	CommittedBatches uint64
	FailedBatches    uint64
	UnknownBatches   uint64
	Retries          uint64
	Duration         time.Duration
}

// AppendDroppedRows contains lifetime TrySend failures by reason.
type AppendDroppedRows struct {
	// BufferFull counts rows rejected because immediate local admission was unavailable.
	BufferFull uint64
	// InvalidRow counts values that did not encode to a JSON object.
	InvalidRow uint64
	// RowTooLarge counts values over the single-request protocol limit.
	RowTooLarge uint64
	// Closed counts attempts after shutdown or a terminal failure.
	Closed uint64
}

// AppendLastFailure describes the last failed batch, including failures before
// dispatch. Status covers all attempts; other diagnostics describe the last one.
type AppendLastFailure struct {
	// At is when the client observed the failure.
	At time.Time
	// Message is the original error message.
	Message string
	// Code and Details preserve the last attempt's structured server error.
	Code    string
	Details json.RawMessage
	// Status is the aggregate rejected or unknown outcome across all attempts.
	Status AppendState
	// HTTPStatus is zero when no HTTP response was available.
	HTTPStatus int
	// RequestID is the request identifier reported by the service, when present.
	RequestID string
	// RetryAfter is the service-provided retry delay, when present.
	RetryAfter time.Duration
	// Retryable reports the underlying error's conservative retry classification;
	// it is false for unknown outcomes. RetryUnknown may opt into replaying such
	// outcomes, so this field is not the stream's retry decision.
	Retryable bool
	// WriteDetails identifies a client timeout or cancellation, when present.
	WriteDetails *WriteErrorDetails
	// RowErrors contains structured validation failures reported for the batch.
	RowErrors []AppendRowError
	// RowErrorsTruncated reports whether the service omitted additional row errors.
	RowErrorsTruncated bool
}

// AppendStreamStats is a race-safe lifetime snapshot.
type AppendStreamStats struct {
	// State is the current stream lifecycle state.
	State           AppendStreamState
	AcceptedRows    uint64
	CommittedRows   uint64
	FailedRows      uint64
	UnknownRows     uint64
	DroppedRows     uint64
	DroppedByReason AppendDroppedRows
	Retries         uint64
	PendingRows     uint64
	PendingBytes    int
	InFlightBatches int
	// LastFailure is nil until a batch fails.
	LastFailure *AppendLastFailure
	// LastReport is nil until a delivery barrier completes.
	LastReport *AppendDeliveryReport
}

type normalizedAppendStreamOptions struct {
	failurePolicy        AppendFailurePolicy
	targetBatchBytes     int
	maxBatchRows         int
	flushInterval        time.Duration
	maxBufferedBytes     int
	maxConcurrentBatches int
	attemptTimeout       time.Duration
	retry                AppendRetryOptions
}

// AppendStream creates a bounded asynchronous append stream for this table.
func (t *Table) AppendStream(options AppendStreamOptions) (*AppendStream, error) {
	if _, err := t.c.resourceURL("catalog", "databases", t.databaseName(), "schemas", t.schemaName(), "tables", t.Name, "ingest"); err != nil {
		return nil, err
	}
	config, err := normalizeAppendStreamOptions(options, t.c.writeTimeout)
	if err != nil {
		return nil, err
	}

	stream := &AppendStream{
		client:        t.c,
		database:      t.databaseName(),
		schema:        t.schemaName(),
		table:         t.Name,
		config:        config,
		commands:      make(chan appendCommand, defaultAppendCommandCapacity),
		commandSlots:  make(chan struct{}, defaultAppendCommandCapacity),
		results:       make(chan appendBatchResult, config.maxConcurrentBatches),
		budget:        newAppendByteBudget(config.maxBufferedBytes),
		admissionDone: make(chan struct{}),
		terminalDone:  make(chan struct{}),
		failureDone:   make(chan struct{}),
	}
	stream.shared.state = AppendStreamOpen
	worker := &appendWorker{stream: stream}
	go worker.run()
	return stream, nil
}

func normalizeAppendStreamOptions(options AppendStreamOptions, writeTimeout time.Duration) (normalizedAppendStreamOptions, error) {
	config := normalizedAppendStreamOptions{
		failurePolicy:        options.FailurePolicy,
		targetBatchBytes:     options.TargetBatchBytes,
		maxBatchRows:         options.MaxBatchRows,
		flushInterval:        options.FlushInterval,
		maxBufferedBytes:     options.MaxBufferedBytes,
		maxConcurrentBatches: options.MaxConcurrentBatches,
		attemptTimeout:       writeTimeout,
	}
	config.retry = AppendRetryOptions{MaxRetries: defaultAppendMaxRetries}
	if options.Retry != nil {
		config.retry = *options.Retry
	}
	if config.retry.MaxRetries < 0 || config.retry.InitialBackoff < 0 || config.retry.MaxBackoff < 0 || config.retry.MaxElapsedTime < 0 {
		return config, newError(ErrorKindConfigInvalid, "retry limits must not be negative", nil)
	}
	if config.retry.InitialBackoff == 0 {
		config.retry.InitialBackoff = defaultAppendInitialBackoff
	}
	if config.retry.MaxBackoff == 0 {
		config.retry.MaxBackoff = defaultAppendMaxBackoff
	}
	if config.retry.MaxElapsedTime == 0 {
		config.retry.MaxElapsedTime = defaultAppendRetryElapsedTime
	}
	if config.retry.InitialBackoff > config.retry.MaxBackoff {
		return config, newError(ErrorKindConfigInvalid, "initial retry backoff exceeds maximum backoff", nil)
	}
	if config.failurePolicy != AppendFailureStop && config.failurePolicy != AppendFailureContinue {
		return config, newError(ErrorKindConfigInvalid, "failure policy is invalid", nil)
	}
	if config.targetBatchBytes == 0 {
		config.targetBatchBytes = defaultAppendTargetBatchBytes
	}
	if config.targetBatchBytes < 0 || config.targetBatchBytes > maxAppendBodyBytes {
		return config, newError(ErrorKindConfigInvalid,
			fmt.Sprintf("target batch bytes must be from 1 to %d", maxAppendBodyBytes), nil)
	}
	if config.maxBatchRows == 0 {
		config.maxBatchRows = defaultAppendMaxBatchRows
	}
	if config.maxBatchRows < 0 || config.maxBatchRows > maxAppendRows {
		return config, newError(ErrorKindConfigInvalid,
			fmt.Sprintf("max batch rows must be from 1 to %d", maxAppendRows), nil)
	}
	if config.flushInterval == 0 {
		config.flushInterval = defaultAppendFlushInterval
	}
	if config.flushInterval < 0 {
		return config, newError(ErrorKindConfigInvalid, "flush interval must be greater than zero", nil)
	}
	if config.maxBufferedBytes == 0 {
		config.maxBufferedBytes = defaultAppendMaxBufferedBytes
	}
	if config.maxBufferedBytes < 0 {
		return config, newError(ErrorKindConfigInvalid, "max buffered bytes must be greater than zero", nil)
	}
	if config.maxConcurrentBatches == 0 {
		config.maxConcurrentBatches = defaultAppendMaxConcurrentBatches
	}
	if config.maxConcurrentBatches < 0 || config.maxConcurrentBatches > maxAppendConcurrentBatches {
		return config, newError(ErrorKindConfigInvalid,
			fmt.Sprintf("max concurrent batches must be from 1 to %d", maxAppendConcurrentBatches), nil)
	}
	if options.AttemptTimeout != nil {
		config.attemptTimeout = *options.AttemptTimeout
	}
	if config.attemptTimeout < 0 {
		return config, newError(ErrorKindConfigInvalid, "attempt timeout must not be negative", nil)
	}
	return config, nil
}

type appendCounters struct {
	acceptedRows     uint64
	committedRows    uint64
	failedRows       uint64
	unknownRows      uint64
	droppedRows      uint64
	committedBatches uint64
	failedBatches    uint64
	unknownBatches   uint64
	retries          uint64
}

type appendSharedState struct {
	mu sync.Mutex

	state           AppendStreamState
	counters        appendCounters
	droppedByReason AppendDroppedRows
	pendingRows     uint64
	pendingBytes    int
	inFlightBatches int
	lastFailure     *AppendLastFailure
	lastReport      *AppendDeliveryReport
	fatal           error
}

// AppendStream asynchronously batches rows into bounded NDJSON append requests.
// Send and TrySend are safe to call from concurrent producer goroutines.
type AppendStream struct {
	client   *Client
	database string
	schema   string
	table    string
	config   normalizedAppendStreamOptions

	commands     chan appendCommand
	commandSlots chan struct{}
	results      chan appendBatchResult
	budget       *appendByteBudget

	admissionMu   sync.Mutex
	admissionDone chan struct{}
	admissionOnce sync.Once
	terminalDone  chan struct{}
	failureDone   chan struct{}

	shared appendSharedState
}

// Send serializes row with encoding/json and admits it, waiting for bounded
// local capacity. Row may be a typed struct or any other value that encodes as
// one top-level JSON object. Success does not validate the destination table
// schema or confirm a remote commit.
func (s *AppendStream) Send(ctx context.Context, row any) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := s.checkOpen(); err != nil {
		return err
	}
	payload, err := marshalAppendRecord(row)
	if err != nil {
		return newError(ErrorKindAppendRowsFailed, err.Error(), err)
	}
	reservedBytes := len(payload) + 1
	if reservedBytes > s.config.maxBufferedBytes {
		return newError(
			ErrorKindAppendRowsFailed,
			fmt.Sprintf("append row needs %d buffered bytes, exceeding max buffered bytes %d", reservedBytes, s.config.maxBufferedBytes),
			ErrAppendStreamFull,
		)
	}
	if err := s.budget.acquire(ctx, reservedBytes); err != nil {
		if errors.Is(err, context.Canceled) || errors.Is(err, context.DeadlineExceeded) {
			return err
		}
		return s.closedOrFatalError()
	}
	select {
	case s.commandSlots <- struct{}{}:
	case <-ctx.Done():
		s.budget.release(reservedBytes)
		return ctx.Err()
	case <-s.admissionDone:
		s.budget.release(reservedBytes)
		return s.closedOrFatalError()
	}

	s.admissionMu.Lock()
	defer s.admissionMu.Unlock()
	if err := ctx.Err(); err != nil {
		<-s.commandSlots
		s.budget.release(reservedBytes)
		return err
	}
	if err := s.checkOpen(); err != nil {
		<-s.commandSlots
		s.budget.release(reservedBytes)
		return err
	}
	s.noteAccepted(reservedBytes)
	s.commands <- appendCommand{record: &appendRecord{
		payload:       payload,
		reservedBytes: reservedBytes,
	}}
	return nil
}

// TrySend serializes row with encoding/json and attempts local admission
// without waiting for capacity. Row must encode as one top-level JSON object.
// Success does not validate the destination table schema or confirm a remote
// commit.
func (s *AppendStream) TrySend(row any) error {
	if err := s.checkOpen(); err != nil {
		s.noteDrop(appendDropClosed)
		return err
	}
	payload, err := marshalAppendRecord(row)
	if err != nil {
		if errors.Is(err, ErrAppendRowTooLarge) {
			s.noteDrop(appendDropRowTooLarge)
		} else {
			s.noteDrop(appendDropInvalidRow)
		}
		return err
	}
	reservedBytes := len(payload) + 1
	if reservedBytes > s.config.maxBufferedBytes || !s.budget.tryAcquire(reservedBytes) {
		return s.trySendCapacityError()
	}
	select {
	case s.commandSlots <- struct{}{}:
	default:
		s.budget.release(reservedBytes)
		return s.trySendCapacityError()
	}
	if !s.admissionMu.TryLock() {
		<-s.commandSlots
		s.budget.release(reservedBytes)
		return s.trySendCapacityError()
	}
	defer s.admissionMu.Unlock()
	if err := s.checkOpen(); err != nil {
		<-s.commandSlots
		s.budget.release(reservedBytes)
		s.noteDrop(appendDropClosed)
		return err
	}
	s.noteAccepted(reservedBytes)
	s.commands <- appendCommand{record: &appendRecord{
		payload:       payload,
		reservedBytes: reservedBytes,
	}}
	return nil
}

func (s *AppendStream) trySendCapacityError() error {
	if err := s.checkOpen(); err != nil {
		s.noteDrop(appendDropClosed)
		return err
	}
	s.noteDrop(appendDropBufferFull)
	return ErrAppendStreamFull
}

// Flush dispatches all rows admitted before this barrier and waits for settlement.
// Its report covers activity since the previous completed barrier. Continue mode
// may return a non-OK report without an error; callers must inspect Outcome.
// Canceling ctx stops only this wait; an already enqueued barrier keeps running.
// If ctx ends first, Flush returns a zero report and ctx.Err(). The barrier
// still advances the report interval when it completes; inspect lifetime Stats
// and Stats().LastReport to account for delivery after an interrupted wait.
func (s *AppendStream) Flush(ctx context.Context) (AppendDeliveryReport, error) {
	select {
	case <-s.terminalDone:
		return s.waitTerminal(ctx)
	default:
	}
	if err := ctx.Err(); err != nil {
		return AppendDeliveryReport{}, err
	}
	select {
	case s.commandSlots <- struct{}{}:
	case <-ctx.Done():
		return AppendDeliveryReport{}, ctx.Err()
	case <-s.terminalDone:
		return s.waitTerminal(ctx)
	}

	s.admissionMu.Lock()
	s.shared.mu.Lock()
	state := s.shared.state
	s.shared.mu.Unlock()
	if state != AppendStreamOpen {
		s.admissionMu.Unlock()
		<-s.commandSlots
		return s.waitTerminal(ctx)
	}
	barrier := &appendBarrier{
		startedAt: time.Now(),
		ack:       make(chan appendBarrierResult, 1),
	}
	s.shared.mu.Lock()
	barrier.acceptedRows = s.shared.counters.acceptedRows
	barrier.droppedRows = s.shared.counters.droppedRows
	s.shared.mu.Unlock()
	s.commands <- appendCommand{barrier: barrier}
	s.admissionMu.Unlock()
	return s.waitBarrier(ctx, barrier)
}

func (s *AppendStream) waitBarrier(ctx context.Context, barrier *appendBarrier) (AppendDeliveryReport, error) {
	select {
	case result := <-barrier.ack:
		return result.report, result.err
	default:
	}
	select {
	case result := <-barrier.ack:
		return result.report, result.err
	case <-ctx.Done():
		select {
		case result := <-barrier.ack:
			return result.report, result.err
		default:
			return AppendDeliveryReport{}, ctx.Err()
		}
	case <-s.terminalDone:
		select {
		case result := <-barrier.ack:
			return result.report, result.err
		default:
			return s.waitTerminal(ctx)
		}
	}
}

// Shutdown permanently closes admission and settles every admitted row.
// It is idempotent and returns the original final report on repeated calls.
// Continue mode may return a non-OK report without an error.
// Canceling ctx stops only the caller's wait.
// If ctx ends first, Shutdown returns a zero report and ctx.Err().
func (s *AppendStream) Shutdown(ctx context.Context) (AppendDeliveryReport, error) {
	start := false
	s.admissionMu.Lock()
	s.shared.mu.Lock()
	if s.shared.state == AppendStreamOpen {
		s.shared.state = AppendStreamClosing
		start = true
	}
	s.shared.mu.Unlock()
	if start {
		s.closeAdmission()
		go s.enqueueShutdown()
	}
	s.admissionMu.Unlock()
	return s.waitTerminal(ctx)
}

// Stats returns a race-safe snapshot. Row and retry counters are lifetime
// totals and are not reset by Flush. LastFailure and LastReport are caller-owned
// copies; PendingRows, PendingBytes, and InFlightBatches are current gauges.
func (s *AppendStream) Stats() AppendStreamStats {
	s.shared.mu.Lock()
	defer s.shared.mu.Unlock()
	stats := AppendStreamStats{
		State:           s.shared.state,
		AcceptedRows:    s.shared.counters.acceptedRows,
		CommittedRows:   s.shared.counters.committedRows,
		FailedRows:      s.shared.counters.failedRows,
		UnknownRows:     s.shared.counters.unknownRows,
		DroppedRows:     s.shared.counters.droppedRows,
		DroppedByReason: s.shared.droppedByReason,
		Retries:         s.shared.counters.retries,
		PendingRows:     s.shared.pendingRows,
		PendingBytes:    s.shared.pendingBytes,
		InFlightBatches: s.shared.inFlightBatches,
	}
	stats.LastFailure = cloneAppendLastFailure(s.shared.lastFailure)
	if s.shared.lastReport != nil {
		report := *s.shared.lastReport
		stats.LastReport = &report
	}
	return stats
}

func (s *AppendStream) enqueueShutdown() {
	select {
	case s.commandSlots <- struct{}{}:
	case <-s.terminalDone:
		return
	}

	s.admissionMu.Lock()
	defer s.admissionMu.Unlock()
	select {
	case <-s.terminalDone:
		<-s.commandSlots
		return
	default:
	}
	barrier := &appendBarrier{
		shutdown:  true,
		startedAt: time.Now(),
		ack:       make(chan appendBarrierResult, 1),
	}
	s.shared.mu.Lock()
	barrier.acceptedRows = s.shared.counters.acceptedRows
	barrier.droppedRows = s.shared.counters.droppedRows
	s.shared.mu.Unlock()
	s.commands <- appendCommand{barrier: barrier}
}

func (s *AppendStream) waitTerminal(ctx context.Context) (AppendDeliveryReport, error) {
	select {
	case <-s.terminalDone:
	default:
		select {
		case <-s.terminalDone:
		case <-ctx.Done():
			return AppendDeliveryReport{}, ctx.Err()
		}
	}
	s.shared.mu.Lock()
	defer s.shared.mu.Unlock()
	return *s.shared.lastReport, s.shared.fatal
}

func (s *AppendStream) checkOpen() error {
	s.shared.mu.Lock()
	defer s.shared.mu.Unlock()
	if s.shared.state == AppendStreamOpen {
		return nil
	}
	if s.shared.fatal != nil {
		return s.shared.fatal
	}
	return ErrAppendStreamClosed
}

func (s *AppendStream) closedOrFatalError() error {
	s.shared.mu.Lock()
	defer s.shared.mu.Unlock()
	if s.shared.fatal != nil {
		return s.shared.fatal
	}
	return ErrAppendStreamClosed
}

func (s *AppendStream) noteAccepted(bytes int) {
	s.shared.mu.Lock()
	s.shared.counters.acceptedRows++
	s.shared.pendingRows++
	s.shared.pendingBytes += bytes
	s.shared.mu.Unlock()
}

type appendDropReason uint8

const (
	appendDropBufferFull appendDropReason = iota
	appendDropInvalidRow
	appendDropRowTooLarge
	appendDropClosed
)

func (s *AppendStream) noteDrop(reason appendDropReason) {
	s.shared.mu.Lock()
	s.shared.counters.droppedRows++
	switch reason {
	case appendDropBufferFull:
		s.shared.droppedByReason.BufferFull++
	case appendDropInvalidRow:
		s.shared.droppedByReason.InvalidRow++
	case appendDropRowTooLarge:
		s.shared.droppedByReason.RowTooLarge++
	case appendDropClosed:
		s.shared.droppedByReason.Closed++
	}
	s.shared.mu.Unlock()
}

func (s *AppendStream) closeAdmission() {
	s.admissionOnce.Do(func() {
		close(s.admissionDone)
		s.budget.close()
	})
}

func (s *AppendStream) finishTerminal(report AppendDeliveryReport, err error, state AppendStreamState) {
	s.shared.mu.Lock()
	s.shared.state = state
	s.shared.fatal = err
	s.shared.lastReport = &report
	s.shared.mu.Unlock()
	s.closeAdmission()
	// Only the worker finishes the stream, immediately before returning.
	close(s.terminalDone)
}

func marshalAppendRecord(row any) ([]byte, error) {
	payload, err := json.Marshal(row)
	if err != nil {
		return nil, fmt.Errorf("%w: %w", ErrAppendRowInvalid, err)
	}
	if len(payload) == 0 || payload[0] != '{' {
		return nil, ErrAppendRowInvalid
	}
	if len(payload) > maxAppendBodyBytes {
		return nil, fmt.Errorf("%w: encoded row is %d bytes, limit is %d", ErrAppendRowTooLarge, len(payload), maxAppendBodyBytes)
	}
	return payload, nil
}
