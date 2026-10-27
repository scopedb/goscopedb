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
	"slices"
	"sync/atomic"
	"time"

	"github.com/google/uuid"
)

// Statement configures a ScopeQL statement before submission.
type Statement struct {
	c *Client

	stmt string

	// ID is an optional caller-provided statement ID.
	//
	// When nil, ScopeDB generates the statement ID.
	ID *uuid.UUID
	// MaxElapsed limits the server-side elapsed time, including queueing.
	// Zero uses the server default of one hour.
	MaxElapsed time.Duration
	// MaxEstimated limits the estimated total execution duration.
	// Zero leaves the estimate unrestricted.
	MaxEstimated time.Duration
}

// Statement creates a new statement with the given ScopeQL statement.
func (c *Client) Statement(stmt string) *Statement {
	return &Statement{c: c, stmt: stmt}
}

// Query submits a ScopeQL statement and waits for all result rows. A context
// error stops local waiting without cancelling server execution.
// Error.StatementID can be used to resume waiting on the same execution.
// Use Statement.Submit and retain its handle for explicit lifecycle management.
func (c *Client) Query(ctx context.Context, scopeql string) (*ResultSet, error) {
	return c.Statement(scopeql).Execute(ctx)
}

// Submit submits the statement to ScopeDB for execution. A lost response does
// not prove rejection; set ID before submission when reconciliation needs a
// caller-known identifier.
func (s *Statement) Submit(ctx context.Context) (*StatementHandle, error) {
	if s.MaxElapsed < 0 || s.MaxEstimated < 0 {
		return nil, newError(ErrorKindConfigInvalid, "statement limits must not be negative", nil)
	}
	resp, err := s.c.submitStatement(ctx, &statementRequest{
		StatementID:         s.ID,
		Statement:           s.stmt,
		MaxElapsedSeconds:   s.MaxElapsed.Seconds(),
		MaxEstimatedSeconds: s.MaxEstimated.Seconds(),
		Format:              statementResultFormat,
	})
	if err != nil {
		if s.ID != nil {
			return nil, statementError(err, s.ID)
		}
		return nil, err
	}
	if s.ID != nil && resp.ID != *s.ID {
		return nil, statementError(newError(ErrorKindUnexpected, "statement response ID does not match submitted ID", nil), s.ID)
	}

	handle := s.c.StatementHandle(resp.ID)
	handle.resp.Store(resp)
	return handle, nil
}

// Execute submits the statement to ScopeDB for execution and waits for its completion.
// After submission, errors include Error.StatementID and are not marked retryable:
// use Client.StatementHandle with that ID to resume the existing execution.
func (s *Statement) Execute(ctx context.Context) (*ResultSet, error) {
	handle, err := s.Submit(ctx)
	if err != nil {
		return nil, err
	}
	result, err := handle.Wait(ctx)
	if err != nil {
		failure := statementError(err, &handle.id)
		failure.Retryable = false
		return nil, failure
	}
	return result, nil
}

// StatementHandle tracks a submitted statement. Its methods are safe to call
// concurrently, including Cancel while another goroutine is waiting.
// A handle must not be copied after first use.
type StatementHandle struct {
	c    *Client
	resp atomic.Pointer[statementResponse]
	id   uuid.UUID
}

// StatementSnapshot is one observation of a statement. Error is present for a
// failed statement; ResultSet is present for a finished statement. Snapshots
// belong to the caller: modifying one does not change the handle's cache.
type StatementSnapshot struct {
	ID        uuid.UUID
	CreatedAt time.Time
	Status    StatementStatus
	Progress  StatementProgress
	Error     *StatementErrorDetails
	ResultSet *ResultSet
}

// ErrStatementCancelled means the server confirmed cancellation. It is distinct
// from context.Canceled, which only describes a local request or wait.
var ErrStatementCancelled = errors.New("statement was cancelled")

// StatementHandle creates a new StatementHandle with the given ID.
func (c *Client) StatementHandle(id uuid.UUID) *StatementHandle {
	return &StatementHandle{
		c:  c,
		id: id,
	}
}

// ID returns the statement ID represented by this handle.
func (h *StatementHandle) ID() uuid.UUID {
	return h.id
}

// Snapshot returns the latest local observation without making a request.
// It is nil until Submit, Refresh, or Cancel receives a response.
func (h *StatementHandle) Snapshot() *StatementSnapshot {
	return h.resp.Load().snapshot()
}

func (r *statementResponse) snapshot() *StatementSnapshot {
	if r == nil {
		return nil
	}
	snapshot := &StatementSnapshot{
		ID: r.ID, CreatedAt: r.Created, Status: r.Status,
		Progress: r.Progress, ResultSet: r.ResultSet.snapshot(),
	}
	if r.Error != nil {
		failure := *r.Error
		failure.Details = slices.Clone(failure.Details)
		snapshot.Error = &failure
	}
	return snapshot
}

// Refresh fetches one complete observation. A cached terminal observation is
// returned without making another request. If a concurrent request caches a
// terminal observation, it also takes precedence over this request's failure.
// Other request failures leave the cache intact and retain their retry classification.
func (h *StatementHandle) Refresh(ctx context.Context) (*StatementSnapshot, error) {
	if cached := h.resp.Load(); cached != nil && cached.Status.Terminated() {
		return cached.snapshot(), nil
	}

	resp, err := h.c.fetchStatement(ctx, h.id)
	if err != nil {
		if cached := h.resp.Load(); cached != nil && cached.Status.Terminated() {
			return cached.snapshot(), nil
		}
		return nil, statementError(err, &h.id)
	}
	return h.remember(resp)
}

func (h *StatementHandle) remember(resp *statementResponse) (*StatementSnapshot, error) {
	if resp.ID != h.id {
		return nil, statementError(newError(ErrorKindUnexpected, "statement response ID does not match handle", nil), &h.id)
	}
	for {
		previous := h.resp.Load()
		// Requests can complete out of order. Never replace a terminal response
		// or move a running statement back to pending. Within one active state,
		// retain the observation with the greater elapsed time.
		if previous != nil && (previous.Status.Terminated() ||
			(previous.Status == StatementStatusRunning && resp.Status == StatementStatusPending) ||
			(previous.Status == resp.Status && previous.Progress.NanosFromSubmitted > resp.Progress.NanosFromSubmitted)) {
			return previous.snapshot(), nil
		}
		if h.resp.CompareAndSwap(previous, resp) {
			return resp.snapshot(), nil
		}
	}
}

// Wait polls until the statement is finished, failed, or cancelled.
//
// A context error stops local waiting; it does not cancel the server statement.
// Use Cancel with a live context to request server cancellation. Terminal
// results remain cached even after the server expires its saved result.
func (h *StatementHandle) Wait(ctx context.Context) (*ResultSet, error) {
	delay := 5 * time.Millisecond
	maxDelay := time.Second
	for {
		snapshot, err := h.Refresh(ctx)
		if err != nil {
			return nil, err
		}

		switch snapshot.Status {
		case StatementStatusFinished:
			return snapshot.ResultSet, nil
		case StatementStatusFailed:
			failure := snapshot.Error
			return nil, &Error{
				Kind:             ErrorKindStatementFailed,
				Code:             string(failure.Code),
				Details:          failure.Details,
				Message:          failure.Message,
				StatementDetails: failure,
				StatementID:      new(h.id),
			}
		case StatementStatusCancelled:
			return nil, &Error{
				Kind:        ErrorKindStatementFailed,
				Message:     fmt.Sprintf("statement %s was cancelled", h.id),
				cause:       ErrStatementCancelled,
				StatementID: new(h.id),
			}
		case StatementStatusPending, StatementStatusRunning:
			// Keep polling below.
		default:
			return nil, &Error{
				Kind:        ErrorKindUnexpected,
				Message:     fmt.Sprintf("unknown statement status %q", snapshot.Status),
				StatementID: new(h.id),
			}
		}

		timer := time.NewTimer(delay)
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, statementError(ctx.Err(), &h.id)
		case <-timer.C:
		}
		if delay < maxDelay {
			delay = min(delay*2, maxDelay)
		}
	}
}

// Cancel requests server cancellation and returns the complete observation.
// Completion may win the race: inspect Status, ResultSet, and Error. A cached
// terminal observation is returned without making another request and takes
// precedence over a failed in-flight cancellation request.
func (h *StatementHandle) Cancel(ctx context.Context) (*StatementSnapshot, error) {
	if cached := h.resp.Load(); cached != nil && cached.Status.Terminated() {
		return cached.snapshot(), nil
	}
	resp, err := h.c.cancelStatement(ctx, h.id)
	if err != nil {
		if cached := h.resp.Load(); cached != nil && cached.Status.Terminated() {
			return cached.snapshot(), nil
		}
		return nil, statementError(err, &h.id)
	}
	return h.remember(resp)
}

// StatementStatus is a string that represents the status of a statement.
type StatementStatus string

const (
	// StatementStatusPending indicates the query is not started yet.
	StatementStatusPending StatementStatus = "pending"
	// StatementStatusRunning indicates the query is not finished yet.
	StatementStatusRunning StatementStatus = "running"
	// StatementStatusFinished indicates the query is finished.
	StatementStatusFinished StatementStatus = "finished"
	// StatementStatusFailed indicates the query is failed.
	StatementStatusFailed StatementStatus = "failed"
	// StatementStatusCancelled indicates the query is cancelled.
	StatementStatusCancelled StatementStatus = "cancelled"
)

// StatementErrorCode identifies the reason a statement failed. Unknown values
// are preserved for forward compatibility.
type StatementErrorCode string

const (
	// StatementErrorCodeFailed indicates that statement preparation or execution failed.
	StatementErrorCodeFailed StatementErrorCode = "statement_failed"
	// StatementErrorCodePendingTimeout indicates that the statement timed out before execution.
	StatementErrorCodePendingTimeout StatementErrorCode = "statement_pending_timeout"
	// StatementErrorCodeExecutionTimeout indicates that the statement exceeded its execution timeout.
	StatementErrorCodeExecutionTimeout StatementErrorCode = "statement_execution_timeout"
	// StatementErrorCodeEstimateLimitExceeded indicates that the estimated duration reached its limit.
	StatementErrorCodeEstimateLimitExceeded StatementErrorCode = "statement_estimate_limit_exceeded"
	// StatementErrorCodeHeartbeatLost indicates that the statement worker stopped reporting progress.
	StatementErrorCodeHeartbeatLost StatementErrorCode = "statement_heartbeat_lost"
)

// StatementErrorDetails contains the structured failure returned for a failed
// statement. Details is code-specific JSON and is nil when the server did not
// provide additional details.
type StatementErrorDetails struct {
	Code    StatementErrorCode `json:"code"`
	Message string             `json:"message"`
	Details json.RawMessage    `json:"details,omitempty"`
}

// Terminated returns true if the statement is finished, failed, or cancelled.
func (s StatementStatus) Terminated() bool {
	switch s {
	case StatementStatusFinished, StatementStatusFailed, StatementStatusCancelled:
		return true
	case StatementStatusRunning, StatementStatusPending:
		return false
	default:
		return false
	}
}

// StatementProgress is a struct that represents the progress of a statement.
type StatementProgress struct {
	// ComputeSeconds is cumulative server compute time, not elapsed wall time.
	ComputeSeconds float64 `json:"compute_seconds"`
	// TotalPercentage denotes the total progress in percentage: [0.0, 100.0].
	TotalPercentage float64 `json:"total_percentage"`
	// NanosFromSubmitted denotes the duration in nanoseconds since the statement is submitted.
	NanosFromSubmitted int64 `json:"nanos_from_submitted"`
	// NanosFromStarted denotes the duration in nanoseconds since the statement is started.
	NanosFromStarted int64 `json:"nanos_from_started"`
	// TotalStages denotes the total number of stages to execute.
	TotalStages int64 `json:"total_stages"`
	// TotalPartitions denotes the estimated total number of partitions to scan.
	TotalPartitions int64 `json:"total_partitions"`
	// TotalRows denotes the estimated total number of rows to scan.
	TotalRows int64 `json:"total_rows"`
	// TotalCompressedBytes denotes the estimated total number of compressed bytes to scan.
	TotalCompressedBytes int64 `json:"total_compressed_bytes"`
	// TotalUncompressedBytes denotes the estimated total number of uncompressed bytes to scan.
	TotalUncompressedBytes int64 `json:"total_uncompressed_bytes"`
	// ScannedStages denotes the total number of stages executed.
	ScannedStages int64 `json:"scanned_stages"`
	// ScannedPartitions denotes the number of partitions scanned.
	ScannedPartitions int64 `json:"scanned_partitions"`
	// ScannedRows denotes the number of rows scanned.
	ScannedRows int64 `json:"scanned_rows"`
	// ScannedCompressedBytes denotes the number of compressed bytes scanned.
	ScannedCompressedBytes int64 `json:"scanned_compressed_bytes"`
	// ScannedUncompressedBytes denotes the number of uncompressed bytes scanned.
	ScannedUncompressedBytes int64 `json:"scanned_uncompressed_bytes"`
	// SkippedPartitions denotes the number of partitions skipped by pruning.
	SkippedPartitions int64 `json:"skipped_partitions"`
	// SkippedRows denotes the number of rows skipped by pruning.
	SkippedRows int64 `json:"skipped_rows"`
	// SkippedCompressedBytes denotes the number of compressed bytes skipped by pruning.
	SkippedCompressedBytes int64 `json:"skipped_compressed_bytes"`
	// SkippedUncompressedBytes denotes the number of uncompressed bytes skipped by pruning.
	SkippedUncompressedBytes int64 `json:"skipped_uncompressed_bytes"`
}

// statementError adds recovery context without mutating a transport-owned error.
func statementError(err error, id *uuid.UUID) *Error {
	failure := newError(ErrorKindUnexpected, err.Error(), err)
	if original, ok := errors.AsType[*Error](err); ok {
		*failure = *original
		failure.cause = err
	}
	failure.StatementID = nil
	if id != nil {
		failure.StatementID = new(*id)
	}
	return failure
}
