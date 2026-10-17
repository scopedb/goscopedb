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

// TODO(legacy): Keep dual-version statement wire handling here. Retire the 0.5 branches
// and deprecated Go API together after ScopeDB 0.5 support ends.

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"

	"github.com/google/uuid"
)

type statementRequest struct {
	StatementID         *uuid.UUID `json:"statement_id,omitempty"`
	Statement           string     `json:"statement"`
	MaxElapsedSeconds   float64    `json:"max_elapsed_seconds,omitempty"`
	MaxEstimatedSeconds float64    `json:"max_estimated_seconds,omitempty"`
	// TODO(legacy): Remove with Statement.ExecTimeout.
	ExecTimeout string       `json:"exec_timeout,omitempty"`
	Format      resultFormat `json:"format"`
}

type statementResponse struct {
	ID       uuid.UUID         `json:"statement_id"`
	Progress StatementProgress `json:"progress"`
	Status   StatementStatus   `json:"status"`
	Created  time.Time         `json:"created_at"`

	Message          *string `json:"message"`
	legacyIncomplete bool
	cancelResult     *StatementCancelResult
	// Error is set when the statement failed with structured details.
	Error *StatementErrorDetails `json:"error"`

	// ResultSet is set when the statement was successfully finished.
	ResultSet *resultSet `json:"result_set"`
}

func (s *Statement) request() (*statementRequest, error) {
	if s.MaxElapsed < 0 || s.MaxEstimated < 0 {
		return nil, newError(ErrorKindConfigInvalid, "statement limits must not be negative", nil)
	}
	request := &statementRequest{
		StatementID:         s.ID,
		Statement:           s.stmt,
		MaxElapsedSeconds:   s.MaxElapsed.Seconds(),
		MaxEstimatedSeconds: s.MaxEstimated.Seconds(),
		Format:              resultFormatJSON,
	}
	if s.ExecTimeout != "" && s.MaxElapsed != 0 {
		return nil, newError(ErrorKindConfigInvalid, "ExecTimeout and MaxElapsed cannot be specified together", nil)
	}
	request.ExecTimeout = s.ExecTimeout
	return request, nil
}

func adaptLegacyCancel(response *statementResponse, data []byte, cancel bool) {
	if !cancel || response.Message == nil || response.Error != nil || response.ResultSet != nil {
		return
	}
	var envelope struct {
		Progress json.RawMessage `json:"progress"`
	}
	if json.Unmarshal(data, &envelope) == nil && len(envelope.Progress) == 0 {
		response.legacyIncomplete = true
	}
}

func (h *StatementHandle) resultResponse(ctx context.Context) (*statementResponse, error) {
	if _, err := h.Status(ctx); err != nil {
		return nil, err
	}
	if !h.resp.legacyIncomplete || (h.resp.Status != StatementStatusFinished && h.resp.Status != StatementStatusFailed) {
		return h.resp, nil
	}
	response, err := h.c.fetchStatementResult(ctx, h.id)
	if err != nil {
		return nil, err
	}
	if !response.Status.Terminated() {
		return nil, newError(ErrorKindUnexpected, fmt.Sprintf("statement cancel reported %s but fetched status is %s", h.resp.Status, response.Status), nil)
	}
	response.cancelResult = h.resp.cancelResult
	h.resp = response
	return response, nil
}

func (r *statementResponse) failure() *Error {
	if details := r.Error; details != nil {
		return &Error{
			Kind:             ErrorKindStatementFailed,
			Code:             string(details.Code),
			Message:          details.Message,
			StatementDetails: details,
		}
	}
	return &Error{Kind: ErrorKindStatementFailed, Message: legacyStatementMessage(r)}
}

func legacyStatementMessage(response *statementResponse) string {
	if response.Message != nil && *response.Message != "" {
		return *response.Message
	}
	return fmt.Sprintf("statement is %s", response.Status)
}

// StatementCancelResult reports the server's complete cancellation outcome.
type StatementCancelResult struct {
	StatementID uuid.UUID       `json:"statement_id"`
	CreatedAt   time.Time       `json:"created_at"`
	Status      StatementStatus `json:"status"`
	Message     string          `json:"message"`
}

// Cancel cancels the statement if it is running or pending.
func (h *StatementHandle) Cancel(ctx context.Context) (StatementCancelResult, error) {
	if h.resp == nil || !h.resp.Status.Terminated() {
		resp, err := h.c.cancelStatement(ctx, h.id)
		if err != nil {
			return StatementCancelResult{}, err
		}
		// TODO(legacy): ScopeDB 0.5 cancellation responses omit progress.
		if resp.legacyIncomplete && h.resp != nil {
			resp.Progress = h.resp.Progress
		}
		h.resp = resp
	}
	if h.resp.cancelResult != nil {
		return *h.resp.cancelResult, nil
	}
	message := legacyStatementMessage(h.resp)
	if h.resp.Error != nil {
		message = h.resp.Error.Message
	}
	result := StatementCancelResult{
		StatementID: h.id,
		CreatedAt:   h.resp.Created,
		Status:      h.resp.Status,
		Message:     message,
	}
	if h.resp.Status.Terminated() {
		h.resp.cancelResult = &result
	}
	return result, nil
}

const (
	// StatementErrorCodePrepareError indicates that statement preparation failed.
	StatementErrorCodePrepareError StatementErrorCode = "prepare_error"
	// StatementErrorCodeExecuteError indicates that statement execution failed.
	StatementErrorCodeExecuteError StatementErrorCode = "execute_error"
	// StatementErrorCodePendingTimeout indicates that the statement timed out before execution.
	StatementErrorCodePendingTimeout StatementErrorCode = "pending_timeout"
	// StatementErrorCodeExecutionTimeout indicates that the statement exceeded its execution timeout.
	StatementErrorCodeExecutionTimeout StatementErrorCode = "execution_timeout"
	// StatementErrorCodeHeartbeatLost indicates that the statement worker stopped reporting progress.
	StatementErrorCodeHeartbeatLost StatementErrorCode = "heartbeat_lost"
	// StatementErrorCodeRowLimitExceeded indicates that the statement exceeded a server-enforced row limit.
	StatementErrorCodeRowLimitExceeded StatementErrorCode = "row_limit_exceeded"
	// StatementErrorCodeScanLimitExceeded indicates that the statement exceeded a server-enforced scan limit.
	StatementErrorCodeScanLimitExceeded StatementErrorCode = "scan_limit_exceeded"
)

func validateLegacyStatementError(statementError *StatementErrorDetails) error {
	if statementError.Code == StatementErrorCodeRowLimitExceeded {
		return validateStatementLimitDetails(statementError.Details, "total_rows", "max_total_rows")
	}
	if statementError.Code == StatementErrorCodeScanLimitExceeded {
		return validateStatementLimitDetails(statementError.Details, "scanned_uncompressed_bytes", "max_scanned_uncompressed_bytes")
	}
	return nil
}

func validateStatementLimitDetails(details json.RawMessage, fields ...string) error {
	if len(details) == 0 {
		return fmt.Errorf("failed statement error has no details")
	}

	var object map[string]json.RawMessage
	if err := json.Unmarshal(details, &object); err != nil || object == nil {
		return fmt.Errorf("failed statement error details must be an object")
	}
	for _, field := range fields {
		raw, ok := object[field]
		if !ok {
			return fmt.Errorf("failed statement error details has no %s", field)
		}
		var value *uint64
		if err := json.Unmarshal(raw, &value); err != nil || value == nil {
			return fmt.Errorf("failed statement error detail %s must be an unsigned integer", field)
		}
	}
	return nil
}

func checkStatementResponse(resp *http.Response, cancel bool) (*statementResponse, error) {
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		return nil, responseMetadataError(
			resp,
			ErrorKindUnexpected,
			"failed to read statement response",
			err,
		)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, responseError(resp, data, ErrorKindUnexpected)
	}

	var stmtResp statementResponse
	if err := json.Unmarshal(data, &stmtResp); err != nil {
		return nil, responseMetadataError(
			resp,
			ErrorKindUnexpected,
			"failed to decode statement response",
			err,
		)
	}
	if stmtResp.Status == "" {
		return nil, responseMetadataError(
			resp,
			ErrorKindUnexpected,
			"statement response is missing status",
			nil,
		)
	}
	// TODO(legacy): Remove recognition of ScopeDB 0.5 cancellation envelopes.
	adaptLegacyCancel(&stmtResp, data, cancel)
	if err := validateStatementResponse(&stmtResp); err != nil {
		return nil, responseMetadataError(
			resp,
			ErrorKindUnexpected,
			"invalid statement response: "+err.Error(),
			err,
		)
	}
	return &stmtResp, nil
}

func validateStatementResponse(resp *statementResponse) error {
	if resp.ID == uuid.Nil {
		return fmt.Errorf("statement_id is missing")
	}
	if resp.Created.IsZero() {
		return fmt.Errorf("created_at is missing")
	}
	switch resp.Status {
	case StatementStatusPending,
		StatementStatusRunning,
		StatementStatusCancelled:
		return nil
	case StatementStatusFailed:
		if resp.Error == nil {
			// TODO(legacy): ScopeDB 0.5 may supply only a top-level failure message.
			if resp.Message != nil && *resp.Message != "" {
				return nil
			}
			return fmt.Errorf("failed statement has no error")
		}
		return validateStatementError(resp.Error)
	case StatementStatusFinished:
		// TODO(legacy): ScopeDB 0.5 cancellation requires a result fetch.
		if resp.legacyIncomplete {
			return nil
		}
		return validateFinishedResult(resp.ResultSet)
	default:
		return fmt.Errorf("unknown status %q", resp.Status)
	}
}

func validateStatementError(statementError *StatementErrorDetails) error {
	if statementError.Code == "" {
		return fmt.Errorf("failed statement error has no code")
	}
	if statementError.Message == "" {
		return fmt.Errorf("failed statement error has no message")
	}

	// TODO(legacy): Remove validation of retired row and scan limit errors with 0.5 support.
	return validateLegacyStatementError(statementError)
}
