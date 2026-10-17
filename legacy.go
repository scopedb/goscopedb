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

// TODO(legacy): Remove these adapters after ScopeDB 0.5 and the deprecated Go API are retired.

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/google/uuid"
)

type legacyStatementResponse struct {
	Message          *string `json:"message"`
	legacyIncomplete bool
}

func applyLegacyTimeout(statement *Statement, request *statementRequest) error {
	if statement.ExecTimeout != "" && statement.MaxElapsed != 0 {
		return newError(ErrorKindConfigInvalid, "ExecTimeout and MaxElapsed cannot be specified together", nil)
	}
	request.ExecTimeout = statement.ExecTimeout
	return nil
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

func (h *StatementHandle) fetchLegacyResult(ctx context.Context) error {
	if !h.resp.legacyIncomplete || (h.resp.Status != StatementStatusFinished && h.resp.Status != StatementStatusFailed) {
		return nil
	}
	response, err := h.c.fetchStatementResult(ctx, h.id)
	if err != nil {
		return err
	}
	if !response.Status.Terminated() {
		return newError(ErrorKindUnexpected, fmt.Sprintf("statement cancel reported %s but fetched status is %s", h.resp.Status, response.Status), nil)
	}
	h.resp = response
	return nil
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

func (h *StatementHandle) cancelResultFromResponse() StatementCancelResult {
	message := legacyStatementMessage(h.resp)
	if h.resp.Error != nil {
		message = h.resp.Error.Message
	}
	return StatementCancelResult{
		StatementID: h.id,
		CreatedAt:   h.resp.Created,
		Status:      h.resp.Status,
		Message:     message,
	}
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
