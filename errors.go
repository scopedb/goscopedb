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
	"io"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"
)

// ErrorKind identifies the operation-level category of a ScopeDB error.
type ErrorKind string

const (
	// ErrorKindUnexpected is an error that has no more specific classification.
	ErrorKindUnexpected ErrorKind = "Unexpected"
	// ErrorKindConfigInvalid indicates invalid client configuration or arguments.
	ErrorKindConfigInvalid ErrorKind = "ConfigInvalid"
	// ErrorKindStatementFailed indicates a failed or cancelled statement.
	ErrorKindStatementFailed ErrorKind = "StatementFailed"
	// ErrorKindCatalogFailed indicates a catalog request failure or unknown commit outcome.
	ErrorKindCatalogFailed ErrorKind = "CatalogFailed"
	// ErrorKindAppendRowsFailed indicates a rejected append or an unknown commit outcome.
	ErrorKindAppendRowsFailed ErrorKind = "AppendRowsFailed"
)

// WriteErrorReason identifies why an in-flight write was interrupted.
type WriteErrorReason string

const (
	// WriteErrorReasonTimeout means a client or transport deadline expired.
	WriteErrorReasonTimeout WriteErrorReason = "timeout"
	// WriteErrorReasonCancelled means the caller cancelled an in-flight write.
	WriteErrorReasonCancelled WriteErrorReason = "cancelled"
)

// WriteErrorDetails describes an interrupted write. An unknown outcome may have
// committed and must not be automatically replayed.
type WriteErrorDetails struct {
	Reason  WriteErrorReason `json:"reason"`
	Outcome AppendState      `json:"outcome"`
}

// Error represents a ScopeDB client or API error.
type Error struct {
	// Details preserves code-specific HTTP or statement error information from the server.
	Details json.RawMessage
	Kind    ErrorKind
	// Code is the server error code, when provided, or catalog_commit_outcome_unknown
	// when a catalog mutation could not be confirmed. Unknown codes are retained.
	Code          string
	Message       string
	HTTPStatus    int
	RequestID     string
	RetryAfter    time.Duration
	Retryable     bool
	AppendDetails *AppendErrorDetails
	// WriteDetails identifies a client timeout or cancellation after sending began.
	// Existing error kinds, causes, and HTTP diagnostics remain available.
	WriteDetails *WriteErrorDetails
	// StatementDetails contains the structured server failure for a failed
	// statement, when available.
	StatementDetails *StatementErrorDetails

	cause error
}

// Error returns the human-readable error message.
func (e *Error) Error() string {
	return e.Message
}

// Unwrap returns the transport, decoding, or configuration error that caused this error.
func (e *Error) Unwrap() error {
	return e.cause
}

func newError(kind ErrorKind, message string, cause error) *Error {
	return &Error{Kind: kind, Message: message, cause: cause}
}

func writeErrorDetails(cause error) *WriteErrorDetails {
	var timeoutErr interface{ Timeout() bool }
	var reason WriteErrorReason
	switch {
	case errors.Is(cause, context.DeadlineExceeded) ||
		(errors.As(cause, &timeoutErr) && timeoutErr.Timeout()):
		reason = WriteErrorReasonTimeout
	case errors.Is(cause, context.Canceled):
		reason = WriteErrorReasonCancelled
	default:
		return nil
	}
	return &WriteErrorDetails{Reason: reason, Outcome: AppendStateUnknown}
}

func (e *Error) withWriteContext(ctx context.Context) *Error {
	if err := ctx.Err(); err != nil {
		e.WriteDetails = writeErrorDetails(err)
	}
	return e
}

type apiErrorPayload struct {
	Code      string           `json:"code"`
	Message   string           `json:"message"`
	Details   json.RawMessage  `json:"details"`
	RequestID string           `json:"request_id"`
	Retryable *bool            `json:"retryable"`
	Nested    *apiErrorPayload `json:"error"`
}

func responseError(resp *http.Response, body []byte, kind ErrorKind) *Error {
	message := string(body)
	if message == "" {
		message = resp.Status
		if message == "" {
			message = fmt.Sprintf("HTTP %d", resp.StatusCode)
		}
	}

	var payload apiErrorPayload
	parsed := json.Unmarshal(body, &payload) == nil
	if parsed && payload.Message == "" && payload.Nested != nil {
		nested := *payload.Nested
		if nested.RequestID == "" {
			nested.RequestID = payload.RequestID
		}
		if nested.Retryable == nil {
			nested.Retryable = payload.Retryable
		}
		payload = nested
	}
	if parsed && payload.Message != "" {
		message = payload.Message
	}

	err := &Error{
		Kind:       kind,
		Message:    message,
		HTTPStatus: resp.StatusCode,
		RequestID:  resp.Header.Get("X-Request-ID"),
		RetryAfter: parseRetryAfter(resp.Header.Get("Retry-After"), time.Now()),
		Retryable: resp.StatusCode == http.StatusRequestTimeout ||
			resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500,
	}
	if parsed {
		err.Code = payload.Code
		if payload.RequestID != "" {
			err.RequestID = payload.RequestID
		}
		if payload.Retryable != nil {
			err.Retryable = *payload.Retryable
		}
		err.Details = payload.Details
		var details AppendErrorDetails
		if json.Unmarshal(payload.Details, &details) == nil &&
			(details.AppendState == AppendStateRejected || details.AppendState == AppendStateUnknown) {
			if details.RowErrors == nil {
				details.RowErrors = []AppendRowError{}
			}
			err.AppendDetails = &details
		}
	}
	return err
}

func responseMetadataError(
	resp *http.Response,
	kind ErrorKind,
	message string,
	cause error,
) *Error {
	return &Error{
		Kind:       kind,
		Message:    message,
		HTTPStatus: resp.StatusCode,
		RequestID:  resp.Header.Get("X-Request-ID"),
		RetryAfter: parseRetryAfter(resp.Header.Get("Retry-After"), time.Now()),
		Retryable: resp.StatusCode == http.StatusRequestTimeout ||
			resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500,
		cause: cause,
	}
}

func parseRetryAfter(value string, now time.Time) time.Duration {
	if value == "" {
		return 0
	}
	if seconds, err := strconv.ParseInt(value, 10, 64); err == nil && seconds >= 0 {
		return time.Duration(seconds) * time.Second
	}
	if retryAt, err := http.ParseTime(value); err == nil {
		if delay := retryAt.Sub(now); delay > 0 {
			return delay
		}
	}
	return 0
}

func checkStatementResponse(resp *http.Response) (*statementResponse, error) {
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
			return fmt.Errorf("failed statement has no error")
		}
		if resp.Error.Code == "" {
			return fmt.Errorf("failed statement error has no code")
		}
		if resp.Error.Message == "" {
			return fmt.Errorf("failed statement error has no message")
		}
		return nil
	case StatementStatusFinished:
		return validateFinishedResult(resp.ResultSet)
	default:
		return fmt.Errorf("unknown status %q", resp.Status)
	}
}

func validateFinishedResult(result *resultSet) error {
	if result == nil {
		return fmt.Errorf("finished statement has no result_set")
	}
	if result.Metadata == nil {
		return fmt.Errorf("finished statement result_set has no metadata")
	}
	if result.Format != resultFormatJSON {
		return fmt.Errorf("unsupported result format %q", result.Format)
	}
	if result.Metadata.Fields == nil {
		return fmt.Errorf("finished statement result_set metadata has no fields")
	}
	for i, field := range result.Metadata.Fields {
		if field == nil {
			return fmt.Errorf("finished statement result_set field %d is null", i)
		}
		if !field.DataType.valid() {
			return fmt.Errorf(
				"finished statement result_set field %d has invalid data_type %q",
				i,
				field.DataType,
			)
		}
	}

	return nil
}

// sneakyBodyClose closes the body and ignores the error.
func sneakyBodyClose(body io.ReadCloser) {
	if body != nil {
		_ = body.Close()
	}
}
