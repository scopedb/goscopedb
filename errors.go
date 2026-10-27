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
	"time"

	"github.com/google/uuid"
)

// ErrResponseTooLarge means an HTTP error body or Append acknowledgement
// exceeded the 64 KiB read limit. The oversized body is not retained.
var ErrResponseTooLarge = errors.New("response body exceeds the 64 KiB limit")

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

// WriteErrorReason identifies why a write attempt was interrupted.
type WriteErrorReason string

const (
	// WriteErrorReasonTimeout means a client or transport deadline expired.
	WriteErrorReasonTimeout WriteErrorReason = "timeout"
	// WriteErrorReasonCancelled means the caller cancelled an in-flight write.
	WriteErrorReasonCancelled WriteErrorReason = "cancelled"
)

// WriteErrorDetails describes an interrupted write. An unknown outcome may have
// committed; replay requires explicit acceptance of possible duplicates.
type WriteErrorDetails struct {
	Reason  WriteErrorReason `json:"reason"`
	Outcome AppendState      `json:"outcome"`
}

// Error represents a ScopeDB client or API error.
type Error struct {
	// Details preserves code-specific HTTP or statement error information from the server.
	Details json.RawMessage
	Kind    ErrorKind
	// Code is the server error code, when provided, or an SDK outcome-unknown
	// code when a Statement submission or Catalog mutation could not be confirmed.
	// Unknown server codes are retained.
	Code       string
	Message    string
	HTTPStatus int
	RequestID  string
	RetryAfter time.Duration
	// Retryable classifies temporary failures. After successful submission,
	// Query and Execute return false because replay would submit another statement.
	Retryable     bool
	AppendDetails *AppendErrorDetails
	// WriteDetails identifies a client or transport timeout or cancellation.
	// Outcome distinguishes an unsent append from a potentially committed write.
	// Existing error kinds, causes, and HTTP diagnostics remain available.
	WriteDetails *WriteErrorDetails
	// StatementID identifies the statement being submitted, inspected, or waited
	// for, when known. It does not prove acceptance or continued result retention.
	StatementID *uuid.UUID
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
