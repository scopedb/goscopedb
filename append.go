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
	"bytes"
	"context"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptrace"
	"sync/atomic"
	"time"
)

const (
	maxAppendBodyBytes = 8 * 1024 * 1024
	maxAppendRows      = 200_000
)

// AppendOutcome describes a write's commit outcome. Table append responses and
// client-side interruption details use these values. String values retain the
// historical JSON spellings committed, rejected, and unknown.
type AppendOutcome string

const (
	// AppendOutcomeSucceeded means all reported rows committed.
	AppendOutcomeSucceeded AppendOutcome = "committed"
	// AppendOutcomeFailed means no rows committed and the request is safe to correct and retry.
	AppendOutcomeFailed AppendOutcome = "rejected"
	// AppendOutcomeAmbiguous means the client cannot determine whether rows committed.
	AppendOutcomeAmbiguous AppendOutcome = "unknown"
)

// AppendRowsResult is the result of a committed table append.
type AppendRowsResult struct {
	Status          AppendOutcome `json:"status"`
	NumRowsInserted int64         `json:"num_rows_inserted"`
}

type appendRowsResponse struct {
	Status          json.RawMessage `json:"status"`
	NumRowsInserted *int64          `json:"num_rows_inserted"`
}

// AppendRowError preserves optional legacy per-row diagnostics. Modern errors
// describe the whole request; do not parse messages to infer row outcomes.
type AppendRowError struct {
	RowIndex uint64 `json:"row_index"`
	Column   string `json:"column"`
	Message  string `json:"message"`
}

// AppendErrorDetails contains the structured outcome of a failed append.
type AppendErrorDetails struct {
	Status             AppendOutcome    `json:"status"`
	RowErrors          []AppendRowError `json:"row_errors"`
	RowErrorsTruncated bool             `json:"row_errors_truncated"`
}

func (c *Client) appendNDJSON(
	ctx context.Context,
	database string,
	schema string,
	table string,
	ndjson []byte,
	timeout time.Duration,
) (AppendRowsResult, error) {
	if err := ctx.Err(); err != nil {
		return AppendRowsResult{}, err
	}
	if len(ndjson) > maxAppendBodyBytes {
		return AppendRowsResult{}, appendDefiniteFailure(fmt.Sprintf(
			"append payload exceeds the %d-byte limit",
			maxAppendBodyBytes,
		))
	}

	numRows := countAppendRows(ndjson)
	if numRows > maxAppendRows {
		return AppendRowsResult{}, appendDefiniteFailure(fmt.Sprintf(
			"append payload exceeds the %d-row limit",
			maxAppendRows,
		))
	}

	u, err := c.resourceURL(
		"catalog",
		"databases",
		database,
		"schemas",
		schema,
		"tables",
		table,
		"ingest",
	)
	if err != nil {
		return AppendRowsResult{}, err
	}
	compressedBody, err := compressRequestBody(ndjson, c.compression)
	if err != nil {
		return AppendRowsResult{}, newError(
			ErrorKindUnexpected,
			"failed to compress table append request body",
			err,
		)
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.String(), &compressedBody)
	if err != nil {
		return AppendRowsResult{}, newError(ErrorKindUnexpected, "failed to build table append request", err)
	}
	req.Header.Set("Content-Type", "application/x-ndjson")
	req.Header.Set("Content-Encoding", string(c.compression))
	if err := ctx.Err(); err != nil {
		return AppendRowsResult{}, err
	}
	if timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, timeout)
		defer cancel()
	}

	var acquiring, connected atomic.Bool
	ctx = httptrace.WithClientTrace(ctx, &httptrace.ClientTrace{
		GetConn: func(string) { acquiring.Store(true) },
		GotConn: func(httptrace.GotConnInfo) { connected.Store(true) },
	})
	resp, err := c.do(req.WithContext(ctx))
	if err != nil {
		apiErr := appendAmbiguousError(err.Error(), err, resp).withWriteContext(ctx)
		// Keep connection evidence across redirects. A later dial failure cannot
		// prove that an earlier endpoint did not process the append. Transports
		// without tracing also retain an unknown outcome.
		if acquiring.Load() && !connected.Load() {
			apiErr.AppendDetails.Status = AppendOutcomeFailed
			_, isCertErr := errors.AsType[*tls.CertificateVerificationError](err)
			apiErr.Retryable = !errors.Is(err, context.Canceled) && !isCertErr
			if apiErr.WriteDetails != nil {
				apiErr.WriteDetails.Outcome = AppendOutcomeFailed
			}
		}
		return AppendRowsResult{}, apiErr
	}
	defer closeResponseBody(resp.Body)

	responseBody, err := readResponseBody(resp.Body, maxDiagnosticBodyBytes)
	if err != nil {
		return AppendRowsResult{}, appendAmbiguousError(err.Error(), err, resp).withWriteContext(ctx)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return AppendRowsResult{}, c.redactError(decodeAppendError(resp, responseBody))
	}

	if resp.StatusCode != http.StatusOK {
		return AppendRowsResult{}, appendAmbiguousError("table append response must have HTTP status 200", nil, resp)
	}
	var response appendRowsResponse
	if err := json.Unmarshal(responseBody, &response); err != nil {
		return AppendRowsResult{}, appendAmbiguousError("failed to decode table append response", err, resp)
	}
	var status AppendOutcome
	if len(response.Status) != 0 && (json.Unmarshal(response.Status, &status) != nil || status != AppendOutcomeSucceeded) {
		return AppendRowsResult{}, appendAmbiguousError(
			"table append response did not confirm a committed outcome",
			nil,
			resp,
		)
	}
	if response.NumRowsInserted == nil {
		return AppendRowsResult{}, appendAmbiguousError(
			"table append response did not report the number of inserted rows",
			nil,
			resp,
		)
	}
	if *response.NumRowsInserted < 0 {
		return AppendRowsResult{}, appendAmbiguousError(
			"table append response reported a negative number of inserted rows",
			nil,
			resp,
		)
	}
	if *response.NumRowsInserted != int64(numRows) {
		return AppendRowsResult{}, appendAmbiguousError(fmt.Sprintf(
			"table append response reported %d inserted rows for a %d-row request",
			*response.NumRowsInserted,
			numRows,
		), nil, resp)
	}
	return AppendRowsResult{
		Status:          AppendOutcomeSucceeded,
		NumRowsInserted: *response.NumRowsInserted,
	}, nil
}

func countAppendRows(ndjson []byte) int {
	rows := 0
	for line := range bytes.Lines(ndjson) {
		if len(bytes.TrimSpace(line)) > 0 {
			rows++
		}
	}
	return rows
}

func appendDefiniteFailure(message string) *Error {
	return &Error{
		Kind:    ErrorKindAppendRowsFailed,
		Message: message,
		AppendDetails: &AppendErrorDetails{
			Status:    AppendOutcomeFailed,
			RowErrors: []AppendRowError{},
		},
	}
}

func appendAmbiguousError(message string, cause error, resp *http.Response) *Error {
	err := &Error{
		Kind:    ErrorKindAppendRowsFailed,
		Message: message,
		AppendDetails: &AppendErrorDetails{
			Status:    AppendOutcomeAmbiguous,
			RowErrors: []AppendRowError{},
		},
		WriteDetails: writeErrorDetails(cause),
		cause:        cause,
	}
	if resp != nil {
		err.HTTPStatus = resp.StatusCode
		err.RequestID = resp.Header.Get("X-Request-ID")
		err.RetryAfter = parseRetryAfter(resp.Header.Get("Retry-After"), time.Now())
	}
	return err
}

// decodeAppendError keeps certainty separate from status-based retry hints.
func decodeAppendError(resp *http.Response, body []byte) *Error {
	failure := decodeResponseError(resp, body, ErrorKindAppendRowsFailed)
	failure.AppendDetails = &AppendErrorDetails{Status: AppendOutcomeAmbiguous, RowErrors: []AppendRowError{}}
	payload := parseErrorEnvelope(resp, body)
	if payload.valid && !payload.modern {
		var details AppendErrorDetails
		if json.Unmarshal(payload.Details, &details) == nil &&
			(details.Status == AppendOutcomeFailed || details.Status == AppendOutcomeAmbiguous) {
			if details.RowErrors == nil {
				details.RowErrors = []AppendRowError{}
			}
			failure.AppendDetails = &details
		}
	}
	if payload.valid && resp.StatusCode < 500 && (payload.modern || failure.AppendDetails.Status == AppendOutcomeFailed) {
		failure.AppendDetails.Status = AppendOutcomeFailed
	} else {
		failure.AppendDetails.Status = AppendOutcomeAmbiguous
	}
	failure.Retryable = payload.valid && failure.AppendDetails.Status == AppendOutcomeFailed &&
		resp.StatusCode == http.StatusTooManyRequests && (payload.Retryable == nil || *payload.Retryable)
	return failure
}
