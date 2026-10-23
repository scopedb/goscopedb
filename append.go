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
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"time"
)

const (
	maxAppendBodyBytes = 8 * 1024 * 1024
	maxAppendRows      = 200_000
)

// AppendState describes a write's commit outcome. Table append responses and
// client-side interruption details use these values.
type AppendState string

const (
	// AppendStateCommitted means all reported rows committed.
	AppendStateCommitted AppendState = "committed"
	// AppendStateRejected means no rows committed and the request is safe to correct and retry.
	AppendStateRejected AppendState = "rejected"
	// AppendStateUnknown means the client cannot determine whether rows committed.
	AppendStateUnknown AppendState = "unknown"
)

// AppendRowsResult is the result of a committed table append.
type AppendRowsResult struct {
	Status          AppendState `json:"status"`
	NumRowsInserted int64       `json:"num_rows_inserted"`
}

type appendRowsResponse struct {
	Status          AppendState `json:"status"`
	NumRowsInserted *int64      `json:"num_rows_inserted"`
}

// AppendRowError describes a validation error for one row in an append request.
type AppendRowError struct {
	RowIndex uint64 `json:"row_index"`
	Column   string `json:"column"`
	Message  string `json:"message"`
}

// AppendErrorDetails contains the structured outcome of a failed append.
type AppendErrorDetails struct {
	Status             AppendState      `json:"status"`
	RowErrors          []AppendRowError `json:"row_errors"`
	RowErrorsTruncated bool             `json:"row_errors_truncated"`
}

func (c *Client) appendNDJSON(
	ctx context.Context,
	database string,
	schema string,
	table string,
	ndjson []byte,
) (AppendRowsResult, error) {
	return c.appendNDJSONWithTimeout(ctx, database, schema, table, ndjson, c.writeTimeout)
}

func (c *Client) appendNDJSONWithTimeout(
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
		return AppendRowsResult{}, appendRejectedError(fmt.Sprintf(
			"append payload exceeds the %d-byte limit",
			maxAppendBodyBytes,
		))
	}

	numRows := countAppendRows(ndjson)
	if numRows > maxAppendRows {
		return AppendRowsResult{}, appendRejectedError(fmt.Sprintf(
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
	compressedBody, contentEncoding, err := compressRequestBody(ndjson, c.http.compression)
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
	req.Header.Set("Content-Encoding", string(contentEncoding))
	if err := ctx.Err(); err != nil {
		return AppendRowsResult{}, err
	}
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	resp, err := c.http.do(req.WithContext(ctx))
	if err != nil {
		return AppendRowsResult{}, appendUnknownError(err.Error(), err, nil).withWriteContext(ctx)
	}
	defer sneakyBodyClose(resp.Body)

	responseBody, err := io.ReadAll(resp.Body)
	if err != nil {
		return AppendRowsResult{}, appendUnknownError(err.Error(), err, resp).withWriteContext(ctx)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		apiErr := responseError(resp, responseBody, ErrorKindAppendRowsFailed)
		if apiErr.AppendDetails == nil {
			apiErr.AppendDetails = &AppendErrorDetails{
				Status:    AppendStateUnknown,
				RowErrors: []AppendRowError{},
			}
		}
		if apiErr.AppendDetails.Status == AppendStateUnknown {
			apiErr.Retryable = false
		}
		return AppendRowsResult{}, apiErr
	}

	var response appendRowsResponse
	if err := json.Unmarshal(responseBody, &response); err != nil {
		return AppendRowsResult{}, appendUnknownError("failed to decode table append response", err, resp)
	}
	if response.Status != AppendStateCommitted {
		return AppendRowsResult{}, appendUnknownError(
			"table append response did not confirm a committed outcome",
			nil,
			resp,
		)
	}
	if response.NumRowsInserted == nil {
		return AppendRowsResult{}, appendUnknownError(
			"table append response did not report the number of inserted rows",
			nil,
			resp,
		)
	}
	if *response.NumRowsInserted < 0 {
		return AppendRowsResult{}, appendUnknownError(
			"table append response reported a negative number of inserted rows",
			nil,
			resp,
		)
	}
	if *response.NumRowsInserted != int64(numRows) {
		return AppendRowsResult{}, appendUnknownError(fmt.Sprintf(
			"table append response reported %d inserted rows for a %d-row request",
			*response.NumRowsInserted,
			numRows,
		), nil, resp)
	}
	return AppendRowsResult{
		Status:          response.Status,
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

func appendRejectedError(message string) *Error {
	return &Error{
		Kind:    ErrorKindAppendRowsFailed,
		Message: message,
		AppendDetails: &AppendErrorDetails{
			Status:    AppendStateRejected,
			RowErrors: []AppendRowError{},
		},
	}
}

func appendUnknownError(message string, cause error, resp *http.Response) *Error {
	err := &Error{
		Kind:    ErrorKindAppendRowsFailed,
		Message: message,
		AppendDetails: &AppendErrorDetails{
			Status:    AppendStateUnknown,
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
