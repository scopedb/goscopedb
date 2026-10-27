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
	"time"

	"github.com/google/uuid"
)

const statementResultFormat = "json"

type statementRequest struct {
	StatementID         *uuid.UUID `json:"statement_id,omitempty"`
	Statement           string     `json:"statement"`
	MaxElapsedSeconds   float64    `json:"max_elapsed_seconds,omitempty"`
	MaxEstimatedSeconds float64    `json:"max_estimated_seconds,omitempty"`
	Format              string     `json:"format"`
}

type statementResponse struct {
	ID       uuid.UUID         `json:"statement_id"`
	Progress StatementProgress `json:"progress"`
	Status   StatementStatus   `json:"status"`
	Created  time.Time         `json:"created_at"`

	// Error is required when the statement failed.
	Error *StatementErrorDetails `json:"error"`

	// ResultSet is set when the statement was successfully finished.
	ResultSet *statementResult `json:"result_set"`
}

type statementResult struct {
	Metadata *statementResultMetadata `json:"metadata"`
	Format   string                   `json:"format"`
	Rows     json.RawMessage          `json:"rows"`
}

type statementResultMetadata struct {
	Fields  []*statementResultField `json:"fields"`
	NumRows uint64                  `json:"num_rows"`
}

type statementResultField struct {
	Name     string   `json:"name"`
	DataType DataType `json:"data_type"`
}

func (rs *statementResult) snapshot() *ResultSet {
	if rs == nil || rs.Metadata == nil {
		return nil
	}
	schema := make(Schema, len(rs.Metadata.Fields))
	for i, field := range rs.Metadata.Fields {
		if field == nil {
			return nil
		}
		schema[i] = &FieldSchema{
			Name: field.Name,
			Type: field.DataType,
		}
	}

	return &ResultSet{
		TotalRows: rs.Metadata.NumRows,
		Schema:    schema,
		rows:      rs.Rows,
	}
}

func (c *Client) submitStatement(ctx context.Context, request *statementRequest) (*statementResponse, error) {
	req, err := c.resourceURL("statements")
	if err != nil {
		return nil, err
	}

	body, err := json.Marshal(request)
	if err != nil {
		return nil, err
	}

	if err := ctx.Err(); err != nil {
		return nil, err
	}
	resp, err := c.postJSON(ctx, req, body)
	if err != nil {
		if ctx.Err() != nil {
			return nil, err
		}
		failure := statementError(err, request.StatementID)
		failure.Code = "statement_submission_outcome_unknown"
		failure.Retryable = false
		return nil, failure
	}
	defer closeResponseBody(resp.Body)
	result, err := readStatementResponse(resp)
	if err != nil {
		failure := statementError(err, request.StatementID)
		// A malformed acknowledgement or unstructured gateway error does not
		// establish rejection. Replaying a mutating statement may run it twice.
		if (resp.StatusCode >= 200 && resp.StatusCode < 300) ||
			(failure.Code == "" && (resp.StatusCode == http.StatusRequestTimeout ||
				resp.StatusCode == http.StatusTooManyRequests || resp.StatusCode >= 500)) {
			failure.Code = "statement_submission_outcome_unknown"
			failure.Retryable = false
		}
		return nil, failure
	}
	return result, nil
}

func (c *Client) fetchStatement(ctx context.Context, id uuid.UUID) (*statementResponse, error) {
	req, err := c.resourceURL("statements", id.String())
	if err != nil {
		return nil, err
	}

	q := req.Query()
	q.Set("format", statementResultFormat)
	req.RawQuery = q.Encode()

	request, err := http.NewRequestWithContext(ctx, http.MethodGet, req.String(), nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.do(request)
	if err != nil {
		return nil, err
	}
	defer closeResponseBody(resp.Body)
	return readStatementResponse(resp)
}

func (c *Client) cancelStatement(ctx context.Context, statementID uuid.UUID) (*statementResponse, error) {
	req, err := c.resourceURL("statements", statementID.String(), "cancel")
	if err != nil {
		return nil, err
	}

	q := req.Query()
	q.Set("format", statementResultFormat)
	req.RawQuery = q.Encode()

	resp, err := c.postJSON(ctx, req, []byte{})
	if err != nil {
		return nil, err
	}
	defer closeResponseBody(resp.Body)
	return readStatementResponse(resp)
}

func readStatementResponse(resp *http.Response) (*statementResponse, error) {
	maxBytes := 0
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		maxBytes = maxDiagnosticBodyBytes
	}
	data, err := readResponseBody(resp.Body, maxBytes)
	if err != nil {
		return nil, newResponseError(
			resp,
			ErrorKindUnexpected,
			"failed to read statement response",
			err,
		)
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, decodeResponseError(resp, data, ErrorKindUnexpected)
	}

	var stmtResp statementResponse
	if err := json.Unmarshal(data, &stmtResp); err != nil {
		return nil, newResponseError(
			resp,
			ErrorKindUnexpected,
			"failed to decode statement response",
			err,
		)
	}
	if stmtResp.Status == "" {
		return nil, newResponseError(
			resp,
			ErrorKindUnexpected,
			"statement response is missing status",
			nil,
		)
	}
	if err := validateStatementResponse(&stmtResp); err != nil {
		return nil, newResponseError(
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

func validateFinishedResult(result *statementResult) error {
	if result == nil {
		return fmt.Errorf("finished statement has no result_set")
	}
	if result.Metadata == nil {
		return fmt.Errorf("finished statement result_set has no metadata")
	}
	if result.Format != statementResultFormat {
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
