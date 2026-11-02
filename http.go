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
	"cmp"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"
)

const (
	// Match net/http's total idle budget for clients targeting one endpoint.
	defaultMaxIdleConnsPerHost = 100
	maxDiagnosticBodyBytes     = 64 * 1024
)

func newDefaultTransport() *http.Transport {
	if transport, ok := http.DefaultTransport.(*http.Transport); ok && transport != nil {
		cloned := transport.Clone()
		if cloned.MaxIdleConnsPerHost == 0 {
			cloned.MaxIdleConnsPerHost = defaultMaxIdleConnsPerHost
		}
		return cloned
	}

	return &http.Transport{
		Proxy: http.ProxyFromEnvironment,
		DialContext: (&net.Dialer{
			Timeout:   30 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		MaxIdleConnsPerHost:   defaultMaxIdleConnsPerHost,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: time.Second,
	}
}

func (c *Client) do(req *http.Request) (*http.Response, error) {
	if c.authorization != "" {
		req.Header.Set("Authorization", c.authorization)
	}
	req.Header.Set("X-ScopeDB-Client", "goscopedb")
	return c.httpClient.Do(req)
}

func (c *Client) postJSON(ctx context.Context, endpoint *url.URL, body []byte) (*http.Response, error) {
	compressedBody, err := compressRequestBody(body, c.compression)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, endpoint.String(), &compressedBody)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Content-Encoding", string(c.compression))
	req.Header.Set("X-ScopeDB-Uncompressed-Content-Length", strconv.Itoa(len(body)))
	return c.do(req)
}

// A zero limit is reserved for successful query results and catalog definitions.
func readResponseBody(body io.Reader, maxBytes int) ([]byte, error) {
	if maxBytes == 0 {
		return io.ReadAll(body)
	}
	data, err := io.ReadAll(io.LimitReader(body, int64(maxBytes)+1))
	if len(data) > maxBytes {
		// A truncated JSON prefix must not establish a write's commit outcome.
		return nil, errors.Join(ErrResponseTooLarge, err)
	}
	return data, err
}

type decodedErrorEnvelope struct {
	Code      *string         `json:"code"`
	Message   *string         `json:"message"`
	RequestID string          `json:"request_id"`
	Retryable *bool           `json:"-"`
	Details   json.RawMessage `json:"details"`
	modern    bool
	parsed    bool
	valid     bool
}

func parseErrorEnvelope(resp *http.Response, body []byte) decodedErrorEnvelope {
	var envelope map[string]json.RawMessage
	parsedMap := json.Unmarshal(body, &envelope) == nil && envelope != nil
	nested, modern := envelope["error"]
	target := body
	fields := envelope
	if modern {
		target = nested
		fields = nil
		if json.Unmarshal(target, &fields) != nil {
			fields = nil
		}
	}
	var payload decodedErrorEnvelope
	payload.modern = modern
	payload.parsed = parsedMap && fields != nil &&
		json.Unmarshal(target, &payload) == nil
	// Retry refusal remains meaningful even when other diagnostic fields are invalid.
	var retryValid bool
	payload.Retryable, retryValid = decodeRetryHint(fields["retryable"])
	payload.parsed = payload.parsed && retryValid
	outerValid := true
	if modern && parsedMap {
		if rawRequestID, ok := envelope["request_id"]; ok {
			var requestID string
			if json.Unmarshal(rawRequestID, &requestID) == nil {
				payload.RequestID = cmp.Or(payload.RequestID, requestID)
			} else {
				outerValid = false
			}
		}
		if rawRetryable, ok := envelope["retryable"]; ok {
			if retryable, valid := decodeRetryHint(rawRetryable); valid {
				if payload.Retryable == nil || (retryable != nil && !*retryable) {
					payload.Retryable = retryable
				}
			} else {
				outerValid = false
			}
		}
	}
	payload.valid = payload.parsed && outerValid &&
		payload.Code != nil && *payload.Code != "" && payload.Message != nil &&
		resp.StatusCode >= 400 && resp.StatusCode < 600 &&
		validHTTPErrorCode(*payload.Code, resp.StatusCode, modern)
	return payload
}

func decodeRetryHint(raw json.RawMessage) (*bool, bool) {
	if len(raw) == 0 {
		return nil, true
	}
	var hint *bool
	if json.Unmarshal(raw, &hint) != nil {
		return nil, false
	}
	return hint, true
}

func validHTTPErrorCode(code string, status int, modern bool) bool {
	if code == "" {
		return false
	}
	var expected int
	switch code {
	case "bad_request":
		expected = 400
	case "not_found":
		expected = 404
	case "method_not_allowed":
		expected = 405
	case "request_timeout":
		expected = 408
	case "conflict":
		expected = 409
	case "precondition_failed":
		expected = 412
	case "too_many_requests":
		expected = 429
	case "internal_server_error":
		expected = 500
	case "service_unavailable":
		expected = 503
	case "statement_timeout":
		return false
	case "invalid_request", "unauthorized", "payload_too_large", "timeout", "unavailable", "internal_error":
		return !modern
	default:
		return true
	}
	return status == expected
}

func (c *Client) redactError(failure *Error) *Error {
	if failure == nil {
		return nil
	}
	key := strings.TrimPrefix(c.authorization, "Bearer ")
	if key != "" {
		failure.Message = strings.ReplaceAll(failure.Message, key, "[redacted]")
		failure.Code = strings.ReplaceAll(failure.Code, key, "[redacted]")
	}
	return failure
}

func decodeResponseError(resp *http.Response, body []byte, kind ErrorKind) (*Error, decodedErrorEnvelope) {
	payload := parseErrorEnvelope(resp, body)
	var message string
	if payload.parsed && payload.Message != nil {
		message = *payload.Message
	}
	message = cmp.Or(message, string(body), resp.Status, fmt.Sprintf("HTTP %d", resp.StatusCode))

	err := newResponseError(resp, kind, message, nil)
	if payload.parsed {
		if payload.Code != nil {
			err.Code = *payload.Code
		}
		err.RequestID = cmp.Or(payload.RequestID, err.RequestID)
		err.Details = payload.Details
	}
	if payload.Retryable != nil {
		err.ServerRetryable = payload.Retryable
		if payload.parsed || !*payload.Retryable {
			err.Retryable = *payload.Retryable
		}
	}
	return err, payload
}

func newResponseError(
	resp *http.Response,
	kind ErrorKind,
	message string,
	cause error,
) *Error {
	if errors.Is(cause, ErrResponseTooLarge) {
		message = ErrResponseTooLarge.Error()
	}
	if resp == nil {
		return newError(kind, message, cause)
	}
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
		const maxSeconds = int64((1<<63 - 1) / time.Second)
		return time.Duration(min(seconds, maxSeconds)) * time.Second
	}
	if retryAt, err := http.ParseTime(value); err == nil {
		if delay := retryAt.Sub(now); delay > 0 {
			return delay
		}
	}
	return 0
}

func closeResponseBody(body io.ReadCloser) {
	if body != nil {
		_ = body.Close()
	}
}
