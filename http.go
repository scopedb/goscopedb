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
	"net"
	"net/http"
	"net/url"
	"strconv"
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

type apiErrorPayload struct {
	Code      string           `json:"code"`
	Message   string           `json:"message"`
	Details   json.RawMessage  `json:"details"`
	RequestID string           `json:"request_id"`
	Retryable *bool            `json:"retryable"`
	Nested    *apiErrorPayload `json:"error"`
}

func decodeResponseError(resp *http.Response, body []byte, kind ErrorKind) *Error {
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

	err := newResponseError(resp, kind, message, nil)
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
		if kind == ErrorKindAppendRowsFailed && json.Unmarshal(payload.Details, &details) == nil &&
			(details.Status == AppendStateRejected || details.Status == AppendStateUnknown) {
			if details.RowErrors == nil {
				details.RowErrors = []AppendRowError{}
			}
			err.AppendDetails = &details
		}
	}
	return err
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
