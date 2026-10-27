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
	"net/http"
	"strings"
)

// CatalogResource manages a selected catalog definition. Obtain one through the
// Catalog resource selectors; T is a definition pointer, or json.RawMessage for Raw.
type CatalogResource[T any] struct {
	client   *Client
	resource []string
}

// Raw selects the same resource as JSON, preserving fields unknown to this SDK.
func (c *CatalogResource[T]) Raw() *CatalogResource[json.RawMessage] {
	return &CatalogResource[json.RawMessage]{client: c.client, resource: c.resource}
}

// Get returns a complete definition, without pagination. Typed definitions reject
// unknown fields to prevent lossy read-modify-write cycles; use Raw to retain them.
func (c *CatalogResource[T]) Get(ctx context.Context) (CatalogSnapshot[T], error) {
	data, resp, err := c.request(ctx, http.MethodGet, nil, CatalogOptions{})
	if err != nil {
		return CatalogSnapshot[T]{}, err
	}
	trimmed := bytes.TrimSpace(data)
	if len(trimmed) == 0 || trimmed[0] != '{' || resp.Header.Get("ETag") == "" {
		return CatalogSnapshot[T]{}, newResponseError(resp, ErrorKindCatalogFailed, "invalid catalog response", nil)
	}
	var definition T
	if err := decodeCatalogDefinition(data, &definition); err != nil {
		return CatalogSnapshot[T]{}, newResponseError(resp, ErrorKindCatalogFailed, err.Error(), err)
	}
	return CatalogSnapshot[T]{Definition: definition, ETag: resp.Header.Get("ETag")}, nil
}

// Apply replaces the resource with definition. Omitted fields and objects are
// cleared. With no options, changes apply without an ETag precondition and
// potentially destructive changes are rejected. DryRun validates and rolls back.
// Catalog writes are never automatically retried.
func (c *CatalogResource[T]) Apply(ctx context.Context, definition T, options ...CatalogOptions) (CatalogResult[T], error) {
	data, err := json.Marshal(definition)
	if err != nil {
		return CatalogResult[T]{}, newError(ErrorKindConfigInvalid, "invalid catalog definition", err)
	}
	return c.mutate(ctx, http.MethodPut, data, options...)
}

// Delete removes a single database, schema, table, or view.
// With no options, potentially destructive changes are rejected.
func (c *CatalogResource[T]) Delete(ctx context.Context, options ...CatalogOptions) (CatalogResult[T], error) {
	return c.mutate(ctx, http.MethodDelete, nil, options...)
}

func (c *CatalogResource[T]) mutate(ctx context.Context, method string, data []byte, options ...CatalogOptions) (CatalogResult[T], error) {
	if len(options) > 1 {
		return CatalogResult[T]{}, newError(ErrorKindConfigInvalid, "expected at most one CatalogOptions value", nil)
	}
	var controls CatalogOptions
	if len(options) == 1 {
		controls = options[0]
	}
	data, resp, err := c.request(ctx, method, data, controls)
	if err != nil {
		return CatalogResult[T]{}, err
	}
	var raw struct {
		Before      json.RawMessage `json:"before"`
		After       json.RawMessage `json:"after"`
		Destructive *bool           `json:"destructive"`
	}
	err = json.Unmarshal(data, &raw)
	etag := resp.Header.Get("ETag")
	if err != nil || len(raw.Before) == 0 || len(raw.After) == 0 || raw.Destructive == nil || etag == "" {
		failure := newResponseError(resp, ErrorKindCatalogFailed, "invalid catalog response", err)
		if !controls.DryRun {
			failure.Code = "catalog_commit_outcome_unknown"
		}
		return CatalogResult[T]{}, failure
	}
	result := CatalogResult[T]{Destructive: *raw.Destructive, ETag: etag}
	err = decodeCatalogDefinition(raw.Before, &result.Before)
	if err == nil {
		err = decodeCatalogDefinition(raw.After, &result.After)
	}
	if err != nil {
		failure := newResponseError(resp, ErrorKindCatalogFailed, err.Error(), err)
		if !controls.DryRun {
			failure.Code = "catalog_commit_outcome_unknown"
		}
		return CatalogResult[T]{}, failure
	}
	return result, nil
}

func decodeCatalogDefinition(data []byte, target any) error {
	data = bytes.TrimSpace(data)
	if !json.Valid(data) || (data[0] != '{' && !bytes.Equal(data, []byte("null"))) {
		return fmt.Errorf("catalog definition must be an object or null")
	}
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return fmt.Errorf("cannot decode complete catalog definition (use Raw for unsupported fields): %w", err)
	}
	return nil
}

func (c *CatalogResource[T]) request(ctx context.Context, method string, data []byte, options CatalogOptions) ([]byte, *http.Response, error) {
	endpoint, err := c.client.resourceURL(c.resource...)
	if err != nil {
		return nil, nil, err
	}
	query := endpoint.Query()
	if options.DryRun {
		query.Set("dry_run", "true")
	}
	if options.AllowDestructive {
		query.Set("allow_destructive", "true")
	}
	endpoint.RawQuery = query.Encode()
	req, err := http.NewRequestWithContext(ctx, method, endpoint.String(), bytes.NewReader(data))
	if err != nil {
		return nil, nil, err
	}
	if method == http.MethodPut {
		req.Header.Set("Content-Type", "application/json")
	}
	if options.IfMatch != "" {
		req.Header.Set("If-Match", options.IfMatch)
	}
	if err := ctx.Err(); err != nil {
		return nil, nil, err
	}
	writing := method != http.MethodGet && !options.DryRun
	if method != http.MethodGet && c.client.writeTimeout > 0 {
		writeCtx, cancel := context.WithTimeout(ctx, c.client.writeTimeout)
		defer cancel()
		req = req.WithContext(writeCtx)
	}
	resp, err := c.client.do(req)
	if err != nil {
		failure := newError(ErrorKindCatalogFailed, "catalog request failed", err)
		if writing {
			failure.Code = "catalog_commit_outcome_unknown"
			failure.Message = "catalog mutation could not be confirmed"
			failure.WriteDetails = writeErrorDetails(err)
			failure = failure.withWriteContext(req.Context())
		}
		return nil, nil, failure
	}
	defer closeResponseBody(resp.Body)
	maxBytes := 0
	if resp.StatusCode != http.StatusOK {
		maxBytes = maxDiagnosticBodyBytes
	}
	body, err := readResponseBody(resp.Body, maxBytes)
	if err != nil {
		failure := newResponseError(resp, ErrorKindCatalogFailed, "failed to read catalog response", err)
		if writing {
			failure.Retryable = false
			failure.WriteDetails = writeErrorDetails(err)
			failure = failure.withWriteContext(req.Context())
			if resp.StatusCode < 400 || resp.StatusCode >= 500 {
				failure.Code = "catalog_commit_outcome_unknown"
			}
		}
		return nil, nil, failure
	}
	if resp.StatusCode != http.StatusOK {
		failure := decodeResponseError(resp, body, ErrorKindCatalogFailed)
		// A failed write is not necessarily safe to replay, even for a transient HTTP status.
		if writing {
			failure.Retryable = false
			if failure.Code == "" && (resp.StatusCode < 400 || resp.StatusCode >= 500) {
				failure.Code = "catalog_commit_outcome_unknown"
			}
		}
		key := strings.TrimPrefix(c.client.authorization, "Bearer ")
		if key != "" {
			failure.Message = strings.ReplaceAll(failure.Message, key, "[redacted]")
			failure.Code = strings.ReplaceAll(failure.Code, key, "[redacted]")
		}
		return nil, nil, failure
	}
	return body, resp, nil
}
