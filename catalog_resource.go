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
	"io"
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

// Get returns a complete definition, without pagination.
func (c *CatalogResource[T]) Get(ctx context.Context) (CatalogSnapshot[T], error) {
	data, resp, err := c.request(ctx, http.MethodGet, nil, CatalogOptions{})
	if err != nil {
		return CatalogSnapshot[T]{}, err
	}
	trimmed := bytes.TrimSpace(data)
	if !json.Valid(trimmed) || trimmed[0] != '{' || resp.Header.Get("ETag") == "" {
		return CatalogSnapshot[T]{}, responseMetadataError(resp, ErrorKindCatalogFailed, "invalid catalog response", nil)
	}
	// Raw callers can retain the response bytes directly.
	if definition, ok := any(json.RawMessage(data)).(T); ok {
		return CatalogSnapshot[T]{Definition: definition, ETag: resp.Header.Get("ETag")}, nil
	}
	var definition T
	if err := json.Unmarshal(data, &definition); err != nil {
		return CatalogSnapshot[T]{}, responseMetadataError(resp, ErrorKindCatalogFailed, "invalid catalog definition", err)
	}
	return CatalogSnapshot[T]{Definition: definition, ETag: resp.Header.Get("ETag")}, nil
}

// Apply replaces the resource with definition. Omitted fields and objects are
// cleared. DryRun performs validation and rolls back all changes.
// Catalog writes are never automatically retried.
func (c *CatalogResource[T]) Apply(ctx context.Context, definition T, options CatalogOptions) (CatalogResult[T], error) {
	data, err := json.Marshal(definition)
	if err != nil {
		return CatalogResult[T]{}, newError(ErrorKindConfigInvalid, "invalid catalog definition", err)
	}
	return c.mutate(ctx, http.MethodPut, data, options)
}

// Delete removes a single database, schema, table, or view.
func (c *CatalogResource[T]) Delete(ctx context.Context, options CatalogOptions) (CatalogResult[T], error) {
	return c.mutate(ctx, http.MethodDelete, nil, options)
}

func (c *CatalogResource[T]) mutate(ctx context.Context, method string, data []byte, options CatalogOptions) (CatalogResult[T], error) {
	data, resp, err := c.request(ctx, method, data, options)
	if err != nil {
		return CatalogResult[T]{}, err
	}
	var raw CatalogResult[json.RawMessage]
	err = json.Unmarshal(data, &raw)
	raw.ETag = resp.Header.Get("ETag")
	if err != nil || len(raw.Before) == 0 || len(raw.After) == 0 || raw.ETag == "" {
		failure := responseMetadataError(resp, ErrorKindCatalogFailed, "invalid catalog response", err)
		if !options.DryRun {
			failure.Code = "catalog_commit_outcome_unknown"
		}
		return CatalogResult[T]{}, failure
	}
	if result, ok := any(raw).(CatalogResult[T]); ok {
		return result, nil
	}
	var result CatalogResult[T]
	if err := json.Unmarshal(data, &result); err != nil {
		failure := responseMetadataError(resp, ErrorKindCatalogFailed, "invalid catalog definition", err)
		if !options.DryRun {
			failure.Code = "catalog_commit_outcome_unknown"
		}
		return CatalogResult[T]{}, failure
	}
	result.ETag = raw.ETag
	return result, nil
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
	resp, err := c.client.http.do(req)
	if err != nil {
		failure := newError(ErrorKindCatalogFailed, "catalog request failed", err)
		if writing {
			failure.Code = "catalog_commit_outcome_unknown"
			failure.Message = "catalog apply could not be confirmed"
		}
		return nil, nil, failure
	}
	defer sneakyBodyClose(resp.Body)
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		failure := responseMetadataError(resp, ErrorKindCatalogFailed, "failed to read catalog response", err)
		if writing {
			failure.Retryable = false
			if resp.StatusCode < 400 || resp.StatusCode >= 500 {
				failure.Code = "catalog_commit_outcome_unknown"
			}
		}
		return nil, nil, failure
	}
	if resp.StatusCode != http.StatusOK {
		failure := responseError(resp, body, ErrorKindCatalogFailed)
		// A failed write is not necessarily safe to replay, even for a transient HTTP status.
		if writing {
			failure.Retryable = false
		}
		key := strings.TrimPrefix(c.client.http.authorization, "Bearer ")
		if key != "" {
			failure.Message = strings.ReplaceAll(failure.Message, key, "[redacted]")
			failure.Code = strings.ReplaceAll(failure.Code, key, "[redacted]")
		}
		return nil, nil, failure
	}
	return body, resp, nil
}
