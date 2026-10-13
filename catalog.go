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

// Catalog addresses a declarative catalog resource.
// Definitions are retained as JSON so exporting and applying them preserves all fields.
type Catalog struct {
	client   *Client
	resource []string
}

// Catalog selects a resource by its unescaped HTTP path segments, relative to /v1/catalog.
// With no segments it selects the complete tenant catalog (/v1/catalog).
// For example: Catalog("databases", "analytics", "schemas", "public", "tables", "events").
// Requires a server implementing the declarative catalog protocol; legacy pagination is not supported.
func (c *Client) Catalog(resource ...string) *Catalog {
	return &Catalog{client: c, resource: append([]string{"catalog"}, resource...)}
}

// CatalogSnapshot contains a complete resource definition and its opaque version.
type CatalogSnapshot struct {
	Definition json.RawMessage
	ETag       string
}

// CatalogOptions controls preview, concurrent updates, and data-loss authorization.
type CatalogOptions struct {
	DryRun           bool
	AllowDestructive bool
	// IfMatch is optional. Use the ETag from Get or a dry-run result to reject concurrent changes.
	IfMatch string
}

// CatalogResult contains the normalized states compared in one server transaction.
// Before or After is JSON null when the resource does not exist.
type CatalogResult struct {
	Before      json.RawMessage `json:"before"`
	After       json.RawMessage `json:"after"`
	Destructive bool            `json:"destructive"`
	// ETag comes from the response header; it is the base version for a dry run.
	ETag string `json:"-"`
}

// Get returns a complete definition, without pagination.
func (c *Catalog) Get(ctx context.Context) (CatalogSnapshot, error) {
	data, resp, err := c.request(ctx, http.MethodGet, nil, CatalogOptions{})
	if err != nil {
		return CatalogSnapshot{}, err
	}
	trimmed := bytes.TrimSpace(data)
	if !json.Valid(trimmed) || trimmed[0] != '{' || resp.Header.Get("ETag") == "" {
		return CatalogSnapshot{}, responseMetadataError(resp, ErrorKindCatalogFailed, "invalid catalog response", nil)
	}
	return CatalogSnapshot{Definition: data, ETag: resp.Header.Get("ETag")}, nil
}

// Apply replaces the resource with definition, which may be a CatalogDocument,
// CatalogTable, another definition type, or json.RawMessage. Omitted fields and
// objects are cleared. DryRun performs validation and rolls back all changes.
// Catalog writes are never automatically retried.
func (c *Catalog) Apply(ctx context.Context, definition any, options CatalogOptions) (CatalogResult, error) {
	data, err := json.Marshal(definition)
	if err != nil {
		return CatalogResult{}, newError(ErrorKindConfigInvalid, "invalid catalog definition", err)
	}
	return c.mutate(ctx, http.MethodPut, data, options)
}

// Delete removes a single database, schema, table, or view.
// To clear the tenant, Apply an empty CatalogDocument instead.
func (c *Catalog) Delete(ctx context.Context, options CatalogOptions) (CatalogResult, error) {
	return c.mutate(ctx, http.MethodDelete, nil, options)
}

func (c *Catalog) mutate(ctx context.Context, method string, data []byte, options CatalogOptions) (CatalogResult, error) {
	data, resp, err := c.request(ctx, method, data, options)
	if err != nil {
		return CatalogResult{}, err
	}
	var result CatalogResult
	err = json.Unmarshal(data, &result)
	result.ETag = resp.Header.Get("ETag")
	if err != nil || len(result.Before) == 0 || len(result.After) == 0 || result.ETag == "" {
		failure := responseMetadataError(resp, ErrorKindCatalogFailed, "invalid catalog response", err)
		if !options.DryRun {
			failure.Code = "catalog_commit_outcome_unknown"
		}
		return CatalogResult{}, failure
	}
	return result, nil
}

func (c *Catalog) request(ctx context.Context, method string, data []byte, options CatalogOptions) ([]byte, *http.Response, error) {
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
