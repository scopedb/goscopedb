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
)

// Catalog manages the complete user catalog of a tenant and selects individual resources.
type Catalog struct {
	resource CatalogResource[*CatalogDocument]
}

// Catalog selects the complete tenant catalog. Names passed to its resource
// selectors are unescaped database, schema, table, or view names.
func (c *Client) Catalog() *Catalog {
	return &Catalog{resource: CatalogResource[*CatalogDocument]{client: c, resource: []string{"catalog"}}}
}

// Get returns the complete tenant definition and its opaque version.
func (c *Catalog) Get(ctx context.Context) (CatalogSnapshot[*CatalogDocument], error) {
	return c.resource.Get(ctx)
}

// Apply replaces the complete tenant catalog. An empty document clears the tenant.
// Options may be omitted; by default, changes apply without a version precondition
// and potentially destructive changes are rejected.
func (c *Catalog) Apply(ctx context.Context, definition *CatalogDocument, options ...CatalogOptions) (CatalogResult[*CatalogDocument], error) {
	return c.resource.Apply(ctx, definition, options...)
}

// Raw selects the tenant catalog as JSON, preserving fields unknown to this SDK.
func (c *Catalog) Raw() *CatalogResource[json.RawMessage] {
	return c.resource.Raw()
}

// Database selects a database and its complete subtree.
func (c *Catalog) Database(database string) *CatalogResource[*CatalogDatabase] {
	return &CatalogResource[*CatalogDatabase]{client: c.resource.client, resource: []string{"catalog", "databases", database}}
}

// Schema selects a schema and its tables and views.
func (c *Catalog) Schema(database, schema string) *CatalogResource[*CatalogSchema] {
	return &CatalogResource[*CatalogSchema]{client: c.resource.client, resource: []string{"catalog", "databases", database, "schemas", schema}}
}

// Table selects a complete table definition.
func (c *Catalog) Table(database, schema, table string) *CatalogResource[*CatalogTable] {
	return &CatalogResource[*CatalogTable]{client: c.resource.client, resource: []string{"catalog", "databases", database, "schemas", schema, "tables", table}}
}

// View selects a view definition.
func (c *Catalog) View(database, schema, view string) *CatalogResource[*CatalogView] {
	return &CatalogResource[*CatalogView]{client: c.resource.client, resource: []string{"catalog", "databases", database, "schemas", schema, "views", view}}
}

// CatalogSnapshot contains a complete resource definition and its opaque version.
type CatalogSnapshot[T any] struct {
	Definition T
	Version    string
}

// CatalogOptions controls preview, concurrent updates, and data-loss authorization.
// Apply and Delete accept zero or one options value.
type CatalogOptions struct {
	DryRun           bool
	AllowDestructive bool
	// ExpectedVersion rejects changes if the catalog has changed since a prior read or dry run.
	// It is optional; pass the opaque Version token unchanged.
	ExpectedVersion string
}

// CatalogResult contains the normalized states compared in one server transaction.
// Typed definitions are nil when absent; Raw resources represent absence as JSON null.
type CatalogResult[T any] struct {
	Before      T    `json:"before"`
	After       T    `json:"after"`
	Destructive bool `json:"destructive"`
	// Version is the opaque base version for a dry run, or the resulting version after a mutation.
	Version string `json:"-"`
}
