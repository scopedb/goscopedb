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

import "encoding/json"

// CatalogDocument defines the complete user catalog of a tenant.
type CatalogDocument struct {
	Databases []CatalogDatabase `json:"databases,omitempty"`
}

// CatalogSchemas defines the schema collection of a database.
type CatalogSchemas struct {
	Schemas []CatalogSchema `json:"schemas,omitempty"`
}

// CatalogTables defines the table collection of a schema.
type CatalogTables struct {
	Tables []CatalogTable `json:"tables,omitempty"`
}

// CatalogViews defines the view collection of a schema.
type CatalogViews struct {
	Views []CatalogView `json:"views,omitempty"`
}

// CatalogDatabase defines a database and its schemas.
type CatalogDatabase struct {
	Name    string          `json:"name"`
	Comment *string         `json:"comment,omitempty"`
	Schemas []CatalogSchema `json:"schemas,omitempty"`
}

// CatalogSchema defines a schema, including its tables and views.
type CatalogSchema struct {
	Name    string         `json:"name"`
	Comment *string        `json:"comment,omitempty"`
	Tables  []CatalogTable `json:"tables,omitempty"`
	Views   []CatalogView  `json:"views,omitempty"`
}

// CatalogTable defines a table. Omitted collections are empty; omitted optional
// fields are cleared. Storage is accepted at creation and omitted from exports.
type CatalogTable struct {
	Name          string          `json:"name"`
	Comment       *string         `json:"comment,omitempty"`
	Columns       []CatalogColumn `json:"columns,omitempty"`
	PartitionBy   []string        `json:"partition_by,omitempty"`
	ClusterBy     []string        `json:"cluster_by,omitempty"`
	Distinct      CatalogDistinct `json:"distinct"`
	RetentionDays *int32          `json:"retention_days,omitempty"`
	Storage       json.RawMessage `json:"storage,omitempty"`
}

// CatalogColumn defines a stored or computed column and its additional indexes.
type CatalogColumn struct {
	Name       string               `json:"name"`
	DataType   DataType             `json:"data_type"`
	Expression *string              `json:"expression,omitempty"`
	Comment    *string              `json:"comment,omitempty"`
	Indexes    []CatalogColumnIndex `json:"indexes,omitempty"`
}

// CatalogColumnIndex defines an additional column index.
// Only Type "search" is supported. Analyzer defaults to "log".
type CatalogColumnIndex struct {
	Type     string `json:"type"`
	Analyzer string `json:"analyzer,omitempty"`
}

// CatalogDistinct defines a table's distinct keys and ordering expressions.
type CatalogDistinct struct {
	On []string `json:"on,omitempty"`
	By []string `json:"by,omitempty"`
}

// CatalogView defines a view.
type CatalogView struct {
	Name      string  `json:"name"`
	Statement string  `json:"statement"`
	Comment   *string `json:"comment,omitempty"`
}
