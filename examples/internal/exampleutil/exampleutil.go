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

// Package exampleutil contains configuration shared by the runnable examples.
package exampleutil

import (
	"cmp"
	"errors"
	"os"
	"strings"

	scopedb "github.com/scopedb/goscopedb"
)

const (
	defaultEndpoint = "http://127.0.0.1:6543"
	defaultDatabase = "scopedb"
	defaultSchema   = "public"
)

// NewClient creates a client from the example environment variables.
func NewClient() (*scopedb.Client, error) {
	return scopedb.NewClient(scopedb.Config{
		Endpoint: cmp.Or(os.Getenv("SCOPEDB_ENDPOINT"), defaultEndpoint),
		APIKey:   cmp.Or(os.Getenv("SCOPEDB_API_KEY"), os.Getenv("SCOPEDB_TOKEN")),
	})
}

// Database returns the configured database name.
func Database() string {
	return cmp.Or(os.Getenv("SCOPEDB_DATABASE"), defaultDatabase)
}

// Schema returns the configured schema name.
func Schema() string {
	return cmp.Or(os.Getenv("SCOPEDB_SCHEMA"), defaultSchema)
}

// WriteTable returns the explicitly selected destination for a write example.
func WriteTable(client *scopedb.Client) (*scopedb.Table, error) {
	name := os.Getenv("SCOPEDB_WRITE_TABLE")
	if strings.TrimSpace(name) == "" {
		return nil, errors.New("SCOPEDB_WRITE_TABLE is required for write examples; set it to the unqualified name of an existing disposable table")
	}

	table := client.Table(name)
	table.Database = Database()
	table.Schema = Schema()
	return table, nil
}
