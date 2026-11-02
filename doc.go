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

// Package scopedb provides a Go client for ScopeDB queries, catalog management,
// and table writes.
//
// Create and reuse a [Client] with [NewClient]. [Client.Query] submits a ScopeQL
// statement and waits for a [ResultSet]. For detached execution, retain a
// [StatementHandle] from [Statement.Submit] to inspect, wait for, or cancel it.
// A local context deadline does not cancel the remote statement.
//
// [Client.Catalog] selects complete definitions at tenant, database, schema,
// table, or view scope. Apply replaces the selected subtree; omitted fields
// are cleared. Use [CatalogOptions] for dry runs, version preconditions, and
// explicit destructive-change authorization.
//
// [Table.AppendStream] batches Go structs or maps with bounded backpressure.
// Send confirms local admission; Flush and Shutdown return delivery reports.
// Always settle accepted rows with Shutdown, including after a producer error.
// [Table.AppendNDJSON] sends one caller-encoded request without SDK retries.
// [Client.Close] releases idle connections and does not shut down streams.
//
// Inspect [Error] for server diagnostics and operation outcomes. An ambiguous
// write may have committed; retryability alone does not make replay safe.
// See [AppendRetryOptions] and [AppendDeliveryReport] for stream recovery rules.
package scopedb
