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

/*
Package scopedb provides the official Go client for ScopeDB. It supports
ScopeQL statements, declarative catalog management, and streaming writes.

# Connect and query

Reuse one [Client] for the application's connection pool:

	client, err := scopedb.NewClient(scopedb.Config{
		Endpoint: os.Getenv("SCOPEDB_ENDPOINT"),
		APIKey:   os.Getenv("SCOPEDB_API_KEY"),
	})
	if err != nil {
		return err
	}
	defer client.Close()

[Client.Query] waits for a statement result:

	result, err := client.Query(ctx, "SELECT 1 AS ready")
	if err != nil {
		return err
	}
	rows, err := result.ToObjects()
	if err != nil {
		return err
	}
	fmt.Println(rows)

[ResultSet.ToObjects] keys rows by column name; [ResultSet.ToValues] returns
positional values, and [ResultSet.RawRows] returns string-or-null wire cells.
Integers retain their int64/uint64 precision. Timestamps become time.Time,
intervals become time.Duration, and binary cells become []byte. Object, array,
and any cells remain JSON strings; use json.Decoder.UseNumber when decoding
embedded JSON without losing integer precision.

Use [Statement.Submit] and retain its [StatementHandle] to inspect, wait for, or
cancel an execution. Snapshot reads the local cache; Refresh fetches one remote
observation while active. Cancel returns the same complete [StatementSnapshot].
Handle methods are safe to call concurrently. A local context deadline does not
cancel the server statement; use Cancel with a live context. Set [Statement.ID]
before submission when recovery requires a caller-known ID. MaxElapsed and
MaxEstimated limit server execution independently of local waiting.

# Manage table definitions

[Client.Catalog] selects the complete tenant catalog. Its Database, Schema,
Table, and View selectors provide typed Get, Apply, and Delete operations.
[Table.Catalog] selects one table. Apply replaces the entire selected subtree,
including children; omitted fields are cleared. Use dry runs and ETag
preconditions to review changes and reject concurrent edits. Potentially
destructive changes require explicit authorization. Typed reads reject
unsupported fields; Raw preserves them as JSON for export and apply workflows.

# Write rows

[Table.AppendStream] accepts Go structs or maps and provides bounded asynchronous
batching. This example uses an existing events table with id int and name string
columns:

	stream, err := client.Table("events").AppendStream(scopedb.AppendStreamOptions{})
	if err != nil {
		return err
	}
	if err := stream.Send(ctx, map[string]any{"id": 1, "name": "page_view"}); err != nil {
		_, _ = stream.Shutdown(ctx)
		return err
	}
	report, err := stream.Shutdown(ctx)
	if err != nil {
		return err
	}
	fmt.Println(report.CommittedRows)

Send uses encoding/json and confirms local admission only. It requires a
top-level JSON object but does not validate the destination schema. Flush waits
for accepted rows while keeping the stream open; Shutdown closes admission and
settles accepted rows. Cancelling either wait does not cancel delivery.
[Client.Close] releases idle connections and does not shut down streams.

The default failure policy stops after a failed batch. [AppendFailureContinue]
keeps processing later batches; inspect [AppendDeliveryReport.Outcome] even when
the barrier returns no error. [AppendStream.Stats] exposes lifetime counters and
failure diagnostics. Unknown outcomes may already have committed and are not
retried unless RetryUnknown is enabled, accepting possible duplicate rows.

[Table.AppendNDJSON] sends one caller-owned NDJSON request without automatic
retries. Direct requests and stream batches use the client's configured
compression, which defaults to zstd. Append has no schema-aware conversions:
encode interval columns as ISO 8601 PT strings, not time.Duration integers.
See the [usage guide] for JSON encodings, limits, timeouts, and delivery contracts.

# Errors and further reading

Use errors.As to inspect [Error] for the server message, code, HTTP diagnostics,
and structured statement or append details. Use errors.Is for underlying causes
and [ErrStatementCancelled], which is distinct from a local context.Canceled.
Retryable alone does not prove that replay is safe; reconcile unknown write
outcomes before resubmitting.

For version upgrades, see the [migration guide]. ScopeQL is documented separately
in the [quickstart], [query guide], and [language reference].

[usage guide]: https://github.com/scopedb/goscopedb/blob/main/docs/usage.md
[migration guide]: https://github.com/scopedb/goscopedb/blob/main/docs/migration-0.7.md
[quickstart]: https://www.scopedb.io/docs/guides/quickstart
[query guide]: https://www.scopedb.io/docs/guides/query-events
[language reference]: https://www.scopedb.io/docs/reference
*/
package scopedb
