# ScopeDB SDK for Go

[![Version](https://img.shields.io/github/v/tag/scopedb/goscopedb?filter=v*&sort=semver)](https://pkg.go.dev/github.com/scopedb/goscopedb) [![Docs](https://img.shields.io/badge/docs-scopedb.io-blue)](https://www.scopedb.io/docs/developer/go) [![CI](https://img.shields.io/github/actions/workflow/status/scopedb/goscopedb/ci.yml?branch=main&label=CI)](https://github.com/scopedb/goscopedb/actions/workflows/ci.yml) [![License](https://img.shields.io/badge/license-Apache--2.0-blue)](https://www.apache.org/licenses/LICENSE-2.0)

The official Go client for [ScopeDB](https://www.scopedb.io). Run ScopeQL queries, browse the catalog, and write rows with bounded asynchronous streams. For application writes, start with `Table.AppendStream`.

This development version requires ScopeDB 0.6.0 or later and uses its native Statement, declarative Catalog, and table ingest APIs. These changes are not yet published.

## Runtime support

Requires Go 1.24 or later. Use the SDK in a trusted server-side process and keep API keys out of source control.

## Installation

From your application module:

```sh
go get github.com/scopedb/goscopedb@latest
```

## Quickstart

Set `SCOPEDB_ENDPOINT` and `SCOPEDB_API_KEY` using the [connection guide](https://www.scopedb.io/docs/guides/connect-to-scopedb). Save this program as `main.go` and run `go run .`:

```go
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	scopedb "github.com/scopedb/goscopedb"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	client, err := scopedb.NewClient(scopedb.Config{
		Endpoint: os.Getenv("SCOPEDB_ENDPOINT"),
		APIKey:   os.Getenv("SCOPEDB_API_KEY"),
	})
	if err != nil {
		return err
	}
	defer client.Close()

	result, err := client.Query(context.Background(), "SELECT 1 AS ready")
	if err != nil {
		return err
	}
	rows, err := result.ToObjects()
	if err != nil {
		return err
	}
	fmt.Println(rows)
	return nil
}
```

## Documentation

- [SDK guide](https://www.scopedb.io/docs/developer/go)
- [API reference](https://pkg.go.dev/github.com/scopedb/goscopedb)
- [Examples](examples/README.md)
- [Changelog](CHANGELOG.md)
- [Report an issue](https://github.com/scopedb/goscopedb/issues)

The SDK executes ScopeQL but does not define the language. Start with the language documentation:

- [Quickstart](https://www.scopedb.io/docs/guides/quickstart)
- [Query guide](https://www.scopedb.io/docs/guides/query-events)
- [Language reference](https://www.scopedb.io/docs/reference)

## Client configuration

`NewClient` validates the endpoint and returns an error for invalid configuration. Set `Config.HTTPClient` when the application needs to own HTTP timeouts, proxies, TLS, or connection pooling. `Client.Close` closes idle connections only for the HTTP client created by the SDK; it never closes a caller-provided client. SDK-owned clients refuse HTTP redirects so a redirect cannot replay a write or turn it into a GET. A supplied HTTP client retains its own redirect and transport policies.

Set `Config.WriteTimeout` to a positive `time.Duration` to limit each table append or Catalog mutation HTTP request, including reading the complete response body. Zero (the default) disables this limit. Its budget starts after local encoding, compression, and request preparation. Streams inherit this timeout unless a positive `AttemptTimeout` is set; their separate delivery/retry budget still applies. Caller context deadlines and HTTP-client timeouts remain in force. Admission, retry backoff, and an entire `Flush`/`Shutdown` barrier are outside the write timeout. Catalog dry runs use the same timeout but cannot commit, so they do not return `WriteDetails` or report an unknown commit.

Client timeouts after an HTTP attempt begins preserve the existing `*scopedb.Error` kind and cause and add `WriteDetails` with `Reason: scopedb.WriteErrorReasonTimeout` and `Outcome: scopedb.AppendStateUnknown`; `Error.Retryable` is false. In-flight caller cancellation uses `WriteErrorReasonCancelled`; cancellation before sending preserves the original context error. These fields remain available on subsequent errors returned by the stream. A timeout does not roll back a write: reconcile a client-interrupted write before replaying it. A server HTTP 408 is still a server response; an explicitly rejected append follows the existing safe retry policy. Append streams do not retry unknown outcomes by default. `AppendRetryOptions.RetryUnknown` explicitly enables retries for other transient unknown outcomes; those retries may duplicate committed rows.

Statement and table append request bodies, including direct `AppendNDJSON` requests and `AppendStream` batches, use zstd compression by default. Set `Config.Compression` to `CompressionGzip` when gzip is required. Catalog requests use uncompressed JSON. Append limits are based on the uncompressed NDJSON body.

## Query and results

`Query` submits a statement and waits for its result:

```go
result, err := client.Query(ctx, "SELECT 1 AS ready")
if err != nil {
	return err
}

rows, err := result.ToObjects()
if err != nil {
	return err
}
fmt.Println(rows)
```

Use `RawRows` for unconverted wire values, `ToValues` for positional values, `ToObjects` for values keyed by column name, or `First` for an optional first row. Integers become `int64`/`uint64` without passing through floating point; out-of-range values return an error with the row and column. Binary values become `[]byte`, timestamps become `time.Time`, and intervals become `time.Duration`. Array, object, and any cells remain wire strings, preserving embedded numeric precision. Decode embedded JSON with `json.Decoder.UseNumber` when needed. Duplicate column names are rejected by keyed conversions; use positional values to retain them.

For a detached or long-running statement, keep its handle and choose between a local status snapshot, one remote status request, or waiting for the result:

```go
handle, err := client.Statement("SELECT 1 AS ready").Submit(ctx)
if err != nil {
	return err
}

fmt.Println("statement ID:", handle.ID())
if cached := handle.Snapshot(); cached != nil {
	fmt.Println("cached status:", cached.Status) // No network request.
}

latest, err := handle.Refresh(ctx) // Fetches one remote snapshot while active.
if err != nil {
	return err
}
fmt.Println("latest status:", latest.Status)

result, err = handle.Wait(ctx) // Polls until the statement terminates.
if err != nil {
	return err
}
```

`Snapshot()` returns a complete local observation, or nil before the first response. `Refresh(ctx)` fetches one observation while active. `Cancel(ctx)` returns the same `StatementSnapshot` type: inspect `Status`, `Progress`, `Error`, and `ResultSet` together. Cancellation can race with completion, so a successful cancellation request may return a finished result or the original statement failure. `Wait` turns failed snapshots into errors and identifies confirmed server cancellation with `errors.Is(err, scopedb.ErrStatementCancelled)`.

Handle methods are safe to call concurrently. Once a terminal response is cached, no further request is needed, and an older in-flight response cannot replace it. Each snapshot belongs to the caller; modifying it does not change the handle. Store `handle.ID()` and use `client.StatementHandle(id)` to resume in another process while the server retains the statement. There is no result cursor or response body to close.

A context deadline stops local requests or waiting; it does not cancel the server statement. Call `Cancel` with a fresh, bounded context when server cancellation is required. If submission loses its response, execution may still have started. Set `Statement.ID` before submission when the application needs a known ID for reconciliation.
`Statement.ID` chooses the statement ID; otherwise ScopeDB generates one. `StatementHandle.ID()` returns the ID confirmed by ScopeDB. `MaxElapsed` limits server-side elapsed time, including queueing, and `MaxEstimated` limits the estimated execution duration. Both use `time.Duration`; zero omits the setting. The server defaults to one hour for elapsed time and no estimate limit. A context deadline limits local waiting independently:

```go
statement := client.Statement("FROM events")
statementID := uuid.New()
statement.ID = &statementID // Optional.
statement.MaxElapsed = 30 * time.Second
statement.MaxEstimated = 10 * time.Second
handle, err := statement.Submit(ctx)
if err != nil {
	return err
}
fmt.Println("statement ID:", handle.ID())
```

Statement requests use the ScopeDB 0.6 protocol. The server upgrade clears statement history and results, so handles saved before the upgrade cannot be resumed.

When `Wait` or `Execute` returns a `*scopedb.Error` with kind `ErrorKindStatementFailed`, `Message` and `Code` expose the server's failure reason. `StatementDetails` also preserves the code-specific JSON details. HTTP errors expose their server code through the same `Code` field:

```go
var scopeErr *scopedb.Error
if errors.As(err, &scopeErr) && scopeErr.StatementDetails != nil {
	fmt.Println("statement error code:", scopeErr.StatementDetails.Code)
	fmt.Println("statement error:", scopeErr.StatementDetails.Message)
	fmt.Println("details:", string(scopeErr.StatementDetails.Details))
}
```

## Initialize a table

At application startup, apply the complete expected definition and start writing after it succeeds. `Table` defaults to database `scopedb` and schema `public`; set its `Database` and `Schema` fields to use another location. The parent database and schema must exist.

```go
table := client.Table("events")
definition := &scopedb.CatalogTable{
    Name: table.Name,
    Columns: []scopedb.CatalogColumn{
        {Name: "id", DataType: scopedb.IntDataType},
        {Name: "message", DataType: scopedb.StringDataType},
    },
}
if _, err := table.Catalog().Apply(ctx, definition); err != nil {
    return err
}

_, err := table.AppendNDJSON(ctx, []byte("{\"id\":1,\"message\":\"service started\"}\n"))
if err != nil {
    return err
}
```

The default call applies immediately and rejects changes that may lose data. Applying the same definition again preserves existing rows. This is complete replacement: omitted fields and objects are cleared. If an older application version omits a physical column added by a newer version, its startup apply fails rather than deleting that column. Use `table.Catalog().Get(ctx)` to read the current definition and its ETag. Use `table.Catalog().Delete(ctx, options)` for conditional deletion with explicit destructive-change authorization.

Pass one `CatalogOptions` value when you need a dry run, an ETag precondition, or explicit authorization for destructive changes. The same optional options are accepted by Catalog `Apply` and `Delete`.

## Manage the catalog

The declarative Catalog API reads complete definitions and replaces resources atomically. It requires a server with the declarative protocol; the old paginated catalog API is not supported.

```go
catalog := client.Catalog().Table("scopedb", "public", "events")
snapshot, err := catalog.Get(ctx)
if err != nil {
    return err
}
definition := snapshot.Definition
retention := int32(30)
definition.RetentionDays = &retention
plan, err := catalog.Apply(ctx, definition, scopedb.CatalogOptions{DryRun: true, IfMatch: snapshot.ETag})
if err != nil {
    return err
}
// Review plan.Before, plan.After, and plan.Destructive before authorizing this write.
result, err := catalog.Apply(ctx, definition, scopedb.CatalogOptions{
    IfMatch: plan.ETag,
    AllowDestructive: plan.Destructive,
})
if err != nil {
    return err
}
fmt.Println(result.ETag)
```

`client.Catalog()` selects the entire tenant. Use `Database(name)`, `Schema(database, schema)`, `Table(database, schema, table)`, or `View(database, schema, view)` to select an individual resource. Pass names directly; the SDK escapes each name for the HTTP path. Each resource includes its complete subtree.

`Get` returns a `CatalogSnapshot[T]` with a typed definition and an ETag. Pass that ETag to the first dry run or write after editing; otherwise a change between Get and preview can be overwritten. `Apply` accepts the same definition type and returns `CatalogResult[T]` with typed `Before` and `After` values. These definitions are pointers: `Before` is nil when creating a resource, and `After` is nil when deleting it.

Omitted fields and objects are cleared, and omitted collections are empty. `Delete` removes one database, schema, table, or view using the same options; clear the tenant by applying `&scopedb.CatalogDocument{}`, or clear children by editing their parent definition. `IfMatch` is optional, while potentially irreversible data loss requires `AllowDestructive`. Dry runs validate and roll back without requiring that authorization. Storage is a creation-only input omitted from exports, so keep the original definition when planning creation.

Typed reads reject unknown fields, including nested fields, instead of silently discarding them before a later replacement. For export and apply workflows that must preserve fields unknown to this SDK, select `Raw()`. Definitions and before/after states are then `json.RawMessage`; absent states are JSON `null`:

```go
catalog := client.Catalog().Raw()
snapshot, err := catalog.Get(ctx)
if err != nil {
    return err
}
// Retain snapshot.Definition as raw JSON when saving or editing it.
plan, err := catalog.Apply(ctx, snapshot.Definition, scopedb.CatalogOptions{DryRun: true, IfMatch: snapshot.ETag})
if err != nil {
    return err
}
fmt.Println(string(plan.After))
```

`Raw()` is also available on each resource selector. Typed and raw resources share the same HTTP behavior, options, and errors.

Errors use `*scopedb.Error`, including the server `Code`, HTTP status, and request ID. Catalog writes are never automatically retried. If `Code` is `catalog_commit_outcome_unknown`, read the catalog and plan again before retrying; a lost response does not imply a rollback.

## Streaming writes

Use `AppendStream` to write Go structs or maps to an existing table. The SDK encodes and batches the rows for you.

### Append rows

Create a stream with the default options, add rows with `Send`, and call `Shutdown` when you are done. Struct fields use standard Go JSON tags.

```go
type Event struct {
	ID   int    `json:"id"`
	Name string `json:"name"`
}

stream, err := table.AppendStream(scopedb.AppendStreamOptions{})
if err != nil {
	return err
}

for _, event := range []Event{
	{ID: 1, Name: "first"},
	{ID: 2, Name: "second"},
} {
	if err := stream.Send(ctx, event); err != nil {
		_, _ = stream.Shutdown(ctx)
		return err
	}
}

report, err := stream.Shutdown(ctx)
if err != nil {
	return err
}
fmt.Println("written rows:", report.CommittedRows)
```

`Send` adds a row to the SDK's queue. `Shutdown` sends any remaining rows, waits for writes to finish, and closes the stream. Check the errors returned by both calls.

For a long-running stream, call `Flush` when you need to wait for the rows sent so far without closing the stream:

```go
report, err := stream.Flush(ctx)
if err != nil {
	_, _ = stream.Shutdown(ctx)
	return err
}
fmt.Println("written rows:", report.CommittedRows)
```

### Continue after write errors

For logs and telemetry, set `FailurePolicy` to `AppendFailureContinue` to keep processing later rows after a write fails:

```go
telemetry, err := table.AppendStream(scopedb.AppendStreamOptions{
	FailurePolicy: scopedb.AppendFailureContinue,
})
if err != nil {
	return err
}

if err := telemetry.TrySend(map[string]any{
	"name":   "request.completed",
	"status": 200,
}); err != nil {
	log.Printf("could not queue telemetry row: %v", err)
}

report, err := telemetry.Shutdown(ctx)
if err != nil {
	return err
}
if report.Outcome != scopedb.AppendDeliveryOK {
	log.Printf("some telemetry rows were not confirmed written: %+v", report)
}
```

`TrySend` adds a row without waiting for queue space. Check the report returned by `Flush` or `Shutdown`: `CommittedRows` counts confirmed writes, `FailedRows` counts failed rows, `UnknownRows` counts writes without a confirmed result, and `DroppedRows` counts rows rejected by `TrySend`. Each report covers activity since the previous completed barrier, including barriers whose caller stopped waiting. `Stats()` holds lifetime totals. Calling `Shutdown` repeatedly returns its original final report, so do not add it to totals twice.

### Delivery and resource lifetime

Streams retry temporary errors only when the server explicitly confirms rejection. The default allows eight retries with bounded backoff and a five-minute elapsed budget per dispatched batch. Missing responses and unknown outcomes stop delivery under the default stop policy. If the application accepts possible duplicates, opt into at-least-once replay:

```go
stream, err := table.AppendStream(scopedb.AppendStreamOptions{
    Retry: &scopedb.AppendRetryOptions{MaxRetries: 8, RetryUnknown: true},
})
```

A later successful acknowledgement confirms the logical rows were written at least once, not how many copies exist. A later rejection cannot resolve an earlier unknown attempt. `Retry: &scopedb.AppendRetryOptions{MaxRetries: 0}` disables all retries. Client timeouts and cancellations are never retried, even with `RetryUnknown` enabled. Direct `AppendNDJSON` always makes one SDK attempt.

The default stream targets 4 MiB batches, accepts at most 200,000 rows per batch, and sends up to four batches concurrently. Set `MaxConcurrentBatches: 1` for serial requests. `MaxBufferedBytes` bounds accepted encoded rows, including in-flight batches; it excludes temporary encoding/compression buffers and rows held by blocked `Send` callers. Limit producer concurrency when bounding total process memory.

Always call `Shutdown` to close admission and settle accepted rows. Cancelling a `Send` context before admission leaves that row unaccepted. Cancelling a `Flush` or `Shutdown` wait does not abandon accepted rows or roll back writes; use a live context to wait again, and inspect `Stats()` for cumulative delivery. `Client.Close` only releases idle HTTP connections and does not shut down streams.

### Append NDJSON directly

If you already have NDJSON, use `AppendNDJSON`. Put one JSON object on each line:

```go
ndjson := []byte("{\"id\":1,\"name\":\"first\"}\n{\"id\":2,\"name\":\"second\"}")
result, err := table.AppendNDJSON(ctx, ndjson)
if err != nil {
	return err
}
fmt.Println("committed rows:", result.NumRowsInserted)
```

One request supports up to 8 MiB of uncompressed NDJSON and 200,000 rows. ScopeDB 0.6 parses NDJSON integer tokens as signed 64-bit integers, including values for `uint` columns and nested objects. Values above `9223372036854775807` are rejected; strings are not coerced into numeric columns. This input limit is independent of query results, which can contain the full `uint64` range.

`Send` uses standard `encoding/json`, without schema-aware conversions. `[]byte` encodes as the base64 string expected by binary columns and `time.Time` as RFC 3339. Encode interval columns as fixed-duration ISO 8601 strings (for example, `"PT1.5S"`), not Go's integer-encoded `time.Duration`. An integer-valued Go float may encode without a decimal point; for floats beyond the signed integer range, supply an explicit floating-point JSON token with `json.RawMessage`, such as `1e20`.

See the examples for [streaming writes](examples/append_stream), [file imports](examples/patterns/bulk_append), [telemetry](examples/patterns/telemetry), and [NDJSON](examples/append_ndjson).

## Migrate to ScopeDB 0.6

This unpublished version intentionally replaces the previous development API:

| Previous API                                       | Replacement                                                                                             |
| -------------------------------------------------- | ------------------------------------------------------------------------------------------------------- |
| `handle.LastStatus()`, `Progress()`, `ResultSet()` | `handle.Snapshot()` and its `Status`, `Progress`, `ResultSet`, and `Error` fields                       |
| `handle.Status(ctx)`                               | `handle.Refresh(ctx)`, returning a complete `*StatementSnapshot`                                        |
| `StatementCancelResult`                            | `*StatementSnapshot` from `Cancel`; `ID` replaces `StatementID`, and `Error` holds the original failure |
| `status.Finished()`                                | `status == scopedb.StatementStatusFinished`; `Terminated()` still covers all terminal states            |
| `table.Describe(ctx)`                              | `table.Catalog().Get(ctx)`, retaining both `Definition` and `ETag`                                      |
| `table.Apply(ctx, definition)`                     | `table.Catalog().Apply(ctx, definition)`                                                                |
| `table.Drop(ctx)`                                  | `table.Catalog().Delete(ctx, scopedb.CatalogOptions{IfMatch: etag, AllowDestructive: true})`            |
| Append result/error/last-failure `.AppendState`    | `.Status`; append JSON uses `status`, matching the service                                              |
| Automatic retries of transient unknown appends     | Explicit `Retry: &scopedb.AppendRetryOptions{MaxRetries: 8, RetryUnknown: true}`                        |
| Historical `u_int` type spelling                   | Native `uint`                                                                                           |

Replace `ExecTimeout` with `MaxElapsed time.Duration`; `MaxEstimated` independently limits estimated execution time. Zero omits a limit, negative durations are invalid, and elapsed time includes queueing. Server failures expose native `statement_*` codes through `Error.Code`; unknown codes remain available as strings. `ErrStatementCancelled` identifies server cancellation independently of a local `context.Canceled`.

Replace paginated Catalog list/fetch/iterator methods with `client.Catalog().Get(ctx)` or a database/schema/table/view selector followed by `.Get(ctx)`. Use `Distinct` and `RetentionDays` in the complete table definition. Typed reads reject unsupported fields; `Raw()` retains them without numeric conversion. Catalog writes are not retried.

Appends use `/v1/catalog/databases/{db}/schemas/{schema}/tables/{table}/ingest`. The commit outcome is `status` on success and `details.status` on failure. The `AppendState` enum values and stream delivery/report types remain. Missing or unreadable outcomes are unknown, and retrying unknown outcomes now requires explicit opt-in.

The server's 0.5-to-0.6 migration clears statement history and saved results; saved handles cannot be resumed.

## Migrate from IngestStream

`Client.IngestStream`, `IngestStream`, `IngestStreamOptions`, and `IngestResult` have been removed. Select the destination with `client.Table(name)` and use `Table.AppendStream` for Go values or `Table.AppendNDJSON` for an existing NDJSON payload. This SDK version requires ScopeDB 0.6.0 or later.

Append accepts rows matching the destination columns, not a ScopeQL statement. Move field renaming, type conversion, filtering, and routing into the producer before calling `Send`. For transformations that must run in ScopeQL, append the original event into an `object` column in a staging table, then execute a separate `FROM ... SELECT ... INSERT INTO ...` statement. Staging and transformation are separate commits; track processed batches before retrying the transformation.

Review these behavior changes when migrating:

- Append batches allow up to 8 MiB of uncompressed NDJSON and 200,000 rows. Reduce an old ingest batch-byte target above 8 MiB.
- `Flush` and `Shutdown` return an `AppendDeliveryReport` for activity since the previous report. Read `CommittedRows` instead of `IngestResult.NumRowsInserted`, and check `Outcome` when using the continue policy.
- Append streams send up to four batches concurrently by default. Set `MaxConcurrentBatches: 1` when the producer needs sequential requests; queries still require `ORDER BY` for ordered results.
- Append streams retry confirmed temporary rejections by default. `RetryUnknown: true` opts into retrying transient unknown outcomes, which can duplicate rows. Set `MaxRetries: 0` to disable all retries. Client timeouts and cancellations are never retried. Direct `AppendNDJSON` makes one attempt.

## Structured errors

`scopedb.Error` exposes the server message, code, and structured details without requiring applications to parse the message. Catalog errors redact the configured API key from messages and codes:

```go
var scopeErr *scopedb.Error
if errors.As(err, &scopeErr) {
	log.Printf("kind=%s status=%d request_id=%s retryable=%t retry_after=%s",
		scopeErr.Kind,
		scopeErr.HTTPStatus,
		scopeErr.RequestID,
		scopeErr.Retryable,
		scopeErr.RetryAfter,
	)
}
```

The main kinds are `ErrorKindConfigInvalid`, `ErrorKindStatementFailed`, `ErrorKindCatalogFailed`, `ErrorKindAppendRowsFailed`, and `ErrorKindUnexpected`. Use `errors.Is` and `errors.As` to inspect the underlying cause. Append streams preserve the last attempt's `Code` and `Details` in returned errors and `Stats().LastFailure`; the delivery outcome remains unknown if an earlier attempt may have committed.

## Examples

The [examples guide](examples/README.md) contains query and write examples with runnable commands.

## Development

Development tasks are defined in [mise.toml](mise.toml); license tasks expect `hawkeye` on `PATH`.

```sh
mise install
mise run check
mise run build
mise run test
```

Release history and the maintainer runbook are in [CHANGELOG.md](CHANGELOG.md) and [RELEASE.md](RELEASE.md).

## License

Licensed under the [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0).

## Live protocol validation

From a source checkout, set `SCOPEDB_ENDPOINT` and `SCOPEDB_API_KEY` to an authenticated ScopeDB 0.6 endpoint, then run:

```sh
mise run test-integration
```

The suite creates uniquely named resources and checks dry-run, ETag conflicts, conditional deletion, committed and rejected appends, statement errors, elapsed timeout, and a cancellation racing with completion. It also checks signed integer boundaries, precise nested integers, the server's NDJSON unsigned-input limit, and full-range unsigned query results. Tests delete their resources before returning. Running cancellation is covered separately with controlled HTTP responses in the unit tests.

With `SCOPEDB_API_KEY` set, Cloud authentication checks verify that missing, malformed, and incorrect credentials return 401 for Catalog, queries, and writes. Rejected stream writes must count as failed rather than unknown, without retries or committed rows. Optional `SCOPEDB_EXPIRED_API_KEY` and `SCOPEDB_REVOKED_API_KEY` extend these checks to expired and revoked credentials.

To verify tenant isolation through the Cloud Proxy, also set `SCOPEDB_WORKSPACE_ID`, `SCOPEDB_SECONDARY_WORKSPACE_ID`, and `SCOPEDB_SECONDARY_API_KEY` for two distinct workspaces served by the same endpoint. The test creates same-named tables with different definitions and rows, checks that authentication overrides a forged tenant header, denies cross-tenant Statement reads and cancellation, and confirms that deleting one tenant's table leaves the other's intact. Use disposable credentials with create/drop access to `scopedb.public`; the tests neither create nor revoke credentials.
