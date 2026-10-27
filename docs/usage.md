# Using the Go SDK

Start with the [runnable quickstart](../README.md#run-a-query). The snippets below use its `client` inside an error-returning function with a live `ctx := context.Background()`. Write snippets use an existing `events` table with `id int` and `name string` columns, unless they explicitly initialize a table. For complete programs, see the [examples](../examples/README.md).

## Read query results

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

Use `RawRows` for unconverted string-or-null wire cells, `ToValues` for positional values, or `ToObjects` for values keyed by column name. `First` returns `(row, found, err)` with `found == false` for an empty result. SQL NULL becomes nil in typed conversions. Integers become `int64`/`uint64` without passing through floating point; out-of-range values return an error with the row and column. Binary values become `[]byte`, timestamps become `time.Time` with nanosecond precision, and intervals become `time.Duration`. Binary query cells use hexadecimal text on the wire; Append input uses Base64. Array, object, and any cells remain wire strings, preserving embedded numeric precision. Decode embedded JSON with `json.Decoder.UseNumber` when needed. Duplicate column names are rejected by keyed conversions; use positional values to retain them.

## Track or cancel a query

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

result, err := handle.Wait(ctx) // Polls until the statement terminates.
if err != nil {
	return err
}
fmt.Println(result.TotalRows)
```

`Snapshot()` returns a complete local observation, or nil before the first response. `Refresh(ctx)` fetches one observation while active. `Cancel(ctx)` returns the same `StatementSnapshot` type: inspect `Status`, `Progress`, `Error`, and `ResultSet` together. Cancellation can race with completion, so a successful cancellation request may return a finished result or the original statement failure. `Wait` turns failed snapshots into errors and identifies confirmed server cancellation with `errors.Is(err, scopedb.ErrStatementCancelled)`.

Handle methods are safe to call concurrently. Once a terminal response is cached, no further request is needed, and an older in-flight response cannot replace it. Each snapshot belongs to the caller; modifying it does not change the handle. Store `handle.ID()` and use `client.StatementHandle(id)` to resume in another process while the server retains the statement. There is no result cursor or response body to close.

A context deadline stops local requests or waiting; it does not cancel the server statement. Call `Cancel` with a fresh, bounded context when server cancellation is required. If submission loses its response, execution may still have started. Set `Statement.ID` before submission when the application needs a known ID for reconciliation.

`Statement.ID` chooses the statement ID; otherwise ScopeDB generates one. `StatementHandle.ID()` returns the ID confirmed by ScopeDB. `MaxElapsed` limits server-side elapsed time, including queueing, and `MaxEstimated` limits the estimated execution duration. Both use `time.Duration`; zero omits the setting. The server defaults to one hour for elapsed time and no estimate limit. A context deadline limits local waiting independently:

```go
statement := client.Statement("FROM events")
statementID := uuid.New()   // github.com/google/uuid
statement.ID = &statementID // Optional.
statement.MaxElapsed = 30 * time.Second
statement.MaxEstimated = 10 * time.Second
handle, err := statement.Submit(ctx)
if err != nil {
	return err
}
fmt.Println("statement ID:", handle.ID())
```

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

The Catalog API reads complete definitions and replaces resources atomically.

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
// Review plan.Before, plan.After, and plan.Destructive before applying.
result, err := catalog.Apply(ctx, definition, scopedb.CatalogOptions{
	IfMatch: plan.ETag,
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

## Append rows

Use `AppendStream` to write Go structs or maps to an existing table. The SDK encodes and batches the rows for you.

### Queue rows and wait for delivery

Create a stream with the default options, add rows with `Send`, and call `Shutdown` when you are done. Struct fields use standard Go JSON tags.

```go
table := client.Table("events")

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
table := client.Table("events")
telemetry, err := table.AppendStream(scopedb.AppendStreamOptions{
	FailurePolicy: scopedb.AppendFailureContinue,
})
if err != nil {
	return err
}

if err := telemetry.TrySend(map[string]any{
	"id":   3,
	"name": "request.completed",
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

`TrySend` adds a row without waiting for queue space. Check the report returned by `Flush` or `Shutdown`: `CommittedRows` counts confirmed writes, `FailedRows` counts failed rows, `UnknownRows` counts writes without a confirmed result, and `DroppedRows` counts rows rejected by `TrySend`. Each report covers activity since the previous completed barrier, including barriers whose caller stopped waiting. `Stats()` holds lifetime totals, pending rows/bytes, in-flight batches, and the latest failure diagnostics. Admission errors returned by `Send` are not counted as dropped rows; count those at the producer. Calling `Shutdown` repeatedly returns its original final report, so do not add it to totals twice. If a `Flush` wait is interrupted, that barrier still completes and advances the report interval; use lifetime counters and `Stats().LastReport` to inspect delivery rather than assuming a later report includes those rows. Continue mode does not retain failed payloads for replay; use an application-owned durable queue when losing rows is unacceptable.

For a fleet already seeing HTTP 429s, start with one stream per destination in each process and `MaxConcurrentBatches: 1`. Concurrency is per stream: twelve processes with a default stream each can issue up to 48 concurrent append requests. This bounds requests in flight, not requests per second or total volume. Observe `Stats().Retries`, `PendingRows`, `PendingBytes`, and `LastFailure.HTTPStatus`; reduce producer rate or coordinate admission across processes if the shared limit is still exceeded. Retries use jittered backoff and honor `Retry-After` even when it exceeds `MaxBackoff`; the delivery budget bounds the wait.

### Delivery and resource lifetime

Streams retry temporary rejections, including HTTP 429 responses without ingest details and connection failures that are known to occur before dispatch. A connection timeout is retryable; caller cancellation and certificate verification failures are not. HTTP 503 responses without ingest details remain unknown because the write may have committed. The default allows eight retries with bounded backoff and a five-minute elapsed budget per dispatched batch. Missing responses and unknown outcomes stop delivery under the default stop policy. If the application accepts possible duplicates, opt into at-least-once replay:

```go
stream, err := table.AppendStream(scopedb.AppendStreamOptions{
	Retry: &scopedb.AppendRetryOptions{MaxRetries: 8, RetryUnknown: true},
})
if err != nil {
	return err
}
// Send rows, then call stream.Shutdown(ctx) and inspect its report and error.
```

A later successful acknowledgement confirms the logical rows were written at least once, not how many copies exist. A later rejection cannot resolve an earlier unknown attempt. `RetryUnknown` includes timeouts after connection acquisition; cancellation and exhaustion of the delivery budget stop retries. `Retry: &scopedb.AppendRetryOptions{MaxRetries: 0}` disables all SDK retries. Direct `AppendNDJSON` always makes one SDK attempt.

The default stream targets 4 MiB batches, flushes partial batches after one second, accepts at most 200,000 rows per batch, and sends up to four batches concurrently. Each request is capped at 8 MiB of uncompressed NDJSON; streams split batches automatically at either protocol limit. `MaxBufferedBytes` defaults to 64 MiB. Set `MaxConcurrentBatches: 1` for serial requests. `MaxBufferedBytes` bounds accepted encoded rows, including in-flight batches; it excludes temporary encoding/compression buffers and rows held by blocked `Send` callers. Limit producer concurrency when bounding total process memory.

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

One request supports up to 8 MiB of uncompressed NDJSON and 200,000 rows. Direct appends do not split oversized input or retry; the caller owns that request boundary.

### JSON encoding for Append

Each nonblank line must be a JSON object keyed by destination column name. `Send` uses standard `encoding/json`, including struct tags and custom `MarshalJSON` methods, without schema-aware conversions. Local admission checks only that the encoded value is an object within the row-size limit. ScopeDB validates the destination fields and types when it processes the batch.

| Column type | JSON input                                              | Go value or example                                                                                                                                                       |
| ----------- | ------------------------------------------------------- | ------------------------------------------------------------------------------------------------------------------------------------------------------------------------- |
| `int`       | Signed 64-bit integer token                             | `int64(42)`; quoted numbers, `42.0`, and `42e0` are not accepted integer tokens.                                                                                          |
| `uint`      | Nonnegative integer token through `9223372036854775807` | `uint64(42)`; larger values are rejected even though query results support the full `uint64` range.                                                                       |
| `float`     | Finite number                                           | `float64(1.5)`; integer tokens must be exactly representable as a float. Overflow and nonzero values that underflow to zero are rejected.                                 |
| `boolean`   | Boolean                                                 | `true` or `false`.                                                                                                                                                        |
| `string`    | String                                                  | `"hello"`; numbers and objects are not implicitly converted to text.                                                                                                      |
| `timestamp` | RFC 3339 string with an explicit offset                 | `time.Time` or `"2026-10-23T12:34:56.123456789Z"`; values must fit signed 64-bit nanoseconds since the Unix epoch. Numeric epochs and timezone-free strings are rejected. |
| `interval`  | Fixed-duration ISO 8601 `PT` string                     | `"PT1H2M3.000000004S"` or `"-PT0.5S"`; up to nine fractional second digits, within signed 64-bit nanoseconds. Calendar years/months/days are not accepted.                |
| `binary`    | Canonical padded standard Base64 string                 | A non-nil `[]byte` encodes automatically; `[]byte("hello")` becomes `"aGVsbG8="`.                                                                                         |
| `object`    | JSON object                                             | A struct, map, or `json.RawMessage` containing an object; pass the object rather than serialized JSON in a string.                                                        |
| `array`     | JSON array                                              | A slice or array; elements may have different JSON types.                                                                                                                 |
| `any`       | Any JSON value                                          | Numbers, booleans, strings, arrays, and objects retain their JSON kinds. A timestamp-looking string remains a string.                                                     |

Missing ordinary columns and explicit `null` become SQL NULL. Nested nulls in objects and arrays remain JSON nulls. Computed columns must be omitted. Unknown fields, duplicate object keys (including nested keys), and type mismatches reject the entire request; other requests in a stream may already have committed. Inspect `Error.AppendDetails.RowErrors` and `RowErrorsTruncated` for diagnostics. With the default stop policy, these failures surface from `Flush` or `Shutdown`; continue mode reports them in delivery counts and `Stats().LastFailure`.

ScopeDB parses all NDJSON integer tokens as signed 64-bit integers, including values inside objects and arrays. `encoding/json` preserves integer-valued Go types without a floating-point conversion, but values above `9223372036854775807` remain invalid input. Avoid a numeric field's `json:",string"` option for integer columns. An integer-valued Go float may encode without a decimal point; for floats beyond the signed integer range, supply an explicit floating-point JSON token with `json.RawMessage`, such as `1e20`. NaN and infinities fail local JSON encoding.

`time.Duration` encodes as an integer count of nanoseconds, which is not accepted for interval columns; format a `PT` string instead. `time.Time` preserves fractional seconds, but still must fit the timestamp range above. Nil slices, maps, and pointers normally encode as `null`; a nil top-level row is not an object and fails admission. Preserve exact imported JSON with `json.RawMessage`, or decode with `json.Decoder.UseNumber` before editing it. The [bulk import example](../examples/patterns/bulk_append) preserves numbers without passing through `float64`.

See the examples for [streaming writes](../examples/append_stream), [file imports](../examples/patterns/bulk_append), [telemetry](../examples/patterns/telemetry), and [NDJSON](../examples/append_ndjson).

## Timeouts and HTTP options

`NewClient` validates the endpoint and returns an error for invalid configuration. Set `Config.HTTPClient` when the application needs to own HTTP timeouts, proxies, TLS, or connection pooling. `Client.Close` closes idle connections only for the HTTP client created by the SDK; it never closes a caller-provided client. SDK-owned clients use Go's default redirect policy. To customize redirects, supply an `http.Client` with `CheckRedirect` through `Config.HTTPClient`.

[Go's default redirect behavior](https://pkg.go.dev/net/http#Client.Do) changes POST requests to GET without a body for HTTP 301, 302, and 303; HTTP 307 and 308 preserve the method and body when the body can be recreated. Redirects happen within one SDK attempt; `AppendRetryOptions` does not control them and `Stats().Retries` does not count them.

Every table append or Catalog mutation HTTP request has an SDK timeout of 30 seconds by default. Queries and Catalog reads have no additional SDK deadline; bound them with the caller context or HTTP client. `Config.WriteTimeout` is a `*time.Duration`: nil uses the default, a pointer to zero disables the limit, and a pointer to a positive duration overrides it. Negative durations are invalid. `NewClient` copies the value, so changing the original variable does not change an existing client.

```go
client, err := scopedb.NewClient(scopedb.Config{
	Endpoint:     os.Getenv("SCOPEDB_ENDPOINT"),
	APIKey:       os.Getenv("SCOPEDB_API_KEY"),
	WriteTimeout: new(2 * time.Minute), // Omit for 30 seconds; use new(time.Duration) to disable.
})
if err != nil {
	return err
}
defer client.Close()
```

The write timeout starts after local encoding, compression, and request preparation and covers one HTTP attempt, including redirects and reading the complete response body. Caller context deadlines on direct appends and Catalog requests still apply, as do HTTP-client timeouts. Catalog dry runs use the same timeout but cannot commit, so they do not return `WriteDetails` or report an unknown commit.

Streams inherit the write timeout when `AppendStreamOptions.AttemptTimeout` is nil; a non-nil pointer overrides it, with zero disabling the per-attempt limit. The stream copies this value at construction. Its separate delivery/retry budget still applies. Admission, retry backoff, and an entire `Flush`/`Shutdown` barrier are outside the write timeout. Contexts passed to stream methods bound admission or waiting, without cancelling already accepted rows.

For Append, client timeouts after a connection is acquired preserve the existing `*scopedb.Error` kind and cause and add `WriteDetails` with `Reason: scopedb.WriteErrorReasonTimeout` and `Outcome: scopedb.AppendStateUnknown`; `Error.Retryable` is false. Connection failures before dispatch report `AppendStateRejected`; a timeout in that phase can be retried safely. A custom HTTP transport that does not provide connection trace events is classified conservatively as unknown. In-flight caller cancellation uses `WriteErrorReasonCancelled`; cancellation before sending preserves the original context error. These fields remain available on subsequent errors returned by the stream. A timeout does not roll back a write. Append streams can replay attempt timeouts with `AppendRetryOptions.RetryUnknown`, accepting possible duplicates within the delivery budget; cancellation still stops retries. A server HTTP 408 is a server response; an explicitly rejected append follows the safe retry policy.

Statement and table append request bodies, including direct `AppendNDJSON` requests and `AppendStream` batches, use zstd compression by default. Set `Config.Compression` to `CompressionGzip` when gzip is required. Catalog requests use uncompressed JSON. Append limits are based on the uncompressed NDJSON body.

## Errors

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

`Retryable` classifies a temporary failure; it does not prove that replay is safe. For direct appends, require both `Retryable` and `AppendDetails.Status == scopedb.AppendStateRejected` before retrying, and honor `RetryAfter` within an application-owned budget. An unknown outcome may already be committed and needs reconciliation. Avoid wrapping a retrying stream in a second retry loop.

After successful submission, errors from `Query` and `Execute` carry `Error.StatementID` and have `Retryable == false`: repeating either method would submit another execution. After deciding a polling failure is recoverable, use `client.StatementHandle(*scopeErr.StatementID).Wait(recoveryCtx)` with a live context to resume the same statement. Check that `StatementID` is non-nil first. Errors from a retained handle's `Refresh` or `Wait` keep their temporary classification. Polling failures are returned without automatic retries. Transport and local context errors retain their causes through `errors.Is` and `errors.As`.

A lost submission acknowledgement, malformed success response, or unstructured HTTP 408/429/5xx response uses code `statement_submission_outcome_unknown` and is not marked retryable. Set `Statement.ID` before submission to retain a caller-known ID on submission errors even when the response is lost. An error's ID identifies the attempted statement; it does not prove acceptance or guarantee that the server still retains its result. Confirmed temporary submission rejections retain their retryable classification.

A terminal observation cached by a concurrent handle request takes precedence over a late request failure. For example, once `Cancel` confirms cancellation, an older failing poll reports that cancellation rather than its stale HTTP error.
