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

`Statement.ID` chooses the statement ID; otherwise ScopeDB generates one. `StatementHandle.ID()` returns the ID confirmed by ScopeDB. `MaxElapsed` is a `time.Duration` that limits server-side time from submission, including queueing, preparation, and execution. Polling does not reset it. Zero uses the server default of one hour; negative durations are invalid. A context deadline limits local waiting independently:

```go
statement := client.Statement("FROM events")
statementID := uuid.New()   // github.com/google/uuid
statement.ID = &statementID // Optional.
statement.MaxElapsed = 30 * time.Second
handle, err := statement.Submit(ctx)
if err != nil {
	return err
}
fmt.Println("statement ID:", handle.ID())
```

For statement failure details and resuming after a transport error, see [error handling](errors.md#statements).

## Manage the catalog

### Initialize a table

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

The default call applies immediately and rejects changes that may lose data. Applying the same definition again preserves existing rows. This is complete replacement: omitted fields and objects are cleared. If an older application version omits a physical column added by a newer version, its startup apply fails rather than deleting that column. Use `table.Catalog().Get(ctx)` to read the current definition and its catalog version. Use `table.Catalog().Delete(ctx, options)` for conditional deletion with explicit destructive-change authorization.

Pass one `CatalogOptions` value when you need a dry run, a version precondition, or explicit authorization for destructive changes. The same optional options are accepted by Catalog `Apply` and `Delete`.

### Review and apply a change

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
plan, err := catalog.Apply(ctx, definition, scopedb.CatalogOptions{DryRun: true, ExpectedVersion: snapshot.Version})
if err != nil {
	return err
}
// Review plan.Before, plan.After, and plan.Destructive before applying.
result, err := catalog.Apply(ctx, definition, scopedb.CatalogOptions{
	ExpectedVersion: plan.Version,
})
if err != nil {
	return err
}
fmt.Println(result.Version)
```

`client.Catalog()` selects the entire tenant. Use `Database(name)`, `Schema(database, schema)`, `Table(database, schema, table)`, or `View(database, schema, view)` to select an individual resource. Pass names directly; the SDK escapes each name for the HTTP path. Each resource includes its complete subtree.

`Get` returns a `CatalogSnapshot[T]` with a typed definition and an opaque `Version`. Pass that version to the first dry run or write after editing; otherwise a change between Get and preview can be overwritten. `Apply` accepts the same definition type and returns `CatalogResult[T]` with typed `Before` and `After` values. These definitions are pointers: `Before` is nil when creating a resource, and `After` is nil when deleting it.

Omitted fields and objects are cleared, and omitted collections are empty. `Delete` removes one database, schema, table, or view using the same options; clear the tenant by applying `&scopedb.CatalogDocument{}`, or clear children by editing their parent definition. `ExpectedVersion` is optional, while potentially irreversible data loss requires `AllowDestructive`. Dry runs validate and roll back without requiring that authorization. Storage is a creation-only input omitted from exports, so keep the original definition when planning creation.

Typed reads reject unknown fields, including nested fields, instead of silently discarding them before a later replacement. For export and apply workflows that must preserve fields unknown to this SDK, select `Raw()`. Definitions and before/after states are then `json.RawMessage`; absent states are JSON `null`:

```go
catalog := client.Catalog().Raw()
snapshot, err := catalog.Get(ctx)
if err != nil {
	return err
}
// Retain snapshot.Definition as raw JSON when saving or editing it.
plan, err := catalog.Apply(ctx, snapshot.Definition, scopedb.CatalogOptions{DryRun: true, ExpectedVersion: snapshot.Version})
if err != nil {
	return err
}
fmt.Println(string(plan.After))
```

`Raw()` is also available on each resource selector. Typed and raw resources share the same HTTP behavior, options, and errors.

Catalog writes make one attempt. If a mutation fails, inspect its [commit outcome](errors.md#catalog-mutations) before replaying it.

`Version` covers the entire tenant catalog, including when reading a single table: unrelated catalog changes can invalidate it, while data appends do not advance it. Preserve the token including its quotes. Dry runs return the base version; writes return the resulting version, and no-op writes retain the version. `ExpectedVersion` is checked atomically; stale tokens fail with HTTP 412 and `precondition_failed`.

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
if report.Outcome != scopedb.AppendDeliverySucceeded {
	log.Printf("some telemetry rows were not confirmed written: %+v", report)
}
```

`TrySend` attempts admission without waiting for queue space. In continue mode, a nil barrier error does not mean every row committed: inspect `report.Outcome` and the row counters. See [delivery reports](writes.md#delivery-reports) for interval and lifetime accounting.

Choose batching, concurrency, and retry settings using the [write delivery guide](writes.md).

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

See [row encoding](writes.md#row-encoding) for column types and numeric boundaries.
