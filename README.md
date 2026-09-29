# ScopeDB SDK for Go

[![Apache License, Version 2.0](https://img.shields.io/:license-Apache%202-brightgreen.svg)](https://www.apache.org/licenses/LICENSE-2.0.txt) [![Go Reference](https://pkg.go.dev/badge/github.com/scopedb/goscopedb.svg)](https://pkg.go.dev/github.com/scopedb/goscopedb)

The ScopeDB Go SDK supports ScopeQL statements, REST catalog discovery, and bounded asynchronous streaming writes. For application writes, start with `Table.AppendStream`.

## Runtime and installation

The SDK requires Go 1.24 or later.

```sh
go get github.com/scopedb/goscopedb@latest
```

## Create a client

Pass the ScopeDB endpoint and API key through application configuration. Keep API keys out of source control.

```go
client, err := scopedb.NewClient(scopedb.Config{
	Endpoint: os.Getenv("SCOPEDB_ENDPOINT"),
	APIKey:   os.Getenv("SCOPEDB_API_KEY"),
})
if err != nil {
	return err
}
defer client.Close()
```

`NewClient` validates the endpoint and returns an error for invalid configuration. Set `Config.HTTPClient` when the application needs to own HTTP timeouts, proxies, TLS, or connection pooling. `Client.Close` closes idle connections only for the HTTP client created by the SDK; it never closes a caller-provided client.

Request bodies, including direct `AppendNDJSON` requests and `AppendStream` batches, use zstd compression by default. Set `Config.Compression` to `CompressionGzip` when gzip is required. Append limits are based on the uncompressed NDJSON body.

## ScopeQL documentation

The SDK executes ScopeQL but does not define the language. Start with the language documentation:

- [Quickstart](https://docs.scopedb.io/guides/quickstart)
- [Query guide](https://docs.scopedb.io/guides/query-events)
- [Language reference](https://docs.scopedb.io/reference/)

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

Use `RawRows` for unconverted wire values, `ToValues` for positional values, `ToObjects` for values keyed by column name, or `First` for an optional first row.

For a detached or long-running statement, keep its handle and choose between a local status snapshot, one remote status request, or waiting for the result:

```go
handle, err := client.Statement("SELECT 1 AS ready").Submit(ctx)
if err != nil {
	return err
}

fmt.Println("statement ID:", handle.ID())
if cached := handle.LastStatus(); cached != nil {
	fmt.Println("cached status:", *cached) // No network request.
}

latest, err := handle.Status(ctx) // Fetches one remote snapshot while active.
if err != nil {
	return err
}
fmt.Println("latest status:", latest)

result, err = handle.Wait(ctx) // Polls until the statement terminates.
if err != nil {
	return err
}
```

Once a handle has a terminal status, `Status` returns the cached status without another request. Store `handle.ID()` and use `client.StatementHandle(id)` to resume the lifecycle in another process. `Cancel` returns the statement ID, creation time, status, and server message. If cancellation finds that a statement already finished or failed, `Wait` fetches the complete statement response needed for its result or structured failure details. A cancelled outcome uses the cancellation message directly.

`Statement.ID` and `Statement.ExecTimeout` are the only optional statement settings. Provide an ID when the application needs to choose the statement ID; otherwise ScopeDB generates one. `StatementHandle.ID()` always returns the ID confirmed by ScopeDB:

```go
statement := client.Statement("FROM events")
statementID := uuid.New()
statement.ID = &statementID // Optional.
statement.ExecTimeout = "30s"
handle, err := statement.Submit(ctx)
if err != nil {
	return err
}
fmt.Println("statement ID:", handle.ID())
```

When `Wait` or `Execute` returns a `*scopedb.Error` with kind `ErrorKindStatementFailed`, `StatementDetails` preserves the server's structured error code, message, and code-specific JSON details. The outer error message remains the server's top-level statement message:

```go
var scopeErr *scopedb.Error
if errors.As(err, &scopeErr) && scopeErr.StatementDetails != nil {
	fmt.Println("statement error code:", scopeErr.StatementDetails.Code)
	fmt.Println("statement error:", scopeErr.StatementDetails.Message)
	fmt.Println("details:", string(scopeErr.StatementDetails.Details))
}
```

## Browse the REST catalog

List methods expose one explicit page. Iterators lazily request later pages and are the simpler choice for discovery:

```go
for database, err := range client.IterateDatabases(ctx, scopedb.CatalogListOptions{
	PageSize: 100,
}) {
	if err != nil {
		return err
	}
	fmt.Println(database.Name)
}

for table, err := range client.IterateTables(
	ctx,
	"scopedb",
	"public",
	scopedb.CatalogListOptions{PageSize: 100},
) {
	if err != nil {
		return err
	}
	fmt.Println(table.Name)
}
```

Use `ListDatabases`, `ListSchemas`, or `ListTables` when the application owns page boundaries. Use `FetchDatabase`, `FetchSchema`, or `FetchTable` for a full resource.

## Describe a table

The table helper defaults to database `scopedb` and schema `public`. Set both explicitly when the destination is application-configured:

```go
table := client.Table("events")
table.Database = "scopedb"
table.Schema = "public"

description, err := table.Describe(ctx)
if err != nil {
	return err
}
fmt.Println(description.Columns)
```

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

`TrySend` adds a row without waiting for queue space. Check the report returned by `Flush` or `Shutdown`: `CommittedRows` counts confirmed writes, `FailedRows` counts failed rows, `UnknownRows` counts writes without a confirmed result, and `DroppedRows` counts rows rejected by `TrySend`. Each report covers activity since the previous report.

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

One request supports up to 8 MiB of uncompressed NDJSON and 200,000 rows.

See the examples for [streaming writes](examples/append_stream), [file imports](examples/patterns/bulk_append), [telemetry](examples/patterns/telemetry), and [NDJSON](examples/append_ndjson).

## Advanced: transform before writing

Use `Client.IngestStream` only when source JSON specifically needs a server-side ScopeQL transformation before it can match the destination table. For normal typed events, shape the row in the producer and use `Table.AppendStream`. See the guarded [`ingest_transform`](examples/ingest_transform) example for the advanced path.

## Structured errors

Server error messages pass through unchanged. `scopedb.Error` adds structured diagnostics without requiring applications to parse the message:

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

The main kinds are `ErrorKindConfigInvalid`, `ErrorKindStatementFailed`, `ErrorKindAppendRowsFailed`, and `ErrorKindUnexpected`. Use `errors.Is` and `errors.As` to inspect the underlying cause.

## Examples and development

The [examples guide](examples/README.md) contains query and write examples with runnable commands. Development tasks are defined in [mise.toml](mise.toml); license tasks expect `hawkeye` on `PATH`.

```sh
mise install
mise run check
mise run build
mise run test
```

Release notes and the maintainer runbook are in [CHANGELOG.md](CHANGELOG.md) and [RELEASE.md](RELEASE.md).

## License

This software is licensed under the [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0.txt).
