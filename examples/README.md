# ScopeDB Go SDK examples

Run these examples from the repository root. They use only the public SDK API.

## Query and catalog examples

| Example | Shows | Run |
| --- | --- | --- |
| [`statement`](statement) | Run a query and read its results | `go run ./examples/statement` |
| [`catalog`](catalog) | List databases, schemas, and tables, and inspect table metadata | `go run ./examples/catalog` |

The shared helper reads:

- `SCOPEDB_ENDPOINT` (defaults to `http://127.0.0.1:6543`)
- `SCOPEDB_API_KEY` (falls back to `SCOPEDB_TOKEN`)
- `SCOPEDB_DATABASE` (defaults to `scopedb`)
- `SCOPEDB_SCHEMA` (defaults to `public`)

ScopeQL is documented in the [quickstart], [query guide], and [language reference].

[quickstart]: https://docs.scopedb.io/guides/quickstart
[query guide]: https://docs.scopedb.io/guides/query-events
[language reference]: https://docs.scopedb.io/reference/

## Before running a write example

Set `SCOPEDB_WRITE_TABLE` to an existing table name. Configure its database and schema with `SCOPEDB_DATABASE` and `SCOPEDB_SCHEMA`:

```sh
export SCOPEDB_WRITE_TABLE=sdk_example_events
```

The write examples use these columns:

| Column        | Value used by examples |
| ------------- | ---------------------- |
| `id`          | integer                |
| `event_id`    | string                 |
| `occurred_at` | timestamp              |
| `name`        | string                 |
| `attributes`  | object                 |

## Choose a write example

Start with `append_stream` to write Go structs. The SDK encodes and batches the rows for you.

| Example | Choose it when | Run |
| --- | --- | --- |
| [`append_stream`](append_stream) | Write Go structs with an append stream | `go run ./examples/append_stream` |
| [`bulk_append`](patterns/bulk_append) | Write a large collection of rows | `go run ./examples/patterns/bulk_append` |
| [`telemetry`](patterns/telemetry) | Write logs or events with `TrySend` and continue after write errors | `go run ./examples/patterns/telemetry` |
| [`append_ndjson`](append_ndjson) | Write data that is already encoded as NDJSON | `go run ./examples/append_ndjson` |

## Using an append stream

1. Create a stream with `table.AppendStream(scopedb.AppendStreamOptions{})`.
2. Add rows to the SDK's queue with `Send(ctx, row)`.
3. Call `Shutdown(ctx)` when you are done to wait for writes to finish and close the stream.

Call `Flush(ctx)` if you need to wait for current writes while keeping the stream open. Check the errors returned by each call.

The telemetry example uses `TrySend` to add rows without waiting for queue space. It sets `FailurePolicy: scopedb.AppendFailureContinue` and checks the report returned by `Shutdown`.

See the [streaming writes guide](../README.md#streaming-writes) for code snippets and report fields.

## Advanced: server-side transformation

Use [`ingest_transform`](ingest_transform) only when source JSON specifically needs a ScopeQL transformation before it can match the destination table. For normal typed events, use `Table.AppendStream`.

```sh
go run ./examples/ingest_transform
```

## Compile every example

```sh
go test ./...
```
