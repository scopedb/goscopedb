# ScopeDB Go SDK examples

Run these examples from the repository root. They use only the public SDK API.

## Query and catalog examples

| Example                  | Shows                                              | Run                           |
| ------------------------ | -------------------------------------------------- | ----------------------------- |
| [`statement`](statement) | Run a query and read its results                   | `go run ./examples/statement` |
| [`catalog`](catalog)     | Read a schema definition and list its tables       | `go run ./examples/catalog`   |

The shared helper reads:

- `SCOPEDB_ENDPOINT` (defaults to `http://127.0.0.1:6543`)
- `SCOPEDB_API_KEY` (falls back to `SCOPEDB_TOKEN`)
- `SCOPEDB_DATABASE` (defaults to `scopedb`)
- `SCOPEDB_SCHEMA` (defaults to `public`)

ScopeQL is documented in the [quickstart], [query guide], and [language reference].

[quickstart]: https://www.scopedb.io/docs/guides/quickstart
[query guide]: https://www.scopedb.io/docs/guides/query-events
[language reference]: https://www.scopedb.io/docs/reference

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

| Example                               | Choose it when                                                      | Run                                                      |
| ------------------------------------- | ------------------------------------------------------------------- | -------------------------------------------------------- |
| [`append_stream`](append_stream)      | Write Go structs with an append stream                              | `go run ./examples/append_stream`                        |
| [`bulk_append`](patterns/bulk_append) | Import an NDJSON file with bounded memory                           | `go run ./examples/patterns/bulk_append < events.ndjson` |
| [`telemetry`](patterns/telemetry)     | Write logs or events with `TrySend` and continue after write errors | `go run ./examples/patterns/telemetry`                   |
| [`append_ndjson`](append_ndjson)      | Write one already encoded NDJSON request                            | `go run ./examples/append_ndjson`                        |

## Using an append stream

1. Create a stream with `table.AppendStream(scopedb.AppendStreamOptions{})`.
2. Add rows to the SDK's queue with `Send(ctx, row)`.
3. Call `Shutdown(ctx)` when you are done to wait for writes to finish and close the stream.

Call `Flush(ctx)` if you need to wait for current writes while keeping the stream open. Check the errors returned by each call.

The telemetry example uses `TrySend` to add rows without waiting for queue space. It sets `FailurePolicy: scopedb.AppendFailureContinue` and checks the report returned by `Shutdown`.

See the [streaming writes guide](../README.md#streaming-writes) for code snippets and report fields.

## Import an NDJSON file

The `bulk_append` example reads standard input one line at a time, skips blank lines, and passes JSON objects to `Send` without converting numbers through `float64`. Each input line must fit within 8 MiB, including its line ending. A file for the example table can contain:

```jsonl
{"id":1,"event_id":"import-1","occurred_at":"2026-10-23T00:00:00Z","name":"checkout.completed","attributes":{"store":"north"}}
{"id":2,"event_id":"import-2","occurred_at":"2026-10-23T00:00:01Z","name":"checkout.completed","attributes":{"store":"south"}}
```

Run `go run ./examples/patterns/bulk_append < events.ndjson`. The stream applies backpressure as its queue fills and `Shutdown` waits for accepted rows to settle. On an input or write error, the example reports the error and the delivery counts before exiting with a nonzero status.

Some rows may commit before an import fails. This example disables automatic retries; reconcile an incomplete import before rerunning the file, since replaying it can duplicate rows. Append does not perform ScopeQL transformations. See the [v0.7 migration guide](../README.md#append-and-ingeststream) for server-side transformations and delivery differences.

## Compile every example

```sh
go test ./...
```
