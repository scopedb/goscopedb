# Upgrade from 0.6 to 0.7

Published v0.7.0 and v0.7.1 require Go 1.26 or later. For newer versions, check the minimum in the [README](../README.md#install). The module and import paths remain `github.com/scopedb/goscopedb`. These changes are relative to the published v0.6.3 SDK.

## Write timeouts

Direct `AppendNDJSON` requests now have a default 30-second timeout, including reading the response body. Configure `Config.WriteTimeout` to change or disable this limit. Catalog mutations and dry runs use the same setting; queries and Catalog reads are governed by the caller's context and HTTP client.

```go
client, err := scopedb.NewClient(scopedb.Config{
	Endpoint:     os.Getenv("SCOPEDB_ENDPOINT"),
	APIKey:       os.Getenv("SCOPEDB_API_KEY"),
	WriteTimeout: new(time.Duration), // Disable the SDK's write attempt timeout.
})
if err != nil {
	return err
}
defer client.Close()
```

`AppendStreamOptions.AttemptTimeout` changes from `time.Duration` to `*time.Duration`. Omit it to inherit the client setting, replace a positive `AttemptTimeout: duration` with `AttemptTimeout: new(duration)`, or set `new(time.Duration)` to disable the attempt limit. An explicit `AttemptTimeout: 0` in old source must become an omitted field or nil to preserve the default. Negative values are invalid.

Caller context deadlines and HTTP-client timeouts still apply. Append streams also have a five-minute default delivery/retry budget per dispatched batch; disabling the attempt timeout does not disable that budget. See [timeouts and HTTP options](usage.md#timeouts-and-http-options) for timeout scope and interruption diagnostics.

## Statements and Catalog

| v0.6 API                                           | v0.7 replacement                                                                                        |
| -------------------------------------------------- | ------------------------------------------------------------------------------------------------------- |
| `handle.LastStatus()`, `Progress()`, `ResultSet()` | `handle.Snapshot()` and its `Status`, `Progress`, `ResultSet`, and `Error` fields                       |
| `handle.Status(ctx)`                               | `handle.Refresh(ctx)`, returning a complete `*StatementSnapshot`                                        |
| `StatementCancelResult`                            | `*StatementSnapshot` from `Cancel`; `ID` replaces `StatementID`, and `Error` holds the original failure |
| `status.Finished()`                                | `status == scopedb.StatementStatusFinished`; `Terminated()` still covers all terminal states            |
| `table.Describe(ctx)`                              | `table.Catalog().Get(ctx)`, retaining both `Definition` and `ETag`                                      |
| `table.Drop(ctx)`                                  | `table.Catalog().Delete(ctx, scopedb.CatalogOptions{IfMatch: etag, AllowDestructive: true})`            |
| Append result/error/last-failure `.AppendState`    | `.Status`; append JSON uses `status`, matching the service                                              |
| Historical `u_int` type spelling                   | `uint`                                                                                                  |

Replace `ExecTimeout` strings such as `"30s"` with `MaxElapsed: 30 * time.Second`; `MaxEstimated` independently limits estimated execution time. Zero omits the setting: the server defaults to one hour for elapsed time and no estimate limit. Negative durations are invalid, and elapsed time includes queueing. `StatementErrorCode` remains a string-backed type on `StatementErrorDetails.Code`; `Error.Code` exposes the same code as a string. Replace `StatementErrorCodePrepareError` and `StatementErrorCodeExecuteError` with `StatementErrorCodeFailed`. The pending/execution timeout and heartbeat constants retain their names but now use `statement_`-prefixed values; replace comparisons against old string literals. The row/scan limit constants have been removed, and `StatementErrorCodeEstimateLimitExceeded` is new. Unknown codes remain available. `ErrStatementCancelled` identifies server cancellation independently of a local `context.Canceled`.

Replace paginated Catalog list/fetch/iterator methods with `client.Catalog().Get(ctx)` or a database/schema/table/view selector followed by `.Get(ctx)`. Use `Distinct` and `RetentionDays` in the complete table definition. Typed reads reject unsupported fields; `Raw()` retains them without numeric conversion. Catalog writes are not retried.

## Append and IngestStream

The `AppendState` enum values and stream delivery/report types remain. Missing or unreadable outcomes are unknown. As in v0.6.3, unknown outcomes are not retried by default; the new `RetryUnknown` option enables explicit replay when the application accepts possible duplicates.

`Client.IngestStream`, `IngestStream`, `IngestStreamOptions`, and `IngestResult` have been removed. Select the destination with `client.Table(name)` and use `Table.AppendStream` for Go values or `Table.AppendNDJSON` for an existing NDJSON payload.

Append accepts rows matching the destination columns, not a ScopeQL statement. Move field renaming, type conversion, filtering, and routing into the producer before calling `Send`. For transformations that must run in ScopeQL, append the original event into an `object` column in a staging table, then execute a separate `FROM ... SELECT ... INSERT INTO ...` statement. Staging and transformation are separate commits; track processed batches before retrying the transformation.

Review these behavior changes when migrating:

- Append batches allow up to 8 MiB of uncompressed NDJSON and 200,000 rows. Reduce an old ingest batch-byte target above 8 MiB. The default stream batch target decreases from 8 MiB to 4 MiB.
- `Flush` and `Shutdown` return an `AppendDeliveryReport` for activity since the previous completed barrier, including barriers whose caller stopped waiting. Read `CommittedRows` instead of `IngestResult.NumRowsInserted`, and check `Outcome` when using the continue policy.
- Append streams send up to four batches concurrently by default. Set `MaxConcurrentBatches: 1` when the producer needs sequential requests; queries still require `ORDER BY` for ordered results.
- Append streams retry confirmed temporary rejections by default. `RetryUnknown: true` opts into retrying transient unknown outcomes, including attempt timeouts, which can duplicate rows. Cancellation and the delivery budget still stop retries. Set `MaxRetries: 0` to disable all SDK retries. Direct `AppendNDJSON` makes one SDK attempt; HTTP redirects follow the configured HTTP client's policy.

## Changes in v0.7.1

Update to v0.7.1 for safe retries of connection failures confirmed to occur before dispatch and HTTP 429 responses without structured append details. With `RetryUnknown` enabled, streams can also replay attempt timeouts within the delivery budget, accepting possible duplicates. A connection timeout known to precede dispatch does not need `RetryUnknown`; an unknown timeout does. Cancellation remains terminal.
