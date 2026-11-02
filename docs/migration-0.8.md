# Upgrade from 0.7 to 0.8

This guide covers changes from v0.7.1 to v0.8.0. Upgrade to Go 1.27.3 or later before updating the SDK. The module and import paths remain `github.com/scopedb/goscopedb`.

## Catalog versions

Replace `CatalogSnapshot.ETag` and `CatalogResult.ETag` with `Version`, and replace `CatalogOptions.IfMatch` with `ExpectedVersion`:

```go
resource := client.Table("events").Catalog()
snapshot, err := resource.Get(ctx)
if err != nil {
    return err
}
snapshot.Definition.Comment = new("Updated events")
preview, err := resource.Apply(ctx, snapshot.Definition, scopedb.CatalogOptions{
    DryRun: true, ExpectedVersion: snapshot.Version,
})
if err != nil {
    return err
}
_, err = resource.Apply(ctx, snapshot.Definition, scopedb.CatalogOptions{
    ExpectedVersion: preview.Version,
})
return err
```

The opaque version represents the whole tenant catalog, even when reading one table. Preserve it including quotes. Changes to unrelated catalog resources can invalidate it; data appends do not advance it. Dry runs return the base version, writes return the resulting version, and no-op writes retain the version. The SDK carries the token through HTTP `ETag` and `If-Match` headers. Stale versions fail atomically with HTTP 412 and `precondition_failed`.

Use `Error.CatalogOutcome` instead of checking an SDK-generated `catalog_commit_outcome_unknown` code. `CatalogMutationRejected` confirms rejection; `CatalogMutationUnknown` means the mutation may have committed, including coded gateway failures, timeouts, transport failures, and invalid acknowledgements. The original server `Code`, `Details`, and HTTP metadata remain available. Read the current definition before deciding whether to replay. Reads, dry runs, and local validation failures have an empty outcome.

## Append outcomes and replay

| Before                          | After                               |
| ------------------------------- | ----------------------------------- |
| `AppendState`                   | `AppendOutcome`                     |
| `AppendStateCommitted`          | `AppendOutcomeSucceeded`            |
| `AppendStateRejected`           | `AppendOutcomeFailed`               |
| `AppendStateUnknown`            | `AppendOutcomeAmbiguous`            |
| `RetryUnknown`                  | `RetryAmbiguous`                    |
| `UnknownRows`, `UnknownBatches` | `AmbiguousRows`, `AmbiguousBatches` |
| `AppendDeliveryOK`              | `AppendDeliverySucceeded`           |
| `AppendDeliveryUnknown`         | `AppendDeliveryAmbiguous`           |

`AppendOutcome` retains the historical string/JSON spellings `committed`, `rejected`, and `unknown`. Delivery outcome `String()` values change from `ok` / `unknown` to `succeeded` / `ambiguous`; update telemetry consumers. Confirmed row counts remain `CommittedRows` and `NumRowsInserted`.

Append acknowledgements no longer require a wire `status` field. If a custom transport or proxy inspects responses, accept HTTP 200 with a matching `num_rows_inserted`. Modern errors describe the whole request; use `Code` and `Details` instead of relying on per-row diagnostics. See the [delivery contract](writes.md#outcomes-and-retries) for response validation.

Streams automatically retry valid HTTP 429 rejections and temporary connection failures proven to precede dispatch. Unlike 0.7.1, a bodyless, malformed, or truncated 429 is ambiguous and is not replayed by default. Valid HTTP 408 failures are not automatically retried, even with a positive server retry hint. Client timeouts after connection acquisition are no longer replayed even with `RetryAmbiguous: true`. Timeouts proven to precede dispatch remain safe to retry; cancellation remains terminal.

`RetryAmbiguous` permits bounded replay of other transient ambiguous failures and may duplicate rows. `Error.ServerRetryable` retains the server hint; explicit refusal prevents replay even with opt-in enabled. A later definite failure cannot resolve an earlier ambiguous attempt. Direct `AppendNDJSON` makes one SDK attempt.

## Statement recovery and diagnostics

After submission, `Query` and `Execute` failures now have `Retryable == false` and retain `Error.StatementID`. Resume that statement with `StatementHandle(id).Wait(ctx)` instead of submitting the query again. Set `Statement.ID` before submission if the application also needs recovery when the submission response is lost. See [statement recovery](errors.md#statements).

HTTP error bodies and Append acknowledgements are now limited to 64 KiB. Use `errors.Is(err, scopedb.ErrResponseTooLarge)` to detect overflow; HTTP metadata remains available, but the oversized body is discarded. Successful query results and Catalog definitions are not subject to this diagnostic limit.
