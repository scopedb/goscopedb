# Upgrade from 0.7 to 0.8

The next minor release aligns Catalog concurrency and Append delivery with the current API contract. These changes are currently unreleased; update application code when upgrading to 0.8. Go 1.27.3 or later is required.

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

HTTP 200 with a matching `num_rows_inserted` confirms success without requiring a wire `status` field. Legacy `status: "committed"` acknowledgements remain accepted. Valid nested HTTP 4xx errors establish failure; HTTP 5xx errors remain ambiguous. Known codes must match their HTTP status; unknown codes are retained. Missing or malformed bodies, inconsistent codes, invalid acknowledgements, and row-count mismatches are ambiguous. Optional legacy row diagnostics remain available, but modern errors describe the entire request; do not parse messages for per-row outcomes.

Streams automatically retry valid HTTP 429 rejections and temporary connection failures proven to precede dispatch. Unlike 0.7.1, a bodyless, malformed, or truncated 429 is ambiguous and is not replayed by default. Valid HTTP 408 failures are not automatically retried, even with a positive server retry hint. Client timeouts after connection acquisition are no longer replayed even with `RetryAmbiguous: true`. Timeouts proven to precede dispatch remain safe to retry; cancellation remains terminal.

`RetryAmbiguous` permits bounded replay of other transient ambiguous failures and may duplicate rows. `Error.ServerRetryable` retains the server hint; explicit refusal prevents replay even with opt-in enabled. A later definite failure cannot resolve an earlier ambiguous attempt. Direct `AppendNDJSON` makes one SDK attempt.
