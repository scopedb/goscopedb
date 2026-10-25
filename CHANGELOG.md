# Changelog

All significant changes to the ScopeDB Go SDK are documented in this file.

## Unreleased

### Breaking Changes

* Rename Catalog snapshot/result `ETag` to `Version` and `CatalogOptions.IfMatch` to `ExpectedVersion`. Pass the opaque version from a read or dry run unchanged to reject concurrent catalog changes.

### Bug Fixes

* Classify connection failures before dispatch and HTTP 429 responses without ingest details as rejected appends. Streams can retry temporary failures safely; certificate verification errors and cancellation remain terminal. Missing outcomes after dispatch, including unstructured HTTP 503 responses, remain unknown.

## v0.7.0 (2026-10-23)

### Breaking Changes

* Require Go 1.26 or later. Upgrade the Go toolchain before updating the SDK.
* `AppendStreamOptions.AttemptTimeout` now takes `*time.Duration`. Omit it to inherit the client write timeout, use `new(duration)` with a positive duration to override it, or use `new(time.Duration)` to disable the per-attempt limit.
* Direct table appends now have a default 30-second HTTP attempt timeout, including response-body reads. Set `Config.WriteTimeout` to `new(time.Duration)` to disable the SDK limit; caller context deadlines and HTTP-client timeouts still apply. The same setting bounds Catalog mutations and dry runs.
* Replace partial Statement accessors and `StatementCancelResult` with complete `StatementSnapshot` values from `Snapshot`, `Refresh`, and `Cancel`. Confirmed cancellation is identifiable through `ErrStatementCancelled`.
* Replace `Statement.ExecTimeout` with `MaxElapsed` and `MaxEstimated` duration fields. Statement failures expose native string codes through `Error.Code` and `StatementErrorDetails.Code` instead of the previous `StatementErrorCode` type and constants.
* Replace paginated Catalog APIs and table descriptions with complete declarative definitions. Use `Table.Catalog().Get` and `Delete` instead of `Table.Describe` and `Drop`, preserving ETags and explicitly authorizing destructive changes. Typed reads reject unknown fields; use `Raw()` for forward-compatible round trips.
* Rename append outcome fields from `AppendState` to `Status` and serialize them as `status`. Remove the historical `u_int` data type spelling; use `uint`.
* Remove the legacy transform Ingest stream API. Use table append APIs and transform rows in the producer. The [v0.6 to v0.7 migration guide](README.md#migrate-from-v06-to-v07) covers all API replacements and write behavior changes.

### New Features

* Add complete Catalog replacements and deletion with dry-run, ETags, conditional updates, views, computed columns, and explicit destructive-change authorization. Preserve structured error codes/details, including through append retries and `Stats().LastFailure`, and statement compute seconds.
* Add `Config.WriteTimeout` (`*time.Duration`): nil uses 30 seconds, zero disables the limit, and a positive duration overrides it. Client interruptions expose `Error.WriteDetails` with a `timeout` or `cancelled` reason and an `unknown` outcome while preserving error kinds, causes, and HTTP diagnostics. Catalog dry runs use the same timeout without reporting an unknown commit.
* Add configurable append retries and a five-minute default delivery budget per dispatched batch. Confirmed temporary rejections remain retryable by default. `RetryUnknown` explicitly opts into replaying other transient unknown outcomes, which may duplicate rows; client timeouts and cancellations are never retried. Direct `AppendNDJSON` remains a single SDK attempt.

### Bug Fixes

* Make Statement handles safe for concurrent waiting, refresh, cancellation, and local snapshots. Late responses cannot replace a terminal result, and snapshots do not share mutable fields with the cache.
* Preserve FIFO order for streaming write byte-budget waiters so newer or smaller records cannot starve an older blocked producer.

### Improvements

* Validate result row widths and identify failing rows and columns in decoding errors. Integer results retain their full `int64` and `uint64` ranges.
* The bulk append example imports NDJSON from standard input with bounded memory and reports partial writes.
* `AppendStream` now targets 4 MiB per batch by default; the maximum configurable target and uncompressed request limit remain 8 MiB.

## v0.6.3 (2026-08-26)

### Improvements

* Direct `AppendNDJSON` requests now use the client's configured compression and reject uncompressed bodies over 8 MiB.

## v0.6.2 (2026-08-22)

### Breaking Changes

* Moved the module from `github.com/scopedb/scopedb-sdk/go` to `github.com/scopedb/goscopedb`. Applications must update their import path.

### Improvements

* `AppendStream` now caps each uncompressed NDJSON request at 8 MiB.

## v0.6.1 (2026-08-20)

### Improvements

* `AppendStream` request batches now use the client's configured compression, which defaults to zstd. Direct `AppendNDJSON` requests remain identity-encoded.

## v0.6.0 (2026-08-08)

### Breaking Changes

* Redesigned `NewClient` to accept `Config` by value and return `(*Client, error)`; invalid endpoints now fail during construction.
* Replaced the old data cable API with bounded `AppendStream` and `IngestStream` write paths.
* Redesigned asynchronous statements around `StatementHandle.ID`, optional `LastStatus`, value-returning `Status`, and `Wait`, while retaining optional caller-provided IDs through `Statement.ID`.
* Removed public result-format configuration; query results use the supported JSON wire format internally.
* Replaced table schema discovery with `Table.Describe`, which returns the complete REST catalog resource.
* Renamed the table helper's destination field from `Table` to `Name`.

### New Features

* Added `Client.Query` and expanded result conversion with `RawRows`, `ToObjects`, and `First` alongside `ToValues`.
* Added structured statement failure details, complete pruning progress, and complete cancellation results.
* Added paged and lazy-iterator REST catalog APIs for databases, schemas, and tables.
* Added direct raw NDJSON table append through `Table.AppendNDJSON`, with committed, rejected, and unknown outcomes.
* Added bounded concurrent `AppendStream` batching with strict and continue failure policies, local admission backpressure, delivery reports, and lifetime statistics.
* Added bounded sequential `IngestStream` batching for transform-oriented JSON ingest.
* Added finite, configurable per-attempt timeouts to both background write streams.
* Added caller-provided HTTP client support while preserving caller ownership.
* Added structured API errors with HTTP status, request ID, retry metadata, append details, and wrapped causes.

### Bug Fixes

* Added the missing binary and null data type constants.
* Fixed `ResultSet.ToValues` conversion for binary and interval columns.
* Fixed result field decoding for the `data_type` wire field.
* Fixed statement polling and terminal failure propagation.
* Fixed cancellation races so cached finished status does not hide the result from `Wait`.
* Prevented blocked `IngestStream` producers from being admitted after a terminal batch failure.
* Preserved configured endpoint base paths and percent-encoded individual REST resource identifiers.
* Kept REST and ScopeQL table destinations aligned when only a database is configured.

## v0.5.0 (2026-04-23)

### Breaking Changes

* POST requests now use `zstd` compression by default. Set `Compression: scopedb.CompressionGzip` when talking to older ScopeDB deployments that do not support `zstd` yet.

## v0.4.0 (2026-04-22)

### Bug Fixes

* Added API key client configuration support for authenticated requests.

## v0.3.0 (2025-07-03)

### Breaking Changes

* Reworked the Go SDK to catch up with the latest ScopeDB APIs.
* Updated the statement lifecycle API to follow the latest server changes.
* Renamed task-related APIs to job-related APIs.
* Renamed variant-related APIs to `any`.
* Renamed `VariantBatchCable` to `RawDataBatchCable`.
* `ArrowBatchCable` now takes ownership of Arrow batches.
* Removed the inaccurate `nanos_to_finish` progress field from statement responses.

### New Features

* Added `ResultSet.ToValues`.
* Added the `Table` helper.
* Improved statement response handling.

### Bug Fixes

* Fixed array aliasing between goroutines in concurrent code paths.

### Improvements

* Improved error handling.
* Caught up new response and schema types from ScopeDB.

## v0.2.6 (2025-03-04)

### New Features

* Added support for caller-provided statement IDs.

## v0.2.5 (2025-02-11)

### New Features

* Added the `X-ScopeDB-Uncompressed-Content-Length` header for POST requests.

## v0.2.4 (2025-01-22)

### New Features

* Added support for `exec_timeout`.
* Added support for gzip-compressed requests.

### Bug Fixes

* Fixed a typo in `StatementStatus`.

## v0.2.3 (2025-01-10)

### New Features

* Added richer error types and cancel responses.

## v0.2.2 (2025-01-04)

### New Features

* Added statement cancellation and progress APIs.
* Added support for ingest formats.

### Improvements

* The client now respects caller-provided context timeouts.

## v0.2.1 (2024-11-30)

### Breaking Changes

* Dropped ingest channel helper functions.

## v0.2.0 (2024-11-27)

### New Features

* Added support for the ingest v2 one-for-all API.

## v0.1.0 (2024-11-20)

### New Features

* Initial release of the ScopeDB Go SDK.
