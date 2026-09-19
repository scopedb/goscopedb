# Append delivery baseline

This document specifies the Go `AppendStream` baseline for an at-least-once
integration. Rust and JavaScript should port the semantics and fault scenarios;
API spelling can remain idiomatic. The cross-SDK comparison below is pinned to
specific source snapshots; it does not describe every SDK version.

## Contract and ownership

`Send` returning nil means local admission. A successful **stop-mode** `Flush`
confirms the prefix admitted before its barrier committed; `Shutdown` closes
admission and waits for all admitted rows. Neither local admission nor a
nonzero committed-row count accompanying an error is a source checkpoint.

Transient failures are retried with the same encoded NDJSON, including failures
where the server may have committed before the response was lost. Delivery can
therefore create duplicates. A successful acknowledgement resolves delivery for
that logical batch, not the number of physical copies in the table. There is no
exactly-once guarantee or defined commit order across concurrent batches.

Retries are finite. Permanent errors or exhaustion stop admission and retain
unconfirmed data for application-owned recovery. At-least-once across crashes or
outages exceeding the retry budget requires a replayable source or durable
outbox, retained until commit is acknowledged. An in-memory SDK cannot provide
that guarantee by itself. `Continue` mode and rejected `TrySend` calls can lose
data and must not be represented as reliable source delivery.

| Component | Responsibility |
| --- | --- |
| ScopeDB append endpoint | Return committed acknowledgement according to its durability contract; report explicit rejection when available |
| SDK stream | Encode once; batch; bound admitted encoded bytes and concurrency; retry transient failures; expose barriers, counters and terminal recovery |
| Application / connector | Retain source data until commit, manage source checkpoints, persist recovery data if required, handle permanent errors |
| Source | Provide replay within its retention window; any upstream best-effort delivery remains best effort |

This adds no server deduplication registry, per-producer sequence tracking,
managed queue or SDK filesystem spool. Retention uses the admitted encoded-byte
budget. Serialization before admission, payload assembly, compression and HTTP
buffers add process-memory overhead; `MaxBufferedBytes` is not an RSS limit.
Applications should also bound their producer concurrency.

## Retry policy

| Condition | Default stream behavior |
| --- | --- |
| Valid committed acknowledgement for every row | Complete logical batch and release its budget |
| Explicitly rejected and service marks retryable | Retry |
| Unknown with transport failure, timeout, HTTP 408/429/5xx, or invalid 2xx acknowledgement | Retry; duplicates are possible |
| Permanent rejection, or unstructured permanent HTTP 4xx such as 401/413 | Stop; retain payload; do not retry unchanged indefinitely |
| Retry count / elapsed budget exhausted | Stop and retain unconfirmed payload |
| Earlier unknown attempt followed by permanent rejection | Stop, but retain overall state `unknown` and last attempt's diagnostic metadata |
| Another batch triggers stop | Interrupt retry waits; allow already-running HTTP requests to settle within their timeout |

Defaults remain 4 MiB target batches, 1 s flush interval, 64 MiB admitted encoded
bytes, four concurrent batches, and 30 s per HTTP attempt. Protocol maximums
remain 8 MiB uncompressed and 200,000 rows per request.

New retry defaults: eight retries after the initial attempt, exponential backoff
from 100 ms to 5 s, equal jitter between half and the full current backoff, and
five minutes total elapsed time per dispatched batch. `Retry-After` is a minimum
wait, even above the backoff cap. If the elapsed budget ends during that wait,
the batch stops without an early retry. Queueing time is outside this budget.
`errors.Is(err, scopedb.ErrAppendRetryExhausted)` identifies exhaustion;
`errors.As` still exposes the last append error and its diagnostic metadata.

A nil `Retry` selects defaults. A non-nil `AppendRetryOptions` uses its explicit
`MaxRetries`; zero disables retries. Zero durations select defaults. For example:

```go
stream, err := table.AppendStream(scopedb.AppendStreamOptions{
    FlushInterval: 5 * time.Second,
    Retry: &scopedb.AppendRetryOptions{
        MaxRetries: 8,
        MaxElapsedTime: 5 * time.Minute,
    },
})
```

`RejectedOnly: true` restores the prior conservative classification; provide
`MaxRetries: 8` as well to retain eight retries. Direct `AppendNDJSON` remains a
single request. `IngestStream` transformations are outside this change.

## Recovery and lifecycle

`TakeUncommitted(ctx)` closes admission, waits for workers to settle, then
transfers failed and unsent encoded batches in admission order. Successfully
acknowledged batches are excluded. This is available only in stop mode.

- Non-empty batches can accompany the terminal delivery error. Process the
  batches even when the error is non-nil.
- A caller wait timeout before settlement transfers nothing. Call again after
  workers finish. Cancelling a wait is not rollback or proof of non-commit.
- Once transferred, bytes are caller-owned and each batch is returned only once.
  Keep them until persisted or successfully replayed. Subsequent calls return
  no batches and may still return the terminal error.
- `unknown` means earlier attempts may have committed. `rejected` on an unsent
  retained batch means it was never submitted; its error identifies the failure
  that stopped the stream, not validation of that unsent payload.
- A process crash destroys memory, including retained payloads. A durable source
  remains necessary when crash recovery is required.

Recovery is intentionally explicit: a schema error stops the stream; the
application can correct the table or source row, replay retained data and open
a new stream. There is no implicit bad-row dropping or poison-batch splitting.
`CommittedRows` counts logical acknowledgements, `Retries` updates while retrying,
and `RetainedRows` / `RetainedBytes` expose data awaiting recovery.

## Source checkpoint pattern

For a sequential source, have one owner coordinate admission and checkpoints:

1. Read a bounded interval after the durable source checkpoint.
2. `Send` every row in that interval. On admission failure, leave its checkpoint
   unchanged and stop that source owner.
3. `Flush`. On any error, leave the checkpoint unchanged. Stop and settle the old
   stream before opening a replacement so independent retry owners do not race.
4. Only after successful commit, persist the source checkpoint for that interval.
5. If checkpoint persistence fails or the process crashes before it, replay the
   interval from the previous durable checkpoint. Duplicate rows are allowed.

Do not derive offsets from `CommittedRows`: concurrently submitted batches can
finish out of order. For partitioned sources, checkpoints must advance only over
a fully acknowledged prefix per partition. A separate stream per partition is a
simple starting point, with a shared process budget or bounded active partition
count to avoid multiplying memory and connections without bound.

Long-lived processes can use the SDK directly. Short-lived functions must await
`Flush`/`Shutdown` before returning or hand events to an existing durable queue.
An HTTP event receiver should acknowledge upstream only after committed append,
or after durable acceptance into its own queue. A collector is useful when the
source cannot run the SDK or needs protocol adaptation; it does not remove the
same checkpoint and crash-recovery obligations.

## Cross-SDK comparison

Source snapshots inspected on 2026-09-19. Only the Go implementation and local
fault tests were executed in this work; the other columns are source review.

| Capability | Go baseline | Rust `694899a` | JS `0199a17` |
| --- | --- | --- | --- |
| Batching, byte backpressure, bounded concurrency | Present | Present | Present |
| Explicit retryable rejection | Retry | Retry | Retry |
| Transient unknown outcome | Retry by default | Stops / accounts for failure | Stops / accounts for failure |
| Retry configuration | New public options | Public options | Public options |
| Jitter | Equal jitter | No jitter in append retry loop | No jitter in append retry loop |
| Retry-After above backoff cap | Preserve minimum wait | Capped | Capped |
| Total elapsed retry budget | Five minutes by default | Count and per-attempt timeout | Count and per-attempt timeout |
| Failed / unsent payload handoff | `TakeUncommitted` in stop mode | No equivalent handoff found | No equivalent handoff found |
| Stop / best-effort continue | Both | Both; also circuit breaker | Both; also circuit breaker |

Go base: `ea96286` in [goscopedb](https://github.com/scopedb/goscopedb/tree/ea96286).
Rust source: [append_stream.rs](https://github.com/scopedb/scopedb-client/blob/694899a/scopedb-client/src/append_stream.rs).
JS source: [append-stream.ts](https://github.com/scopedb/scopedb-js/blob/0199a17/src/append-stream.ts).
Verify these snapshots when porting; this matrix is not a perpetual claim about
upstream behavior.

## Portable acceptance tests

The Go fault suite is in `append_delivery_test.go`; existing stream tests cover
barriers, row validation, limits, serialization and concurrent admission.

| Scenario | Required result |
| --- | --- |
| Server commits, then loses the acknowledgement | Same payload retried; logical row commits; duplicate physical rows allowed |
| Attempt timeout then successful retry | Committed report; retry visible |
| Sustained transient outage | Pending bytes bounded; `Send` backpressures; `TrySend` reports full; retry can recover |
| Retry exhaustion / retries disabled | Expected attempt count; unconfirmed payload recoverable |
| Permanent 401/413/schema failure | No unchanged retry loop; payload recoverable |
| Rejected-only compatibility option | Unknown not retried |
| Earlier unknown, later rejected | Overall unknown preserved; last HTTP status available |
| Retry-After exceeds elapsed budget | No retry before Retry-After; terminal recovery remains available |
| Permanent failure while another batch sleeps for Retry-After | Sleep interrupted; recovery does not wait for the full delay |
| Failure with queued unsent data | All failed and unsent payloads returned in admission order exactly once |
| Recovery wait times out while another request is in flight | No premature transfer; later recovery excludes successful in-flight batches |
| Schema corrected after failure | Returned NDJSON can be appended successfully |
| Cancellation immediately before admission | No accepted row and no leaked capacity |
| Best-effort continue mode | Failed rows counted, payloads released, recovery call rejected |

Before claiming production end-to-end delivery, connector tests must additionally
kill/restart the process before and after commit/checkpoint persistence, exercise
source replay limits, and verify backlog recovery against a real deployment.
These are connector acceptance tests, not guarantees supplied by in-memory SDK
unit tests.

## Local validation

Validated with Go 1.24.3:

- SDK unit and fault tests: `go test -race . -parallel 1 -timeout 90s`.
  Test-runner parallelism is limited because an existing ingest timeout test
  assumes the server is entered before its 25 ms deadline; it can hang under
  race instrumentation and parallel CPU load. Tests still exercise concurrent
  SDK producers, in-flight requests and recovery ownership.
- All examples: `go test -race ./examples/...` (compilation; no example test cases).
- Build and analysis: `go build ./...`, `go vet ./...`, repository-pinned
  `golangci-lint` 2.1.6, and its formatter diff check.
- Dependency and license checks: `go mod tidy -diff`, Hawkeye 7.0.0, and
  `git diff --check`.

HTTP tests use local fault-injection servers and a timeout transport stub.
Separate application-level validation used a live Bluesky collector with this SDK:
a forced process restart replayed 167 journaled events with no missing IDs, and an
independent source replay verified all 28,317 IDs across an upgrade interval.
Those checks exercised client recovery against a real endpoint; the SDK endpoint
integration suite, server node/storage failure tests, and controlled load
benchmarks were not run.
