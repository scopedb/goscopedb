# Write encoding and delivery

For sending structs, maps, or existing NDJSON, start with [Append rows](usage.md#append-rows). This guide explains how to choose limits and interpret delivery results.

## Row encoding

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

Missing ordinary columns and explicit `null` become SQL NULL; nested nulls remain JSON nulls. Omit computed columns. Unknown fields, duplicate object keys, and type mismatches reject the entire request. Other requests in the stream may already have committed. Modern errors describe the whole request; `RowErrors` and `RowErrorsTruncated` preserve optional legacy diagnostics only. Do not infer row outcomes from message text.

ScopeDB parses all NDJSON integer tokens as signed 64-bit integers, including values inside objects and arrays. `encoding/json` preserves integer-valued Go types without a floating-point conversion, but values above `9223372036854775807` remain invalid input. Avoid a numeric field's `json:",string"` option for integer columns. An integer-valued Go float may encode without a decimal point; for floats beyond the signed integer range, supply an explicit floating-point JSON token with `json.RawMessage`, such as `1e20`. NaN and infinities fail local JSON encoding.

`time.Duration` encodes as an integer count of nanoseconds, which is not accepted for interval columns; format a `PT` string instead. `time.Time` preserves fractional seconds, but still must fit the timestamp range above. Nil slices, maps, and pointers normally encode as `null`; a nil top-level row is not an object and fails admission. Preserve exact imported JSON with `json.RawMessage`, or decode with `json.Decoder.UseNumber` before editing it. The [bulk import example](../examples/patterns/bulk_append) preserves numbers without passing through `float64`.

## Batching and backpressure

| Setting                | Default  | Effect                                                              |
| ---------------------- | -------- | ------------------------------------------------------------------- |
| `TargetBatchBytes`     | 4 MiB    | Seals a batch near this uncompressed size.                          |
| `MaxBatchRows`         | 200,000  | Seals a batch at this row count.                                    |
| `FlushInterval`        | 1 second | Sends a partial batch after its first row has waited this long.     |
| `MaxConcurrentBatches` | 4        | Bounds concurrent requests per stream. Set 1 for serial submission. |
| `MaxBufferedBytes`     | 64 MiB   | Bounds accepted encoded rows, including in-flight batches.          |

Each Append request allows at most 8 MiB of uncompressed NDJSON and 200,000 rows. Streams split batches at these limits; direct `AppendNDJSON` calls do not split input. The byte budget excludes temporary encoding/compression buffers and records held by blocked `Send` callers, so also limit producer concurrency when bounding process memory.

`Send` waits for capacity; `TrySend` returns an admission error immediately when capacity is unavailable. Both confirm local admission, not remote delivery. If a producer fails partway through its input, settle the accepted rows before deciding what to replay.

Concurrency is per stream, not per process or tenant. When rate limited, reduce `MaxConcurrentBatches` and producer rate before adding retries. Observe pending bytes, in-flight batches, retries, and `Stats().LastFailure`; coordinate admission across processes when they share a service limit.

## Outcomes and retries

A successful Append requires HTTP 200 and a valid `num_rows_inserted` matching the submitted row count. The wire response does not require `status`; a legacy `status: "committed"` is also accepted.

| Response or failure                                           | Outcome                  | Default stream retry                                   |
| ------------------------------------------------------------- | ------------------------ | ------------------------------------------------------ |
| Valid nested HTTP 429 error                                   | `AppendOutcomeFailed`    | Yes, within the retry budget.                          |
| Other valid nested HTTP 4xx error, including 408              | `AppendOutcomeFailed`    | No.                                                    |
| HTTP 5xx, transport failure after dispatch, or client timeout | `AppendOutcomeAmbiguous` | No.                                                    |
| Missing, malformed, truncated, or inconsistent response       | `AppendOutcomeAmbiguous` | No, including HTTP 429.                                |
| Temporary connection failure proven to precede dispatch       | `AppendOutcomeFailed`    | Yes; cancellation and certificate errors are excluded. |

Known error codes must agree with their HTTP status. Unknown codes are preserved and follow the status guarantee when the response is otherwise valid. Optional legacy rejection details cannot make a 5xx response definite. An ambiguous outcome means rows may already have committed or may still commit later.

Streams default to eight retries and a five-minute delivery budget per dispatched batch. Backoff starts at 100 ms, grows to 5 seconds, and includes jitter. `Retry-After` is a minimum delay even when it exceeds `MaxBackoff`; the delivery budget still bounds the wait. `Retry: &scopedb.AppendRetryOptions{MaxRetries: 0}` disables SDK retries. Direct `AppendNDJSON` makes one SDK attempt.

Set `RetryAmbiguous: true` only when the application accepts duplicates or can reconcile them:

```go
stream, err := table.AppendStream(scopedb.AppendStreamOptions{
	Retry: &scopedb.AppendRetryOptions{MaxRetries: 8, RetryAmbiguous: true},
})
if err != nil {
	return err
}
// Send rows, then call Shutdown and inspect its report and error.
```

Opt-in replay covers transient ambiguous failures. It does not override client timeouts or cancellations after connection acquisition, explicit server retry refusal, or the delivery budget. A later success confirms at least one copy; a later definite failure cannot resolve an earlier ambiguous attempt. Avoid wrapping a retrying stream in another retry loop.

## Delivery reports

The default failure policy stops admission after a failed batch. `AppendFailureContinue` processes later batches and returns delivery reports even when some writes fail. Neither mode retains failed payloads for replay; keep an application-owned durable source when losing rows is unacceptable.

| Counter         | Meaning                                                                                         |
| --------------- | ----------------------------------------------------------------------------------------------- |
| `AcceptedRows`  | Rows admitted locally.                                                                          |
| `CommittedRows` | Rows confirmed committed.                                                                       |
| `FailedRows`    | Admitted rows rejected remotely or discarded before dispatch.                                   |
| `AmbiguousRows` | Admitted rows whose commit result cannot be determined.                                         |
| `DroppedRows`   | Records rejected by `TrySend`; failures returned by `Send` are counted by the producer instead. |

`Flush` and `Shutdown` report activity since the previous completed barrier. `Outcome` summarizes whether that interval succeeded, partially succeeded, failed, or remains ambiguous. `Stats()` contains lifetime counters, pending rows/bytes, in-flight batches, and the most recent report and failure.

Cancelling a barrier wait does not cancel delivery: the barrier still completes and advances the reporting interval. Inspect `Stats().LastReport` and lifetime counters rather than assuming the next `Flush` includes those rows. Repeated `Shutdown` calls return the same final report; do not add it to totals twice.

## Closing and cancellation

Always call `Shutdown` with a live context to close admission and settle accepted rows, including after a producer or encoding error. A cancelled `Send` that has not admitted its row leaves it unaccepted. Cancelling a `Flush` or `Shutdown` wait neither abandons accepted rows nor rolls back writes; wait again with a live context when needed.

`Client.Close` releases idle connections and does not close streams. Configure request deadlines separately from admission and barrier waits; see [timeouts](configuration.md#timeouts).
