# Handling errors

Inspect `*scopedb.Error` for the server code, structured details, HTTP status, request ID, and retry delay. Use `errors.Is` for underlying transport/context causes and sentinels; do not parse message text to decide whether an operation committed.

```go
var scopeErr *scopedb.Error
if errors.As(err, &scopeErr) {
	log.Printf("kind=%s code=%s status=%d request_id=%s",
		scopeErr.Kind, scopeErr.Code, scopeErr.HTTPStatus, scopeErr.RequestID)
}
```

`Kind` identifies the operation-level category. Unknown server codes remain available in `Code`, and `Details` retains code-specific JSON. `ServerRetryable` preserves the server's hint; `Retryable` reflects SDK classification. Neither alone proves that replaying a write is safe.

## Statements

A failed execution exposes `StatementDetails`; confirmed server cancellation is identified by `errors.Is(err, scopedb.ErrStatementCancelled)`. A local `context.Canceled` stops the client's request or wait without confirming server cancellation.

After successful submission, `Query` and `Execute` errors retain `StatementID` and have `Retryable == false`, because calling them again would submit a new execution. After deciding a polling failure is recoverable, resume the same statement:

```go
var scopeErr *scopedb.Error
if errors.As(err, &scopeErr) && scopeErr.StatementID != nil {
	result, err := client.StatementHandle(*scopeErr.StatementID).Wait(recoveryCtx)
	if err != nil {
		return err
	}
	fmt.Println(result.TotalRows)
}
```

A lost or malformed submission acknowledgement, or an unstructured HTTP 408/429/5xx response, reports `statement_submission_outcome_unknown`. Set `Statement.ID` before submission when recovery requires a caller-known ID. That ID identifies the attempted execution; it does not prove acceptance or guarantee result retention.

Handle polling returns errors without automatic retries and retains their temporary classification. A cached terminal observation takes precedence over an older in-flight response or failure.

## Catalog mutations

`CatalogOutcome == CatalogMutationRejected` confirms rejection. `CatalogMutationUnknown` means the mutation may have committed, including after a coded gateway failure, timeout, transport error, or invalid acknowledgement. The original `Code` and diagnostics remain available. Read the current definition and plan again before deciding to replay.

Reads, dry runs, and local validation failures have no mutation outcome. Catalog writes make one attempt. A stale `ExpectedVersion` fails with HTTP 412 and `precondition_failed`; read the latest version before revising the change.

## Append

Use `AppendDetails.Status` to distinguish `AppendOutcomeFailed` from `AppendOutcomeAmbiguous`. For direct-appended rows, require both a definite failure and a retryable classification before implementing an automatic retry. Follow the [write retry contract](writes.md#outcomes-and-retries), including `RetryAfter` and an application-owned budget.

`WriteDetails` records a client timeout or cancellation and its outcome. A failure proven to occur before dispatch is definite; an interrupted in-flight write may have committed. Stream errors and `Stats().LastFailure` preserve the last attempt's diagnostics while the aggregate outcome remains ambiguous if any unresolved attempt may have committed.

## Response limits

HTTP error bodies and Append acknowledgements are limited to 64 KiB after decompression. `errors.Is(err, scopedb.ErrResponseTooLarge)` identifies overflow. HTTP metadata remains available, but the oversized body is discarded. Oversized Append responses remain ambiguous, including HTTP 429. Successful Statement results and Catalog definitions are outside this diagnostic limit.
