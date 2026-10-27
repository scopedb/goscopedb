# Live protocol validation

From a source checkout, set `SCOPEDB_ENDPOINT` and `SCOPEDB_API_KEY` for a disposable test workspace, then run:

```sh
mise run test-integration
```

The lifecycle test creates one uniquely named table and checks dry-run, ETag conflicts, destructive deletion protection, direct and streamed appends, statement errors, and cancellation after completion. It also checks signed integer boundaries, precise nested integers, the server's NDJSON unsigned-input limit, and full-range unsigned query results. Tests delete their resources before returning. Running cancellation, network failures, retry budgets, and concurrent stream settlement are covered with controlled HTTP responses and virtual time in the unit tests.

With `SCOPEDB_API_KEY` set, the authentication test verifies that an unauthenticated append returns a non-retryable 401 rejection and leaves the owner's table empty.

To verify tenant isolation between two tenants, also set `SCOPEDB_WORKSPACE_ID`, `SCOPEDB_SECONDARY_WORKSPACE_ID`, and `SCOPEDB_SECONDARY_API_KEY` for two distinct workspaces served by the same endpoint. Two clients create same-named tables with different rows, read only their own data, and cannot read the other tenant's Statement. Use disposable credentials with create/drop access to `scopedb.public`; the tests neither create nor revoke credentials.
