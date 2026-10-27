# Live protocol validation

From a source checkout, set `SCOPEDB_ENDPOINT` and `SCOPEDB_API_KEY` for a disposable test workspace, then run:

```sh
mise run test-integration
```

The suite creates uniquely named resources and checks dry-run, ETag conflicts, conditional deletion, committed and rejected appends, statement errors, elapsed timeout, and a cancellation racing with completion. It also checks signed integer boundaries, precise nested integers, the server's NDJSON unsigned-input limit, and full-range unsigned query results. Tests delete their resources before returning. Running cancellation is covered separately with controlled HTTP responses in the unit tests.

With `SCOPEDB_API_KEY` set, Cloud authentication checks verify that missing, malformed, and incorrect credentials return 401 for Catalog, queries, and writes. Rejected stream writes must count as failed rather than unknown, without retries or committed rows. Optional `SCOPEDB_EXPIRED_API_KEY` and `SCOPEDB_REVOKED_API_KEY` extend these checks to expired and revoked credentials.

To verify tenant isolation through the Cloud Proxy, also set `SCOPEDB_WORKSPACE_ID`, `SCOPEDB_SECONDARY_WORKSPACE_ID`, and `SCOPEDB_SECONDARY_API_KEY` for two distinct workspaces served by the same endpoint. The test creates same-named tables with different definitions and rows, checks that authentication overrides a forged tenant header, denies cross-tenant Statement reads and cancellation, and confirms that deleting one tenant's table leaves the other's intact. Use disposable credentials with create/drop access to `scopedb.public`; the tests neither create nor revoke credentials.
