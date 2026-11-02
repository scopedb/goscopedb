# Developing the Go SDK

Use the tool versions and tasks declared in [mise.toml](../mise.toml). The minimum supported Go version is declared in [go.mod](../go.mod).

## Local checks

From the repository root:

```sh
mise install
mise run check
mise run build
mise run test:race
go doc .
git diff --check
```

`check` runs license validation, module tidiness checks, and static analysis. `build` compiles the library and examples. `test:race` runs unit tests and benchmarks with the race detector; it does not contact a live service. CI runs these checks on Linux, macOS, and Windows.

For focused iteration, use `go test -run <pattern> .` or `go test -bench <pattern> -run '^$' .`. The unit tests use controlled transports and virtual time to exercise request failures, retry budgets, cancellation, and concurrent delivery.

## Live protocol validation

Set `SCOPEDB_ENDPOINT` and `SCOPEDB_API_KEY` for a disposable workspace, then run:

```sh
mise run test-integration
```

The integration suites create uniquely named tables, exercise Catalog, Append, and Statement behavior, and clean up their resources. Credentials need create/drop access to `scopedb.public`. The authentication test also attempts an unauthenticated write.

For tenant isolation tests, set `SCOPEDB_WORKSPACE_ID`, `SCOPEDB_SECONDARY_WORKSPACE_ID`, and `SCOPEDB_SECONDARY_API_KEY` for two distinct workspaces at the same endpoint. These tests use existing credentials; they do not create or revoke them.

## Documentation and releases

Keep the README focused on installation, a first query/write, and navigation. Put application workflows in the usage guide; place detailed write, configuration, and error contracts in their respective guides. API comments describe individual types and methods. Migration guides explain version-to-version changes, and the changelog records released behavior. Examples document their own inputs and how to run them.

Follow [RELEASE.md](../RELEASE.md) when preparing or publishing a version.
