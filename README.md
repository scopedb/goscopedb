# ScopeDB SDK for Go

[![Version](https://img.shields.io/github/v/tag/scopedb/goscopedb?filter=v*&sort=semver)](https://pkg.go.dev/github.com/scopedb/goscopedb) [![Docs](https://img.shields.io/badge/docs-scopedb.io-blue)](https://www.scopedb.io/docs/developer/go) [![CI](https://img.shields.io/github/actions/workflow/status/scopedb/goscopedb/ci.yml?branch=main&label=CI)](https://github.com/scopedb/goscopedb/actions/workflows/ci.yml) [![License](https://img.shields.io/badge/license-Apache--2.0-blue)](https://www.apache.org/licenses/LICENSE-2.0)

The official Go client for [ScopeDB](https://www.scopedb.io). Run ScopeQL queries, browse the catalog, and write rows with bounded asynchronous streams. For application writes, start with `Table.AppendStream`.

Upgrading from v0.6? Follow the [v0.7 migration guide](docs/migration-0.7.md), including the move from Ingest to Append.

## Install

Requires Go 1.27.3 or later. Use the SDK in a trusted server-side process and keep API keys out of source control.

From your application module:

```sh
go get github.com/scopedb/goscopedb@latest
```

## Run a query

Set `SCOPEDB_ENDPOINT` and `SCOPEDB_API_KEY` using the [connection guide](https://www.scopedb.io/docs/guides/connect-to-scopedb). Save this program as `main.go` and run `go run .`:

```go
package main

import (
	"context"
	"fmt"
	"log"
	"os"

	scopedb "github.com/scopedb/goscopedb"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	client, err := scopedb.NewClient(scopedb.Config{
		Endpoint: os.Getenv("SCOPEDB_ENDPOINT"),
		APIKey:   os.Getenv("SCOPEDB_API_KEY"),
	})
	if err != nil {
		return err
	}
	defer client.Close()

	result, err := client.Query(context.Background(), "SELECT 1 AS ready")
	if err != nil {
		return err
	}
	rows, err := result.ToObjects()
	if err != nil {
		return err
	}
	fmt.Println(rows)
	return nil
}
```

`Query` waits for the result. `ToObjects` returns rows keyed by column name; integers retain their full `int64`/`uint64` precision. See [result conversion](docs/usage.md#read-query-results) for other representations.

## Write rows

Inside `run`, using the client above and an existing `events` table with `id int` and `name string` columns:

```go
ctx := context.Background()
stream, err := client.Table("events").AppendStream(scopedb.AppendStreamOptions{})
if err != nil {
	return err
}
if err := stream.Send(ctx, map[string]any{"id": 1, "name": "page_view"}); err != nil {
	_, _ = stream.Shutdown(ctx)
	return err
}
report, err := stream.Shutdown(ctx)
if err != nil {
	return err
}
fmt.Println("written rows:", report.CommittedRows)
```

`Send` queues a row; `Shutdown` waits for pending writes and closes the stream. Use `Flush` to wait while keeping it open. If you already have NDJSON, use [`Table.AppendNDJSON`](docs/usage.md#append-ndjson-directly).

## Find what you need

| Task                         | Documentation                                                         |
| ---------------------------- | --------------------------------------------------------------------- |
| Read query results           | [Result conversion](docs/usage.md#read-query-results)                 |
| Track or cancel a query      | [Statement handles](docs/usage.md#track-or-cancel-a-query)            |
| Initialize a table           | [Startup definitions](docs/usage.md#initialize-a-table)               |
| Read or change table schemas | [Catalog](docs/usage.md#manage-the-catalog)                           |
| Encode rows for Append       | [JSON encodings](docs/usage.md#json-encoding-for-append)              |
| Tune batching and retries    | [Delivery and lifetime](docs/usage.md#delivery-and-resource-lifetime) |
| Write logs or telemetry      | [Continue mode](docs/usage.md#continue-after-write-errors)            |
| Set timeouts or HTTP options | [Client configuration](docs/usage.md#timeouts-and-http-options)       |
| Inspect a failure            | [Errors](docs/usage.md#errors)                                        |
| Upgrade from v0.6            | [Migration guide](docs/migration-0.7.md)                              |

For complete programs, see the [examples](examples/README.md). The [SDK guide](https://www.scopedb.io/docs/developer/go), [API reference](https://pkg.go.dev/github.com/scopedb/goscopedb), and [changelog](CHANGELOG.md) provide additional context. Report problems through [GitHub issues](https://github.com/scopedb/goscopedb/issues).

For ScopeQL syntax, see the [quickstart](https://www.scopedb.io/docs/guides/quickstart), [query guide](https://www.scopedb.io/docs/guides/query-events), and [language reference](https://www.scopedb.io/docs/reference).

## Development

Development tasks are defined in [mise.toml](mise.toml):

```sh
mise install
mise run check
mise run build
mise run test
```

For live endpoint tests, see [protocol validation](docs/development.md). Release history and the maintainer runbook are in [CHANGELOG.md](CHANGELOG.md) and [RELEASE.md](RELEASE.md).

## License

Licensed under the [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0).
