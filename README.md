# ScopeDB SDK for Go

[![Version](https://img.shields.io/github/v/tag/scopedb/goscopedb?filter=v*&sort=semver)](https://pkg.go.dev/github.com/scopedb/goscopedb) [![Docs](https://img.shields.io/badge/docs-scopedb.io-blue)](https://www.scopedb.io/docs/developer/go) [![CI](https://img.shields.io/github/actions/workflow/status/scopedb/goscopedb/ci.yml?branch=main&label=CI)](https://github.com/scopedb/goscopedb/actions/workflows/ci.yml) [![License](https://img.shields.io/badge/license-Apache--2.0-blue)](https://www.apache.org/licenses/LICENSE-2.0)

The official Go client for [ScopeDB](https://www.scopedb.io). Run ScopeQL queries, browse the catalog, and write rows with bounded asynchronous streams. For application writes, start with `Table.AppendStream`.

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

| Task                                 | Documentation                                      |
| ------------------------------------ | -------------------------------------------------- |
| Query, manage schemas, or write rows | [Usage guide](docs/usage.md)                       |
| Encode rows and tune delivery        | [Write contracts](docs/writes.md)                  |
| Configure timeouts and HTTP          | [Client configuration](docs/configuration.md)      |
| Diagnose failures and recover        | [Error handling](docs/errors.md)                   |
| Run complete programs                | [Examples](examples/README.md)                     |
| Upgrade an application               | [Release notes and migration guides](CHANGELOG.md) |

The [API reference](https://pkg.go.dev/github.com/scopedb/goscopedb) describes individual types and methods. For ScopeQL syntax, see the [query guide](https://www.scopedb.io/docs/guides/query-events) and [language reference](https://www.scopedb.io/docs/reference). Report problems through [GitHub issues](https://github.com/scopedb/goscopedb/issues).

## Contribute

See the [contribution guide](CONTRIBUTING.md) for development setup and checks.

## License

Licensed under the [Apache License, Version 2.0](https://www.apache.org/licenses/LICENSE-2.0).
