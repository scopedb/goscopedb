# Client configuration

Create a client with `NewClient(Config{Endpoint: ..., APIKey: ...})` and reuse it across queries and writes. The SDK validates configuration at construction. `APIKey` supplies bearer authentication; keep credentials in trusted server-side code.

## Timeouts

| Operation                                            | Deadline                                                                  |
| ---------------------------------------------------- | ------------------------------------------------------------------------- |
| Query, Statement request, Catalog read               | Caller context or configured HTTP client.                                 |
| Direct Append, Catalog mutation or dry run           | `Config.WriteTimeout`, default 30 seconds.                                |
| Stream HTTP attempt                                  | `AppendStreamOptions.AttemptTimeout`, inheriting `WriteTimeout` when nil. |
| Stream batch delivery, including retries and backoff | `AppendRetryOptions.MaxElapsedTime`, default 5 minutes.                   |
| Stream admission or barrier wait                     | Context passed to `Send`, `Flush`, or `Shutdown`.                         |

`WriteTimeout` and `AttemptTimeout` are `*time.Duration`: nil inherits the default, a pointer to zero disables the attempt limit, and a positive value overrides it. Negative values are invalid. The SDK copies these settings when constructing the client or stream.

```go
client, err := scopedb.NewClient(scopedb.Config{
	Endpoint:     os.Getenv("SCOPEDB_ENDPOINT"),
	APIKey:       os.Getenv("SCOPEDB_API_KEY"),
	WriteTimeout: new(2 * time.Minute),
})
if err != nil {
	return err
}
defer client.Close()
```

An attempt timeout starts after local encoding, compression, and request preparation, and covers redirects and reading the response body. Caller context and HTTP-client deadlines still apply. Disabling an attempt timeout does not disable a stream's delivery budget.

Stream method contexts bound admission or waiting; they do not cancel already accepted rows. A local query deadline does not cancel the remote statement. Use `StatementHandle.Cancel` when server cancellation is required. Timeouts do not roll back writes; inspect [error outcomes](errors.md) before replaying them.

## HTTP ownership

By default the SDK owns an independent HTTP client and keeps up to 100 idle HTTP/1.1 connections per host. `Client.Close` releases those idle connections. Supply `Config.HTTPClient` to configure HTTP timeouts, proxies, TLS, connection pooling, or redirect policy. A supplied client remains caller-owned and is never closed by the SDK.

Redirects follow the configured HTTP client's policy and happen within one SDK attempt. Append retry settings do not control redirects, and stream retry counters do not count them.

## Compression

Statement and Append request bodies use zstd by default. Set `Config.Compression` to `CompressionGzip` when gzip is required. Catalog requests use uncompressed JSON. Append limits apply to the uncompressed NDJSON body.
