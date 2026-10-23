/*
 * Copyright 2024 ScopeDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/*
Package scopedb provides the official Go client for ScopeDB. It supports
ScopeQL statements, declarative catalog management, and streaming writes.

Create and close one Client for the application's connection pool:

	client, err := scopedb.NewClient(scopedb.Config{
		Endpoint: os.Getenv("SCOPEDB_ENDPOINT"),
		APIKey:   os.Getenv("SCOPEDB_API_KEY"),
	})
	if err != nil {
		return err
	}
	defer client.Close()

Query waits for a statement result, which can be converted to keyed objects:

	result, err := client.Query(ctx, "SELECT 1 AS ready")
	if err != nil {
		return err
	}
	rows, err := result.ToObjects()
	if err != nil {
		return err
	}
	fmt.Println(rows)

Use Statement.Submit when an application needs the statement ID, a local status
snapshot, an explicit remote status request, or a separate wait. Omit ID to let
ScopeDB generate it. MaxElapsed and MaxEstimated set server-side duration limits.
Failed statements expose the server message and code on Error, with structured
details on Error.StatementDetails.
Client.Catalog reads and replaces the complete tenant catalog. Its Database,
Schema, Table, and View selectors provide typed Get, Apply, and Delete operations.
Raw retains JSON definitions including fields unknown to the SDK. Apply supports
dry runs, ETag preconditions, and authorization for destructive changes.
Table.Apply initializes a complete table definition before writing; options are
optional and potentially destructive changes are rejected by default.
Table.Describe returns a CatalogTable definition.

For application writes, Table.AppendStream is the recommended path:
it accepts typed rows and adds bounded asynchronous batching that is safe for
concurrent producers. Table.AppendStream and Table.AppendNDJSON use the
client's configured compression, which defaults to zstd. Table.AppendNDJSON is
the low-level path for one caller-owned NDJSON request.

AppendStream.Send uses encoding/json and accepts typed structs and other values
that encode as a top-level JSON object. Send does not validate the destination
table schema. ScopeDB validation failures surface when Flush or Shutdown
settles: the stop policy returns the error, while continue mode reports failed
rows in the delivery report and failure details in Stats().LastFailure.

Stream Send methods confirm local admission only. Flush and Shutdown wait for
the accepted prefix. AppendStream reports rejected and unknown outcomes.

ScopeQL is documented separately in the [quickstart], [query guide], and
[language reference].

[quickstart]: https://www.scopedb.io/docs/guides/quickstart
[query guide]: https://www.scopedb.io/docs/guides/query-events
[language reference]: https://www.scopedb.io/docs/reference
*/
package scopedb
