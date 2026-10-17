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

package integration_test

// TODO(legacy): Keep this workflow on both server versions until ScopeDB 0.5 support ends.

import (
	"context"
	"testing"
	"time"

	scopedb "github.com/scopedb/goscopedb"
	"github.com/stretchr/testify/require"
)

func TestHTTPCompatibility(t *testing.T) {
	client := NewClient(t)
	defer client.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	database, err := client.FetchDatabase(ctx, "scopedb")
	require.NoError(t, err)
	require.Equal(t, "scopedb", database.Name)
	schema, err := client.FetchSchema(ctx, "scopedb", "public")
	require.NoError(t, err)
	require.Equal(t, "scopedb", schema.Database)
	var schemas []string
	for item, err := range client.IterateSchemas(ctx, "scopedb", scopedb.CatalogListOptions{PageSize: 1}) {
		require.NoError(t, err)
		schemas = append(schemas, item.Name)
	}
	require.Contains(t, schemas, "public")
	require.Contains(t, schemas, "system")

	table := client.Table(RandomName(t))
	_, err = client.Statement("create table " + table.Identifier() + " (id int)").Execute(ctx)
	require.NoError(t, err)
	defer func() { require.NoError(t, table.Drop(context.Background())) }()
	_, err = table.AppendNDJSON(ctx, []byte("{\"id\":1}\n"))
	require.NoError(t, err)
	statement := client.Statement("from " + table.Identifier())
	statement.MaxElapsed = 10 * time.Second
	statement.MaxEstimated = 10 * time.Second
	handle, err := statement.Submit(ctx)
	require.NoError(t, err)
	_, err = handle.Wait(ctx)
	require.NoError(t, err)

	// Use a fresh handle to force an HTTP cancel on an already finished durable statement.
	resumed := client.StatementHandle(handle.ID())
	outcome, err := resumed.Cancel(ctx)
	require.NoError(t, err)
	require.Equal(t, scopedb.StatementStatusFinished, outcome.Status)
	result, err := resumed.Wait(ctx)
	require.NoError(t, err)
	row, exists, err := result.First()
	require.NoError(t, err)
	require.True(t, exists)
	require.Equal(t, int64(1), row["id"])

	statement = client.Statement("from " + table.Identifier())
	statement.MaxElapsed = time.Nanosecond
	_, err = statement.Execute(ctx)
	var failure *scopedb.Error
	require.ErrorAs(t, err, &failure)
	require.Equal(t, scopedb.ErrorKindStatementFailed, failure.Kind)
	require.Contains(t, failure.Code, "timeout")
	require.NotEmpty(t, failure.StatementDetails.Message)
}
