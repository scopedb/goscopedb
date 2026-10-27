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

import (
	"context"
	"os"
	"testing"
	"time"

	scopedb "github.com/scopedb/goscopedb"
	"github.com/stretchr/testify/require"
)

func TestCatalogAppendAndStatementLifecycle(t *testing.T) {
	client := integrationClient(t, os.Getenv("SCOPEDB_API_KEY"))
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	table := client.Table(tableName())
	definition := &scopedb.CatalogTable{Name: table.Name, Columns: []scopedb.CatalogColumn{
		{Name: "i", DataType: scopedb.IntDataType}, {Name: "u", DataType: scopedb.UIntDataType}, {Name: "object", DataType: scopedb.ObjectDataType},
	}}
	plan, err := table.Catalog().Apply(ctx, definition, scopedb.CatalogOptions{DryRun: true})
	require.NoError(t, err)
	_, err = table.Catalog().Get(ctx)
	require.Equal(t, 404, httpFailure(t, err).HTTPStatus, "dry run must not create the table")
	cleanupTable(t, table)
	created, err := table.Catalog().Apply(ctx, definition, scopedb.CatalogOptions{IfMatch: plan.ETag})
	require.NoError(t, err)
	definition.Comment = new("updated")
	_, err = table.Catalog().Apply(ctx, definition, scopedb.CatalogOptions{IfMatch: created.ETag})
	require.NoError(t, err)
	_, err = table.Catalog().Delete(ctx, scopedb.CatalogOptions{IfMatch: created.ETag, AllowDestructive: true})
	require.Equal(t, 412, httpFailure(t, err).HTTPStatus)
	_, err = table.Catalog().Delete(ctx)
	require.Equal(t, 409, httpFailure(t, err).HTTPStatus)

	_, err = table.AppendNDJSON(ctx, []byte(`{"u":18446744073709551615}`))
	require.Equal(t, scopedb.AppendStateRejected, httpFailure(t, err).AppendDetails.Status)
	_, err = table.AppendNDJSON(ctx, []byte(`{"i":-9223372036854775808,"u":9223372036854775807,"object":{"id":9007199254740993}}`))
	require.NoError(t, err)
	stream, err := table.AppendStream(scopedb.AppendStreamOptions{})
	require.NoError(t, err)
	require.NoError(t, stream.Send(ctx, map[string]any{"i": int64(1<<63 - 1), "u": uint64(1<<63 - 1)}))
	report, err := stream.Shutdown(ctx)
	require.NoError(t, err)
	require.EqualValues(t, 1, report.CommittedRows)

	handle, err := client.Statement("FROM " + table.Identifier() + " ORDER BY i").Submit(ctx)
	require.NoError(t, err)
	result, err := handle.Wait(ctx)
	require.NoError(t, err)
	rows, err := result.ToObjects()
	require.NoError(t, err)
	require.Len(t, rows, 2)
	require.Equal(t, int64(-1<<63), rows[0]["i"])
	require.Equal(t, int64(1<<63-1), rows[1]["i"])
	require.Equal(t, uint64(1<<63-1), rows[0]["u"])
	require.JSONEq(t, `{"id":9007199254740993}`, rows[0]["object"].(string))
	late, err := client.StatementHandle(handle.ID()).Cancel(ctx)
	require.NoError(t, err)
	require.Equal(t, scopedb.StatementStatusFinished, late.Status)
	require.NotNil(t, late.ResultSet, "cancellation of a completed statement must preserve its result")

	result, err = client.Query(ctx, "SELECT '18446744073709551615'::uint AS u")
	require.NoError(t, err)
	row, found, err := result.First()
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, uint64(1<<64-1), row["u"])
	_, err = client.Query(ctx, "SELECT UNKNOWN_FUNCTION()")
	require.NotEmpty(t, httpFailure(t, err).Code)
}
