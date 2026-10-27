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
	"fmt"
	"os"
	"testing"
	"time"

	scopedb "github.com/scopedb/goscopedb"
	"github.com/stretchr/testify/require"
)

func TestCloudRejectsUnauthenticatedAppend(t *testing.T) {
	key := os.Getenv("SCOPEDB_API_KEY")
	if key == "" {
		t.Skip("SCOPEDB_API_KEY not set")
	}
	owner := integrationClient(t, key)
	ctx, cancel := context.WithTimeout(t.Context(), time.Minute)
	defer cancel()
	table := owner.Table(tableName())
	cleanupTable(t, table)
	_, err := table.Catalog().Apply(ctx, &scopedb.CatalogTable{Name: table.Name, Columns: []scopedb.CatalogColumn{{Name: "id", DataType: scopedb.IntDataType}}})
	require.NoError(t, err)
	unauthorized := integrationClient(t, "")
	_, err = unauthorized.Table(table.Name).AppendNDJSON(ctx, []byte(`{"id":1}`))
	failure := httpFailure(t, err)
	require.Equal(t, 401, failure.HTTPStatus)
	require.Equal(t, scopedb.AppendStateRejected, failure.AppendDetails.Status)
	require.False(t, failure.Retryable)
	result, err := owner.Query(ctx, "FROM "+table.Identifier())
	require.NoError(t, err)
	require.Zero(t, result.TotalRows)
}

func TestCloudClientsKeepTenantCredentialsIsolated(t *testing.T) {
	primary, secondary := os.Getenv("SCOPEDB_API_KEY"), os.Getenv("SCOPEDB_SECONDARY_API_KEY")
	primaryID, secondaryID := os.Getenv("SCOPEDB_WORKSPACE_ID"), os.Getenv("SCOPEDB_SECONDARY_WORKSPACE_ID")
	if primary == "" || secondary == "" || primaryID == "" || secondaryID == "" {
		t.Skip("requires credentials for two different Cloud tenants")
	}
	require.NotEqual(t, primaryID, secondaryID)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	name := tableName()
	first, second := integrationClient(t, primary), integrationClient(t, secondary)
	for i, client := range []*scopedb.Client{first, second} {
		table := client.Table(name)
		cleanupTable(t, table)
		_, err := table.Catalog().Apply(ctx, &scopedb.CatalogTable{Name: name, Columns: []scopedb.CatalogColumn{{Name: "id", DataType: scopedb.IntDataType}}})
		require.NoError(t, err)
		_, err = table.AppendNDJSON(ctx, []byte(fmt.Sprintf(`{"id":%d}`, i+1)))
		require.NoError(t, err)
	}
	for i, client := range []*scopedb.Client{first, second} {
		handle, err := client.Statement("FROM " + client.Table(name).Identifier()).Submit(ctx)
		require.NoError(t, err)
		result, err := handle.Wait(ctx)
		require.NoError(t, err)
		rows, err := result.ToObjects()
		require.NoError(t, err)
		require.Equal(t, []map[string]scopedb.Value{{"id": int64(i + 1)}}, rows)
		other := []*scopedb.Client{second, first}[i]
		_, err = other.StatementHandle(handle.ID()).Refresh(ctx)
		require.Equal(t, 404, httpFailure(t, err).HTTPStatus)
	}
}
