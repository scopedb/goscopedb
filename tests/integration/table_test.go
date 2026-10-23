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
	"testing"

	scopedb "github.com/scopedb/goscopedb"
	"github.com/stretchr/testify/require"
)

func TestTableCatalogLifecycle(t *testing.T) {
	c := NewClient(t)
	defer c.Close()

	ctx := context.Background()
	tbl := c.Table(RandomName(t))
	_, err := c.Statement(fmt.Sprintf(`
		CREATE TABLE %s (
			i int,
			u uint,
			f float,
			s string,
			b boolean,
			ts timestamp,
			var any,
		)
	`, tbl.Identifier())).Execute(ctx)
	require.NoError(t, err)
	defer func() {
		_, err := tbl.Catalog().Delete(ctx, scopedb.CatalogOptions{AllowDestructive: true})
		require.NoError(t, err)
		_, err = tbl.Catalog().Get(ctx)
		var failure *scopedb.Error
		require.ErrorAs(t, err, &failure)
		require.Equal(t, 404, failure.HTTPStatus)
	}()

	description, err := tbl.Catalog().Get(ctx)
	require.NoError(t, err)
	require.Equal(t, tbl.Name, description.Definition.Name)
	require.Equal(t, []scopedb.CatalogColumn{
		{Name: "i", DataType: scopedb.IntDataType, Indexes: []scopedb.CatalogColumnIndex{}},
		{Name: "u", DataType: scopedb.UIntDataType, Indexes: []scopedb.CatalogColumnIndex{}},
		{Name: "f", DataType: scopedb.FloatDataType, Indexes: []scopedb.CatalogColumnIndex{}},
		{Name: "s", DataType: scopedb.StringDataType, Indexes: []scopedb.CatalogColumnIndex{}},
		{Name: "b", DataType: scopedb.BooleanDataType, Indexes: []scopedb.CatalogColumnIndex{}},
		{Name: "ts", DataType: scopedb.TimestampDataType, Indexes: []scopedb.CatalogColumnIndex{}},
		{Name: "var", DataType: scopedb.AnyDataType, Indexes: []scopedb.CatalogColumnIndex{}},
	}, description.Definition.Columns)
	comment := "updated through the conditional catalog API"
	description.Definition.Comment = &comment
	updated, err := tbl.Catalog().Apply(ctx, description.Definition, scopedb.CatalogOptions{IfMatch: description.ETag})
	require.NoError(t, err)
	_, err = tbl.Catalog().Delete(ctx, scopedb.CatalogOptions{IfMatch: description.ETag, AllowDestructive: true})
	var failure *scopedb.Error
	require.ErrorAs(t, err, &failure)
	require.Equal(t, 412, failure.HTTPStatus)
	_, err = tbl.Catalog().Delete(ctx, scopedb.CatalogOptions{IfMatch: updated.ETag})
	require.ErrorAs(t, err, &failure)
	require.Equal(t, 409, failure.HTTPStatus)
	current, err := tbl.Catalog().Get(ctx)
	require.NoError(t, err)
	require.Equal(t, &comment, current.Definition.Comment)
}
