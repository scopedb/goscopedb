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
	"errors"
	"strings"
	"testing"
	"time"

	scopedb "github.com/scopedb/goscopedb"
	"github.com/stretchr/testify/require"
)

func TestNative06Lifecycle(t *testing.T) {
	check := func(err error) { t.Helper(); require.NoError(t, err) }
	c := NewClient(t)
	defer c.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer cancel()
	name := "sdk06_go_" + RandomName(t)
	resource := c.Catalog().Schema("scopedb", name)
	def := &scopedb.CatalogSchema{Name: name, Tables: []scopedb.CatalogTable{{Name: "events", Columns: []scopedb.CatalogColumn{{Name: "id", DataType: scopedb.IntDataType}, {Name: "source", DataType: scopedb.StringDataType}}}}}
	preview, err := resource.Apply(ctx, def, scopedb.CatalogOptions{DryRun: true})
	check(err)
	if preview.Before != nil || preview.After == nil || preview.ETag == "" {
		t.Fatal("invalid dry-run")
	}
	_, err = resource.Get(ctx)
	var api *scopedb.Error
	if !errors.As(err, &api) || api.HTTPStatus != 404 {
		t.Fatal("dry-run changed catalog")
	}
	defer func() {
		cleanup, done := context.WithTimeout(context.Background(), 30*time.Second)
		defer done()
		_, err := resource.Delete(cleanup, scopedb.CatalogOptions{AllowDestructive: true})
		check(err)
		t.Log("CLEANUP schema removed")
	}()
	applied, err := resource.Apply(ctx, def, scopedb.CatalogOptions{IfMatch: preview.ETag})
	check(err)
	snap, err := resource.Get(ctx)
	check(err)
	if snap.ETag != applied.ETag {
		t.Fatal("etag mismatch")
	}
	_, err = resource.Apply(ctx, def, scopedb.CatalogOptions{IfMatch: preview.ETag})
	if !errors.As(err, &api) || api.HTTPStatus != 412 {
		t.Fatalf("stale ETag: %v", err)
	}
	t.Logf("STALE_ETAG code=%s\n", api.Code)
	table := c.Table("events")
	table.Schema = name
	inserted, err := table.AppendNDJSON(ctx, []byte("{\"id\":1,\"source\":\"go\"}\n{\"id\":2,\"source\":\"go\"}"))
	check(err)
	if inserted.NumRowsInserted != 2 {
		t.Fatal("row count")
	}
	_, err = table.AppendNDJSON(ctx, []byte("{\"id\":\"invalid\",\"source\":\"go\"}"))
	if !errors.As(err, &api) || api.AppendDetails == nil || api.AppendDetails.AppendState != scopedb.AppendStateRejected {
		t.Fatalf("invalid row: %v", err)
	}
	t.Logf("REJECTED code=%s row_errors=%d\n", api.Code, len(api.AppendDetails.RowErrors))
	stmt, err := c.Statement("FROM " + table.Identifier()).Submit(ctx)
	check(err)
	result, err := stmt.Wait(ctx)
	check(err)
	if result.TotalRows != 2 {
		t.Fatal("rejected rows committed")
	}
	t.Logf("QUERY rows=%d compute_seconds=%g\n", result.TotalRows, stmt.Snapshot().Progress.ComputeSeconds)
	late := c.StatementHandle(stmt.ID())
	cancelled, err := late.Cancel(ctx)
	check(err)
	if cancelled.ID != stmt.ID() || cancelled.Status != scopedb.StatementStatusFinished || late.Snapshot().ResultSet == nil {
		t.Fatal("cancel lost result")
	}
	t.Log("CANCEL completed statement retained result")
	_, err = c.Query(ctx, "SELECT UNKNOWN_FUNCTION()")
	if !errors.As(err, &api) || api.Code == "" || api.Message == "" {
		t.Fatalf("statement failure: %v", err)
	}
	t.Logf("FAILED code=%s\n", api.Code)
	timed := c.Statement("FROM " + table.Identifier())
	timed.MaxElapsed = time.Nanosecond
	_, err = timed.Execute(ctx)
	if !errors.As(err, &api) || !strings.Contains(api.Code, "timeout") {
		t.Fatalf("elapsed timeout: %v", err)
	}
	t.Logf("TIMEOUT code=%s\n", api.Code)
	t.Log("PASS native 0.6 lifecycle")
}
