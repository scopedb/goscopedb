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
	"sync"
	"testing"
	"time"

	scopedb "github.com/scopedb/goscopedb"
	"github.com/stretchr/testify/require"
)

func TestTableAppendLifecycle(t *testing.T) {
	client := NewClient(t)
	defer client.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	table := client.Table(RandomName(t))
	definition := &scopedb.CatalogTable{
		Name: table.Name,
		Columns: []scopedb.CatalogColumn{
			{Name: "id", DataType: scopedb.IntDataType},
			{Name: "source", DataType: scopedb.StringDataType},
		},
	}
	_, err := table.Catalog().Apply(ctx, definition)
	require.NoError(t, err)
	defer func() {
		_, err := table.Catalog().Delete(context.Background(), scopedb.CatalogOptions{AllowDestructive: true})
		require.NoError(t, err)
	}()

	direct, err := table.AppendNDJSON(ctx, []byte(
		"{\"id\":1,\"source\":\"direct\"}\n"+
			"{\"id\":2,\"source\":\"direct\"}\n",
	))
	require.NoError(t, err)
	require.Equal(t, int64(2), direct.NumRowsInserted)

	stream, err := table.AppendStream(scopedb.AppendStreamOptions{
		TargetBatchBytes:     1,
		MaxConcurrentBatches: 4,
		AttemptTimeout:       5 * time.Second,
	})
	require.NoError(t, err)

	const streamedRows = 16
	errors := make(chan error, streamedRows)
	var senders sync.WaitGroup
	for id := 3; id < 3+streamedRows; id++ {
		senders.Add(1)
		go func() {
			defer senders.Done()
			errors <- stream.Send(ctx, map[string]any{
				"id":     id,
				"source": "stream",
			})
		}()
	}
	senders.Wait()
	close(errors)
	for sendErr := range errors {
		require.NoError(t, sendErr)
	}

	report, err := stream.Shutdown(ctx)
	require.NoError(t, err)
	require.Equal(t, scopedb.AppendDeliveryOK, report.Outcome)
	require.Equal(t, uint64(streamedRows), report.AcceptedRows)
	require.Equal(t, uint64(streamedRows), report.CommittedRows)

	_, err = table.Catalog().Apply(ctx, definition)
	require.NoError(t, err)

	description, err := table.Catalog().Get(ctx)
	require.NoError(t, err)
	require.Equal(t, table.Name, description.Definition.Name)
	require.Len(t, description.Definition.Columns, 2)

	statement := client.Statement(fmt.Sprintf("FROM %s", table.Identifier()))
	result, err := statement.Execute(ctx)
	require.NoError(t, err)
	rows, err := result.ToObjects()
	require.NoError(t, err)
	require.Len(t, rows, 2+streamedRows)
}
