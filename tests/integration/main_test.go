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
	"net/http"
	"os"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	scopedb "github.com/scopedb/goscopedb"
	"github.com/stretchr/testify/require"
	"go.uber.org/goleak"
)

func TestMain(m *testing.M) { goleak.VerifyTestMain(m) }

func integrationClient(t *testing.T, key string) *scopedb.Client {
	t.Helper()
	endpoint := os.Getenv("SCOPEDB_ENDPOINT")
	if endpoint == "" {
		t.Skip("SCOPEDB_ENDPOINT not set")
	}
	client, err := scopedb.NewClient(scopedb.Config{Endpoint: endpoint, APIKey: key})
	require.NoError(t, err)
	t.Cleanup(client.Close)
	return client
}

func tableName() string { return "go_sdk_" + strings.ReplaceAll(uuid.NewString(), "-", "") }

func cleanupTable(t *testing.T, table *scopedb.Table) {
	t.Helper()
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, err := table.Catalog().Delete(ctx, scopedb.CatalogOptions{AllowDestructive: true})
		if err != nil {
			require.Equal(t, http.StatusNotFound, httpFailure(t, err).HTTPStatus)
		}
	})
}

func httpFailure(t *testing.T, err error) *scopedb.Error {
	t.Helper()
	var failure *scopedb.Error
	require.ErrorAs(t, err, &failure)
	return failure
}
