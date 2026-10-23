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
)

func TestCloudAuthentication(t *testing.T) {
	key := os.Getenv("SCOPEDB_API_KEY")
	if key == "" {
		t.Skip("SCOPEDB_API_KEY not set; requires an authenticated Cloud Proxy")
	}
	client := NewClient(t)
	t.Cleanup(client.Close)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	table := cloudTable(t, client, "go_sdk_auth_"+strings.ReplaceAll(uuid.NewString(), "-", ""), "owner")
	_, err := table.AppendNDJSON(ctx, []byte(`{"marker":"accepted"}`))
	require.NoError(t, err)

	wrongSecret := key[:len(key)-1] + "0"
	if wrongSecret == key {
		wrongSecret = key[:len(key)-1] + "1"
	}
	cases := []struct {
		name string
		key  string
	}{
		{name: "missing"},
		{name: "malformed", key: "not-a-valid-api-key"},
		{name: "wrong_secret", key: wrongSecret},
		{name: "expired", key: os.Getenv("SCOPEDB_EXPIRED_API_KEY")},
		{name: "revoked", key: os.Getenv("SCOPEDB_REVOKED_API_KEY")},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if tc.key == "" && tc.name != "missing" {
				t.Skip("optional credential not set")
			}
			unauthorized := cloudClient(t, tc.key, "")
			target := unauthorized.Table(table.Name)
			_, err := target.Catalog().Get(ctx)
			failure := requireHTTPFailure(t, err, http.StatusUnauthorized)
			require.Equal(t, "unauthorized", failure.Code)
			_, err = unauthorized.Query(ctx, "SELECT 1 AS ready")
			_ = requireHTTPFailure(t, err, http.StatusUnauthorized)
			_, err = target.AppendNDJSON(ctx, []byte(`{"marker":"must-not-commit"}`))
			failure = requireHTTPFailure(t, err, http.StatusUnauthorized)
			require.Equal(t, "unauthorized", failure.Code)
			require.NotNil(t, failure.AppendDetails)
			require.Equal(t, scopedb.AppendStateRejected, failure.AppendDetails.Status)
			require.False(t, failure.Retryable)

			stream, err := target.AppendStream(scopedb.AppendStreamOptions{})
			require.NoError(t, err)
			require.NoError(t, stream.Send(ctx, map[string]string{"marker": "must-not-commit"}))
			report, err := stream.Shutdown(ctx)
			_ = requireHTTPFailure(t, err, http.StatusUnauthorized)
			require.EqualValues(t, 1, report.FailedRows)
			require.Zero(t, report.UnknownRows)
			require.Zero(t, report.CommittedRows)
			require.Zero(t, stream.Stats().Retries)
		})
	}
	result, err := client.Query(ctx, "FROM "+table.Identifier())
	require.NoError(t, err)
	rows, err := result.ToObjects()
	require.NoError(t, err)
	require.Len(t, rows, 1)
	require.Equal(t, "accepted", rows[0]["marker"])
}

func TestCloudTenantIsolation(t *testing.T) {
	primaryKey, secondaryKey := os.Getenv("SCOPEDB_API_KEY"), os.Getenv("SCOPEDB_SECONDARY_API_KEY")
	primaryID, secondaryID := os.Getenv("SCOPEDB_WORKSPACE_ID"), os.Getenv("SCOPEDB_SECONDARY_WORKSPACE_ID")
	if primaryKey == "" || secondaryKey == "" || primaryID == "" || secondaryID == "" {
		t.Skip("requires keys and workspace IDs for two Cloud tenants on the same endpoint")
	}
	require.NotEqual(t, primaryID, secondaryID)
	ctx, cancel := context.WithTimeout(t.Context(), 2*time.Minute)
	defer cancel()
	primary, secondary := cloudClient(t, primaryKey, ""), cloudClient(t, secondaryKey, "")
	name := "go_sdk_isolation_" + strings.ReplaceAll(uuid.NewString(), "-", "")
	first := cloudTable(t, primary, name, "primary_only")
	_, err := secondary.Table(name).Catalog().Get(ctx)
	_ = requireHTTPFailure(t, err, http.StatusNotFound)
	second := cloudTable(t, secondary, name, "secondary_only")

	for _, tc := range []struct {
		name         string
		client       *scopedb.Client
		other        *scopedb.Client
		key          string
		forgedTenant string
		column       string
	}{
		{name: "primary", client: primary, other: secondary, key: primaryKey, forgedTenant: secondaryID, column: "primary_only"},
		{name: "secondary", client: secondary, other: primary, key: secondaryKey, forgedTenant: primaryID, column: "secondary_only"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			forged := cloudClient(t, tc.key, tc.forgedTenant)
			snapshot, err := forged.Table(name).Catalog().Get(ctx)
			require.NoError(t, err)
			require.Equal(t, tc.column, snapshot.Definition.Columns[1].Name)
			_, err = forged.Table(name).AppendNDJSON(ctx, []byte(`{"marker":"`+tc.name+`"}`))
			require.NoError(t, err)
			handle, err := forged.Statement("FROM " + first.Identifier()).Submit(ctx)
			require.NoError(t, err)
			result, err := handle.Wait(ctx)
			require.NoError(t, err)
			rows, err := result.ToObjects()
			require.NoError(t, err)
			require.Len(t, rows, 1)
			require.Equal(t, tc.name, rows[0]["marker"])

			otherHandle := tc.other.StatementHandle(handle.ID())
			_, err = otherHandle.Refresh(ctx)
			_ = requireHTTPFailure(t, err, http.StatusNotFound)
			_, err = otherHandle.Cancel(ctx)
			_ = requireHTTPFailure(t, err, http.StatusNotFound)
			ownerSnapshot, err := tc.client.StatementHandle(handle.ID()).Refresh(ctx)
			require.NoError(t, err)
			require.Equal(t, scopedb.StatementStatusFinished, ownerSnapshot.Status)
		})
	}
	_, err = first.Catalog().Delete(ctx, scopedb.CatalogOptions{AllowDestructive: true})
	require.NoError(t, err)
	_, err = second.Catalog().Get(ctx)
	require.NoError(t, err)
}

func cloudTable(t *testing.T, client *scopedb.Client, name, column string) *scopedb.Table {
	t.Helper()
	table := client.Table(name)
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cancel()
		_, err := table.Catalog().Delete(ctx, scopedb.CatalogOptions{AllowDestructive: true})
		if err != nil {
			var failure *scopedb.Error
			require.ErrorAs(t, err, &failure)
			require.Equal(t, http.StatusNotFound, failure.HTTPStatus)
		}
		_, err = table.Catalog().Get(ctx)
		_ = requireHTTPFailure(t, err, http.StatusNotFound)
	})
	ctx, cancel := context.WithTimeout(t.Context(), 30*time.Second)
	defer cancel()
	_, err := table.Catalog().Apply(ctx, &scopedb.CatalogTable{
		Name: name,
		Columns: []scopedb.CatalogColumn{
			{Name: "marker", DataType: scopedb.StringDataType},
			{Name: column, DataType: scopedb.StringDataType},
		},
	})
	require.NoError(t, err)
	return table
}

type tenantHeaderTransport struct {
	base   http.RoundTripper
	tenant string
}

func (t tenantHeaderTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	request = request.Clone(request.Context())
	request.Header.Set("X-ScopeDB-Tenant-Id", t.tenant)
	return t.base.RoundTrip(request)
}

func cloudClient(t *testing.T, key, tenant string) *scopedb.Client {
	t.Helper()
	config := scopedb.Config{Endpoint: os.Getenv("SCOPEDB_ENDPOINT"), APIKey: key}
	if tenant != "" {
		transport := http.DefaultTransport.(*http.Transport).Clone()
		t.Cleanup(transport.CloseIdleConnections)
		config.HTTPClient = &http.Client{
			Transport: tenantHeaderTransport{base: transport, tenant: tenant},
			CheckRedirect: func(_ *http.Request, _ []*http.Request) error {
				return http.ErrUseLastResponse
			},
		}
	}
	client, err := scopedb.NewClient(config)
	require.NoError(t, err)
	t.Cleanup(client.Close)
	return client
}

func requireHTTPFailure(t *testing.T, err error, status int) *scopedb.Error {
	t.Helper()
	var failure *scopedb.Error
	require.ErrorAs(t, err, &failure)
	require.Equal(t, status, failure.HTTPStatus)
	require.NotEmpty(t, failure.RequestID)
	return failure
}
