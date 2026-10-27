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

package scopedb

import (
	"encoding/json"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestResultPreservesWirePrecisionAndNulls(t *testing.T) {
	result := &ResultSet{
		TotalRows: 1,
		Schema: Schema{
			{Name: "signed", Type: IntDataType},
			{Name: "unsigned", Type: UIntDataType},
			{Name: "payload", Type: BinaryDataType},
			{Name: "elapsed", Type: IntervalDataType},
			{Name: "at", Type: TimestampDataType},
			{Name: "object", Type: ObjectDataType},
			{Name: "missing", Type: NullDataType},
		},
		rows: json.RawMessage(`[["-9223372036854775808","18446744073709551615","00aBff","-PT1M0.000000001S","2026-08-08T00:00:00.123456789Z","{\"id\":9007199254740993}",null]]`),
	}
	wire, err := result.RawRows()
	require.NoError(t, err)
	require.Equal(t, "18446744073709551615", *wire[0][1])
	require.Nil(t, wire[0][6])
	values, err := result.ToValues()
	require.NoError(t, err)
	require.Equal(t, [][]Value{{int64(-1 << 63), uint64(1<<64 - 1), []byte{0, 0xab, 0xff}, -time.Minute - time.Nanosecond, time.Date(2026, 8, 8, 0, 0, 0, 123456789, time.UTC), `{"id":9007199254740993}`, nil}}, values)
	objects, err := result.ToObjects()
	require.NoError(t, err)
	for i, field := range result.Schema {
		require.Equal(t, values[0][i], objects[0][field.Name])
	}
}

func TestResultRejectsInvalidRowsAndLossyObjects(t *testing.T) {
	for _, tc := range []struct {
		name, rows string
		total      uint64
	}{
		{"null rows", `null`, 0},
		{"row count", `[]`, 1},
		{"column count", `[[]]`, 1},
		{"invalid cell", `[["not an integer"]]`, 1},
	} {
		t.Run(tc.name, func(t *testing.T) {
			result := &ResultSet{TotalRows: tc.total, Schema: Schema{{Name: "n", Type: IntDataType}}, rows: json.RawMessage(tc.rows)}
			_, err := result.ToValues()
			require.Error(t, err)
			if tc.name == "invalid cell" {
				require.ErrorContains(t, err, `result row 0: column "n"`)
			}
		})
	}
	result := &ResultSet{TotalRows: 1, Schema: Schema{{Name: "n", Type: IntDataType}, {Name: "n", Type: IntDataType}}, rows: json.RawMessage(`[["1","2"]]`)}
	_, err := result.ToObjects()
	require.ErrorContains(t, err, "duplicate result column")
	_, _, err = result.First()
	require.ErrorContains(t, err, "duplicate result column")
}

func TestResultFirstConvertsOnlyTheFirstRow(t *testing.T) {
	result := &ResultSet{TotalRows: 2, Schema: Schema{{Name: "n", Type: IntDataType}}, rows: json.RawMessage(`[["9007199254740993"],["invalid"]]`)}
	row, found, err := result.First()
	require.NoError(t, err)
	require.True(t, found)
	require.Equal(t, int64(9007199254740993), row["n"])
	_, err = result.ToObjects()
	require.ErrorContains(t, err, "result row 1")
	result.TotalRows, result.rows = 0, json.RawMessage(`[]`)
	row, found, err = result.First()
	require.NoError(t, err)
	require.False(t, found)
	require.Nil(t, row)
}

func TestResultConversionBoundaries(t *testing.T) {
	for _, tc := range []struct {
		kind    DataType
		wire    string
		want    Value
		invalid bool
	}{
		{IntervalDataType, "PT2562047H47M16.854775807S", time.Duration(1<<63 - 1), false},
		{IntervalDataType, "-PT2562047H47M16.854775808S", time.Duration(-1 << 63), false},
		{IntervalDataType, "PT2562047H47M16.854775808S", nil, true},
		{IntervalDataType, "P1D", nil, true},
		{IntervalDataType, "PT", nil, true},
		{IntDataType, "9223372036854775808", nil, true},
		{UIntDataType, "-1", nil, true},
		{BinaryDataType, "not hex", nil, true},
	} {
		t.Run(string(tc.kind)+"/"+tc.wire, func(t *testing.T) {
			data, err := json.Marshal([][]string{{tc.wire}})
			require.NoError(t, err)
			result := &ResultSet{TotalRows: 1, Schema: Schema{{Name: "value", Type: tc.kind}}, rows: data}
			values, err := result.ToValues()
			if tc.invalid {
				require.Error(t, err)
			} else {
				require.NoError(t, err)
				require.Equal(t, tc.want, values[0][0])
			}
		})
	}
}
