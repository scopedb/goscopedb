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

// Package main demonstrates shaping source events before appending them.
package main

import (
	"context"
	"fmt"
	"log"
	"time"

	scopedb "github.com/scopedb/goscopedb"
	"github.com/scopedb/goscopedb/examples/internal/exampleutil"
)

func main() {
	if err := run(context.Background()); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context) error {
	client, err := exampleutil.NewClient()
	if err != nil {
		return err
	}
	defer client.Close()

	table, err := exampleutil.WriteTable(client)
	if err != nil {
		return err
	}
	stream, err := table.AppendStream(scopedb.AppendStreamOptions{
		MaxConcurrentBatches: 1,
		Retry:                &scopedb.AppendRetryOptions{MaxRetries: 0},
	})
	if err != nil {
		return err
	}

	now := time.Now().UTC()
	for id := 1; id <= 2; id++ {
		source := map[string]any{
			"id":         id,
			"event_id":   fmt.Sprintf("go-transform-%d-%d", now.UnixNano(), id),
			"ts":         now.Format(time.RFC3339Nano),
			"name":       "example.transform",
			"attributes": map[string]any{"source": "go"},
		}
		row := map[string]any{
			"id":          source["id"],
			"event_id":    source["event_id"],
			"occurred_at": source["ts"],
			"name":        source["name"],
			"attributes":  source["attributes"],
		}
		// Send confirms local admission only; Shutdown is the commit barrier.
		if err := stream.Send(ctx, row); err != nil {
			_, _ = stream.Shutdown(ctx)
			return err
		}
	}

	result, err := stream.Shutdown(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("inserted %d transformed rows into %s\n", result.CommittedRows, table.Identifier())
	return nil
}
