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

// Package main imports NDJSON from standard input with bounded streaming writes.
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"

	scopedb "github.com/scopedb/goscopedb"
	"github.com/scopedb/goscopedb/examples/internal/exampleutil"
)

func main() {
	if err := run(context.Background(), os.Stdin); err != nil {
		log.Fatal(err)
	}
}

func run(ctx context.Context, input io.Reader) error {
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
		// Let the caller reconcile a failed import before replaying any rows.
		Retry: &scopedb.AppendRetryOptions{MaxRetries: 0},
	})
	if err != nil {
		return err
	}

	readErr := sendRows(ctx, stream, input)
	report, writeErr := stream.Shutdown(ctx)
	fmt.Printf(
		"accepted=%d committed=%d failed=%d unknown=%d duration=%s\n",
		report.AcceptedRows,
		report.CommittedRows,
		report.FailedRows,
		report.UnknownRows,
		report.Duration,
	)
	return errors.Join(readErr, writeErr)
}

func sendRows(ctx context.Context, stream *scopedb.AppendStream, input io.Reader) error {
	scanner := bufio.NewScanner(input)
	// One row must fit in the SDK's 8 MiB uncompressed request limit.
	scanner.Buffer(make([]byte, 64*1024), 8*1024*1024)
	for line := 1; scanner.Scan(); line++ {
		row := bytes.TrimSpace(scanner.Bytes())
		if len(row) == 0 {
			continue
		}
		// RawMessage preserves JSON numbers; Send copies the row and waits for
		// bounded queue space without waiting for a remote commit.
		if err := stream.Send(ctx, json.RawMessage(row)); err != nil {
			return fmt.Errorf("line %d: %w", line, err)
		}
	}
	return scanner.Err()
}
