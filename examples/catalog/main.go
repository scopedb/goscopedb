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

// Package main demonstrates declarative catalog discovery.
package main

import (
	"context"
	"fmt"
	"log"

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

	snapshot, err := client.Catalog().Schema(exampleutil.Database(), exampleutil.Schema()).Get(ctx)
	if err != nil {
		return err
	}
	fmt.Printf("tables in %s.%s (version %s):\n", exampleutil.Database(), exampleutil.Schema(), snapshot.Version)
	for _, table := range snapshot.Definition.Tables {
		fmt.Printf(" - %s (%d columns)\n", table.Name, len(table.Columns))
	}
	return nil
}
