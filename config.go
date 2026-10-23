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
	"net/http"
	"time"
)

// Compression defines the compression used for request bodies.
type Compression string

const (
	// CompressionZstd uses Zstandard compression.
	CompressionZstd Compression = "zstd"
	// CompressionGzip uses gzip compression.
	CompressionGzip Compression = "gzip"
)

// Config defines the configuration for the client.
type Config struct {
	// Endpoint is the URL of the ScopeDB service.
	Endpoint string `json:"endpoint"`
	// APIKey is the API key used for authentication.
	//
	// When provided, the client sends it as the Authorization header using the
	// Bearer scheme.
	APIKey string `json:"api_key"`
	// Compression controls how request bodies are compressed, including direct
	// table appends and AppendStream batches.
	//
	// The default is CompressionZstd. CompressionGzip is also supported.
	Compression Compression `json:"compression"`
	// WriteTimeout bounds each table append or catalog mutation request,
	// including reading its response body. Zero (the default) disables this limit;
	// caller context deadlines and HTTPClient timeouts still apply.
	// The budget starts after local encoding, compression, and request preparation.
	// Positive stream AttemptTimeout settings override this limit. A timeout leaves
	// the commit outcome unknown; it does not roll back the write.
	WriteTimeout time.Duration `json:"write_timeout"`
	// HTTPClient is the HTTP client used to send requests.
	//
	// When nil, the SDK creates and owns an independent HTTP client using Go's
	// default redirect policy. A supplied client retains its redirect policy,
	// remains owned by the caller, and is never closed by the SDK.
	HTTPClient *http.Client `json:"-"`
}
