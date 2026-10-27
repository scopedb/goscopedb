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
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Client provides access to ScopeDB APIs and is safe for concurrent use.
// Reuse a client to share its HTTP connection pool.
type Client struct {
	endpoint       *url.URL
	httpClient     *http.Client
	authorization  string
	compression    Compression
	ownsHTTPClient bool
	writeTimeout   time.Duration
}

const defaultWriteTimeout = 30 * time.Second

// NewClient creates a new ScopeDB client with the given configuration.
func NewClient(config Config) (*Client, error) {
	endpoint, err := normalizeEndpoint(config.Endpoint)
	if err != nil {
		return nil, newError(
			ErrorKindConfigInvalid,
			fmt.Sprintf("invalid ScopeDB endpoint: %v", err),
			err,
		)
	}

	compression := config.Compression
	if compression == "" {
		compression = CompressionZstd
	}
	if compression != CompressionZstd && compression != CompressionGzip {
		return nil, newError(
			ErrorKindConfigInvalid,
			fmt.Sprintf("unsupported compression: %q", compression),
			nil,
		)
	}
	writeTimeout := defaultWriteTimeout
	if config.WriteTimeout != nil {
		writeTimeout = *config.WriteTimeout
	}
	if writeTimeout < 0 {
		return nil, newError(ErrorKindConfigInvalid, "write timeout must not be negative", nil)
	}

	standardClient := config.HTTPClient
	owned := false
	if standardClient == nil {
		standardClient = &http.Client{Transport: newDefaultTransport()}
		owned = true
	}
	var authorization string
	if config.APIKey != "" {
		authorization = "Bearer " + config.APIKey
	}

	return &Client{
		endpoint:       endpoint,
		httpClient:     standardClient,
		authorization:  authorization,
		compression:    compression,
		ownsHTTPClient: owned,
		writeTimeout:   writeTimeout,
	}, nil
}

// Close releases idle connections owned by the client. It does not close a
// caller-provided HTTP client.
func (c *Client) Close() {
	if c.ownsHTTPClient {
		c.httpClient.CloseIdleConnections()
	}
}

func normalizeEndpoint(raw string) (*url.URL, error) {
	endpoint, err := url.Parse(raw)
	if err != nil {
		return nil, err
	}
	if endpoint.Scheme != "http" && endpoint.Scheme != "https" {
		return nil, fmt.Errorf("endpoint scheme must be http or https")
	}
	if endpoint.Host == "" {
		return nil, fmt.Errorf("endpoint host is required")
	}
	if endpoint.User != nil {
		return nil, fmt.Errorf("endpoint must not contain user information; use APIKey instead")
	}
	if endpoint.RawQuery != "" || endpoint.Fragment != "" {
		return nil, fmt.Errorf("endpoint must not contain a query or fragment")
	}
	return endpoint, nil
}

func (c *Client) resourceURL(segments ...string) (*url.URL, error) {
	rawPath := strings.TrimRight(c.endpoint.EscapedPath(), "/") + "/v1"
	for _, segment := range segments {
		if segment == "" || segment == "." || segment == ".." {
			return nil, newError(ErrorKindConfigInvalid, "resource names must not be empty or dot segments", nil)
		}
		rawPath += "/" + url.PathEscape(segment)
	}
	path, err := url.PathUnescape(rawPath)
	if err != nil {
		return nil, newError(ErrorKindConfigInvalid, "invalid ScopeDB endpoint path", err)
	}

	u := *c.endpoint
	u.Path = path
	u.RawPath = rawPath
	return &u, nil
}
