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
	"bytes"
	"context"
	"fmt"
	"math/rand/v2"
	"net"
	"net/http"
	"net/http/httptest"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/stretchr/testify/require"
)

func TestConcurrentCompressionOwnsRequestBodies(t *testing.T) {
	for _, compression := range []Compression{CompressionZstd, CompressionGzip} {
		t.Run(string(compression), func(t *testing.T) {
			const requests = 8
			payloads := make([][]byte, requests)
			encoded := make([][]byte, requests)
			failures := make([]error, requests)
			start := make(chan struct{})
			var workers sync.WaitGroup
			for i := range requests {
				if i > 0 {
					payloads[i] = bytes.Repeat([]byte(fmt.Sprintf(`{"id":%d}`, i)), i*100)
				}
				if i == requests-1 {
					payloads[i] = bytes.Repeat(payloads[i], 64)
				}
				workers.Go(func() {
					<-start
					body, err := compressRequestBody(payloads[i], compression)
					encoded[i], failures[i] = body.Bytes(), err
				})
			}
			close(start)
			workers.Wait()
			_, err := compressRequestBody(bytes.Repeat([]byte("replacement"), 1024), compression)
			require.NoError(t, err)
			// Decode only after every encoder has been returned for reuse.
			for i := range requests {
				require.NoError(t, failures[i])
				require.NotEmpty(t, encoded[i], "empty requests still need a compressed frame")
				req := httptest.NewRequest(http.MethodPost, "http://example.com", bytes.NewReader(encoded[i]))
				req.Header.Set("Content-Encoding", string(compression))
				decoded, err := decodeCompressedRequestBody(req)
				require.NoError(t, err)
				require.Equal(t, string(payloads[i]), string(decoded))
			}
		})
	}
}

func TestClientReusesConnectionsAcrossWaves(t *testing.T) {
	wave, connections := newHTTP1RequestWave(t)
	for range 3 {
		wave()
	}
	require.EqualValues(t, 8, connections.Load())
}

func BenchmarkRequestCompression(b *testing.B) {
	for _, compression := range []Compression{CompressionZstd, CompressionGzip} {
		for _, size := range []int{1024, 4 * 1024 * 1024} {
			b.Run(fmt.Sprintf("%s/%dB", compression, size), func(b *testing.B) {
				random := rand.New(rand.NewPCG(1, 2)) // #nosec G404 -- benchmark inputs must be deterministic, not cryptographically random.
				payload := make([]byte, 0, size+256)
				for id := 0; len(payload) < size; id++ {
					payload = fmt.Appendf(payload, "{\"id\":%d,\"request_id\":\"%016x\",\"name\":\"request.completed\",\"service\":\"checkout\",\"status\":200,\"latency_ms\":%d}\n", id, random.Uint64(), random.IntN(1000))
				}
				payload = payload[:size]
				// Exclude one-time encoder setup; measure sustained request encoding.
				_, err := compressRequestBody(payload, compression)
				require.NoError(b, err)
				b.ReportAllocs()
				b.SetBytes(int64(len(payload)))
				for b.Loop() {
					if _, err := compressRequestBody(payload, compression); err != nil {
						b.Fatal(err)
					}
				}
			})
		}
	}
}

func BenchmarkHTTP1ConnectionReuse(b *testing.B) {
	wave, connections := newHTTP1RequestWave(b)
	wave()
	initial := connections.Load()
	var waves int64
	b.ReportAllocs()
	for b.Loop() {
		wave()
		waves++
	}
	b.ReportMetric(float64(connections.Load()-initial)/float64(waves), "connections/wave")
}

// newHTTP1RequestWave holds eight requests open together, then releases all
// responses. Each wave ends with idle connections, exposing pool eviction.
func newHTTP1RequestWave(tb testing.TB) (func(), *atomic.Int64) {
	tb.Helper()
	const concurrency = 8
	entered, release := make(chan struct{}, concurrency), make(chan struct{}, concurrency)
	var connections atomic.Int64
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		entered <- struct{}{}
		select {
		case <-release:
			w.WriteHeader(http.StatusNoContent)
		case <-r.Context().Done():
		}
	}))
	server.Config.ConnState = func(_ net.Conn, state http.ConnState) {
		if state == http.StateNew {
			connections.Add(1)
		}
	}
	server.Start()
	tb.Cleanup(server.Close)
	client, err := NewClient(Config{Endpoint: server.URL})
	require.NoError(tb, err)
	tb.Cleanup(client.Close)
	return func() {
		ctx, cancel := context.WithTimeout(tb.Context(), 5*time.Second)
		defer cancel()
		var workers sync.WaitGroup
		failures := make(chan error, concurrency)
		for range concurrency {
			workers.Go(func() {
				req, err := http.NewRequestWithContext(ctx, http.MethodGet, server.URL, nil)
				if err != nil {
					failures <- err
					return
				}
				resp, err := client.do(req)
				if err == nil {
					err = resp.Body.Close()
				}
				failures <- err
			})
		}
		for range concurrency {
			select {
			case <-entered:
			case err := <-failures:
				tb.Fatalf("request ended before the wave was released: %v", err)
			case <-ctx.Done():
				tb.Fatal(ctx.Err())
			}
		}
		for range concurrency {
			release <- struct{}{}
		}
		workers.Wait()
		close(failures)
		for err := range failures {
			require.NoError(tb, err)
		}
	}, &connections
}
