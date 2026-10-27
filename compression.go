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
	"compress/gzip"
	"errors"
	"fmt"
	"io"
	"sync"

	"github.com/klauspost/compress/zstd"
)

// Pools cache temporary compression state, never request-owned output buffers.
var (
	zstdEncoderPool sync.Pool
	gzipWriterPool  sync.Pool
)

func compressRequestBody(body []byte, compression Compression) (bytes.Buffer, error) {
	var b bytes.Buffer

	switch compression {
	case CompressionZstd:
		encoder, _ := zstdEncoderPool.Get().(*zstd.Encoder)
		if encoder == nil {
			var err error
			encoder, err = zstd.NewWriter(nil)
			if err != nil {
				return bytes.Buffer{}, err
			}
		}
		encoder.Reset(&b)
		_, writeErr := encoder.Write(body)
		closeErr := encoder.Close()
		encoder.Reset(io.Discard)
		zstdEncoderPool.Put(encoder)
		if err := errors.Join(writeErr, closeErr); err != nil {
			return bytes.Buffer{}, err
		}
	case CompressionGzip:
		writer, _ := gzipWriterPool.Get().(*gzip.Writer)
		if writer == nil {
			writer = gzip.NewWriter(io.Discard)
		}
		writer.Reset(&b)
		_, writeErr := writer.Write(body)
		closeErr := writer.Close()
		// Release the output buffer before caching the writer between requests.
		writer.Reset(io.Discard)
		gzipWriterPool.Put(writer)
		if err := errors.Join(writeErr, closeErr); err != nil {
			return bytes.Buffer{}, err
		}
	default:
		return bytes.Buffer{}, fmt.Errorf("unsupported compression: %q", compression)
	}

	return b, nil
}
