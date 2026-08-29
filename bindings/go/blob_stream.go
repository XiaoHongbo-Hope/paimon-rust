/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package paimon

import (
	"context"
	"fmt"
	"io"
	"runtime"
	"sync"
	"unsafe"

	"github.com/jupiterrider/ffi"
)

// BlobStream incrementally reads one BlobDescriptor.
type BlobStream struct {
	ctx   context.Context
	lib   *libRef
	inner *paimonBlobStream
	mu    sync.Mutex
}

var _ io.ReadCloser = (*BlobStream)(nil)

// OpenBlob opens one descriptor without reading its contents.
func (r *BlobReader) OpenBlob(descriptor []byte) (*BlobStream, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	if r.inner == nil {
		return nil, ErrClosed
	}
	inner, err := ffiBlobReaderOpenBlob.symbol(r.ctx)(r.inner, descriptor)
	if err != nil {
		return nil, err
	}
	r.lib.acquire()
	return &BlobStream{ctx: r.ctx, lib: r.lib, inner: inner}, nil
}

// Read implements io.Reader.
func (s *BlobStream) Read(buffer []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.inner == nil {
		return 0, ErrClosed
	}
	if len(buffer) == 0 {
		return 0, nil
	}

	read, err := ffiBlobStreamRead.symbol(s.ctx)(s.inner, buffer)
	if err != nil {
		return 0, err
	}
	if read > len(buffer) {
		return 0, fmt.Errorf("paimon: native BlobStream returned %d bytes for a %d-byte buffer", read, len(buffer))
	}
	if read == 0 {
		return 0, io.EOF
	}
	return read, nil
}

// Close releases the stream and is idempotent.
func (s *BlobStream) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.inner == nil {
		return nil
	}
	ffiBlobStreamFree.symbol(s.ctx)(s.inner)
	s.inner = nil
	s.lib.release()
	return nil
}

var ffiBlobReaderOpenBlob = newFFI(ffiOpts{
	sym:   "paimon_blob_reader_open_blob",
	rType: &typeResultBlobStream,
	aTypes: []*ffi.Type{
		&ffi.TypePointer,
		&ffi.TypePointer,
		&ffi.TypePointer,
	},
}, func(ctx context.Context, ffiCall ffiCall) func(*paimonBlobReader, []byte) (*paimonBlobStream, error) {
	return func(reader *paimonBlobReader, descriptor []byte) (*paimonBlobStream, error) {
		var descriptorPtr unsafe.Pointer
		if len(descriptor) > 0 {
			descriptorPtr = unsafe.Pointer(&descriptor[0])
		}
		descriptorLen := uintptr(len(descriptor))
		var result resultBlobStream
		ffiCall(
			unsafe.Pointer(&result),
			unsafe.Pointer(&reader),
			unsafe.Pointer(&descriptorPtr),
			unsafe.Pointer(&descriptorLen),
		)
		runtime.KeepAlive(descriptor)
		if result.error != nil {
			return nil, parseError(ctx, result.error)
		}
		return result.stream, nil
	}
})

var ffiBlobStreamRead = newFFI(ffiOpts{
	sym:   "paimon_blob_stream_read",
	rType: &typeResultBlobStreamRead,
	aTypes: []*ffi.Type{
		&ffi.TypePointer,
		&ffi.TypePointer,
		&ffi.TypePointer,
	},
}, func(ctx context.Context, ffiCall ffiCall) func(*paimonBlobStream, []byte) (int, error) {
	return func(stream *paimonBlobStream, buffer []byte) (int, error) {
		bufferPtr := unsafe.Pointer(&buffer[0])
		bufferLen := uintptr(len(buffer))
		var result resultBlobStreamRead
		ffiCall(
			unsafe.Pointer(&result),
			unsafe.Pointer(&stream),
			unsafe.Pointer(&bufferPtr),
			unsafe.Pointer(&bufferLen),
		)
		runtime.KeepAlive(buffer)
		if result.error != nil {
			return 0, parseError(ctx, result.error)
		}
		return int(result.bytesRead), nil
	}
})

var ffiBlobStreamFree = newFFI(ffiOpts{
	sym:    "paimon_blob_stream_free",
	rType:  &ffi.TypeVoid,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(_ context.Context, ffiCall ffiCall) func(*paimonBlobStream) {
	return func(stream *paimonBlobStream) {
		ffiCall(nil, unsafe.Pointer(&stream))
	}
})
