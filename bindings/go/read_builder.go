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
	"sync"
	"unsafe"

	"github.com/jupiterrider/ffi"
)

// ReadBuilder creates TableScan and TableRead instances.
type ReadBuilder struct {
	ctx       context.Context
	lib       *libRef
	inner     *paimonReadBuilder
	closeOnce sync.Once
}

// Close releases the read builder resources. Safe to call multiple times.
func (rb *ReadBuilder) Close() {
	rb.closeOnce.Do(func() {
		ffiReadBuilderFree.symbol(rb.ctx)(rb.inner)
		rb.inner = nil
		rb.lib.release()
	})
}

// NewScan creates a TableScan for planning which data files to read.
func (rb *ReadBuilder) NewScan() (*TableScan, error) {
	if rb.inner == nil {
		return nil, ErrClosed
	}
	createFn := ffiReadBuilderNewScan.symbol(rb.ctx)
	inner, err := createFn(rb.inner)
	if err != nil {
		return nil, err
	}
	rb.lib.acquire()
	return &TableScan{ctx: rb.ctx, lib: rb.lib, inner: inner}, nil
}

// NewRead creates a TableRead for reading data from splits.
func (rb *ReadBuilder) NewRead() (*TableRead, error) {
	if rb.inner == nil {
		return nil, ErrClosed
	}
	createFn := ffiReadBuilderNewRead.symbol(rb.ctx)
	inner, err := createFn(rb.inner)
	if err != nil {
		return nil, err
	}
	rb.lib.acquire()
	return &TableRead{ctx: rb.ctx, lib: rb.lib, inner: inner}, nil
}

var ffiReadBuilderFree = newFFI(ffiOpts{
	sym:    "paimon_read_builder_free",
	rType:  &ffi.TypeVoid,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(_ context.Context, ffiCall ffiCall) func(rb *paimonReadBuilder) {
	return func(rb *paimonReadBuilder) {
		ffiCall(
			nil,
			unsafe.Pointer(&rb),
		)
	}
})

var ffiReadBuilderNewScan = newFFI(ffiOpts{
	sym:    "paimon_read_builder_new_scan",
	rType:  &typeResultTableScan,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(ctx context.Context, ffiCall ffiCall) func(rb *paimonReadBuilder) (*paimonTableScan, error) {
	return func(rb *paimonReadBuilder) (*paimonTableScan, error) {
		var result resultTableScan
		ffiCall(
			unsafe.Pointer(&result),
			unsafe.Pointer(&rb),
		)
		if result.error != nil {
			return nil, parseError(ctx, result.error)
		}
		return result.scan, nil
	}
})

var ffiReadBuilderNewRead = newFFI(ffiOpts{
	sym:    "paimon_read_builder_new_read",
	rType:  &typeResultNewRead,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(ctx context.Context, ffiCall ffiCall) func(rb *paimonReadBuilder) (*paimonTableRead, error) {
	return func(rb *paimonReadBuilder) (*paimonTableRead, error) {
		var result resultNewRead
		ffiCall(
			unsafe.Pointer(&result),
			unsafe.Pointer(&rb),
		)
		if result.error != nil {
			return nil, parseError(ctx, result.error)
		}
		return result.read, nil
	}
})
