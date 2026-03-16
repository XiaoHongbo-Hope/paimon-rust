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
	"errors"
	"os"
	"sync/atomic"
	"unsafe"

	"github.com/jupiterrider/ffi"
)

// libRef is an atomic reference counter for the loaded shared library.
// Every object that may call FFI (including during Close/Release) must
// hold a reference. The library is freed only when the count drops to zero.
type libRef struct {
	count atomic.Int32
	lib   uintptr
}

func newLibRef(lib uintptr) *libRef {
	r := &libRef{lib: lib}
	r.count.Store(1)
	return r
}

func (r *libRef) acquire() { r.count.Add(1) }

func (r *libRef) release() {
	if r.count.Add(-1) == 0 {
		_ = freeLibrary(r.lib)
	}
}

type ffiOpts struct {
	sym    contextKey
	rType  *ffi.Type
	aTypes []*ffi.Type
}

type ffiCall func(rValue unsafe.Pointer, aValues ...unsafe.Pointer)

type contextKey string

func (c contextKey) String() string {
	return string(c)
}

type contextWithFFI func(ctx context.Context, lib uintptr) (context.Context, error)

// FFI is a generic type-safe wrapper for a foreign function.
type FFI[T any] struct {
	opts     ffiOpts
	withFunc func(ctx context.Context, ffiCall ffiCall) T
}

func newFFI[T any](opts ffiOpts, withFunc func(ctx context.Context, ffiCall ffiCall) T) *FFI[T] {
	f := &FFI[T]{
		opts:     opts,
		withFunc: withFunc,
	}
	withFFIs = append(withFFIs, f.withFFI)
	return f
}

func (f *FFI[T]) symbol(ctx context.Context) T {
	return ctx.Value(f.opts.sym).(T)
}

func (f *FFI[T]) withFFI(ctx context.Context, lib uintptr) (context.Context, error) {
	var cif ffi.Cif
	if status := ffi.PrepCif(
		&cif,
		ffi.DefaultAbi,
		uint32(len(f.opts.aTypes)),
		f.opts.rType,
		f.opts.aTypes...,
	); status != ffi.OK {
		return nil, errors.New(status.String())
	}
	fn, err := getProcAddress(lib, f.opts.sym.String())
	if err != nil {
		return nil, err
	}
	val := f.withFunc(ctx, func(rValue unsafe.Pointer, aValues ...unsafe.Pointer) {
		ffi.Call(&cif, fn, rValue, aValues...)
	})
	return context.WithValue(ctx, f.opts.sym, val), nil
}

var withFFIs []contextWithFFI

func newContext(path string) (ctx context.Context, lib *libRef, err error) {
	handle, err := loadLibrary(path)
	if err != nil {
		return
	}
	ctx = context.Background()
	for _, withFFI := range withFFIs {
		ctx, err = withFFI(ctx, handle)
		if err != nil {
			_ = freeLibrary(handle)
			return
		}
	}
	if removeErr := os.Remove(path); removeErr != nil && !errors.Is(removeErr, os.ErrNotExist) {
		_ = freeLibrary(handle)
		err = removeErr
		return
	}
	lib = newLibRef(handle)
	return
}
