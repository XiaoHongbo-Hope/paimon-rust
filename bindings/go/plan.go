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
	"runtime"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/jupiterrider/ffi"
)

// Plan holds the result of a table scan, containing data splits to read.
type Plan struct {
	handle    *planHandle
	closeOnce sync.Once
}

// Close releases the plan resources. Safe to call multiple times.
// DataSplits obtained from Splits() remain valid after Close.
func (p *Plan) Close() {
	p.closeOnce.Do(func() {
		p.handle.release()
		p.handle = nil
	})
}

// NumSplits returns the number of data splits in this plan.
func (p *Plan) NumSplits() int {
	if p.handle == nil {
		panic("paimon: NumSplits called on closed Plan")
	}
	return ffiPlanNumSplits.symbol(p.handle.ctx)(p.handle.inner)
}

// Splits returns all data splits in this plan. The returned DataSplits
// keep the underlying plan data alive via GC-attached reference counting,
// so they remain valid even after Plan.Close() is called.
func (p *Plan) Splits() []DataSplit {
	if p.handle == nil {
		panic("paimon: Splits called on closed Plan")
	}
	n := p.NumSplits()
	set := newSplitSet(p.handle)
	splits := make([]DataSplit, n)
	for i := 0; i < n; i++ {
		splits[i] = DataSplit{set: set, index: i}
	}
	return splits
}

// planHandle wraps the C plan pointer with reference counting.
// The C plan is freed when the last reference is released.
type planHandle struct {
	ctx   context.Context
	lib   *libRef
	inner *paimonPlan
	refs  atomic.Int32
}

func newPlanHandle(ctx context.Context, lib *libRef, inner *paimonPlan) *planHandle {
	h := &planHandle{ctx: ctx, lib: lib, inner: inner}
	h.refs.Store(1) // initial ref for the creator
	return h
}

func (h *planHandle) acquire() { h.refs.Add(1) }

func (h *planHandle) release() {
	if h.refs.Add(-1) == 0 {
		ffiPlanFree.symbol(h.ctx)(h.inner)
		h.lib.release()
	}
}

// splitSet ties DataSplit values to a planHandle via a GC finalizer.
// When all DataSplits (and the slice backing them) become unreachable,
// the GC collects the splitSet and its finalizer releases the planHandle ref.
type splitSet struct {
	handle *planHandle
}

func newSplitSet(h *planHandle) *splitSet {
	h.acquire()
	s := &splitSet{handle: h}
	runtime.SetFinalizer(s, (*splitSet).release)
	return s
}

func (s *splitSet) release() {
	runtime.SetFinalizer(s, nil)
	s.handle.release()
}

// DataSplit identifies a single data split within a plan.
// DataSplits keep the underlying plan data alive via GC-attached
// reference counting, so they are safe to use independently.
type DataSplit struct {
	set   *splitSet
	index int
}

var ffiPlanFree = newFFI(ffiOpts{
	sym:    "paimon_plan_free",
	rType:  &ffi.TypeVoid,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(_ context.Context, ffiCall ffiCall) func(plan *paimonPlan) {
	return func(plan *paimonPlan) {
		ffiCall(
			nil,
			unsafe.Pointer(&plan),
		)
	}
})

var ffiPlanNumSplits = newFFI(ffiOpts{
	sym:    "paimon_plan_num_splits",
	rType:  &ffi.TypePointer, // usize == pointer-sized on 64-bit
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(_ context.Context, ffiCall ffiCall) func(plan *paimonPlan) int {
	return func(plan *paimonPlan) int {
		var count uintptr
		ffiCall(
			unsafe.Pointer(&count),
			unsafe.Pointer(&plan),
		)
		return int(count)
	}
})
