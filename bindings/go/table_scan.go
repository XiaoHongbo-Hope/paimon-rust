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

// TableScan scans a table and produces a Plan containing data splits.
type TableScan struct {
	ctx       context.Context
	lib       *libRef
	inner     *paimonTableScan
	closeOnce sync.Once
}

// Close releases the table scan resources. Safe to call multiple times.
func (ts *TableScan) Close() {
	ts.closeOnce.Do(func() {
		ffiTableScanFree.symbol(ts.ctx)(ts.inner)
		ts.inner = nil
		ts.lib.release()
	})
}

// Plan executes the scan and returns a Plan containing data splits to read.
func (ts *TableScan) Plan() (*Plan, error) {
	if ts.inner == nil {
		return nil, ErrClosed
	}
	planFn := ffiTableScanPlan.symbol(ts.ctx)
	inner, err := planFn(ts.inner)
	if err != nil {
		return nil, err
	}
	ts.lib.acquire()
	return &Plan{handle: newPlanHandle(ts.ctx, ts.lib, inner)}, nil
}

var ffiTableScanFree = newFFI(ffiOpts{
	sym:    "paimon_table_scan_free",
	rType:  &ffi.TypeVoid,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(_ context.Context, ffiCall ffiCall) func(scan *paimonTableScan) {
	return func(scan *paimonTableScan) {
		ffiCall(
			nil,
			unsafe.Pointer(&scan),
		)
	}
})

var ffiTableScanPlan = newFFI(ffiOpts{
	sym:    "paimon_table_scan_plan",
	rType:  &typeResultPlan,
	aTypes: []*ffi.Type{&ffi.TypePointer},
}, func(ctx context.Context, ffiCall ffiCall) func(scan *paimonTableScan) (*paimonPlan, error) {
	return func(scan *paimonTableScan) (*paimonPlan, error) {
		var result resultPlan
		ffiCall(
			unsafe.Pointer(&result),
			unsafe.Pointer(&scan),
		)
		if result.error != nil {
			return nil, parseError(ctx, result.error)
		}
		return result.plan, nil
	}
})
