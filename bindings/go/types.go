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
	"unsafe"

	"github.com/jupiterrider/ffi"
)

// FFI type definitions mirroring C repr structs from paimon-c.
var (
	// Result types: { value, *error }
	// paimon_result_catalog_new { catalog: paimon_catalog, error: *paimon_error }
	typeResultCatalogNew = ffi.Type{
		Type: ffi.Struct,
		Elements: &[]*ffi.Type{
			&ffi.TypePointer,
			&ffi.TypePointer,
			nil,
		}[0],
	}

	// paimon_result_get_table { table: paimon_table, error: *paimon_error }
	typeResultGetTable = ffi.Type{
		Type: ffi.Struct,
		Elements: &[]*ffi.Type{
			&ffi.TypePointer,
			&ffi.TypePointer,
			nil,
		}[0],
	}

	// paimon_result_identifier_new { identifier: paimon_identifier, error: *paimon_error }
	typeResultIdentifierNew = ffi.Type{
		Type: ffi.Struct,
		Elements: &[]*ffi.Type{
			&ffi.TypePointer,
			&ffi.TypePointer,
			nil,
		}[0],
	}

	// paimon_result_new_read { read: paimon_table_read, error: *paimon_error }
	typeResultNewRead = ffi.Type{
		Type: ffi.Struct,
		Elements: &[]*ffi.Type{
			&ffi.TypePointer,
			&ffi.TypePointer,
			nil,
		}[0],
	}

	// paimon_result_read_builder { read_builder: paimon_read_builder, error: *paimon_error }
	typeResultReadBuilder = ffi.Type{
		Type: ffi.Struct,
		Elements: &[]*ffi.Type{
			&ffi.TypePointer,
			&ffi.TypePointer,
			nil,
		}[0],
	}

	// paimon_result_table_scan { scan: paimon_table_scan, error: *paimon_error }
	typeResultTableScan = ffi.Type{
		Type: ffi.Struct,
		Elements: &[]*ffi.Type{
			&ffi.TypePointer,
			&ffi.TypePointer,
			nil,
		}[0],
	}

	// paimon_result_plan { plan: paimon_plan, error: *paimon_error }
	typeResultPlan = ffi.Type{
		Type: ffi.Struct,
		Elements: &[]*ffi.Type{
			&ffi.TypePointer,
			&ffi.TypePointer,
			nil,
		}[0],
	}

	// paimon_result_record_batch_reader { reader: *paimon_record_batch_reader, error: *paimon_error }
	typeResultRecordBatchReader = ffi.Type{
		Type: ffi.Struct,
		Elements: &[]*ffi.Type{
			&ffi.TypePointer,
			&ffi.TypePointer,
			nil,
		}[0],
	}

	// paimon_arrow_batch { array: *c_void, schema: *c_void }
	typeArrowBatch = ffi.Type{
		Type: ffi.Struct,
		Elements: &[]*ffi.Type{
			&ffi.TypePointer,
			&ffi.TypePointer,
			nil,
		}[0],
	}

	// paimon_result_next_batch { batch: paimon_arrow_batch, error: *paimon_error }
	typeResultNextBatch = ffi.Type{
		Type: ffi.Struct,
		Elements: &[]*ffi.Type{
			&ffi.TypePointer, // batch.array
			&ffi.TypePointer, // batch.schema
			&ffi.TypePointer, // error
			nil,
		}[0],
	}
)

// Go mirror structs for C types.

type paimonBytes struct {
	data *byte
	len  uintptr
}

type paimonError struct {
	code    int32
	message paimonBytes
}

// Opaque pointer wrappers
type paimonCatalog struct{}
type paimonIdentifier struct{}
type paimonTable struct{}
type paimonReadBuilder struct{}
type paimonTableScan struct{}
type paimonTableRead struct{}
type paimonPlan struct{}
type paimonRecordBatchReader struct{}

// Result types matching the C repr structs
type resultCatalogNew struct {
	catalog *paimonCatalog
	error   *paimonError
}

type resultGetTable struct {
	table *paimonTable
	error *paimonError
}

type resultIdentifierNew struct {
	identifier *paimonIdentifier
	error      *paimonError
}

type resultNewRead struct {
	read  *paimonTableRead
	error *paimonError
}

type resultReadBuilder struct {
	readBuilder *paimonReadBuilder
	error       *paimonError
}

type resultTableScan struct {
	scan  *paimonTableScan
	error *paimonError
}

type resultPlan struct {
	plan  *paimonPlan
	error *paimonError
}

type resultRecordBatchReader struct {
	reader *paimonRecordBatchReader
	error  *paimonError
}

// arrowBatch holds a single Arrow record batch via the Arrow C Data Interface.
type arrowBatch struct {
	ctx      context.Context
	lib      *libRef
	array    unsafe.Pointer
	schema   unsafe.Pointer
	released bool
}

func (b *arrowBatch) release() {
	if b.released {
		return
	}
	b.released = true
	runtime.SetFinalizer(b, nil)
	ffiArrowBatchFree.symbol(b.ctx)(b.array, b.schema)
	b.lib.release()
}

type resultNextBatch struct {
	array  unsafe.Pointer
	schema unsafe.Pointer
	error  *paimonError
}

func parseBytes(b paimonBytes) []byte {
	if b.len == 0 {
		return nil
	}
	data := make([]byte, b.len)
	copy(data, unsafe.Slice(b.data, b.len))
	return data
}
