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

	"github.com/apache/arrow-go/v18/arrow"
)

// WriteBuilder creates writers and committers that share one commit identity.
// Writers whose messages are combined into one logical commit must use builders
// created with the same caller-provided commit user.
type WriteBuilder struct {
	ctx       context.Context
	lib       *libRef
	inner     *paimonWriteBuilder
	closeOnce sync.Once
}

// NewWriteBuilder creates a write builder with an automatically generated
// commit identity.
func (t *Table) NewWriteBuilder() (*WriteBuilder, error) {
	if t.inner == nil {
		return nil, ErrClosed
	}
	inner, err := ffiTableNewWriteBuilder.symbol(t.ctx)(t.inner)
	if err != nil {
		return nil, err
	}
	t.lib.acquire()
	return &WriteBuilder{ctx: t.ctx, lib: t.lib, inner: inner}, nil
}

// NewWriteBuilderWithCommitUser creates a write builder with a stable commit
// identity. Use the same commitUser for distributed writers whose messages will
// be merged into one commit, or when retrying a commit with an identifier.
func (t *Table) NewWriteBuilderWithCommitUser(commitUser string) (*WriteBuilder, error) {
	if t.inner == nil {
		return nil, ErrClosed
	}
	inner, err := ffiTableNewWriteBuilderWithCommitUser.symbol(t.ctx)(t.inner, commitUser)
	if err != nil {
		return nil, err
	}
	t.lib.acquire()
	return &WriteBuilder{ctx: t.ctx, lib: t.lib, inner: inner}, nil
}

// Close releases the write builder resources. Safe to call multiple times.
func (wb *WriteBuilder) Close() {
	wb.closeOnce.Do(func() {
		ffiWriteBuilderFree.symbol(wb.ctx)(wb.inner)
		wb.inner = nil
		wb.lib.release()
	})
}

// WithOverwrite enables overwrite mode for writers created by this builder.
// Commit their messages with TableCommit.Overwrite rather than Commit.
func (wb *WriteBuilder) WithOverwrite() error {
	if wb.inner == nil {
		return ErrClosed
	}
	return ffiWriteBuilderWithOverwrite.symbol(wb.ctx)(wb.inner)
}

// NewWrite creates a writer that accumulates Arrow record batches.
func (wb *WriteBuilder) NewWrite() (*TableWrite, error) {
	if wb.inner == nil {
		return nil, ErrClosed
	}
	inner, err := ffiWriteBuilderNewWrite.symbol(wb.ctx)(wb.inner)
	if err != nil {
		return nil, err
	}
	wb.lib.acquire()
	return &TableWrite{ctx: wb.ctx, lib: wb.lib, inner: inner}, nil
}

// NewCommit creates a committer that shares this builder's commit identity.
func (wb *WriteBuilder) NewCommit() (*TableCommit, error) {
	if wb.inner == nil {
		return nil, ErrClosed
	}
	inner, err := ffiWriteBuilderNewCommit.symbol(wb.ctx)(wb.inner)
	if err != nil {
		return nil, err
	}
	wb.lib.acquire()
	return &TableCommit{ctx: wb.ctx, lib: wb.lib, inner: inner}, nil
}

// TableWrite accumulates Arrow record batches until PrepareCommit is called.
type TableWrite struct {
	ctx       context.Context
	lib       *libRef
	inner     *paimonTableWrite
	closeOnce sync.Once
}

// Close releases the writer resources. Unprepared data is discarded. Safe to
// call multiple times.
func (tw *TableWrite) Close() {
	tw.closeOnce.Do(func() {
		ffiTableWriteFree.symbol(tw.ctx)(tw.inner)
		tw.inner = nil
		tw.lib.release()
	})
}

// WriteArrowBatch writes one Arrow record batch. Its field count, order, names,
// and types must match the table schema. The record remains owned by the caller.
func (tw *TableWrite) WriteArrowBatch(record arrow.Record) error {
	if tw.inner == nil {
		return ErrClosed
	}
	return withOwnedArrowRecord(
		record,
		"paimon: record batch must not be nil",
		func(array, schema unsafe.Pointer) error {
			return ffiTableWriteWriteArrowBatch.symbol(tw.ctx)(tw.inner, array, schema)
		},
	)
}

// PrepareCommit closes the current file writers and returns opaque commit
// messages. The writer can be reused for another round of writes afterwards.
func (tw *TableWrite) PrepareCommit() (*CommitMessages, error) {
	if tw.inner == nil {
		return nil, ErrClosed
	}
	inner, err := ffiTableWritePrepareCommit.symbol(tw.ctx)(tw.inner)
	if err != nil {
		return nil, err
	}
	tw.lib.acquire()
	return &CommitMessages{ctx: tw.ctx, lib: tw.lib, inner: inner}, nil
}

// CommitMessages contains the files produced by one or more writers.
type CommitMessages struct {
	ctx       context.Context
	lib       *libRef
	inner     *paimonCommitMessages
	closeOnce sync.Once
}

// Close releases the commit messages. Safe to call multiple times.
func (m *CommitMessages) Close() {
	m.closeOnce.Do(func() {
		ffiCommitMessagesFree.symbol(m.ctx)(m.inner)
		m.inner = nil
		m.lib.release()
	})
}

// Merge appends a copy of source's messages to this handle. Both handles remain
// valid and must be closed separately. They must share a table and commit user.
func (m *CommitMessages) Merge(source *CommitMessages) error {
	if m.inner == nil {
		return ErrClosed
	}
	if source == nil || source.inner == nil {
		return ErrClosed
	}
	return ffiCommitMessagesMerge.symbol(m.ctx)(m.inner, source.inner)
}

// TableCommit persists or aborts prepared commit messages.
type TableCommit struct {
	ctx       context.Context
	lib       *libRef
	inner     *paimonTableCommit
	closeOnce sync.Once
}

// Close releases the committer resources. Safe to call multiple times.
func (tc *TableCommit) Close() {
	tc.closeOnce.Do(func() {
		ffiTableCommitFree.symbol(tc.ctx)(tc.inner)
		tc.inner = nil
		tc.lib.release()
	})
}

func (tc *TableCommit) withMessages(
	messages *CommitMessages,
	operation func(*paimonTableCommit, *paimonCommitMessages) error,
) error {
	if tc.inner == nil {
		return ErrClosed
	}
	if messages == nil || messages.inner == nil {
		return ErrClosed
	}
	return operation(tc.inner, messages.inner)
}

func (tc *TableCommit) withMessagesAndIdentifier(
	messages *CommitMessages,
	commitIdentifier int64,
	operation func(*paimonTableCommit, *paimonCommitMessages, int64) error,
) error {
	if tc.inner == nil {
		return ErrClosed
	}
	if messages == nil || messages.inner == nil {
		return ErrClosed
	}
	return operation(tc.inner, messages.inner, commitIdentifier)
}

// Commit appends the prepared data to the table.
func (tc *TableCommit) Commit(messages *CommitMessages) error {
	return tc.withMessages(messages, ffiTableCommitCommit.symbol(tc.ctx))
}

// CommitWithIdentifier appends data with a caller-provided monotonically
// increasing identifier.
func (tc *TableCommit) CommitWithIdentifier(messages *CommitMessages, commitIdentifier int64) error {
	return tc.withMessagesAndIdentifier(
		messages,
		commitIdentifier,
		ffiTableCommitCommitWithIdentifier.symbol(tc.ctx),
	)
}

// FilterAndCommitWithIdentifier makes a retry idempotent by filtering a
// previously committed identifier before committing it if it is new.
func (tc *TableCommit) FilterAndCommitWithIdentifier(
	messages *CommitMessages,
	commitIdentifier int64,
) error {
	return tc.withMessagesAndIdentifier(
		messages,
		commitIdentifier,
		ffiTableCommitFilterAndCommitWithIdentifier.symbol(tc.ctx),
	)
}

// Overwrite replaces data in the partitions written by an overwrite-enabled
// WriteBuilder.
func (tc *TableCommit) Overwrite(messages *CommitMessages) error {
	return tc.withMessages(messages, ffiTableCommitOverwrite.symbol(tc.ctx))
}

// OverwriteWithIdentifier overwrites data with a stable commit identifier.
func (tc *TableCommit) OverwriteWithIdentifier(
	messages *CommitMessages,
	commitIdentifier int64,
) error {
	return tc.withMessagesAndIdentifier(
		messages,
		commitIdentifier,
		ffiTableCommitOverwriteWithIdentifier.symbol(tc.ctx),
	)
}

// TruncateTable removes all table data.
func (tc *TableCommit) TruncateTable() error {
	if tc.inner == nil {
		return ErrClosed
	}
	return ffiTableCommitTruncateTable.symbol(tc.ctx)(tc.inner)
}

// TruncateTableWithIdentifier removes all table data with a stable commit
// identifier.
func (tc *TableCommit) TruncateTableWithIdentifier(commitIdentifier int64) error {
	if tc.inner == nil {
		return ErrClosed
	}
	return ffiTableCommitTruncateTableWithIdentifier.symbol(tc.ctx)(tc.inner, commitIdentifier)
}

// Abort performs best-effort cleanup of files created for a prepared commit.
func (tc *TableCommit) Abort(messages *CommitMessages) error {
	return tc.withMessages(messages, ffiTableCommitAbort.symbol(tc.ctx))
}
