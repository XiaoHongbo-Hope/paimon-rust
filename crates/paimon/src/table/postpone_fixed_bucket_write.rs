// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

use super::postpone_bucket_plan::data_invalid;
use super::postpone_fixed_bucket_router::validate_postpone_fixed_bucket_table;
use crate::spec::CoreOptions;
use crate::table::{
    CommitMessage, PostponeBucketPlan, PostponeFixedBucketRouter, Table, TableCommit, TableWrite,
};
use crate::Result;
use arrow_array::RecordBatch;

pub struct PostponeFixedBucketTableWrite {
    inner: TableWrite,
    router: PostponeFixedBucketRouter,
    prepare_started: bool,
}

impl PostponeFixedBucketTableWrite {
    pub(crate) fn new(
        table: &Table,
        commit_user: String,
        plan: PostponeBucketPlan,
        overwrite: bool,
    ) -> Result<Self> {
        validate_postpone_fixed_bucket_write(table)?;
        let inner = TableWrite::new(table, commit_user)?;
        Ok(Self {
            inner: if overwrite {
                inner.with_overwrite()
            } else {
                inner
            },
            router: PostponeFixedBucketRouter::new(table, plan)?,
            prepare_started: false,
        })
    }

    pub async fn write_arrow_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        self.ensure_writable()?;
        let Some(batch) = self.inner.normalize_write_batch(batch)? else {
            return Ok(());
        };
        for routed in self.router.route(&batch)? {
            self.inner
                .write_partition_bucket_batch(routed.partition, routed.bucket, routed.batch)
                .await?;
        }
        Ok(())
    }

    pub async fn write_arrow(&mut self, batches: &[RecordBatch]) -> Result<()> {
        for batch in batches {
            self.write_arrow_batch(batch).await?;
        }
        Ok(())
    }

    pub async fn prepare_commit(&mut self) -> Result<Vec<CommitMessage>> {
        self.ensure_writable()?;
        self.prepare_started = true;
        let mut messages = self.inner.prepare_commit().await?;
        for message in &mut messages {
            message.total_buckets = self.router.total_buckets(&message.partition);
        }
        Ok(messages)
    }

    fn ensure_writable(&self) -> Result<()> {
        if self.prepare_started {
            return Err(data_invalid("Fixed-bucket postpone TableWrite only supports one prepare_commit call; create a new writer for the next batch"));
        }
        Ok(())
    }
}

pub struct PostponeFixedBucketTableCommit {
    inner: TableCommit,
    overwrite: bool,
}

impl PostponeFixedBucketTableCommit {
    pub(crate) fn new(table: &Table, commit_user: String, overwrite: bool) -> Self {
        Self {
            inner: TableCommit::new(table.clone(), commit_user),
            overwrite,
        }
    }

    pub async fn commit(&self, messages: Vec<CommitMessage>) -> Result<()> {
        if self.overwrite {
            self.inner.overwrite(messages, None).await
        } else {
            self.inner.commit(messages).await
        }
    }

    pub async fn commit_with_identifier(
        &self,
        messages: Vec<CommitMessage>,
        commit_identifier: i64,
    ) -> Result<()> {
        if self.overwrite {
            self.inner
                .overwrite_with_identifier(messages, None, commit_identifier)
                .await
        } else {
            self.inner
                .commit_with_identifier(messages, commit_identifier)
                .await
        }
    }

    pub async fn filter_and_commit_with_identifier(
        &self,
        messages: Vec<CommitMessage>,
        commit_identifier: i64,
    ) -> Result<()> {
        if self.overwrite {
            self.inner
                .overwrite_with_identifier(messages, None, commit_identifier)
                .await
        } else {
            self.inner
                .filter_and_commit_with_identifier(messages, commit_identifier)
                .await
        }
    }

    pub async fn overwrite(&self, messages: Vec<CommitMessage>) -> Result<()> {
        self.inner.overwrite(messages, None).await
    }

    pub async fn overwrite_with_identifier(
        &self,
        messages: Vec<CommitMessage>,
        commit_identifier: i64,
    ) -> Result<()> {
        self.inner
            .overwrite_with_identifier(messages, None, commit_identifier)
            .await
    }

    pub async fn truncate_table(&self) -> Result<()> {
        self.inner.truncate_table().await
    }

    pub async fn truncate_table_with_identifier(&self, commit_identifier: i64) -> Result<()> {
        self.inner
            .truncate_table_with_identifier(commit_identifier)
            .await
    }

    pub async fn abort(&self, messages: &[CommitMessage]) -> Result<()> {
        self.inner.abort(messages).await
    }
}

fn validate_postpone_fixed_bucket_write(table: &Table) -> Result<()> {
    validate_postpone_fixed_bucket_table(table)?;
    if CoreOptions::new(table.schema().options()).deletion_vectors_enabled() {
        return Err(crate::Error::Unsupported {
            message: format!(
                "Table '{}' cannot use postpone fixed-bucket writes with deletion-vectors.enabled=true because deletion-vector scans skip the level-0 files produced by batch writers; use the normal postpone writer or disable deletion vectors",
                table.identifier().full_name()
            ),
        });
    }
    Ok(())
}
