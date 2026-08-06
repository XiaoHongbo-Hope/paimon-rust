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

//! Write builder for assigning fixed buckets to one postpone-table batch.

use super::write_builder::{ensure_table_write_allowed, validate_commit_user};
use super::{PostponeBucketPlan, Table, TableCommit, TableWrite};
use crate::spec::{CoreOptions, POSTPONE_BUCKET};
use uuid::Uuid;

/// Builder for one-shot fixed-bucket writes to a postpone table.
///
/// This builder is intentionally separate from [`super::WriteBuilder`]. A
/// normal write builder retains `bucket = -2` postpone semantics, while this
/// builder buffers rows for new partitions until it can assign real buckets.
/// Distributed writers should use [`Self::with_bucket_plan`] so each writer
/// receives the same plan computed from global partition statistics.
pub struct PostponeFixedBucketWriteBuilder<'a> {
    table: &'a Table,
    commit_user: String,
    overwrite: bool,
    bucket_plan: Option<PostponeBucketPlan>,
}

impl<'a> PostponeFixedBucketWriteBuilder<'a> {
    pub(crate) fn new(table: &'a Table) -> crate::Result<Self> {
        let schema = table.schema();
        let bucket = CoreOptions::new(schema.options()).bucket();
        if table.is_format_table() || bucket != POSTPONE_BUCKET || schema.primary_keys().is_empty()
        {
            return Err(crate::Error::Unsupported {
                message: format!(
                    "Postpone fixed-bucket writes require a Paimon primary-key table with bucket=-2, but table '{}' has bucket={bucket}",
                    table.identifier().full_name()
                ),
            });
        }

        Ok(Self {
            table,
            commit_user: Uuid::new_v4().to_string(),
            overwrite: false,
            bucket_plan: None,
        })
    }

    /// Get the commit user shared by writers and committers from this builder.
    pub fn commit_user(&self) -> &str {
        &self.commit_user
    }

    /// Set the stable identity shared by all writers and the committer.
    pub fn with_commit_user(mut self, commit_user: impl Into<String>) -> crate::Result<Self> {
        let commit_user = commit_user.into();
        validate_commit_user(&commit_user)?;
        self.commit_user = commit_user;
        Ok(self)
    }

    /// Mark the writer as overwrite-aware.
    pub fn with_overwrite(mut self) -> Self {
        self.overwrite = true;
        self
    }

    /// Supply a bucket plan shared by every writer in one distributed batch.
    ///
    /// The plan must be computed from global partition statistics and cover
    /// every partition written by this builder.
    pub fn with_bucket_plan(mut self, bucket_plan: PostponeBucketPlan) -> Self {
        self.bucket_plan = Some(bucket_plan);
        self
    }

    /// Create a committer sharing this builder's commit user.
    pub fn new_commit(&self) -> TableCommit {
        TableCommit::new(self.table.clone(), self.commit_user.clone())
    }

    /// Try to create a committer sharing this builder's commit user.
    pub fn try_new_commit(&self) -> crate::Result<TableCommit> {
        self.table.ensure_not_branch_reference_for_write()?;
        Ok(self.new_commit())
    }

    /// Create a one-shot fixed-bucket table writer.
    pub fn new_write(&self) -> crate::Result<TableWrite> {
        ensure_table_write_allowed(self.table)?;
        let write = match self.bucket_plan.clone() {
            Some(bucket_plan) => TableWrite::new_postpone_fixed_bucket_with_plan(
                self.table,
                self.commit_user.clone(),
                bucket_plan,
            )?,
            None => TableWrite::new_postpone_fixed_bucket(self.table, self.commit_user.clone())?,
        };
        Ok(if self.overwrite {
            write.with_overwrite()
        } else {
            write
        })
    }
}
