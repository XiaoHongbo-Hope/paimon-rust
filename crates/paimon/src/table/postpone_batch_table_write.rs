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

//! Fixed-bucket batch writes for postpone tables.

use crate::spec::{
    batch_to_serialized_bytes, BucketFunctionType, CoreOptions, DataField, EMPTY_SERIALIZED_ROW,
    POSTPONE_BUCKET,
};
use crate::table::bucket_function::{batch_bucket_ids, validate_bucket_function};
use crate::table::table_write::take_rows;
use crate::table::write_builder::{ensure_table_write_allowed, validate_commit_user};
use crate::table::{CommitMessage, Table, TableCommit, TableWrite};
use crate::Result;
use arrow_array::{Array, Int32Array, RecordBatch};
use std::collections::HashMap;
use uuid::Uuid;

pub const POSTPONE_BUCKET_PLAN_TOTAL_BUCKETS_FIELD: &str = "total_buckets";

fn data_invalid(message: impl Into<String>) -> crate::Error {
    crate::Error::DataInvalid {
        message: message.into(),
        source: None,
    }
}

#[derive(Debug, Clone, Default)]
pub struct PostponeBucketPlan {
    bucket_counts: HashMap<Vec<u8>, i32>,
}

impl PostponeBucketPlan {
    pub fn from_arrow(table: &Table, batch: &RecordBatch) -> Result<Self> {
        let partition_fields = table.schema().partition_fields();
        let partition_count = partition_fields.len();
        if batch.num_columns() != partition_count + 1 {
            return Err(data_invalid(format!(
                "Postpone bucket plan expected {} partition column(s) plus '{POSTPONE_BUCKET_PLAN_TOTAL_BUCKETS_FIELD}', got {} columns",
                partition_count,
                batch.num_columns()
            )));
        }

        let expected = crate::arrow::build_target_arrow_schema(&partition_fields)?;
        let actual = batch.schema();
        if !expected
            .fields()
            .iter()
            .zip(actual.fields())
            .all(|(expected, actual)| {
                actual.name() == expected.name() && actual.data_type() == expected.data_type()
            })
        {
            return Err(data_invalid(
                "Postpone bucket plan partition fields do not match the table schema",
            ));
        }
        for (index, field) in expected.fields().iter().enumerate() {
            if !field.is_nullable() && batch.column(index).null_count() != 0 {
                return Err(data_invalid(format!(
                    "Postpone bucket plan partition column '{}' is NOT NULL but contains null values",
                    field.name()
                )));
            }
        }

        let count_field = actual.field(partition_count);
        if count_field.name() != POSTPONE_BUCKET_PLAN_TOTAL_BUCKETS_FIELD
            || count_field.data_type() != &arrow_schema::DataType::Int32
        {
            return Err(data_invalid(format!(
                "Postpone bucket plan final field must be '{POSTPONE_BUCKET_PLAN_TOTAL_BUCKETS_FIELD}': Int32, got '{}': {:?}",
                count_field.name(),
                count_field.data_type()
            )));
        }
        let counts = batch
            .column(partition_count)
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| {
                data_invalid("Postpone bucket plan total_buckets column is not Int32")
            })?;
        let partition_indices = (0..partition_count).collect::<Vec<_>>();
        let partitions = batch_to_serialized_bytes(batch, &partition_indices, &partition_fields)?;
        let mut bucket_counts = HashMap::with_capacity(batch.num_rows());
        for (row, partition) in partitions.into_iter().enumerate() {
            if counts.is_null(row) {
                return Err(data_invalid(format!(
                    "Postpone bucket plan total_buckets is null at row {row}"
                )));
            }
            let total_buckets = counts.value(row);
            if total_buckets <= 0 {
                return Err(data_invalid(format!(
                    "Postpone bucket plan total_buckets must be positive at row {row}, got {total_buckets}"
                )));
            }
            if let Some(previous) = bucket_counts.insert(partition, total_buckets) {
                if previous != total_buckets {
                    return Err(data_invalid(format!(
                        "Postpone bucket plan contains conflicting total bucket counts {previous} and {total_buckets} for one partition"
                    )));
                }
            }
        }
        Ok(Self { bucket_counts })
    }

    fn total_buckets(&self, partition: &[u8]) -> Option<i32> {
        self.bucket_counts.get(partition).copied()
    }
}

#[derive(Debug)]
pub struct PostponeBucketBatch {
    pub partition: Vec<u8>,
    pub bucket: i32,
    pub batch: RecordBatch,
}

pub struct PostponeFixedBucketRouter {
    fields: Vec<DataField>,
    partition_field_indices: Vec<usize>,
    bucket_key_indices: Vec<usize>,
    bucket_function_type: BucketFunctionType,
    plan: PostponeBucketPlan,
}

impl PostponeFixedBucketRouter {
    pub fn new(table: &Table, plan: PostponeBucketPlan) -> Result<Self> {
        validate_postpone_fixed_bucket_table(table)?;
        let schema = table.schema();
        let options = CoreOptions::new(schema.options());
        let bucket_key_indices = field_indices(schema.fields(), &schema.bucket_keys());
        let bucket_key_fields = bucket_key_indices
            .iter()
            .map(|&index| schema.fields()[index].clone())
            .collect::<Vec<_>>();
        let bucket_function_type = options.bucket_function_type()?;
        if !bucket_key_fields.is_empty() {
            validate_bucket_function(bucket_function_type, &bucket_key_fields)?;
        }
        Ok(Self {
            fields: schema.fields().to_vec(),
            partition_field_indices: field_indices(schema.fields(), schema.partition_keys()),
            bucket_key_indices,
            bucket_function_type,
            plan,
        })
    }

    pub fn route(&self, batch: &RecordBatch) -> Result<Vec<PostponeBucketBatch>> {
        let mut output = Vec::new();
        for (partition, batch) in
            partition_batches(batch, &self.partition_field_indices, &self.fields)?
        {
            let total_buckets = self.plan.total_buckets(&partition).ok_or_else(|| {
                data_invalid("Postpone bucket plan does not contain an input partition")
            })?;
            let buckets = if total_buckets <= 1 || self.bucket_key_indices.is_empty() {
                vec![0; batch.num_rows()]
            } else {
                batch_bucket_ids(
                    &batch,
                    &self.bucket_key_indices,
                    &self.fields,
                    self.bucket_function_type,
                    total_buckets,
                )?
            };
            let mut groups: HashMap<i32, Vec<usize>> = HashMap::new();
            for (row, bucket) in buckets.into_iter().enumerate() {
                groups.entry(bucket).or_default().push(row);
            }
            for (bucket, rows) in groups {
                output.push(PostponeBucketBatch {
                    partition: partition.clone(),
                    bucket,
                    batch: take_rows(&batch, &rows)?,
                });
            }
        }
        Ok(output)
    }

    fn total_buckets(&self, partition: &[u8]) -> Option<i32> {
        self.plan.total_buckets(partition)
    }
}

pub struct PostponeFixedBucketTableWrite {
    inner: TableWrite,
    router: PostponeFixedBucketRouter,
    prepare_started: bool,
}

impl PostponeFixedBucketTableWrite {
    fn new(
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
        self.write_normalized_batch(batch).await
    }

    async fn write_normalized_batch(&mut self, batch: RecordBatch) -> Result<()> {
        self.ensure_writable()?;
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
            return Err(one_shot_error());
        }
        Ok(())
    }
}

pub struct PostponeFixedBucketTableCommit {
    inner: TableCommit,
    overwrite: bool,
}

impl PostponeFixedBucketTableCommit {
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

    pub fn into_inner(self) -> TableCommit {
        self.inner
    }

    pub async fn abort(&self, messages: &[CommitMessage]) -> Result<()> {
        self.inner.abort(messages).await
    }
}

pub struct PostponeFixedBucketWriteBuilder<'a> {
    table: &'a Table,
    commit_user: String,
    overwrite: bool,
    bucket_plan: Option<PostponeBucketPlan>,
}

impl<'a> PostponeFixedBucketWriteBuilder<'a> {
    pub(crate) fn new(table: &'a Table) -> Result<Self> {
        validate_postpone_fixed_bucket_table(table)?;
        Ok(Self {
            table,
            commit_user: Uuid::new_v4().to_string(),
            overwrite: false,
            bucket_plan: None,
        })
    }

    pub fn commit_user(&self) -> &str {
        &self.commit_user
    }

    pub fn with_commit_user(mut self, commit_user: impl Into<String>) -> Result<Self> {
        let commit_user = commit_user.into();
        validate_commit_user(&commit_user)?;
        self.commit_user = commit_user;
        Ok(self)
    }

    pub fn with_overwrite(mut self) -> Self {
        self.overwrite = true;
        self
    }

    pub fn with_bucket_plan(mut self, bucket_plan: PostponeBucketPlan) -> Self {
        self.bucket_plan = Some(bucket_plan);
        self
    }

    pub fn new_commit(&self) -> PostponeFixedBucketTableCommit {
        PostponeFixedBucketTableCommit {
            inner: TableCommit::new(self.table.clone(), self.commit_user.clone()),
            overwrite: self.overwrite,
        }
    }

    pub fn try_new_commit(&self) -> Result<PostponeFixedBucketTableCommit> {
        self.table.ensure_not_branch_reference_for_write()?;
        Ok(self.new_commit())
    }

    pub fn new_write(&self) -> Result<PostponeFixedBucketTableWrite> {
        ensure_table_write_allowed(self.table)?;
        let plan = self
            .bucket_plan
            .clone()
            .ok_or_else(|| data_invalid("A resolved postpone bucket plan is required"))?;
        PostponeFixedBucketTableWrite::new(
            self.table,
            self.commit_user.clone(),
            plan,
            self.overwrite,
        )
    }
}

fn validate_postpone_fixed_bucket_table(table: &Table) -> Result<()> {
    let schema = table.schema();
    let bucket = CoreOptions::new(schema.options()).bucket();
    if table.is_format_table() || bucket != POSTPONE_BUCKET || schema.primary_keys().is_empty() {
        return Err(crate::Error::Unsupported {
            message: format!(
                "Postpone fixed-bucket writes require a Paimon primary-key table with bucket=-2, but table '{}' has bucket={bucket}",
                table.identifier().full_name()
            ),
        });
    }
    Ok(())
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

fn field_indices(fields: &[DataField], names: &[String]) -> Vec<usize> {
    names
        .iter()
        .filter_map(|name| fields.iter().position(|field| field.name() == name))
        .collect()
}

fn partition_batches(
    batch: &RecordBatch,
    partition_field_indices: &[usize],
    fields: &[DataField],
) -> Result<Vec<(Vec<u8>, RecordBatch)>> {
    let partitions = if partition_field_indices.is_empty() {
        vec![EMPTY_SERIALIZED_ROW.clone(); batch.num_rows()]
    } else {
        batch_to_serialized_bytes(batch, partition_field_indices, fields)?
    };
    let mut groups: HashMap<Vec<u8>, Vec<usize>> = HashMap::new();
    for (row, partition) in partitions.into_iter().enumerate() {
        groups.entry(partition).or_default().push(row);
    }
    groups
        .into_iter()
        .map(|(partition, rows)| Ok((partition, take_rows(batch, &rows)?)))
        .collect()
}

fn one_shot_error() -> crate::Error {
    data_invalid("Fixed-bucket postpone TableWrite only supports one prepare_commit call; create a new writer for the next batch")
}
