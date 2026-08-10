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
use crate::table::postpone_bucket::binary_row_batch_size;
use crate::table::table_write::take_rows;
use crate::table::write_builder::{ensure_table_write_allowed, validate_commit_user};
use crate::table::{CommitMessage, SnapshotManager, Table, TableCommit, TableScan, TableWrite};
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

    fn contains(&self, partition: &[u8]) -> bool {
        self.bucket_counts.contains_key(partition)
    }

    fn total_buckets(&self, partition: &[u8]) -> Option<i32> {
        self.bucket_counts.get(partition).copied()
    }
}

#[derive(Debug, Clone, Default)]
pub struct PostponePartitionStats {
    stats: HashMap<Vec<u8>, (i64, i64)>,
}

impl PostponePartitionStats {
    pub fn merge(&mut self, other: &Self) {
        for (partition, (rows, size)) in &other.stats {
            let entry = self.stats.entry(partition.clone()).or_default();
            entry.0 = entry.0.saturating_add(*rows);
            entry.1 = entry.1.saturating_add(*size);
        }
    }

    fn add(&mut self, partition: Vec<u8>, rows: i64, size: i64) {
        let entry = self.stats.entry(partition).or_default();
        entry.0 = entry.0.saturating_add(rows);
        entry.1 = entry.1.saturating_add(size);
    }
}

pub struct PostponeBucketPlanner {
    fields: Vec<DataField>,
    partition_field_indices: Vec<usize>,
    max_parallelism: i32,
    target_rows_per_bucket: Option<i64>,
    target_size_per_bucket: Option<i64>,
    current_plan: PostponeBucketPlan,
    postpone_row_counts: HashMap<Vec<u8>, i64>,
}

impl PostponeBucketPlanner {
    pub async fn new(table: &Table) -> Result<Self> {
        validate_postpone_fixed_bucket_table(table)?;
        let schema = table.schema();
        let options = CoreOptions::new(schema.options());
        let target_rows_per_bucket = options.postpone_target_row_num_per_bucket()?;
        let target_size_per_bucket = if target_rows_per_bucket.is_none() {
            Some(options.postpone_target_size_per_bucket()?)
        } else {
            None
        };
        let (bucket_counts, postpone_row_counts) = load_bucket_metadata(table).await?;
        Ok(Self {
            fields: schema.fields().to_vec(),
            partition_field_indices: field_indices(schema.fields(), schema.partition_keys()),
            max_parallelism: options.postpone_batch_write_fixed_bucket_max_parallelism()?,
            target_rows_per_bucket,
            target_size_per_bucket,
            current_plan: PostponeBucketPlan { bucket_counts },
            postpone_row_counts,
        })
    }

    pub fn current_plan(&self) -> PostponeBucketPlan {
        self.current_plan.clone()
    }

    pub fn input_partition_stats(&self, batch: &RecordBatch) -> Result<PostponePartitionStats> {
        let mut stats = PostponePartitionStats::default();
        for (partition, batch) in self.partition_batches(batch)? {
            if !self.current_plan.contains(&partition) {
                let size = if self.target_rows_per_bucket.is_none() {
                    binary_row_batch_size(&batch, &self.fields)?
                } else {
                    0
                };
                stats.add(partition, batch.num_rows() as i64, size);
            }
        }
        Ok(stats)
    }

    pub fn plan(
        &self,
        stats: &PostponePartitionStats,
        include_postpone_rows: bool,
    ) -> PostponeBucketPlan {
        let mut plan = self.current_plan.clone();
        for (partition, (input_rows, input_size)) in &stats.stats {
            if plan.contains(partition) {
                continue;
            }
            let postpone_rows = if include_postpone_rows {
                self.postpone_row_counts
                    .get(partition)
                    .copied()
                    .unwrap_or(0)
            } else {
                0
            };
            plan.bucket_counts.insert(
                partition.clone(),
                infer_bucket_count(
                    *input_rows,
                    *input_size,
                    postpone_rows,
                    self.target_rows_per_bucket,
                    self.target_size_per_bucket,
                    self.max_parallelism,
                ),
            );
        }
        plan
    }

    fn partition_batches(&self, batch: &RecordBatch) -> Result<Vec<(Vec<u8>, RecordBatch)>> {
        partition_batches(batch, &self.partition_field_indices, &self.fields)
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

    fn normalize_write_batch(&self, batch: &RecordBatch) -> Result<Option<RecordBatch>> {
        self.inner.normalize_write_batch(batch)
    }

    fn ensure_writable(&self) -> Result<()> {
        if self.prepare_started {
            return Err(one_shot_error());
        }
        Ok(())
    }
}

enum BatchWriteMode {
    Planned(Box<PostponeFixedBucketTableWrite>),
    Local(Box<LocalBatchWrite>),
}

struct LocalBatchWrite {
    table: Table,
    commit_user: String,
    overwrite: bool,
    planner: Option<PostponeBucketPlanner>,
    known_writer: Option<PostponeFixedBucketTableWrite>,
    pending: Vec<RecordBatch>,
    pending_stats: PostponePartitionStats,
}

pub struct PostponeFixedBucketBatchTableWrite {
    mode: BatchWriteMode,
    prepare_started: bool,
}

impl PostponeFixedBucketBatchTableWrite {
    fn new(
        table: &Table,
        commit_user: String,
        plan: Option<PostponeBucketPlan>,
        overwrite: bool,
    ) -> Result<Self> {
        validate_postpone_fixed_bucket_write(table)?;
        let mode =
            match plan {
                Some(plan) => BatchWriteMode::Planned(Box::new(
                    PostponeFixedBucketTableWrite::new(table, commit_user, plan, overwrite)?,
                )),
                None => BatchWriteMode::Local(Box::new(LocalBatchWrite {
                    table: table.clone(),
                    commit_user,
                    overwrite,
                    planner: None,
                    known_writer: None,
                    pending: Vec::new(),
                    pending_stats: PostponePartitionStats::default(),
                })),
            };
        Ok(Self {
            mode,
            prepare_started: false,
        })
    }

    pub async fn write_arrow_batch(&mut self, batch: &RecordBatch) -> Result<()> {
        self.ensure_writable()?;
        match &mut self.mode {
            BatchWriteMode::Planned(writer) => writer.write_arrow_batch(batch).await,
            BatchWriteMode::Local(local) => {
                if local.planner.is_none() {
                    let loaded = PostponeBucketPlanner::new(&local.table).await?;
                    local.known_writer = Some(PostponeFixedBucketTableWrite::new(
                        &local.table,
                        local.commit_user.clone(),
                        loaded.current_plan(),
                        local.overwrite,
                    )?);
                    local.planner = Some(loaded);
                }
                let planner = local.planner.as_ref().unwrap();
                let writer = local.known_writer.as_mut().unwrap();
                let Some(batch) = writer.normalize_write_batch(batch)? else {
                    return Ok(());
                };
                for (partition, batch) in planner.partition_batches(&batch)? {
                    if planner.current_plan.contains(&partition) {
                        writer.write_normalized_batch(batch).await?;
                    } else {
                        local
                            .pending_stats
                            .merge(&planner.input_partition_stats(&batch)?);
                        local.pending.push(batch);
                    }
                }
                Ok(())
            }
        }
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
        match &mut self.mode {
            BatchWriteMode::Planned(writer) => writer.prepare_commit().await,
            BatchWriteMode::Local(local) => {
                let Some(planner) = local.planner.as_ref() else {
                    return Ok(Vec::new());
                };
                let mut messages = local
                    .known_writer
                    .as_mut()
                    .unwrap()
                    .prepare_commit()
                    .await?;
                if !local.pending.is_empty() {
                    let plan = planner.plan(&local.pending_stats, !local.overwrite);
                    let mut writer = PostponeFixedBucketTableWrite::new(
                        &local.table,
                        local.commit_user.clone(),
                        plan,
                        local.overwrite,
                    )?;
                    for batch in std::mem::take(&mut local.pending) {
                        writer.write_normalized_batch(batch).await?;
                    }
                    messages.extend(writer.prepare_commit().await?);
                }
                Ok(messages)
            }
        }
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

    pub fn new_write(&self) -> Result<PostponeFixedBucketBatchTableWrite> {
        ensure_table_write_allowed(self.table)?;
        PostponeFixedBucketBatchTableWrite::new(
            self.table,
            self.commit_user.clone(),
            self.bucket_plan.clone(),
            self.overwrite,
        )
    }

    pub fn new_planned_write(&self) -> Result<PostponeFixedBucketTableWrite> {
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

async fn load_bucket_metadata(
    table: &Table,
) -> Result<(HashMap<Vec<u8>, i32>, HashMap<Vec<u8>, i64>)> {
    let mut known_bucket_counts = HashMap::new();
    let mut postpone_row_counts = HashMap::new();
    let snapshot_manager =
        SnapshotManager::new(table.file_io().clone(), table.location().to_string());
    let Some(snapshot) = snapshot_manager.get_latest_snapshot().await? else {
        return Ok((known_bucket_counts, postpone_row_counts));
    };

    let scan = TableScan::new(table, None, vec![], None, None, None).with_scan_all_files();
    for entry in scan.plan_manifest_entries(&snapshot).await? {
        let partition = entry.partition().to_vec();
        if entry.bucket() == POSTPONE_BUCKET {
            let rows = postpone_row_counts.entry(partition).or_insert(0_i64);
            *rows = rows.saturating_add(entry.file().row_count);
        } else if entry.bucket() >= 0 && entry.total_buckets() > 0 {
            if let Some(previous) =
                known_bucket_counts.insert(partition.clone(), entry.total_buckets())
            {
                if previous != entry.total_buckets() {
                    return Err(data_invalid(format!(
                        "Partition has inconsistent total bucket counts: {previous} and {}",
                        entry.total_buckets()
                    )));
                }
            }
        }
    }
    Ok((known_bucket_counts, postpone_row_counts))
}

fn infer_bucket_count(
    input_rows: i64,
    input_size: i64,
    postpone_rows: i64,
    target_rows_per_bucket: Option<i64>,
    target_size_per_bucket: Option<i64>,
    max_parallelism: i32,
) -> i32 {
    let buckets = if let Some(target_rows) = target_rows_per_bucket {
        let total_rows = input_rows.saturating_add(postpone_rows);
        total_rows.saturating_add(target_rows - 1) / target_rows
    } else {
        let target_size = target_size_per_bucket
            .expect("size target is validated when row-count target is absent");
        let estimated_size = if postpone_rows > 0 && input_rows > 0 {
            let numerator = i128::from(input_size)
                .saturating_mul(i128::from(input_rows.saturating_add(postpone_rows)));
            let estimate = (numerator + i128::from(input_rows - 1)) / i128::from(input_rows);
            estimate.min(i128::from(i64::MAX)) as i64
        } else {
            input_size
        };
        estimated_size.saturating_add(target_size - 1) / target_size
    };
    buckets.max(1).min(i64::from(max_parallelism)) as i32
}
