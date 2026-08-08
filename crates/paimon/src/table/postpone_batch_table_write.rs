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

//! Fixed-bucket planning for postpone batch writes.

use crate::spec::{
    batch_to_serialized_bytes, BucketFunctionType, CoreOptions, DataField, EMPTY_SERIALIZED_ROW,
    POSTPONE_BUCKET,
};
use crate::table::bucket_function::{batch_bucket_ids, validate_bucket_function};
use crate::table::postpone_bucket::binary_row_batch_size;
use crate::table::table_write::take_rows;
use crate::table::{SnapshotManager, Table, TableScan};
use crate::Result;
use arrow_array::{Array, Int32Array, RecordBatch};
use std::collections::HashMap;

/// Column name used by [`PostponeBucketPlan::from_arrow`] for bucket counts.
pub const POSTPONE_BUCKET_PLAN_TOTAL_BUCKETS_FIELD: &str = "total_buckets";

/// A shared bucket plan for distributed writers.
#[derive(Debug, Clone)]
pub struct PostponeBucketPlan {
    bucket_counts: HashMap<Vec<u8>, i32>,
}

impl PostponeBucketPlan {
    /// Build a plan from partition columns followed by an Int32 `total_buckets`.
    pub fn from_arrow(table: &Table, batch: &RecordBatch) -> Result<Self> {
        let partition_fields = table.schema().partition_fields();
        let partition_count = partition_fields.len();
        if batch.num_columns() != partition_count + 1 {
            return Err(crate::Error::DataInvalid {
                message: format!(
                    "Postpone bucket plan expected {} partition column(s) plus '{POSTPONE_BUCKET_PLAN_TOTAL_BUCKETS_FIELD}', got {} columns",
                    partition_count,
                    batch.num_columns()
                ),
                source: None,
            });
        }

        let expected_partition_schema = crate::arrow::build_target_arrow_schema(&partition_fields)?;
        let batch_schema = batch.schema();
        for (index, expected) in expected_partition_schema.fields().iter().enumerate() {
            let actual = batch_schema.field(index);
            if actual.name() != expected.name() || actual.data_type() != expected.data_type() {
                return Err(crate::Error::DataInvalid {
                    message: format!(
                        "Postpone bucket plan partition field mismatch at index {index}: expected '{}': {:?}, got '{}': {:?}",
                        expected.name(),
                        expected.data_type(),
                        actual.name(),
                        actual.data_type()
                    ),
                    source: None,
                });
            }
            if !expected.is_nullable() && batch.column(index).null_count() != 0 {
                return Err(crate::Error::DataInvalid {
                    message: format!(
                        "Postpone bucket plan partition column '{}' is NOT NULL but contains null values",
                        expected.name()
                    ),
                    source: None,
                });
            }
        }

        let count_field = batch_schema.field(partition_count);
        if count_field.name() != POSTPONE_BUCKET_PLAN_TOTAL_BUCKETS_FIELD
            || count_field.data_type() != &arrow_schema::DataType::Int32
        {
            return Err(crate::Error::DataInvalid {
                message: format!(
                    "Postpone bucket plan final field must be '{POSTPONE_BUCKET_PLAN_TOTAL_BUCKETS_FIELD}': Int32, got '{}': {:?}",
                    count_field.name(),
                    count_field.data_type()
                ),
                source: None,
            });
        }
        let counts = batch
            .column(partition_count)
            .as_any()
            .downcast_ref::<Int32Array>()
            .ok_or_else(|| crate::Error::DataInvalid {
                message: "Postpone bucket plan total_buckets column is not Int32".to_string(),
                source: None,
            })?;
        let partition_indices = (0..partition_count).collect::<Vec<_>>();
        let partitions = batch_to_serialized_bytes(batch, &partition_indices, &partition_fields)?;
        let mut bucket_counts = HashMap::with_capacity(batch.num_rows());
        for (row, partition) in partitions.into_iter().enumerate() {
            if counts.is_null(row) {
                return Err(crate::Error::DataInvalid {
                    message: format!("Postpone bucket plan total_buckets is null at row {row}"),
                    source: None,
                });
            }
            let total_buckets = counts.value(row);
            if total_buckets <= 0 {
                return Err(crate::Error::DataInvalid {
                    message: format!(
                        "Postpone bucket plan total_buckets must be positive at row {row}, got {total_buckets}"
                    ),
                    source: None,
                });
            }
            if let Some(previous) = bucket_counts.insert(partition, total_buckets) {
                if previous != total_buckets {
                    return Err(crate::Error::DataInvalid {
                        message: format!(
                            "Postpone bucket plan contains conflicting total bucket counts {previous} and {total_buckets} for one partition"
                        ),
                        source: None,
                    });
                }
            }
        }
        Ok(Self { bucket_counts })
    }

    fn into_bucket_counts(self) -> HashMap<Vec<u8>, i32> {
        self.bucket_counts
    }
}

pub(super) fn validate_postpone_fixed_bucket_table(table: &Table) -> Result<()> {
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

pub(super) struct PostponeBucketBatch {
    pub(super) partition: Vec<u8>,
    pub(super) bucket: i32,
    pub(super) batch: RecordBatch,
}

pub(super) struct PostponeFixedBucketWriter {
    partition_field_indices: Vec<usize>,
    bucket_key_indices: Vec<usize>,
    bucket_function_type: BucketFunctionType,
    max_parallelism: i32,
    target_rows_per_bucket: Option<i64>,
    target_size_per_bucket: Option<i64>,
    plan_provided: bool,
    metadata_loaded: bool,
    known_bucket_counts: HashMap<Vec<u8>, i32>,
    postpone_row_counts: HashMap<Vec<u8>, i64>,
    buffered_batches: HashMap<Vec<u8>, Vec<RecordBatch>>,
    prepare_started: bool,
}

impl PostponeFixedBucketWriter {
    pub(super) fn new(
        table: &Table,
        partition_field_indices: Vec<usize>,
        bucket_key_indices: Vec<usize>,
        bucket_function_type: BucketFunctionType,
        bucket_plan: Option<PostponeBucketPlan>,
    ) -> Result<Self> {
        validate_postpone_fixed_bucket_table(table)?;
        let schema = table.schema();
        let options = CoreOptions::new(schema.options());
        if options.deletion_vectors_enabled() {
            return Err(crate::Error::Unsupported {
                message: format!(
                    "Table '{}' cannot use postpone fixed-bucket writes with deletion-vectors.enabled=true because deletion-vector scans skip the level-0 files produced by batch writers; use the normal postpone writer or disable deletion vectors",
                    table.identifier().full_name()
                ),
            });
        }

        let bucket_key_fields: Vec<DataField> = bucket_key_indices
            .iter()
            .map(|&index| schema.fields()[index].clone())
            .collect();
        if !bucket_key_fields.is_empty() {
            validate_bucket_function(bucket_function_type, &bucket_key_fields)?;
        }

        let target_rows_per_bucket = options.postpone_target_row_num_per_bucket()?;
        let target_size_per_bucket = if target_rows_per_bucket.is_none() {
            Some(options.postpone_target_size_per_bucket()?)
        } else {
            None
        };
        let plan_provided = bucket_plan.is_some();
        let known_bucket_counts = bucket_plan
            .map(PostponeBucketPlan::into_bucket_counts)
            .unwrap_or_default();

        Ok(Self {
            partition_field_indices,
            bucket_key_indices,
            bucket_function_type,
            max_parallelism: options.postpone_batch_write_fixed_bucket_max_parallelism()?,
            target_rows_per_bucket,
            target_size_per_bucket,
            plan_provided,
            metadata_loaded: plan_provided,
            known_bucket_counts,
            postpone_row_counts: HashMap::new(),
            buffered_batches: HashMap::new(),
            prepare_started: false,
        })
    }

    pub(super) fn ensure_writable(&self) -> Result<()> {
        if self.prepare_started {
            return Err(Self::one_shot_error());
        }
        Ok(())
    }

    pub(super) fn start_prepare(&mut self) -> Result<()> {
        self.ensure_writable()?;
        // Failed preparation is not safely retryable.
        self.prepare_started = true;
        Ok(())
    }

    pub(super) async fn write_batch(
        &mut self,
        table: &Table,
        batch: &RecordBatch,
    ) -> Result<Vec<PostponeBucketBatch>> {
        self.ensure_metadata_loaded(table).await?;

        let partitions = if self.partition_field_indices.is_empty() {
            vec![EMPTY_SERIALIZED_ROW.clone(); batch.num_rows()]
        } else {
            batch_to_serialized_bytes(
                batch,
                &self.partition_field_indices,
                table.schema().fields(),
            )?
        };

        let mut groups: HashMap<Vec<u8>, Vec<usize>> = HashMap::new();
        for (row, partition) in partitions.into_iter().enumerate() {
            groups.entry(partition).or_default().push(row);
        }

        if self.plan_provided
            && groups
                .keys()
                .any(|partition| !self.known_bucket_counts.contains_key(partition))
        {
            return Err(crate::Error::DataInvalid {
                message: "Postpone bucket plan does not contain an input partition".to_string(),
                source: None,
            });
        }

        let mut output = Vec::new();
        for (partition, rows) in groups {
            let sub_batch = take_rows(batch, &rows)?;
            if let Some(total_buckets) = self.known_bucket_counts.get(&partition).copied() {
                output.extend(self.route_batch(table, partition, sub_batch, total_buckets)?);
            } else {
                self.buffered_batches
                    .entry(partition)
                    .or_default()
                    .push(sub_batch);
            }
        }
        Ok(output)
    }

    pub(super) async fn prepare_batch(
        &mut self,
        table: &Table,
        is_overwrite: bool,
    ) -> Result<Vec<PostponeBucketBatch>> {
        if self.buffered_batches.is_empty() {
            return Ok(Vec::new());
        }

        let buffered_batches = std::mem::take(&mut self.buffered_batches);
        let mut output = Vec::new();
        for (partition, batches) in buffered_batches {
            let input_rows = batches.iter().fold(0_i64, |rows, batch| {
                rows.saturating_add(batch.num_rows() as i64)
            });
            // Row-count planning does not inspect row sizes.
            let input_size = if self.target_rows_per_bucket.is_none() {
                batches.iter().try_fold(0_i64, |size, batch| {
                    Ok::<_, crate::Error>(
                        size.saturating_add(binary_row_batch_size(batch, table.schema().fields())?),
                    )
                })?
            } else {
                0
            };
            let postpone_rows = if is_overwrite {
                0
            } else {
                self.postpone_row_counts
                    .get(&partition)
                    .copied()
                    .unwrap_or(0)
            };
            let total_buckets = infer_bucket_count(
                input_rows,
                input_size,
                postpone_rows,
                self.target_rows_per_bucket,
                self.target_size_per_bucket,
                self.max_parallelism,
            );
            self.known_bucket_counts
                .insert(partition.clone(), total_buckets);

            for batch in batches {
                output.extend(self.route_batch(table, partition.clone(), batch, total_buckets)?);
            }
        }
        Ok(output)
    }

    pub(super) fn total_buckets(&self, partition: &[u8]) -> Option<i32> {
        self.known_bucket_counts.get(partition).copied()
    }

    pub(super) fn finish(&mut self) {
        self.known_bucket_counts.clear();
        self.postpone_row_counts.clear();
    }

    #[cfg(test)]
    pub(super) fn buffered_partition_count(&self) -> usize {
        self.buffered_batches.len()
    }

    async fn ensure_metadata_loaded(&mut self, table: &Table) -> Result<()> {
        if self.metadata_loaded {
            return Ok(());
        }
        let (known_bucket_counts, postpone_row_counts) = load_bucket_metadata(table).await?;
        self.known_bucket_counts = known_bucket_counts;
        self.postpone_row_counts = postpone_row_counts;
        self.metadata_loaded = true;
        Ok(())
    }

    fn route_batch(
        &self,
        table: &Table,
        partition: Vec<u8>,
        batch: RecordBatch,
        total_buckets: i32,
    ) -> Result<Vec<PostponeBucketBatch>> {
        let buckets = if total_buckets <= 1 || self.bucket_key_indices.is_empty() {
            vec![0; batch.num_rows()]
        } else {
            batch_bucket_ids(
                &batch,
                &self.bucket_key_indices,
                table.schema().fields(),
                self.bucket_function_type,
                total_buckets,
            )?
        };
        let mut groups: HashMap<i32, Vec<usize>> = HashMap::new();
        for (row, bucket) in buckets.into_iter().enumerate() {
            groups.entry(bucket).or_default().push(row);
        }
        groups
            .into_iter()
            .map(|(bucket, rows)| {
                Ok(PostponeBucketBatch {
                    partition: partition.clone(),
                    bucket,
                    batch: take_rows(&batch, &rows)?,
                })
            })
            .collect()
    }

    fn one_shot_error() -> crate::Error {
        crate::Error::DataInvalid {
            message: "Fixed-bucket postpone TableWrite only supports one prepare_commit call; create a new writer for the next batch".to_string(),
            source: None,
        }
    }
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
                    return Err(crate::Error::DataInvalid {
                        message: format!(
                            "Partition has inconsistent total bucket counts: {previous} and {}",
                            entry.total_buckets()
                        ),
                        source: None,
                    });
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
        let target_size_per_bucket = target_size_per_bucket
            .expect("size target is validated when row-count target is absent");
        let estimated_size = if postpone_rows > 0 && input_rows > 0 {
            let numerator = i128::from(input_size)
                .saturating_mul(i128::from(input_rows.saturating_add(postpone_rows)));
            let estimate = (numerator + i128::from(input_rows - 1)) / i128::from(input_rows);
            estimate.min(i128::from(i64::MAX)) as i64
        } else {
            input_size
        };
        estimated_size.saturating_add(target_size_per_bucket - 1) / target_size_per_bucket
    };
    buckets.max(1).min(i64::from(max_parallelism)) as i32
}
