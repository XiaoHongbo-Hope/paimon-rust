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

use std::fmt::Debug;
use std::sync::Arc;

use std::collections::HashMap;

use async_trait::async_trait;
use datafusion::arrow::array::{
    Array, ArrayRef, Int64Array, RecordBatch, RecordBatchOptions, UInt32Array,
};
use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::SchemaRef as ArrowSchemaRef;
use datafusion::catalog::Session;
use datafusion::catalog::TableFunctionImpl;
use datafusion::common::project_schema;
use datafusion::datasource::memory::MemorySourceConfig;
use datafusion::datasource::{TableProvider, TableType};
use datafusion::error::{DataFusionError, Result as DFResult};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_plan::empty::EmptyExec;
use datafusion::physical_plan::ExecutionPlan;
use datafusion::prelude::SessionContext;
use futures::TryStreamExt;
use paimon::catalog::Catalog;
use paimon::spec::{
    BigIntType, CoreOptions, DataField, DataType, ROW_ID_FIELD_ID, ROW_ID_FIELD_NAME,
};

use crate::error::to_datafusion_error;
use crate::runtime::{await_with_runtime, block_on_with_runtime};
use crate::table::{datafusion_read_fields, PaimonTableProvider};
use crate::table_function_args::{
    extract_int_literal, extract_string_literal, parse_table_identifier,
};
use crate::table_loader::load_data_table_for_read;

const FUNCTION_NAME: &str = "vector_search";

pub fn register_vector_search(
    ctx: &SessionContext,
    catalog: Arc<dyn Catalog>,
    default_database: &str,
) {
    ctx.register_udtf(
        "vector_search",
        Arc::new(VectorSearchFunction::new(catalog, default_database)),
    );
}

pub struct VectorSearchFunction {
    catalog: Arc<dyn Catalog>,
    default_database: String,
}

impl Debug for VectorSearchFunction {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VectorSearchFunction")
            .field("default_database", &self.default_database)
            .finish()
    }
}

impl VectorSearchFunction {
    pub fn new(catalog: Arc<dyn Catalog>, default_database: &str) -> Self {
        Self {
            catalog,
            default_database: default_database.to_string(),
        }
    }
}

impl TableFunctionImpl for VectorSearchFunction {
    fn call(&self, args: &[Expr]) -> DFResult<Arc<dyn TableProvider>> {
        if args.len() != 4 {
            return Err(datafusion::error::DataFusionError::Plan(
                "vector_search requires 4 arguments: (table_name, column_name, query_vector_json, limit)".to_string(),
            ));
        }

        let table_name = extract_string_literal(FUNCTION_NAME, &args[0], "table_name")?;
        let column_name = extract_string_literal(FUNCTION_NAME, &args[1], "column_name")?;
        let limit = extract_int_literal(FUNCTION_NAME, &args[3], "limit")?;

        if limit <= 0 {
            return Err(DataFusionError::Plan(
                "vector_search: limit must be positive".to_string(),
            ));
        }

        let identifier =
            parse_table_identifier(FUNCTION_NAME, &table_name, &self.default_database)?;

        let catalog = Arc::clone(&self.catalog);
        let table = block_on_with_runtime(
            async move { load_data_table_for_read(&catalog, &identifier, FUNCTION_NAME).await },
            "vector_search: catalog access thread panicked",
        )?;

        let inner = PaimonTableProvider::try_new(table)?;
        let query_vector_json =
            match extract_string_literal(FUNCTION_NAME, &args[2], "query_vector_json") {
                Ok(value) => value,
                Err(_) if matches!(args[2], Expr::Column(_)) => {
                    return Ok(Arc::new(LateralVectorSearchTableProvider {
                        inner,
                        column_name,
                        query_vector_expr: args[2].clone(),
                        limit: limit as usize,
                    }));
                }
                Err(err) => return Err(err),
            };

        let query_vector: Vec<f32> = serde_json::from_str(&query_vector_json).map_err(|e| {
            DataFusionError::Plan(format!(
                "vector_search: query_vector_json must be a JSON array of floats, got '{}': {}",
                query_vector_json, e
            ))
        })?;

        if query_vector.is_empty() {
            return Err(DataFusionError::Plan(
                "vector_search: query vector cannot be empty".to_string(),
            ));
        }

        Ok(Arc::new(VectorSearchTableProvider {
            inner,
            column_name,
            query_vector,
            limit: limit as usize,
        }))
    }
}

#[derive(Debug)]
pub(crate) struct LateralVectorSearchTableProvider {
    inner: PaimonTableProvider,
    column_name: String,
    query_vector_expr: Expr,
    limit: usize,
}

impl LateralVectorSearchTableProvider {
    pub(crate) fn inner(&self) -> &PaimonTableProvider {
        &self.inner
    }

    pub(crate) fn column_name(&self) -> &str {
        &self.column_name
    }

    pub(crate) fn query_vector_expr(&self) -> &Expr {
        &self.query_vector_expr
    }

    pub(crate) fn limit(&self) -> usize {
        self.limit
    }
}

#[async_trait]
impl TableProvider for LateralVectorSearchTableProvider {
    fn schema(&self) -> ArrowSchemaRef {
        self.inner.schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        _projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        Err(DataFusionError::Plan(
            "lateral vector_search must be planned through a lateral join".to_string(),
        ))
    }
}

#[derive(Debug)]
struct VectorSearchTableProvider {
    inner: PaimonTableProvider,
    column_name: String,
    query_vector: Vec<f32>,
    limit: usize,
}

#[async_trait]
impl TableProvider for VectorSearchTableProvider {
    fn schema(&self) -> ArrowSchemaRef {
        self.inner.schema()
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        limit: Option<usize>,
    ) -> DFResult<Arc<dyn ExecutionPlan>> {
        let table = self.inner.table();
        let projected_schema = project_schema(&self.schema(), projection)?;

        // Honor the outer query limit: we only need the top `limit` most relevant
        // rows, so bounding the search here also bounds the read and the in-memory
        // materialization below (e.g. `vector_search(..., 1_000_000) LIMIT 1` only
        // searches/reads/materializes one row instead of a million).
        let effective_limit = match limit {
            Some(outer) => self.limit.min(outer),
            None => self.limit,
        };
        if effective_limit == 0 {
            return Ok(Arc::new(EmptyExec::new(projected_schema)));
        }

        // Best-first row-ids from the index (data-evolution / global-index path;
        // PK-vector tables are unsupported here, as before).
        let search_result = await_with_runtime(async {
            let mut builder = table.new_vector_search_builder();
            builder
                .with_vector_column(&self.column_name)
                .with_query_vector(self.query_vector.clone())
                .with_limit(effective_limit);
            builder.execute_scored().await.map_err(to_datafusion_error)
        })
        .await?;

        if search_result.is_empty() {
            return Ok(Arc::new(EmptyExec::new(projected_schema)));
        }

        // Read the projected columns (+ internal `_ROW_ID`); the row-range scan yields
        // file order, realigned to relevance rank below.
        let read_fields = projected_read_fields(table, projection)?;
        let row_ranges = search_result.to_row_ranges().map_err(to_datafusion_error)?;
        let batches = await_with_runtime(async {
            let mut read_builder = table.new_read_builder();
            read_builder
                .with_read_type(read_fields)
                .with_row_ranges(row_ranges);
            let scan = read_builder.new_scan();
            let plan = scan.plan().await.map_err(to_datafusion_error)?;
            let table_read = read_builder.new_read().map_err(to_datafusion_error)?;
            let mut stream = table_read
                .to_arrow(plan.splits())
                .map_err(to_datafusion_error)?;
            let mut batches: Vec<RecordBatch> = Vec::new();
            while let Some(batch) = stream.try_next().await.map_err(to_datafusion_error)? {
                batches.push(batch);
            }
            Ok::<_, DataFusionError>(batches)
        })
        .await?;

        // Realign file-order rows to best-first rank and drop `_ROW_ID` — this is what
        // honors the documented "top-k rows ordered by relevance score" contract.
        let ordered = gather_rows_by_rank(&batches, &search_result.row_ids, &projected_schema)?;

        Ok(MemorySourceConfig::try_new_exec(
            &[vec![ordered]],
            projected_schema,
            None,
        )?)
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> DFResult<Vec<TableProviderFilterPushDown>> {
        Ok(vec![
            TableProviderFilterPushDown::Unsupported;
            filters.len()
        ])
    }
}

/// Projected user columns (+ internal `_ROW_ID`, needed to realign rows to rank).
/// Errors if the table has no row tracking, since results then can't be ordered.
fn projected_read_fields(
    table: &paimon::table::Table,
    projection: Option<&Vec<usize>>,
) -> DFResult<Vec<DataField>> {
    let base_fields = datafusion_read_fields(table);
    let mut read_fields: Vec<DataField> = match projection {
        Some(indices) => indices.iter().map(|&i| base_fields[i].clone()).collect(),
        None => base_fields,
    };
    if !read_fields
        .iter()
        .any(|field| field.name() == ROW_ID_FIELD_NAME)
    {
        if !CoreOptions::new(table.schema().options()).row_tracking_enabled() {
            return Err(DataFusionError::Plan(
                "vector_search: cannot order results by relevance because _ROW_ID is not available"
                    .to_string(),
            ));
        }
        read_fields.push(DataField::new(
            ROW_ID_FIELD_ID,
            ROW_ID_FIELD_NAME.to_string(),
            DataType::BigInt(BigIntType::with_nullable(true)),
        ));
    }
    Ok(read_fields)
}

/// Gather the file-order `batches` into `ranked_row_ids` order (rank == slice index),
/// producing `output_schema` (which excludes `_ROW_ID`). A permutation driven by the
/// index's existing ranking, not a re-sort.
fn gather_rows_by_rank(
    batches: &[RecordBatch],
    ranked_row_ids: &[u64],
    output_schema: &ArrowSchemaRef,
) -> DFResult<RecordBatch> {
    let input_schema = batches.first().map(|batch| batch.schema()).ok_or_else(|| {
        DataFusionError::Internal("vector_search: no rows materialized".to_string())
    })?;
    let combined = arrow_select::concat::concat_batches(&input_schema, batches)
        .map_err(DataFusionError::from)?;

    let row_id_index = combined.schema().index_of(ROW_ID_FIELD_NAME).map_err(|_| {
        DataFusionError::Internal(format!(
            "vector_search: materialized rows are missing the {ROW_ID_FIELD_NAME} column"
        ))
    })?;
    let row_ids = combined
        .column(row_id_index)
        .as_any()
        .downcast_ref::<Int64Array>()
        .ok_or_else(|| {
            DataFusionError::Internal(format!("vector_search: {ROW_ID_FIELD_NAME} must be Int64"))
        })?;

    // Map global row id -> physical position in the materialized batch.
    let mut position_of: HashMap<i64, u32> = HashMap::with_capacity(combined.num_rows());
    for row in 0..combined.num_rows() {
        if !row_ids.is_null(row) {
            position_of.insert(row_ids.value(row), row as u32);
        }
    }

    // Emit in rank order; extra scanned rows are ignored, but a missing scored id
    // fails loud rather than silently shrinking the top-k.
    let mut take_indices: Vec<u32> = Vec::with_capacity(ranked_row_ids.len());
    for &row_id in ranked_row_ids {
        let position = position_of.get(&(row_id as i64)).ok_or_else(|| {
            DataFusionError::Internal(format!(
                "vector_search: scored row id {row_id} was not materialized; \
                 cannot return the requested top-k"
            ))
        })?;
        take_indices.push(*position);
    }
    let take_indices = UInt32Array::from(take_indices);
    let row_count = take_indices.len();

    let columns = output_schema
        .fields()
        .iter()
        .map(|field| -> DFResult<ArrayRef> {
            let index = combined.schema().index_of(field.name()).map_err(|_| {
                DataFusionError::Internal(format!(
                    "vector_search: materialized rows are missing expected column '{}'",
                    field.name()
                ))
            })?;
            let taken =
                arrow_select::take::take(combined.column(index).as_ref(), &take_indices, None)
                    .map_err(DataFusionError::from)?;
            // The Paimon read keeps its own arrow types (e.g. `Utf8`), but the provider
            // schema may differ (e.g. DataFusion's `Utf8View`); cast to match, as the
            // normal scan path does via `to_datafusion_batch`.
            if taken.data_type() == field.data_type() {
                Ok(taken)
            } else {
                cast(taken.as_ref(), field.data_type()).map_err(DataFusionError::from)
            }
        })
        .collect::<DFResult<Vec<_>>>()?;

    // Preserve the row count for a zero-column projection (e.g. `COUNT(*)`).
    let options = RecordBatchOptions::new().with_row_count(Some(row_count));
    RecordBatch::try_new_with_options(Arc::clone(output_schema), columns, &options)
        .map_err(DataFusionError::from)
}
