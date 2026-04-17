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

use crate::globalindex::{
    GlobalIndexIOMeta, ScoredGlobalIndexResult, SimpleScoredGlobalIndexResult, VectorSearch,
};
use crate::io::{FileIO, FileRead};
use crate::lumina::reader::LuminaVectorGlobalIndexReader;
use crate::lumina::LUMINA_VECTOR_ANN_IDENTIFIER;
use crate::spec::{DataField, FileKind, IndexManifest, IndexManifestEntry};
use crate::table::snapshot_manager::SnapshotManager;
use crate::table::Table;
use std::collections::HashMap;
use std::io::{self, BufReader, Read, Seek, SeekFrom};
use std::ops::Range;
use std::path::Path;
use std::sync::Arc;
use tokio::runtime::Handle;
use url::Url;

const INDEX_DIR: &str = "index";

trait ReadSeekSend: Read + Seek + Send {}

impl<T: Read + Seek + Send> ReadSeekSend for T {}

type BoxedIndexStream = Box<dyn ReadSeekSend>;

struct RangeReadSeekStream {
    reader: Arc<dyn FileRead>,
    pos: u64,
    len: u64,
    runtime_handle: Handle,
    runtime_is_multithread: bool,
}

impl RangeReadSeekStream {
    fn new(reader: Arc<dyn FileRead>, len: u64) -> crate::Result<Self> {
        let runtime_handle = Handle::try_current().map_err(|_| crate::Error::UnexpectedError {
            message: "Vector search on non-file backends requires a Tokio runtime".to_string(),
            source: None,
        })?;
        let runtime_is_multithread = matches!(
            runtime_handle.runtime_flavor(),
            tokio::runtime::RuntimeFlavor::MultiThread
        );

        Ok(Self {
            reader,
            pos: 0,
            len,
            runtime_handle,
            runtime_is_multithread,
        })
    }
}

impl Read for RangeReadSeekStream {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        if buf.is_empty() || self.pos >= self.len {
            return Ok(0);
        }

        let end = self.len.min(self.pos.saturating_add(buf.len() as u64));
        let data = block_on_file_read(
            &self.runtime_handle,
            self.runtime_is_multithread,
            &self.reader,
            self.pos..end,
        )?;
        let read_len = data.len();
        buf[..read_len].copy_from_slice(&data);
        self.pos = self.pos.saturating_add(read_len as u64);
        Ok(read_len)
    }
}

impl Seek for RangeReadSeekStream {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        let new_pos = match pos {
            SeekFrom::Start(offset) => offset as i128,
            SeekFrom::End(offset) => self.len as i128 + offset as i128,
            SeekFrom::Current(offset) => self.pos as i128 + offset as i128,
        };

        if new_pos < 0 || new_pos > u64::MAX as i128 {
            return Err(io::Error::new(
                io::ErrorKind::InvalidInput,
                format!("Invalid seek target: {}", new_pos),
            ));
        }

        self.pos = new_pos as u64;
        Ok(self.pos)
    }
}

pub struct VectorSearchBuilder<'a> {
    table: Option<&'a Table>,
    options: HashMap<String, String>,
    limit: usize,
    vector_column: Option<String>,
    query_vector: Option<Vec<f32>>,
}

impl<'a> VectorSearchBuilder<'a> {
    pub fn new(options: HashMap<String, String>) -> Self {
        Self {
            table: None,
            options,
            limit: 0,
            vector_column: None,
            query_vector: None,
        }
    }

    pub(crate) fn from_table(table: &'a Table) -> Self {
        Self {
            table: Some(table),
            options: table.schema().options().clone(),
            limit: 0,
            vector_column: None,
            query_vector: None,
        }
    }

    pub fn with_limit(mut self, limit: usize) -> Self {
        self.limit = limit;
        self
    }

    pub fn with_vector_column(mut self, name: String) -> Self {
        self.vector_column = Some(name);
        self
    }

    pub fn with_query_vector(mut self, vector: Vec<f32>) -> Self {
        self.query_vector = Some(vector);
        self
    }

    pub async fn execute(&self) -> crate::Result<Box<dyn ScoredGlobalIndexResult>> {
        let table = self.table.ok_or_else(|| crate::Error::ConfigInvalid {
            message:
                "Vector search execute() requires a table-bound builder; use Table::new_vector_search_builder()"
                    .to_string(),
        })?;
        let vector_search = self.build_search()?;

        let snapshot_manager =
            SnapshotManager::new(table.file_io().clone(), table.location().to_string());

        let snapshot = match snapshot_manager.get_latest_snapshot().await? {
            Some(snapshot) => snapshot,
            None => return Ok(create_empty_result()),
        };

        let index_manifest_name = match snapshot.index_manifest() {
            Some(index_manifest_name) => index_manifest_name.to_string(),
            None => return Ok(create_empty_result()),
        };

        let manifest_path = format!(
            "{}/manifest/{}",
            table.location().trim_end_matches('/'),
            index_manifest_name
        );
        let index_entries = IndexManifest::read(table.file_io(), &manifest_path).await?;

        evaluate_vector_search(
            self,
            table.file_io(),
            table.location(),
            &index_entries,
            &vector_search,
            table.schema().fields(),
        )
        .await
    }

    pub fn execute_local<F, S>(
        &self,
        io_meta: GlobalIndexIOMeta,
        index_type: &str,
        stream_fn: F,
    ) -> crate::Result<Box<dyn ScoredGlobalIndexResult>>
    where
        F: FnOnce(&str) -> crate::Result<S>,
        S: std::io::Read + std::io::Seek + Send + 'static,
    {
        let vector_search = self.build_search()?;
        let stream = stream_fn(&io_meta.file_path)?;
        self.execute_with_vector_search(io_meta, index_type, &vector_search, stream)
    }

    fn build_search(&self) -> crate::Result<VectorSearch> {
        if self.limit == 0 {
            return Err(crate::Error::DataInvalid {
                message: "Limit must be positive, set via with_limit()".to_string(),
                source: None,
            });
        }

        let vector_column =
            self.vector_column
                .as_ref()
                .ok_or_else(|| crate::Error::DataInvalid {
                    message: "Vector column must be set via with_vector_column()".to_string(),
                    source: None,
                })?;
        let query_vector = self
            .query_vector
            .as_ref()
            .ok_or_else(|| crate::Error::DataInvalid {
                message: "Query vector must be set via with_query_vector()".to_string(),
                source: None,
            })?;

        VectorSearch::new(query_vector.clone(), self.limit, vector_column.clone())
    }

    fn execute_with_vector_search<S>(
        &self,
        io_meta: GlobalIndexIOMeta,
        index_type: &str,
        vector_search: &VectorSearch,
        stream: S,
    ) -> crate::Result<Box<dyn ScoredGlobalIndexResult>>
    where
        S: Read + Seek + Send + 'static,
    {
        if index_type != LUMINA_VECTOR_ANN_IDENTIFIER {
            return Err(crate::Error::Unsupported {
                message: format!("Unsupported vector index type: '{}'", index_type),
            });
        }

        let mut reader = LuminaVectorGlobalIndexReader::new(io_meta, self.options.clone());
        let result = reader.visit_vector_search(vector_search, move |_| Ok(stream))?;
        match result {
            Some(r) => Ok(r),
            None => Ok(create_empty_result()),
        }
    }
}

async fn evaluate_vector_search(
    builder: &VectorSearchBuilder<'_>,
    file_io: &FileIO,
    table_path: &str,
    index_entries: &[IndexManifestEntry],
    vector_search: &VectorSearch,
    schema_fields: &[DataField],
) -> crate::Result<Box<dyn ScoredGlobalIndexResult>> {
    let field_id = match find_field_id_by_name(schema_fields, &vector_search.field_name) {
        Some(field_id) => field_id,
        None => return Ok(create_empty_result()),
    };

    let mut merged: Box<dyn ScoredGlobalIndexResult> = create_empty_result();
    let table_path = table_path.trim_end_matches('/');
    let mut has_vector_entry = false;

    for entry in index_entries.iter().filter(|entry| {
        entry.kind == FileKind::Add
            && entry.index_file.index_type == LUMINA_VECTOR_ANN_IDENTIFIER
            && entry
                .index_file
                .global_index_meta
                .as_ref()
                .is_some_and(|meta| meta.index_field_id == field_id)
    }) {
        has_vector_entry = true;

        let global_meta = entry.index_file.global_index_meta.as_ref().unwrap();
        if global_meta.row_range_end < global_meta.row_range_start {
            return Err(crate::Error::DataInvalid {
                message: format!(
                    "Invalid vector index row range [{}, {}] for '{}'",
                    global_meta.row_range_start,
                    global_meta.row_range_end,
                    entry.index_file.file_name
                ),
                source: None,
            });
        }

        let row_range_start =
            u64::try_from(global_meta.row_range_start).map_err(|_| crate::Error::DataInvalid {
                message: format!(
                    "Negative row_range_start {} for '{}'",
                    global_meta.row_range_start, entry.index_file.file_name
                ),
                source: None,
            })?;
        let row_range_end =
            u64::try_from(global_meta.row_range_end).map_err(|_| crate::Error::DataInvalid {
                message: format!(
                    "Negative row_range_end {} for '{}'",
                    global_meta.row_range_end, entry.index_file.file_name
                ),
                source: None,
            })?;
        let file_size =
            u64::try_from(entry.index_file.file_size).map_err(|_| crate::Error::DataInvalid {
                message: format!(
                    "Negative file size {} for '{}'",
                    entry.index_file.file_size, entry.index_file.file_name
                ),
                source: None,
            })?;
        let metadata = global_meta
            .index_meta
            .clone()
            .ok_or_else(|| crate::Error::DataInvalid {
                message: format!(
                    "Missing vector index metadata for '{}'",
                    entry.index_file.file_name
                ),
                source: None,
            })?;
        let file_path = format!("{table_path}/{INDEX_DIR}/{}", entry.index_file.file_name);
        let io_meta = GlobalIndexIOMeta::new(file_path.clone(), file_size, metadata);
        let scoped_search = vector_search.offset_range(row_range_start, row_range_end);
        let stream = open_index_stream(file_io, &file_path, file_size).await?;
        let result = builder
            .execute_with_vector_search(
                io_meta,
                &entry.index_file.index_type,
                &scoped_search,
                stream,
            )?
            .scored_offset(row_range_start);
        merged = merged.scored_or(result.as_ref());
    }

    if !has_vector_entry {
        return Ok(create_empty_result());
    }

    Ok(merged.top_k(vector_search.limit))
}

fn find_field_id_by_name(fields: &[DataField], name: &str) -> Option<i32> {
    fields
        .iter()
        .find(|field| field.name() == name)
        .map(|field| field.id())
}

fn create_empty_result() -> Box<dyn ScoredGlobalIndexResult> {
    Box::new(SimpleScoredGlobalIndexResult::create_empty())
}

async fn open_index_stream(
    file_io: &FileIO,
    path: &str,
    file_size: u64,
) -> crate::Result<BoxedIndexStream> {
    if let Some(local_stream) = try_open_local_file(path)? {
        return Ok(local_stream);
    }

    let input = file_io.new_input(path)?;
    let reader = Arc::new(input.reader().await?);
    Ok(Box::new(RangeReadSeekStream::new(reader, file_size)?))
}

fn try_open_local_file(path: &str) -> crate::Result<Option<BoxedIndexStream>> {
    if let Ok(url) = Url::parse(path) {
        if url.scheme() != "file" {
            return Ok(None);
        }

        let local_path = url
            .to_file_path()
            .map_err(|_| crate::Error::ConfigInvalid {
                message: format!("Invalid file URL: {path}"),
            })?;
        return open_local_path(&local_path, path).map(Some);
    }

    if path.contains("://") {
        return Ok(None);
    }

    open_local_path(Path::new(path), path).map(Some)
}

fn open_local_path(path: &Path, display_path: &str) -> crate::Result<BoxedIndexStream> {
    let file = std::fs::File::open(path).map_err(|e| crate::Error::DataInvalid {
        message: format!("Failed to open vector index file '{}': {}", display_path, e),
        source: None,
    })?;
    Ok(Box::new(BufReader::new(file)))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::catalog::Identifier;
    use crate::io::FileIOBuilder;
    use crate::spec::{
        ArrayType, CommitKind, DataType, FloatType, IntType, Schema, Snapshot, TableSchema,
    };
    use bytes::Bytes;
    use std::path::Path;

    fn dummy_stream_fn(_path: &str) -> crate::Result<std::io::Cursor<Vec<u8>>> {
        Ok(std::io::Cursor::new(vec![]))
    }

    fn dummy_io_meta() -> GlobalIndexIOMeta {
        GlobalIndexIOMeta::new("dummy".into(), 0, vec![])
    }

    #[test]
    fn execute_local_missing_limit() {
        let builder = VectorSearchBuilder::new(HashMap::new())
            .with_vector_column("emb".to_string())
            .with_query_vector(vec![1.0]);
        let result = builder.execute_local(dummy_io_meta(), "any", dummy_stream_fn);
        assert!(result.is_err());
    }

    #[test]
    fn execute_local_missing_vector_column() {
        let builder = VectorSearchBuilder::new(HashMap::new())
            .with_limit(10)
            .with_query_vector(vec![1.0]);
        let result = builder.execute_local(dummy_io_meta(), "any", dummy_stream_fn);
        assert!(result.is_err());
    }

    #[test]
    fn execute_local_missing_query_vector() {
        let builder = VectorSearchBuilder::new(HashMap::new())
            .with_limit(10)
            .with_vector_column("emb".to_string());
        let result = builder.execute_local(dummy_io_meta(), "any", dummy_stream_fn);
        assert!(result.is_err());
    }

    #[test]
    fn execute_local_unsupported_index_type() {
        let builder = VectorSearchBuilder::new(HashMap::new())
            .with_limit(10)
            .with_vector_column("emb".to_string())
            .with_query_vector(vec![1.0, 2.0]);
        let result = builder.execute_local(dummy_io_meta(), "unknown-index", dummy_stream_fn);
        assert!(result.is_err());
    }

    #[tokio::test]
    async fn execute_without_snapshot_returns_empty() {
        let tmp = tempfile::tempdir().unwrap();
        std::fs::create_dir_all(tmp.path().join("snapshot")).unwrap();

        let table = test_table(local_file_path(tmp.path()));
        let result = table
            .new_vector_search_builder()
            .with_limit(5)
            .with_vector_column("vec".to_string())
            .with_query_vector(vec![1.0, 2.0])
            .execute()
            .await
            .unwrap();

        assert!(result.results().is_empty());
    }

    #[tokio::test]
    async fn execute_with_non_vector_manifest_returns_empty() {
        let tmp = tempfile::tempdir().unwrap();
        let snapshot_dir = tmp.path().join("snapshot");
        let manifest_dir = tmp.path().join("manifest");
        std::fs::create_dir_all(&snapshot_dir).unwrap();
        std::fs::create_dir_all(&manifest_dir).unwrap();

        let fixture_name = "index-manifest-7e816ed9-9f3b-4786-9985-8937d4e07b6e-0";
        let fixture_path = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("tests/fixtures/manifest")
            .join(fixture_name);
        std::fs::copy(&fixture_path, manifest_dir.join(fixture_name)).unwrap();

        let snapshot = Snapshot::builder()
            .version(3)
            .id(1)
            .schema_id(0)
            .base_manifest_list("base-list".to_string())
            .delta_manifest_list("delta-list".to_string())
            .index_manifest(Some(fixture_name.to_string()))
            .commit_user("test-user".to_string())
            .commit_identifier(0)
            .commit_kind(CommitKind::APPEND)
            .time_millis(1000)
            .build();
        std::fs::write(
            snapshot_dir.join("snapshot-1"),
            serde_json::to_vec(&snapshot).unwrap(),
        )
        .unwrap();

        let table = test_table(local_file_path(tmp.path()));
        let result = table
            .new_vector_search_builder()
            .with_limit(5)
            .with_vector_column("vec".to_string())
            .with_query_vector(vec![1.0, 2.0])
            .execute()
            .await
            .unwrap();

        assert!(result.results().is_empty());
    }

    #[tokio::test(flavor = "current_thread")]
    async fn open_index_stream_memory_is_seekable_current_thread() {
        assert_stream_seekable().await;
    }

    #[tokio::test(flavor = "multi_thread")]
    async fn open_index_stream_memory_is_seekable_multi_thread() {
        assert_stream_seekable().await;
    }

    fn test_table(table_path: String) -> Table {
        let schema = Schema::builder()
            .column("id", DataType::Int(IntType::new()))
            .column(
                "vec",
                DataType::Array(ArrayType::new(DataType::Float(FloatType::new()))),
            )
            .build()
            .unwrap();

        Table::new(
            FileIOBuilder::new("file").build().unwrap(),
            Identifier::new("default", "t"),
            table_path,
            TableSchema::new(0, &schema),
            None,
        )
    }

    fn local_file_path(path: &Path) -> String {
        let normalized = path.to_string_lossy().replace('\\', "/");
        if normalized.starts_with('/') {
            format!("file:{normalized}")
        } else {
            format!("file:/{normalized}")
        }
    }

    async fn assert_stream_seekable() {
        let file_io = FileIOBuilder::new("memory").build().unwrap();
        let path = "memory:/vector-index";
        file_io
            .new_output(path)
            .unwrap()
            .write(Bytes::from_static(b"abcdef"))
            .await
            .unwrap();

        let mut stream = open_index_stream(&file_io, path, 6).await.unwrap();

        let mut buf = [0u8; 2];
        assert_eq!(stream.read(&mut buf).unwrap(), 2);
        assert_eq!(&buf, b"ab");

        assert_eq!(stream.seek(SeekFrom::Start(3)).unwrap(), 3);
        assert_eq!(stream.read(&mut buf).unwrap(), 2);
        assert_eq!(&buf, b"de");

        assert_eq!(stream.seek(SeekFrom::End(-1)).unwrap(), 5);
        assert_eq!(stream.read(&mut buf[..1]).unwrap(), 1);
        assert_eq!(&buf[..1], b"f");
    }
}

fn block_on_file_read(
    handle: &Handle,
    runtime_is_multithread: bool,
    reader: &Arc<dyn FileRead>,
    range: Range<u64>,
) -> io::Result<bytes::Bytes> {
    let do_read = || {
        handle
            .block_on(reader.read(range.clone()))
            .map_err(|e| io::Error::other(e.to_string()))
    };

    if Handle::try_current().is_err() {
        return do_read();
    }

    if runtime_is_multithread {
        tokio::task::block_in_place(do_read)
    } else {
        let handle = handle.clone();
        let reader = Arc::clone(reader);
        std::thread::scope(|s| {
            s.spawn(move || {
                handle
                    .block_on(reader.read(range))
                    .map_err(|e| io::Error::other(e.to_string()))
            })
            .join()
            .map_err(|_| io::Error::other("reader thread panicked"))?
        })
    }
}
