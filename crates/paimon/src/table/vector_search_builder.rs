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
use crate::lumina::reader::LuminaVectorGlobalIndexReader;
use crate::lumina::LUMINA_VECTOR_ANN_IDENTIFIER;
use std::collections::HashMap;

pub struct VectorSearchBuilder {
    options: HashMap<String, String>,
    limit: usize,
    vector_column: Option<String>,
    query_vector: Option<Vec<f32>>,
}

impl VectorSearchBuilder {
    pub fn new(options: HashMap<String, String>) -> Self {
        Self {
            options,
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

    pub fn execute_local<F, S>(
        &self,
        io_metas: Vec<GlobalIndexIOMeta>,
        index_type: &str,
        stream_fn: F,
    ) -> crate::Result<Box<dyn ScoredGlobalIndexResult>>
    where
        F: FnOnce(&str) -> crate::Result<S>,
        S: std::io::Read + std::io::Seek + Send + 'static,
    {
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

        let vector_search =
            VectorSearch::new(query_vector.clone(), self.limit, vector_column.clone())?;

        if index_type != LUMINA_VECTOR_ANN_IDENTIFIER {
            return Err(crate::Error::Unsupported {
                message: format!("Unsupported vector index type: '{}'", index_type),
            });
        }

        let mut reader = LuminaVectorGlobalIndexReader::new(io_metas, self.options.clone())?;
        let result = reader.visit_vector_search(&vector_search, stream_fn)?;
        match result {
            Some(r) => Ok(r),
            None => Ok(Box::new(SimpleScoredGlobalIndexResult::create_empty())),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn dummy_stream_fn(_path: &str) -> crate::Result<std::io::Cursor<Vec<u8>>> {
        Ok(std::io::Cursor::new(vec![]))
    }

    #[test]
    fn execute_local_missing_limit() {
        let builder = VectorSearchBuilder::new(HashMap::new())
            .with_vector_column("emb".to_string())
            .with_query_vector(vec![1.0]);
        let result = builder.execute_local(vec![], "any", dummy_stream_fn);
        assert!(result.is_err());
    }

    #[test]
    fn execute_local_missing_vector_column() {
        let builder = VectorSearchBuilder::new(HashMap::new())
            .with_limit(10)
            .with_query_vector(vec![1.0]);
        let result = builder.execute_local(vec![], "any", dummy_stream_fn);
        assert!(result.is_err());
    }

    #[test]
    fn execute_local_missing_query_vector() {
        let builder = VectorSearchBuilder::new(HashMap::new())
            .with_limit(10)
            .with_vector_column("emb".to_string());
        let result = builder.execute_local(vec![], "any", dummy_stream_fn);
        assert!(result.is_err());
    }

    #[test]
    fn execute_local_unsupported_index_type() {
        let builder = VectorSearchBuilder::new(HashMap::new())
            .with_limit(10)
            .with_vector_column("emb".to_string())
            .with_query_vector(vec![1.0, 2.0]);
        let result = builder.execute_local(vec![], "unknown-index", dummy_stream_fn);
        assert!(result.is_err());
    }
}
