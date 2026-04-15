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

//! Integration tests for Lumina vector index, aligned with
//! pypaimon/tests/lumina_vector_index_test.py

#[cfg(feature = "lumina")]
mod lumina_tests {
    use paimon::globalindex::{GlobalIndexIOMeta, VectorSearch};
    use paimon::lumina::ffi::LuminaBuilder;
    use paimon::lumina::reader::LuminaVectorGlobalIndexReader;
    use paimon::lumina::LuminaIndexMeta;
    use roaring::RoaringTreemap;
    use std::collections::HashMap;
    use std::fs;

    const DIM: i32 = 4;
    const N: i32 = 100;

    fn build_options() -> HashMap<String, String> {
        [
            ("index.dimension", DIM.to_string()),
            ("index.type", "diskann".to_string()),
            ("distance.metric", "l2".to_string()),
            ("encoding.type", "rawf32".to_string()),
            ("diskann.build.ef_construction", "64".to_string()),
            ("diskann.build.neighbor_count", "32".to_string()),
            ("diskann.build.thread_count", "2".to_string()),
        ]
        .into_iter()
        .map(|(k, v)| (k.to_string(), v))
        .collect()
    }

    fn generate_vectors(n: usize, dim: usize, seed: u64) -> Vec<f32> {
        let mut vectors = Vec::with_capacity(n * dim);
        let mut val = seed as f32;
        for _ in 0..(n * dim) {
            // Simple deterministic pseudo-random
            val = (val * 1.1 + 0.3) % 100.0;
            vectors.push(val);
        }
        vectors
    }

    fn build_index(index_path: &str, seed: u64) -> HashMap<String, String> {
        let opts = build_options();
        let vectors = generate_vectors(N as usize, DIM as usize, seed);
        let ids: Vec<u64> = (0..N as u64).collect();

        let builder = LuminaBuilder::create(&opts).expect("Failed to create builder");
        builder
            .pretrain(&vectors, N, DIM)
            .expect("Failed to pretrain");
        builder
            .insert(&vectors, &ids, N, DIM)
            .expect("Failed to insert");
        builder.dump(index_path).expect("Failed to dump");

        opts
    }

    fn make_index_meta(opts: &HashMap<String, String>) -> Vec<u8> {
        let meta = LuminaIndexMeta::new(opts.clone());
        meta.serialize().expect("Failed to serialize meta")
    }

    /// Aligned with Python test_build_and_read
    #[test]
    fn test_build_and_read() {
        let dir = tempfile::tempdir().expect("Failed to create temp dir");
        let index_path = dir.path().join("index.lmi");
        let index_path_str = index_path.to_str().unwrap();

        let opts = build_index(index_path_str, 42);
        let file_size = fs::metadata(index_path_str).unwrap().len();
        let meta_bytes = make_index_meta(&opts);

        let io_meta = GlobalIndexIOMeta::new(index_path_str.to_string(), file_size, meta_bytes);
        let mut reader = LuminaVectorGlobalIndexReader::new(vec![io_meta], HashMap::new()).unwrap();

        // Query with the first vector
        let vectors = generate_vectors(N as usize, DIM as usize, 42);
        let query: Vec<f32> = vectors[..DIM as usize].to_vec();
        let vs = VectorSearch::new(query, 5, "vec".to_string()).unwrap();

        let result = reader
            .visit_vector_search(&vs, |path| {
                let file = fs::File::open(path).map_err(|e| paimon::Error::DataInvalid {
                    message: format!("Failed to open index file: {}", e),
                    source: None,
                })?;
                Ok(std::io::BufReader::new(file))
            })
            .expect("Search failed");

        let result = result.expect("Expected non-empty result");
        assert!(result.results().len() > 0, "Expected results");
        assert!(result.results().contains(0), "Expected row 0 (self-match)");
        let score = result.score_getter()(0);
        assert!(score.is_finite(), "Score should be finite");
    }

    /// Aligned with Python test_filtered_search
    #[test]
    fn test_filtered_search() {
        let dir = tempfile::tempdir().expect("Failed to create temp dir");
        let index_path = dir.path().join("index.lmi");
        let index_path_str = index_path.to_str().unwrap();

        let opts = build_index(index_path_str, 99);
        let file_size = fs::metadata(index_path_str).unwrap().len();
        let meta_bytes = make_index_meta(&opts);

        let io_meta = GlobalIndexIOMeta::new(index_path_str.to_string(), file_size, meta_bytes);
        let mut reader = LuminaVectorGlobalIndexReader::new(vec![io_meta], HashMap::new()).unwrap();

        // Only include even row IDs
        let mut include_ids = RoaringTreemap::new();
        for i in (0..N as u64).step_by(2) {
            include_ids.insert(i);
        }

        let vectors = generate_vectors(N as usize, DIM as usize, 99);
        let query: Vec<f32> = vectors[..DIM as usize].to_vec();
        let vs = VectorSearch::new(query, 3, "vec".to_string())
            .unwrap()
            .with_include_row_ids(include_ids);

        let result = reader
            .visit_vector_search(&vs, |path| {
                let file = fs::File::open(path).map_err(|e| paimon::Error::DataInvalid {
                    message: format!("Failed to open index file: {}", e),
                    source: None,
                })?;
                Ok(std::io::BufReader::new(file))
            })
            .expect("Search failed");

        let result = result.expect("Expected non-empty result");
        for row_id in result.results().iter() {
            assert_eq!(row_id % 2, 0, "Expected only even row IDs, got {}", row_id);
        }
    }
}
