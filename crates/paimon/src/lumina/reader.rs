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
    DictBasedScoredIndexResult, GlobalIndexIOMeta, ScoredGlobalIndexResult, VectorSearch,
};
use crate::lumina::ffi::LuminaSearcher;
use crate::lumina::{strip_lumina_options, LuminaIndexMeta, LuminaVectorMetric};
use std::collections::HashMap;
use std::io::{Read, Seek};

const MIN_SEARCH_LIST_SIZE: usize = 16;
const SENTINEL: u64 = 0xFFFFFFFFFFFFFFFF;

fn ensure_search_list_size(search_options: &mut HashMap<String, String>, top_k: usize) {
    if !search_options.contains_key("diskann.search.list_size") {
        let list_size = std::cmp::max((top_k as f64 * 1.5) as usize, MIN_SEARCH_LIST_SIZE);
        search_options.insert(
            "diskann.search.list_size".to_string(),
            list_size.to_string(),
        );
    }
}

fn convert_distance_to_score(distance: f32, metric: LuminaVectorMetric) -> f32 {
    match metric {
        LuminaVectorMetric::L2 => 1.0 / (1.0 + distance),
        LuminaVectorMetric::Cosine => 1.0 - distance,
        LuminaVectorMetric::InnerProduct => distance,
    }
}

fn filter_search_options(options: &HashMap<String, String>) -> HashMap<String, String> {
    options
        .iter()
        .filter(|(k, _)| k.starts_with("search.") || k.starts_with("diskann.search."))
        .map(|(k, v)| (k.clone(), v.clone()))
        .collect()
}

pub struct LuminaVectorGlobalIndexReader {
    io_meta: GlobalIndexIOMeta,
    options: HashMap<String, String>,
    searcher: Option<LuminaSearcher>,
    index_meta: Option<LuminaIndexMeta>,
    search_options: Option<HashMap<String, String>>,
}

impl LuminaVectorGlobalIndexReader {
    pub fn new(
        io_metas: Vec<GlobalIndexIOMeta>,
        options: HashMap<String, String>,
    ) -> crate::Result<Self> {
        if io_metas.len() != 1 {
            return Err(crate::Error::DataInvalid {
                message: "Expected exactly one index file per shard".to_string(),
                source: None,
            });
        }
        let mut metas = io_metas;
        let io_meta = metas.remove(0);
        Ok(Self {
            io_meta,
            options,
            searcher: None,
            index_meta: None,
            search_options: None,
        })
    }

    pub fn visit_vector_search<S: Read + Seek + Send + 'static>(
        &mut self,
        vector_search: &VectorSearch,
        stream_fn: impl FnOnce(&str) -> crate::Result<S>,
    ) -> crate::Result<Option<Box<dyn ScoredGlobalIndexResult>>> {
        self.ensure_loaded(stream_fn)?;
        self.search(vector_search)
    }

    fn search(
        &self,
        vector_search: &VectorSearch,
    ) -> crate::Result<Option<Box<dyn ScoredGlobalIndexResult>>> {
        let index_meta = self
            .index_meta
            .as_ref()
            .expect("index_meta must be initialized via ensure_loaded()");
        let searcher = self
            .searcher
            .as_ref()
            .expect("searcher must be initialized via ensure_loaded()");
        let search_options_base = self
            .search_options
            .as_ref()
            .expect("search_options must be initialized via ensure_loaded()");

        let expected_dim = index_meta.dim()? as usize;
        if vector_search.vector.len() != expected_dim {
            return Err(crate::Error::DataInvalid {
                message: format!(
                    "Query vector dimension mismatch: index expects {}, but got {}",
                    expected_dim,
                    vector_search.vector.len()
                ),
                source: None,
            });
        }

        let limit = vector_search.limit;
        let index_metric = index_meta.metric()?;
        let count = searcher.get_count()? as usize;
        let effective_k = std::cmp::min(limit, count);
        if effective_k == 0 {
            return Ok(None);
        }

        let include_row_ids = &vector_search.include_row_ids;

        let (distances, labels) = if let Some(ref include_ids) = include_row_ids {
            let filter_id_list: Vec<u64> = include_ids.iter().collect();
            if filter_id_list.is_empty() {
                return Ok(None);
            }
            let ek = std::cmp::min(effective_k, filter_id_list.len());
            let mut distances = vec![0.0f32; ek];
            let mut labels = vec![0u64; ek];
            let mut search_opts: HashMap<String, String> = search_options_base.clone();
            search_opts.insert("search.thread_safe_filter".to_string(), "true".to_string());
            ensure_search_list_size(&mut search_opts, ek);
            let filtered_opts = filter_search_options(&search_opts);
            searcher.search_with_filter(
                &vector_search.vector,
                1,
                ek as i32,
                &mut distances,
                &mut labels,
                &filter_id_list,
                &filtered_opts,
            )?;
            (distances, labels)
        } else {
            let mut distances = vec![0.0f32; effective_k];
            let mut labels = vec![0u64; effective_k];
            let mut search_opts: HashMap<String, String> = search_options_base.clone();
            ensure_search_list_size(&mut search_opts, effective_k);
            let filtered_opts = filter_search_options(&search_opts);
            searcher.search(
                &vector_search.vector,
                1,
                effective_k as i32,
                &mut distances,
                &mut labels,
                &filtered_opts,
            )?;
            (distances, labels)
        };

        let mut id_to_scores: HashMap<u64, f32> = HashMap::new();
        for i in 0..labels.len() {
            let row_id = labels[i];
            if row_id == SENTINEL {
                continue;
            }
            let score = convert_distance_to_score(distances[i], index_metric);
            id_to_scores.insert(row_id, score);
        }

        Ok(Some(Box::new(DictBasedScoredIndexResult::new(
            id_to_scores,
        ))))
    }

    fn ensure_loaded<S: Read + Seek + Send + 'static>(
        &mut self,
        stream_fn: impl FnOnce(&str) -> crate::Result<S>,
    ) -> crate::Result<()> {
        if self.searcher.is_some() {
            return Ok(());
        }

        let index_meta = LuminaIndexMeta::deserialize(&self.io_meta.metadata)?;

        let mut searcher_options = strip_lumina_options(&self.options);
        for (k, v) in index_meta.options().iter() {
            searcher_options.insert(k.to_string(), v.to_string());
        }

        let searcher_opts_map: HashMap<String, String> = searcher_options.into_iter().collect();
        let mut searcher = LuminaSearcher::create(&searcher_opts_map)?;

        let stream = stream_fn(&self.io_meta.file_path)?;
        searcher.open_stream(stream)?;

        self.search_options = Some(searcher_opts_map);
        self.index_meta = Some(index_meta);
        self.searcher = Some(searcher);
        Ok(())
    }

    pub fn close(&mut self) {
        self.searcher = None;
        self.index_meta = None;
        self.search_options = None;
    }
}

impl Drop for LuminaVectorGlobalIndexReader {
    fn drop(&mut self) {
        self.close();
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::globalindex::GlobalIndexIOMeta;

    // Aligned with Java: testDifferentMetrics — score conversion per metric
    #[test]
    fn test_convert_distance_to_score() {
        assert_eq!(convert_distance_to_score(0.0, LuminaVectorMetric::L2), 1.0);
        assert_eq!(convert_distance_to_score(1.0, LuminaVectorMetric::L2), 0.5);
        assert_eq!(convert_distance_to_score(0.0, LuminaVectorMetric::Cosine), 1.0);
        assert_eq!(convert_distance_to_score(1.0, LuminaVectorMetric::Cosine), 0.0);
        assert_eq!(convert_distance_to_score(0.75, LuminaVectorMetric::InnerProduct), 0.75);
    }

    #[test]
    fn test_ensure_search_list_size() {
        let mut opts = HashMap::new();
        ensure_search_list_size(&mut opts, 10);
        assert_eq!(opts.get("diskann.search.list_size").unwrap(), "16"); // max(15, 16)

        let mut opts = HashMap::new();
        ensure_search_list_size(&mut opts, 100);
        assert_eq!(opts.get("diskann.search.list_size").unwrap(), "150"); // 100*1.5

        // does not override existing
        let mut opts = HashMap::new();
        opts.insert("diskann.search.list_size".to_string(), "999".to_string());
        ensure_search_list_size(&mut opts, 100);
        assert_eq!(opts.get("diskann.search.list_size").unwrap(), "999");
    }

    #[test]
    fn test_filter_search_options() {
        let mut opts = HashMap::new();
        opts.insert("search.beam_width".to_string(), "4".to_string());
        opts.insert("diskann.search.list_size".to_string(), "16".to_string());
        opts.insert("index.dimension".to_string(), "128".to_string());
        let filtered = filter_search_options(&opts);
        assert_eq!(filtered.len(), 2);
        assert!(!filtered.contains_key("index.dimension"));
    }

    #[test]
    fn test_reader_requires_exactly_one_meta() {
        assert!(LuminaVectorGlobalIndexReader::new(vec![], HashMap::new()).is_err());
        let m1 = GlobalIndexIOMeta::new("a".into(), 100, vec![]);
        let m2 = GlobalIndexIOMeta::new("b".into(), 200, vec![]);
        assert!(LuminaVectorGlobalIndexReader::new(vec![m1, m2], HashMap::new()).is_err());
    }
}
