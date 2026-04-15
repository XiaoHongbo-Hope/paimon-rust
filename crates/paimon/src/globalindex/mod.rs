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

use roaring::RoaringTreemap;
use std::collections::BinaryHeap;
use std::sync::Arc;

pub type ScoreGetter = Arc<dyn Fn(u64) -> f32 + Send + Sync>;

pub trait GlobalIndexResult: Send + Sync {
    fn results(&self) -> &RoaringTreemap;

    fn as_scored(&self) -> Option<&dyn ScoredGlobalIndexResult> {
        None
    }

    fn offset(&self, start_offset: u64) -> Box<dyn GlobalIndexResult> {
        let bitmap = self.results();
        let offset_bitmap = if start_offset == 0 {
            bitmap.clone()
        } else {
            let mut offset_bitmap = RoaringTreemap::new();
            for row_id in bitmap.iter() {
                offset_bitmap.insert(row_id + start_offset);
            }
            offset_bitmap
        };

        if let Some(scored) = self.as_scored() {
            let sg = scored.clone_score_getter();
            let score_getter = if start_offset == 0 {
                sg
            } else {
                Arc::new(move |row_id| sg(row_id - start_offset))
            };
            return Box::new(SimpleScoredGlobalIndexResult::new(
                offset_bitmap,
                score_getter,
            ));
        }

        Box::new(SimpleGlobalIndexResult::new_ready(offset_bitmap))
    }

    fn and(&self, other: &dyn GlobalIndexResult) -> Box<dyn GlobalIndexResult> {
        // Scored intersection has ambiguous score semantics, so we drop scores
        // and return an unscored result.
        let result = self.results() & other.results();
        Box::new(SimpleGlobalIndexResult::new_ready(result))
    }

    fn or(&self, other: &dyn GlobalIndexResult) -> Box<dyn GlobalIndexResult> {
        match (self.as_scored(), other.as_scored()) {
            (Some(this), Some(other)) => {
                let this_row_ids = self.results().clone();
                let other_row_ids = other.results().clone();
                let result_or = &this_row_ids | &other_row_ids;
                let this_sg = this.clone_score_getter();
                let other_sg = other.clone_score_getter();
                return Box::new(SimpleScoredGlobalIndexResult::new(
                    result_or,
                    Arc::new(move |row_id| {
                        let in_this = this_row_ids.contains(row_id);
                        let in_other = other_row_ids.contains(row_id);
                        match (in_this, in_other) {
                            (true, true) => this_sg(row_id).max(other_sg(row_id)),
                            (true, false) => this_sg(row_id),
                            _ => other_sg(row_id),
                        }
                    }),
                ));
            }
            (None, None) => {}
            _ => panic!("Cannot union scored and unscored global index results"),
        }

        let result = self.results() | other.results();
        Box::new(SimpleGlobalIndexResult::new_ready(result))
    }

    fn is_empty(&self) -> bool {
        self.results().is_empty()
    }
}

pub struct SimpleGlobalIndexResult {
    bitmap: RoaringTreemap,
}

impl SimpleGlobalIndexResult {
    pub fn new_ready(bitmap: RoaringTreemap) -> Self {
        Self { bitmap }
    }

    pub fn create_empty() -> Self {
        Self::new_ready(RoaringTreemap::new())
    }
}

impl GlobalIndexResult for SimpleGlobalIndexResult {
    fn results(&self) -> &RoaringTreemap {
        &self.bitmap
    }
}

pub trait ScoredGlobalIndexResult: GlobalIndexResult {
    fn score_getter(&self) -> &ScoreGetter;

    fn scored_offset(&self, offset: u64) -> Box<dyn ScoredGlobalIndexResult> {
        if offset == 0 {
            let bitmap = self.results().clone();
            let sg = self.clone_score_getter();
            return Box::new(SimpleScoredGlobalIndexResult::new(bitmap, sg));
        }
        let bitmap = self.results();
        let mut offset_bitmap = RoaringTreemap::new();
        for row_id in bitmap.iter() {
            offset_bitmap.insert(row_id + offset);
        }
        let sg = self.clone_score_getter();
        Box::new(SimpleScoredGlobalIndexResult::new(
            offset_bitmap,
            Arc::new(move |row_id| sg(row_id - offset)),
        ))
    }

    fn scored_or(&self, other: &dyn ScoredGlobalIndexResult) -> Box<dyn ScoredGlobalIndexResult> {
        let this_row_ids = self.results().clone();
        let other_row_ids = other.results().clone();
        let result_or = &this_row_ids | &other_row_ids;
        let this_sg = self.clone_score_getter();
        let other_sg = other.clone_score_getter();
        let this_ids = this_row_ids;
        let other_ids = other_row_ids;
        Box::new(SimpleScoredGlobalIndexResult::new(
            result_or,
            Arc::new(move |row_id| {
                let in_this = this_ids.contains(row_id);
                let in_other = other_ids.contains(row_id);
                match (in_this, in_other) {
                    (true, true) => this_sg(row_id).max(other_sg(row_id)),
                    (true, false) => this_sg(row_id),
                    _ => other_sg(row_id),
                }
            }),
        ))
    }

    fn top_k(&self, k: usize) -> Box<dyn ScoredGlobalIndexResult> {
        let row_ids = self.results();
        if row_ids.len() as usize <= k {
            let bitmap = row_ids.clone();
            let sg = self.clone_score_getter();
            return Box::new(SimpleScoredGlobalIndexResult::new(bitmap, sg));
        }

        let score_getter_fn = self.score_getter();

        #[derive(PartialEq)]
        struct ScoredEntry {
            row_id: u64,
            score: f32,
        }
        impl Eq for ScoredEntry {}
        impl PartialOrd for ScoredEntry {
            fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
                Some(self.cmp(other))
            }
        }
        impl Ord for ScoredEntry {
            fn cmp(&self, other: &Self) -> std::cmp::Ordering {
                other
                    .score
                    .partial_cmp(&self.score)
                    .unwrap_or(std::cmp::Ordering::Equal)
            }
        }

        let mut min_heap: BinaryHeap<ScoredEntry> = BinaryHeap::with_capacity(k + 1);
        for row_id in row_ids.iter() {
            let score = score_getter_fn(row_id);
            if min_heap.len() < k {
                min_heap.push(ScoredEntry { row_id, score });
            } else if let Some(peek) = min_heap.peek() {
                if score > peek.score {
                    min_heap.pop();
                    min_heap.push(ScoredEntry { row_id, score });
                }
            }
        }

        let mut top_k_ids = RoaringTreemap::new();
        for entry in &min_heap {
            top_k_ids.insert(entry.row_id);
        }

        let sg = self.clone_score_getter();
        Box::new(SimpleScoredGlobalIndexResult::new(top_k_ids, sg))
    }

    fn clone_score_getter(&self) -> ScoreGetter;
}

pub struct SimpleScoredGlobalIndexResult {
    bitmap: RoaringTreemap,
    score_getter: ScoreGetter,
}

impl SimpleScoredGlobalIndexResult {
    pub fn new(bitmap: RoaringTreemap, score_getter: ScoreGetter) -> Self {
        Self {
            bitmap,
            score_getter,
        }
    }

    pub fn create_empty() -> Self {
        Self {
            bitmap: RoaringTreemap::new(),
            score_getter: Arc::new(|_| 0.0),
        }
    }
}

impl GlobalIndexResult for SimpleScoredGlobalIndexResult {
    fn results(&self) -> &RoaringTreemap {
        &self.bitmap
    }

    fn as_scored(&self) -> Option<&dyn ScoredGlobalIndexResult> {
        Some(self)
    }
}

impl ScoredGlobalIndexResult for SimpleScoredGlobalIndexResult {
    fn score_getter(&self) -> &ScoreGetter {
        &self.score_getter
    }

    fn clone_score_getter(&self) -> ScoreGetter {
        self.score_getter.clone()
    }
}

pub struct DictBasedScoredIndexResult {
    id_to_scores: Arc<std::collections::HashMap<u64, f32>>,
    bitmap: RoaringTreemap,
    score_getter_fn: ScoreGetter,
}

impl DictBasedScoredIndexResult {
    pub fn new(id_to_scores: std::collections::HashMap<u64, f32>) -> Self {
        let mut bitmap = RoaringTreemap::new();
        for &row_id in id_to_scores.keys() {
            bitmap.insert(row_id);
        }
        let map = Arc::new(id_to_scores);
        let map_ref = map.clone();
        let score_getter_fn: ScoreGetter =
            Arc::new(move |row_id| map_ref.get(&row_id).copied().unwrap_or(0.0));
        Self {
            id_to_scores: map,
            bitmap,
            score_getter_fn,
        }
    }
}

impl GlobalIndexResult for DictBasedScoredIndexResult {
    fn results(&self) -> &RoaringTreemap {
        &self.bitmap
    }

    fn as_scored(&self) -> Option<&dyn ScoredGlobalIndexResult> {
        Some(self)
    }
}

impl ScoredGlobalIndexResult for DictBasedScoredIndexResult {
    fn score_getter(&self) -> &ScoreGetter {
        &self.score_getter_fn
    }

    fn clone_score_getter(&self) -> ScoreGetter {
        let map = self.id_to_scores.clone();
        Arc::new(move |row_id| map.get(&row_id).copied().unwrap_or(0.0))
    }
}

pub struct VectorSearch {
    pub vector: Vec<f32>,
    pub limit: usize,
    pub field_name: String,
    pub include_row_ids: Option<RoaringTreemap>,
}

impl VectorSearch {
    pub fn new(vector: Vec<f32>, limit: usize, field_name: String) -> crate::Result<Self> {
        if vector.is_empty() {
            return Err(crate::Error::DataInvalid {
                message: "Search vector cannot be empty".to_string(),
                source: None,
            });
        }
        if limit == 0 {
            return Err(crate::Error::DataInvalid {
                message: format!("Limit must be positive, got: {}", limit),
                source: None,
            });
        }
        if field_name.is_empty() {
            return Err(crate::Error::DataInvalid {
                message: "Field name cannot be null or empty".to_string(),
                source: None,
            });
        }
        Ok(Self {
            vector,
            limit,
            field_name,
            include_row_ids: None,
        })
    }

    pub fn with_include_row_ids(mut self, include_row_ids: RoaringTreemap) -> Self {
        self.include_row_ids = Some(include_row_ids);
        self
    }

    pub fn offset_range(&self, from: u64, to: u64) -> Self {
        if let Some(ref include_row_ids) = self.include_row_ids {
            let mut range_bitmap = RoaringTreemap::new();
            range_bitmap.insert_range(from..to);
            let and_result = include_row_ids & &range_bitmap;
            let mut offset_bitmap = RoaringTreemap::new();
            for row_id in and_result.iter() {
                offset_bitmap.insert(row_id - from);
            }
            VectorSearch {
                vector: self.vector.clone(),
                limit: self.limit,
                field_name: self.field_name.clone(),
                include_row_ids: Some(offset_bitmap),
            }
        } else {
            VectorSearch {
                vector: self.vector.clone(),
                limit: self.limit,
                field_name: self.field_name.clone(),
                include_row_ids: None,
            }
        }
    }
}

impl std::fmt::Display for VectorSearch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(
            f,
            "VectorSearch(field_name={}, limit={})",
            self.field_name, self.limit
        )
    }
}

pub struct GlobalIndexIOMeta {
    pub file_path: String,
    pub file_size: u64,
    pub metadata: Vec<u8>,
}

impl GlobalIndexIOMeta {
    pub fn new(file_path: String, file_size: u64, metadata: Vec<u8>) -> Self {
        Self {
            file_path,
            file_size,
            metadata,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    // Aligned with Java VectorSearchTest.testVectorSearchOffset
    #[test]
    fn test_vector_search_offset_range() {
        let mut bitmap = RoaringTreemap::new();
        bitmap.insert_range(100..200);
        let vs = VectorSearch::new(vec![1.0, 2.0], 10, "vec".to_string())
            .unwrap()
            .with_include_row_ids(bitmap);

        let result = vs.offset_range(60, 150);
        let ids = result.include_row_ids.unwrap();
        // [100,200) & [60,150) = [100,150), offset by -60 => [40,90)
        assert_eq!(ids.len(), 50);
        assert!(ids.contains(40));
        assert!(ids.contains(89));
        assert!(!ids.contains(39));
        assert!(!ids.contains(90));
    }

    // Aligned with Java LuminaVectorGlobalIndexTest.testInvalidTopK
    #[test]
    fn test_invalid_top_k() {
        assert!(VectorSearch::new(vec![1.0], 0, "f".to_string()).is_err());
    }

    #[test]
    fn test_offset_range_no_filter() {
        let vs = VectorSearch::new(vec![1.0], 5, "f".to_string()).unwrap();
        let result = vs.offset_range(100, 200);
        assert!(result.include_row_ids.is_none());
    }

    // Scored results: top_k, scored_offset, scored_or — exercised indirectly in Java integration tests
    fn make_dict(entries: Vec<(u64, f32)>) -> DictBasedScoredIndexResult {
        DictBasedScoredIndexResult::new(entries.into_iter().collect())
    }

    #[test]
    fn test_top_k_selects_highest() {
        let r = make_dict(vec![(1, 0.1), (2, 0.9), (3, 0.5), (4, 0.8), (5, 0.3)]);
        let top = r.top_k(2);
        assert_eq!(top.results().len(), 2);
        assert!(top.results().contains(2));
        assert!(top.results().contains(4));
    }

    #[test]
    fn test_scored_offset_preserves_scores() {
        let r = make_dict(vec![(1, 0.5), (2, 0.6)]);
        let o = r.scored_offset(100);
        assert!(o.results().contains(101));
        assert_eq!(o.score_getter()(101), 0.5);
        assert_eq!(o.score_getter()(102), 0.6);
    }

    #[test]
    fn test_base_offset_preserves_scores() {
        let r = make_dict(vec![(1, 0.5), (2, 0.6)]);
        let o = r.offset(100);
        let scored = o
            .as_scored()
            .expect("offset should preserve scored results");
        assert!(o.results().contains(101));
        assert_eq!(scored.score_getter()(101), 0.5);
        assert_eq!(scored.score_getter()(102), 0.6);
    }

    #[test]
    fn test_base_or_preserves_scores() {
        let left = make_dict(vec![(1, 0.5), (2, 0.6)]);
        let right = make_dict(vec![(3, 0.7), (4, 0.8)]);
        let merged = left.or(&right);
        let scored = merged
            .as_scored()
            .expect("or should preserve scored results");
        assert_eq!(merged.results().len(), 4);
        assert_eq!(scored.score_getter()(1), 0.5);
        assert_eq!(scored.score_getter()(4), 0.8);
    }

    #[test]
    fn test_or_overlapping_takes_max_score() {
        let left = make_dict(vec![(1, 0.3), (2, 0.9)]);
        let right = make_dict(vec![(1, 0.7), (2, 0.4)]);
        let merged = left.or(&right);
        let scored = merged
            .as_scored()
            .expect("or should preserve scored results");
        assert_eq!(merged.results().len(), 2);
        // row 1: max(0.3, 0.7) = 0.7
        assert_eq!(scored.score_getter()(1), 0.7);
        // row 2: max(0.9, 0.4) = 0.9
        assert_eq!(scored.score_getter()(2), 0.9);
    }

    #[test]
    fn test_scored_and_drops_scores() {
        let left = make_dict(vec![(1, 0.5), (2, 0.6)]);
        let right = make_dict(vec![(1, 0.7), (3, 0.8)]);
        let result = left.and(&right);
        assert_eq!(result.results().len(), 1);
        assert!(result.results().contains(1));
        // Scores are dropped for intersection
        assert!(result.as_scored().is_none());
    }

    #[test]
    fn test_clone_score_getter() {
        let r = make_dict(vec![(10, 1.5), (20, 2.5)]);
        let cloned = r.clone_score_getter();
        assert_eq!(cloned(10), 1.5);
        assert_eq!(cloned(20), 2.5);
    }
}
