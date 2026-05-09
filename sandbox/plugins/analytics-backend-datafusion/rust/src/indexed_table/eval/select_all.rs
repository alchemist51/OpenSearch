/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Select-all bitset source for Approach 2 (IndexedPredicateOnly).
//!
//! When `emit_row_ids=true` and `FilterClass::None` (no index filters),
//! this evaluator selects all rows in every row group without any bitmap
//! evaluation or FFM calls. Row IDs are computed from position by the
//! `IndexedStream`'s existing `global_base + rg.first_row + position` logic.
//!
//! This exercises the indexed pipeline's segment partitioning and prefetch
//! machinery without any collector/FFM overhead.

use std::any::Any;
use std::sync::Arc;
use std::time::Instant;

use datafusion::arrow::array::BooleanArray;
use datafusion::arrow::record_batch::RecordBatch;
use roaring::RoaringBitmap;

use super::{PrefetchedRg, RowGroupBitsetSource};
use crate::indexed_table::row_selection::PositionMap;
use crate::indexed_table::stream::RowGroupInfo;

/// A bitset source that selects all rows in every row group.
/// Used for Approach 2 (IndexedPredicateOnly) when there are no index filters
/// but we want to emit row IDs through the indexed pipeline.
pub struct SelectAllBitsetSource;

impl RowGroupBitsetSource for SelectAllBitsetSource {
    fn prefetch_rg(
        &self,
        rg: &RowGroupInfo,
        min_doc: i32,
        max_doc: i32,
    ) -> Result<Option<PrefetchedRg>, String> {
        let t = Instant::now();
        // Select all rows in the doc range, clamped to the RG boundaries
        let rg_start = rg.first_row as i32;
        let rg_end = (rg.first_row + rg.num_rows) as i32;

        // Effective range is intersection of [min_doc, max_doc) and [rg_start, rg_end)
        let effective_min = min_doc.max(rg_start);
        let effective_max = max_doc.min(rg_end);

        if effective_min >= effective_max {
            return Ok(None);
        }

        // Convert to RG-relative positions
        let range_start = (effective_min - rg_start) as u32;
        let range_end = (effective_max - rg_start) as u32;

        let mut candidates = RoaringBitmap::new();
        candidates.insert_range(range_start..range_end);

        Ok(Some(PrefetchedRg {
            candidates,
            eval_nanos: t.elapsed().as_nanos() as u64,
            context: Box::new(()),
            mask_buffer: None,
        }))
    }

    fn on_batch_mask(
        &self,
        _rg_state: &dyn Any,
        _rg_first_row: i64,
        _position_map: &PositionMap,
        _batch_offset: usize,
        _batch_len: usize,
        _batch: &RecordBatch,
    ) -> Result<Option<BooleanArray>, String> {
        // No refinement needed — all rows pass.
        Ok(None)
    }

    fn needs_row_mask(&self) -> bool {
        false
    }

    fn forbid_parquet_pushdown(&self) -> bool {
        false
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn select_all_selects_full_rg() {
        let source = SelectAllBitsetSource;
        let rg = RowGroupInfo {
            index: 0,
            first_row: 0,
            num_rows: 100,
        };
        let result = source.prefetch_rg(&rg, 0, 100).unwrap();
        assert!(result.is_some());
        let prefetched = result.unwrap();
        assert_eq!(prefetched.candidates.len(), 100);
    }

    #[test]
    fn select_all_with_doc_range_subset() {
        let source = SelectAllBitsetSource;
        let rg = RowGroupInfo {
            index: 0,
            first_row: 0,
            num_rows: 100,
        };
        // Only select docs 20..50
        let result = source.prefetch_rg(&rg, 20, 50).unwrap();
        assert!(result.is_some());
        let prefetched = result.unwrap();
        assert_eq!(prefetched.candidates.len(), 30);
    }

    #[test]
    fn select_all_empty_range_returns_none() {
        let source = SelectAllBitsetSource;
        let rg = RowGroupInfo {
            index: 0,
            first_row: 100,
            num_rows: 50,
        };
        // Doc range doesn't overlap with RG
        let result = source.prefetch_rg(&rg, 0, 50).unwrap();
        assert!(result.is_none());
    }

    #[test]
    fn on_batch_mask_returns_none() {
        let source = SelectAllBitsetSource;
        let schema = Arc::new(datafusion::arrow::datatypes::Schema::new(
            Vec::<datafusion::arrow::datatypes::Field>::new(),
        ));
        let batch = RecordBatch::new_empty(schema);
        let pm = PositionMap::from_selection(
            &datafusion::parquet::arrow::arrow_reader::RowSelection::from(Vec::<
                datafusion::parquet::arrow::arrow_reader::RowSelector,
            >::new()),
        );
        let result = source.on_batch_mask(&(), 0, &pm, 0, 0, &batch).unwrap();
        assert!(result.is_none());
    }
}
