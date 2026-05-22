/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Partition assignment for distributing row groups across DataFusion partitions.
//!
//! Same model as DataFusion's `repartition_evenly_by_size`: flatten all row
//! groups across all segments, iterate sequentially, cut a new partition when
//! accumulated rows exceed `ceil(total_rows / num_partitions)`.
//!
//! A single partition may span multiple segments — each segment's RGs become
//! a separate `SegmentChunk`.
//!
//! Partitions MUST align to RG boundaries because:
//! 1. Splitting mid-RG causes duplicate processing.
//! 2. Row indices within a RG are contiguous `[0, num_rows)`.
//! 3. Doc IDs map 1:1 to row indices within each segment's parquet file.
//!
//! Reference: <https://github.com/apache/datafusion/blob/49776a6/datafusion/datasource/src/file_groups.rs#L204>
//!
//! Ported verbatim from PR #21164.

use super::stream::RowGroupInfo;

/// One contiguous chunk of row groups within a single segment.
#[derive(Debug, Clone)]
pub struct SegmentChunk {
    pub segment_idx: usize,
    pub doc_min: i32,
    pub doc_max: i32,
    pub row_group_indices: Vec<usize>,
}

/// A partition which can span multiple segments.
#[derive(Debug, Clone)]
pub struct PartitionAssignment {
    pub chunks: Vec<SegmentChunk>,
}

/// Info about a segment needed for partition assignment.
pub struct SegmentLayout {
    pub row_groups: Vec<RowGroupInfo>,
}

/// Strategy for assigning row groups to DataFusion partitions. Different execution paths
/// pick the strategy that matches their work shape: the bitmap path uses `RowGroupBalanced`
/// (RG-streaming wants sub-segment granularity to keep per-RG memory bounded); the count
/// path uses `SegmentGrouped` (a single FFM `count_docs` call per segment, no decode, so
/// segment-aligned buckets maximize parallelism without splitting).
pub trait PartitionStrategy: Send + Sync {
    fn assign(
        &self,
        segments: &[SegmentLayout],
        target_partitions: usize,
    ) -> Vec<PartitionAssignment>;
}

/// Today's algorithm — flatten all RGs across all segments, cut every
/// `ceil(total_rows / num_partitions)` rows, never split mid-RG. A single segment may
/// span multiple partitions; a single partition may span multiple segments. Used by the
/// bitmap path because RG-streaming needs sub-segment work units to keep per-RG memory
/// bounded during candidate + refinement stages.
pub struct RowGroupBalanced;

impl PartitionStrategy for RowGroupBalanced {
    fn assign(
        &self,
        segments: &[SegmentLayout],
        target_partitions: usize,
    ) -> Vec<PartitionAssignment> {
        compute_assignments(segments, target_partitions)
    }
}

/// COUNT_DELEGATION strategy: each partition owns one or more whole segments. Greedy
/// longest-processing-time bin-packing keeps row counts roughly balanced across
/// `min(target_partitions, num_segments)` buckets without ever splitting a segment.
///
/// Why never split: count delegation makes one FFM `count_docs` call per (segment,
/// chunk) pair. Splitting a segment into N chunks turns 1 FFM call into N + loses the
/// `Weight.count(leaf)` metadata fast path (which requires `minDoc=0`, `maxDoc=segMaxDoc`).
pub struct SegmentGrouped;

impl PartitionStrategy for SegmentGrouped {
    fn assign(
        &self,
        segments: &[SegmentLayout],
        target_partitions: usize,
    ) -> Vec<PartitionAssignment> {
        if segments.is_empty() {
            return vec![];
        }
        let buckets = target_partitions.max(1).min(segments.len());

        // Sort segments by row count descending so the largest land first. Greedy LPT
        // (assign each next segment to the lightest current bucket) gives a 4/3 OPT
        // approximation, which is fine — segment counts are usually <100 and counts
        // come from parquet metadata, not estimates.
        let mut indexed: Vec<(usize, &SegmentLayout)> = segments.iter().enumerate().collect();
        indexed.sort_by_key(|(_, s)| {
            std::cmp::Reverse(s.row_groups.iter().map(|rg| rg.num_rows).sum::<i64>())
        });

        let mut assignments: Vec<PartitionAssignment> =
            (0..buckets).map(|_| PartitionAssignment { chunks: Vec::new() }).collect();
        let mut bucket_rows = vec![0i64; buckets];

        for (seg_idx, seg) in indexed {
            let target = bucket_rows
                .iter()
                .enumerate()
                .min_by_key(|(_, &rows)| rows)
                .map(|(idx, _)| idx)
                .expect("buckets >= 1");
            let row_group_indices: Vec<usize> = seg.row_groups.iter().map(|rg| rg.index).collect();
            let total_rows = seg.row_groups.iter().map(|rg| rg.num_rows).sum::<i64>();

            // Invariant: COUNT_DELEGATION requires whole-segment chunks so the Lucene
            // backend's Weight.count(leaf) fast path is reachable. Any chunk built
            // here must have doc_min=0, doc_max=total_rows, and include every RG.
            // Failing this invariant silently turns Weight.count into the iterate
            // fallback (correct but slow) — debug_assert flags planner/refactor bugs
            // that would otherwise be invisible in production.
            debug_assert_eq!(
                row_group_indices.len(),
                seg.row_groups.len(),
                "SegmentGrouped chunk must include every RG of segment {} (have {}, expected {})",
                seg_idx, row_group_indices.len(), seg.row_groups.len()
            );
            debug_assert!(
                total_rows <= i32::MAX as i64,
                "segment {} has {} rows; doc_max overflows i32 — segment-level partitioning \
                 cannot represent doc ranges this large in SegmentChunk's i32 fields",
                seg_idx, total_rows
            );

            assignments[target].chunks.push(SegmentChunk {
                segment_idx: seg_idx,
                doc_min: 0,
                doc_max: total_rows as i32,
                row_group_indices,
            });
            bucket_rows[target] += total_rows;
        }

        // Cross-check: every input segment must appear exactly once across the buckets.
        // Catches any future refactor that accidentally drops or duplicates a segment.
        debug_assert_eq!(
            assignments.iter().map(|a| a.chunks.len()).sum::<usize>(),
            segments.len(),
            "SegmentGrouped: total chunks emitted ({}) must equal segment count ({})",
            assignments.iter().map(|a| a.chunks.len()).sum::<usize>(),
            segments.len()
        );

        assignments
    }
}

/// Compute partition assignments aligned to row group boundaries.
pub fn compute_assignments(
    segments: &[SegmentLayout],
    num_partitions: usize,
) -> Vec<PartitionAssignment> {
    struct RGEntry {
        segment_idx: usize,
        rg_index: usize,
        first_row: i64,
        num_rows: i64,
    }

    let all_rgs: Vec<RGEntry> = segments
        .iter()
        .enumerate()
        .flat_map(|(seg_idx, seg)| {
            seg.row_groups.iter().map(move |rg| RGEntry {
                segment_idx: seg_idx,
                rg_index: rg.index,
                first_row: rg.first_row,
                num_rows: rg.num_rows,
            })
        })
        .collect();

    if all_rgs.is_empty() {
        return vec![];
    }

    let total_rows: i64 = all_rgs.iter().map(|rg| rg.num_rows).sum();
    let rows_per_partition = (total_rows as f64 / num_partitions as f64).ceil() as i64;

    let mut assignments: Vec<PartitionAssignment> = Vec::new();
    let mut current_chunks: Vec<SegmentChunk> = Vec::new();
    let mut current_rows: i64 = 0;

    let mut chunk_seg: Option<usize> = None;
    let mut chunk_rg_indices: Vec<usize> = Vec::new();
    let mut chunk_doc_min: i32 = 0;
    let mut chunk_doc_max: i32 = 0;

    for (i, rg) in all_rgs.iter().enumerate() {
        // Flush in-progress chunk if segment changed
        if chunk_seg.is_some() && chunk_seg != Some(rg.segment_idx) {
            if !chunk_rg_indices.is_empty() {
                current_chunks.push(SegmentChunk {
                    segment_idx: chunk_seg.unwrap(),
                    doc_min: chunk_doc_min,
                    doc_max: chunk_doc_max,
                    row_group_indices: chunk_rg_indices.clone(),
                });
                chunk_rg_indices.clear();
            }
        }

        if chunk_seg != Some(rg.segment_idx) {
            chunk_seg = Some(rg.segment_idx);
            chunk_doc_min = rg.first_row as i32;
        }

        chunk_rg_indices.push(rg.rg_index);
        chunk_doc_max = (rg.first_row + rg.num_rows) as i32;
        current_rows += rg.num_rows;

        let is_last = i == all_rgs.len() - 1;

        if current_rows >= rows_per_partition && assignments.len() < num_partitions - 1 && !is_last
        {
            current_chunks.push(SegmentChunk {
                segment_idx: chunk_seg.unwrap(),
                doc_min: chunk_doc_min,
                doc_max: chunk_doc_max,
                row_group_indices: chunk_rg_indices.clone(),
            });
            chunk_rg_indices.clear();
            chunk_doc_min = chunk_doc_max;

            assignments.push(PartitionAssignment {
                chunks: std::mem::take(&mut current_chunks),
            });
            current_rows = 0;
        }
    }

    // Flush remaining
    if !chunk_rg_indices.is_empty() {
        current_chunks.push(SegmentChunk {
            segment_idx: chunk_seg.unwrap(),
            doc_min: chunk_doc_min,
            doc_max: chunk_doc_max,
            row_group_indices: chunk_rg_indices,
        });
    }
    if !current_chunks.is_empty() {
        assignments.push(PartitionAssignment {
            chunks: current_chunks,
        });
    }

    assignments
}

#[cfg(test)]
mod tests {
    use super::*;

    fn segment(row_counts: &[i64]) -> SegmentLayout {
        let mut row_groups = Vec::with_capacity(row_counts.len());
        let mut first_row = 0i64;
        for (i, &n) in row_counts.iter().enumerate() {
            row_groups.push(RowGroupInfo {
                index: i,
                first_row,
                num_rows: n,
            });
            first_row += n;
        }
        SegmentLayout { row_groups }
    }

    #[test]
    fn segment_grouped_one_segment_one_partition() {
        let segs = vec![segment(&[100, 200])];
        let result = SegmentGrouped.assign(&segs, 1);
        assert_eq!(result.len(), 1);
        assert_eq!(result[0].chunks.len(), 1);
        assert_eq!(result[0].chunks[0].segment_idx, 0);
        assert_eq!(result[0].chunks[0].doc_min, 0);
        assert_eq!(result[0].chunks[0].doc_max, 300);
        assert_eq!(result[0].chunks[0].row_group_indices, vec![0, 1]);
    }

    #[test]
    fn segment_grouped_caps_partitions_at_segment_count() {
        let segs = vec![segment(&[100]), segment(&[200])];
        let result = SegmentGrouped.assign(&segs, 8);
        assert_eq!(result.len(), 2, "cannot exceed num_segments");
    }

    #[test]
    fn segment_grouped_balances_via_lpt() {
        // 4 segments × {100, 200, 50, 150} → 2 partitions.
        // LPT order: 200, 150, 100, 50.
        // Bucket A: 200 (then 50)  = 250
        // Bucket B: 150 (then 100) = 250
        let segs = vec![segment(&[100]), segment(&[200]), segment(&[50]), segment(&[150])];
        let result = SegmentGrouped.assign(&segs, 2);
        assert_eq!(result.len(), 2);
        let bucket_rows: Vec<i64> = result
            .iter()
            .map(|p| p.chunks.iter().map(|c| (c.doc_max - c.doc_min) as i64).sum())
            .collect();
        let total: i64 = bucket_rows.iter().sum();
        assert_eq!(total, 500);
        let max = *bucket_rows.iter().max().unwrap();
        let min = *bucket_rows.iter().min().unwrap();
        assert!(max - min <= 50, "LPT imbalance too high: {:?}", bucket_rows);
    }

    #[test]
    fn segment_grouped_chunks_cover_whole_segment() {
        // doc_min must be 0 and doc_max must equal sum(num_rows). Required for
        // Lucene's Weight.count(leaf) fast path to fire.
        let segs = vec![segment(&[100, 200, 50])];
        let result = SegmentGrouped.assign(&segs, 1);
        let chunk = &result[0].chunks[0];
        assert_eq!(chunk.doc_min, 0);
        assert_eq!(chunk.doc_max, 350);
        assert_eq!(chunk.row_group_indices, vec![0, 1, 2]);
    }

    #[test]
    fn segment_grouped_empty_segments() {
        let segs: Vec<SegmentLayout> = vec![];
        let result = SegmentGrouped.assign(&segs, 4);
        assert!(result.is_empty());
    }

    #[test]
    fn segment_grouped_each_segment_appears_exactly_once() {
        // The conservation invariant: every input segment must appear in exactly one
        // chunk across the buckets. Internal debug_assert covers this; the test makes
        // it visible at unit-test runtime even in release-mode test runs.
        let segs = vec![
            segment(&[100]),
            segment(&[50]),
            segment(&[200]),
            segment(&[75]),
            segment(&[125]),
        ];
        let result = SegmentGrouped.assign(&segs, 3);
        let mut seen: Vec<usize> = result
            .iter()
            .flat_map(|p| p.chunks.iter().map(|c| c.segment_idx))
            .collect();
        seen.sort();
        assert_eq!(seen, vec![0, 1, 2, 3, 4]);
    }

    #[test]
    fn row_group_balanced_matches_legacy_function() {
        // RowGroupBalanced is a thin wrapper around compute_assignments — verify
        // pre-existing callers see identical output through either entry point.
        let segs = vec![segment(&[100, 200]), segment(&[150])];
        let via_trait = RowGroupBalanced.assign(&segs, 2);
        let via_fn = compute_assignments(&segs, 2);
        assert_eq!(via_trait.len(), via_fn.len());
        for (a, b) in via_trait.iter().zip(via_fn.iter()) {
            assert_eq!(a.chunks.len(), b.chunks.len());
            for (ca, cb) in a.chunks.iter().zip(b.chunks.iter()) {
                assert_eq!(ca.segment_idx, cb.segment_idx);
                assert_eq!(ca.doc_min, cb.doc_min);
                assert_eq!(ca.doc_max, cb.doc_max);
                assert_eq!(ca.row_group_indices, cb.row_group_indices);
            }
        }
    }
}
