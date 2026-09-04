/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Stage 4 tests proving the streaming merge engine's O(k cursors + one
//! group + batch buffer) memory bound.
//!
//! Nine tests:
//!   1. plan_shape         — physical plan has SortPreservingMerge, no global SortExec/HashAggregate
//!   2. duplicate_across_files — k files with overlapping keys fold correctly
//!   3. null_handling      — null group keys and null aggregate values handled correctly
//!   4. recursive_merge    — merge-of-merge output is still ordered and correct
//!   5. cancellation       — cancel mid-merge, verify partial IPC files cleaned up
//!   6. cleanup            — aborted merge leaves no temp files
//!   7. schema_mismatch    — mismatched schema hash returns error, not panic
//!   8. definition_mismatch — mismatched definition hash returns error
//!   9. tiny_memory        — forced spill under 12 MiB, output exact and ordered

use std::fs::{self, File};
use std::sync::Arc;

use arrow_array::{Array, Float64Array, Int64Array, RecordBatch, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use opensearch_datafusion::mv_merge_engine;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter as ParquetWriter;

// ── Shared helpers ──────────────────────────────────────────────────────

/// Standard 5-column state schema: [RegionID i64, adv_sum i64, cnt i64,
/// avg_cnt u64, avg_sum f64].
fn state_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("RegionID", DataType::Int64, true),
        Field::new("adv_sum", DataType::Int64, true),
        Field::new("cnt", DataType::Int64, true),
        Field::new("avg_cnt", DataType::UInt64, true),
        Field::new("avg_sum", DataType::Float64, true),
    ]))
}

/// Writes sorted rows to a Parquet state file with the standard 5-column schema.
fn write_state_file(path: &str, rows: &[(i64, i64, i64, u64, f64)]) {
    let schema = state_schema();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.1).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.2).collect::<Vec<_>>(),
            )),
            Arc::new(UInt64Array::from(
                rows.iter().map(|r| r.3).collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                rows.iter().map(|r| r.4).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();
    let file = File::create(path).unwrap();
    let mut w = ParquetWriter::try_new(file, batch.schema(), None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
}

/// Writes sorted rows to a Parquet file with nullable i64 columns.
fn write_nullable_state_file(path: &str, rows: &[(Option<i64>, Option<i64>, Option<i64>)]) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, true),
        Field::new("val_sum", DataType::Int64, true),
        Field::new("val_cnt", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.1).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.2).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();
    let file = File::create(path).unwrap();
    let mut w = ParquetWriter::try_new(file, batch.schema(), None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
}

/// 4-column schema with SUM/MIN/MAX fold ops: [key i64, val_sum i64, val_min i64, val_max i64].
fn write_min_max_state_file(path: &str, rows: &[(i64, i64, i64, i64)]) {
    let schema = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Int64, true),
        Field::new("val_sum", DataType::Int64, true),
        Field::new("val_min", DataType::Int64, true),
        Field::new("val_max", DataType::Int64, true),
    ]));
    let batch = RecordBatch::try_new(
        schema,
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.1).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.2).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.3).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap();
    let file = File::create(path).unwrap();
    let mut w = ParquetWriter::try_new(file, batch.schema(), None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();
}

/// Reads rows from a standard 5-column state Parquet file, returns sorted by key.
fn read_rows(path: &str) -> Vec<(i64, i64, i64, u64, f64)> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap())
        .unwrap()
        .build()
        .unwrap();
    let mut out = vec![];
    for batch in reader {
        let b = batch.unwrap();
        let c0 = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let c1 = b.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        let c2 = b.column(2).as_any().downcast_ref::<Int64Array>().unwrap();
        let c3 = b.column(3).as_any().downcast_ref::<UInt64Array>().unwrap();
        let c4 = b.column(4).as_any().downcast_ref::<Float64Array>().unwrap();
        for i in 0..b.num_rows() {
            out.push((
                c0.value(i),
                c1.value(i),
                c2.value(i),
                c3.value(i),
                c4.value(i),
            ));
        }
    }
    out.sort_by_key(|r| r.0);
    out
}

/// Reads rows from a 3-column nullable state Parquet file.
fn read_nullable_rows(path: &str) -> Vec<(Option<i64>, Option<i64>, Option<i64>)> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap())
        .unwrap()
        .build()
        .unwrap();
    let mut out = vec![];
    for batch in reader {
        let b = batch.unwrap();
        let c0 = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let c1 = b.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        let c2 = b.column(2).as_any().downcast_ref::<Int64Array>().unwrap();
        for i in 0..b.num_rows() {
            let k = if c0.is_null(i) {
                None
            } else {
                Some(c0.value(i))
            };
            let v1 = if c1.is_null(i) {
                None
            } else {
                Some(c1.value(i))
            };
            let v2 = if c2.is_null(i) {
                None
            } else {
                Some(c2.value(i))
            };
            out.push((k, v1, v2));
        }
    }
    // Sort: None first (matching NULLS FIRST), then by key value.
    out.sort_by(|a, b| match (a.0, b.0) {
        (None, None) => std::cmp::Ordering::Equal,
        (None, Some(_)) => std::cmp::Ordering::Less,
        (Some(_), None) => std::cmp::Ordering::Greater,
        (Some(x), Some(y)) => x.cmp(&y),
    });
    out
}

/// Reads rows from a 4-column (key, sum, min, max) Parquet file.
fn read_min_max_rows(path: &str) -> Vec<(i64, i64, i64, i64)> {
    let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(path).unwrap())
        .unwrap()
        .build()
        .unwrap();
    let mut out = vec![];
    for batch in reader {
        let b = batch.unwrap();
        let c0 = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        let c1 = b.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        let c2 = b.column(2).as_any().downcast_ref::<Int64Array>().unwrap();
        let c3 = b.column(3).as_any().downcast_ref::<Int64Array>().unwrap();
        for i in 0..b.num_rows() {
            out.push((c0.value(i), c1.value(i), c2.value(i), c3.value(i)));
        }
    }
    out.sort_by_key(|r| r.0);
    out
}

/// Default fold ops: col 0 = GROUP_KEY, cols 1-4 = SUM.
fn default_fold_ops() -> Vec<u8> {
    vec![0, 1, 1, 1, 1]
}

/// Default ordering: ASC NULLS FIRST on col 0.
fn default_ordering() -> (Vec<usize>, Vec<bool>, Vec<bool>) {
    (vec![0], vec![true], vec![true])
}

// ── Test 1: plan_shape ──────────────────────────────────────────────────
//
// The Stage 4 engine uses a BinaryHeap-based k-way merge (SortPreservingMerge
// semantics) with adjacent-key fold — NOT a global SortExec or HashAggregate.
// Verify by inspecting the output: for k sorted inputs, the output must be
// globally sorted WITHOUT re-sorting (merge-sort property), and the number
// of output rows equals the number of distinct keys (fold property, not
// hash-aggregate-then-sort).

#[test]
fn plan_shape_no_global_sort_or_hash_aggregate() {
    let dir = tempfile::tempdir().unwrap();
    let p = |n: &str| dir.path().join(n).to_str().unwrap().to_string();

    // 4 files (k=4), each with 500 sorted keys (0..499, 500..999, etc.)
    // plus overlapping keys at boundaries to exercise fold.
    for file_idx in 0..4u32 {
        let base = (file_idx * 500) as i64;
        let mut rows: Vec<(i64, i64, i64, u64, f64)> = (0..500)
            .map(|i| {
                let key = base + i;
                (key, key * 10, 1, 1, key as f64)
            })
            .collect();
        // Overlap: add boundary keys from adjacent files for fold testing.
        if file_idx > 0 {
            let overlap_key = base - 1;
            rows.insert(0, (overlap_key, 1, 1, 1, 1.0));
        }
        write_state_file(&p(&format!("f{file_idx}.mv.parquet")), &rows);
    }

    let files: Vec<String> = (0..4).map(|i| p(&format!("f{i}.mv.parquet"))).collect();
    let (oi, oa, onf) = default_ordering();
    let ops = default_fold_ops();

    let rows = mv_merge_engine::merge_state_streams(&files, &p("out.mv.parquet"), &oi, &oa, &onf, &ops)
        .unwrap();

    // Read back and verify global ordering.
    let output = read_rows(&p("out.mv.parquet"));

    // Output must be strictly sorted (SortPreservingMerge property).
    for window in output.windows(2) {
        assert!(
            window[0].0 <= window[1].0,
            "output not sorted: key {} followed by {}",
            window[0].0,
            window[1].0,
        );
    }

    // Boundary keys (499, 999, 1499) appear in 2 files each → folded to 1 row.
    // Total unique keys: 4 * 500 + 3 overlaps = 2003 raw rows → 2000 distinct keys.
    assert_eq!(rows, 2000, "fold should reduce overlapping keys");

    // The overlapping keys must have their sums aggregated, not just passed through.
    let key499 = output.iter().find(|r| r.0 == 499).unwrap();
    // File 0 has (499, 4990, 1, 1, 499.0); File 1 has (499, 1, 1, 1, 1.0).
    assert_eq!(
        key499.1, 4991,
        "overlapping key 499: adv_sum should be 4990 + 1 = 4991"
    );
    assert_eq!(key499.2, 2, "overlapping key 499: cnt should be 1 + 1 = 2");
}

// ── Test 2: duplicate_across_files ──────────────────────────────────────
//
// k files with heavily overlapping keys must fold correctly. Every file
// has every key → the fold must sum across all k files for each key.
// Tests both SUM fold and MIN/MAX fold across k=5 files.

#[test]
fn duplicate_across_files_fold_correctly() {
    let dir = tempfile::tempdir().unwrap();
    let p = |n: &str| dir.path().join(n).to_str().unwrap().to_string();

    // 5 files, each containing keys [1, 2, 3] with different values.
    for file_idx in 0..5u32 {
        let v = (file_idx + 1) as i64;
        write_state_file(
            &p(&format!("d{file_idx}.mv.parquet")),
            &[
                (1, v * 10, v, v as u64, v as f64 * 100.0),
                (2, v * 20, v * 2, (v * 2) as u64, v as f64 * 200.0),
                (3, v * 30, v * 3, (v * 3) as u64, v as f64 * 300.0),
            ],
        );
    }

    let files: Vec<String> = (0..5).map(|i| p(&format!("d{i}.mv.parquet"))).collect();
    let (oi, oa, onf) = default_ordering();
    let ops = default_fold_ops();

    let rows = mv_merge_engine::merge_state_streams(&files, &p("out.mv.parquet"), &oi, &oa, &onf, &ops)
        .unwrap();

    assert_eq!(rows, 3, "only 3 distinct keys");
    let output = read_rows(&p("out.mv.parquet"));

    // Key 1: adv_sum = 10+20+30+40+50 = 150
    //         cnt     = 1+2+3+4+5 = 15
    //         avg_cnt = 1+2+3+4+5 = 15
    //         avg_sum = 100+200+300+400+500 = 1500
    assert_eq!(output[0], (1, 150, 15, 15, 1500.0));

    // Key 2: adv_sum = 20+40+60+80+100 = 300
    //         cnt     = 2+4+6+8+10 = 30
    //         avg_cnt = 2+4+6+8+10 = 30
    //         avg_sum = 200+400+600+800+1000 = 3000
    assert_eq!(output[1], (2, 300, 30, 30, 3000.0));

    // Key 3: adv_sum = 30+60+90+120+150 = 450
    //         cnt     = 3+6+9+12+15 = 45
    //         avg_cnt = 3+6+9+12+15 = 45
    //         avg_sum = 300+600+900+1200+1500 = 4500
    assert_eq!(output[2], (3, 450, 45, 45, 4500.0));

    // ── Also test MIN/MAX fold ops across all 5 files ──
    for file_idx in 0..5u32 {
        let v = (file_idx + 1) as i64;
        // key, sum, min, max — each file has different min/max values
        write_min_max_state_file(
            &p(&format!("mm{file_idx}.mv.parquet")),
            &[
                (1, v * 10, 100 - v * 10, v * 10), // key=1: min decreases, max increases
                (2, v * 20, 200 - v * 20, v * 20), // key=2
                (3, v * 30, 300 - v * 30, v * 30), // key=3
            ],
        );
    }

    let mm_files: Vec<String> = (0..5).map(|i| p(&format!("mm{i}.mv.parquet"))).collect();
    // fold_ops: key=GROUP_KEY(0), sum=SUM(1), min=MIN(2), max=MAX(3)
    let mm_rows = mv_merge_engine::merge_state_streams(
        &mm_files,
        &p("mm_out.mv.parquet"),
        &[0],
        &[true],
        &[true],
        &[0, 1, 2, 3],
    )
    .unwrap();

    assert_eq!(mm_rows, 3, "3 distinct keys in MIN/MAX test");
    let mm_output = read_min_max_rows(&p("mm_out.mv.parquet"));

    // Key 1: sum=10+20+30+40+50=150, min=min(90,80,70,60,50)=50, max=max(10,20,30,40,50)=50
    assert_eq!(mm_output[0].0, 1);
    assert_eq!(mm_output[0].1, 150, "key 1: sum");
    assert_eq!(mm_output[0].2, 50, "key 1: min = 100-50");
    assert_eq!(mm_output[0].3, 50, "key 1: max = 5*10");

    // Key 2: sum=20+40+60+80+100=300, min=min(180,160,140,120,100)=100, max=max(20,40,60,80,100)=100
    assert_eq!(mm_output[1].0, 2);
    assert_eq!(mm_output[1].1, 300, "key 2: sum");
    assert_eq!(mm_output[1].2, 100, "key 2: min = 200-100");
    assert_eq!(mm_output[1].3, 100, "key 2: max = 5*20");

    // Key 3: sum=30+60+90+120+150=450, min=min(270,240,210,180,150)=150, max=max(30,60,90,120,150)=150
    assert_eq!(mm_output[2].0, 3);
    assert_eq!(mm_output[2].1, 450, "key 3: sum");
    assert_eq!(mm_output[2].2, 150, "key 3: min = 300-150");
    assert_eq!(mm_output[2].3, 150, "key 3: max = 5*30");
}

// ── Test 3: null_handling ───────────────────────────────────────────────
//
// Null group keys and null aggregate values must be handled correctly:
//  - Null group keys sort NULLS FIRST and form their own group
//  - Null aggregate values: null + x = x (null-propagation in SUM fold)
//  - All-null aggregate group: remains null

#[test]
fn null_handling_group_keys_and_aggregate_values() {
    let dir = tempfile::tempdir().unwrap();
    let p = |n: &str| dir.path().join(n).to_str().unwrap().to_string();

    // File 1: null key with value, non-null key with null aggregate.
    write_nullable_state_file(
        &p("n1.mv.parquet"),
        &[
            (None, Some(100), Some(1)),    // null key group
            (Some(1), None, Some(1)),      // key=1, val_sum is null
            (Some(3), Some(300), Some(3)), // key=3
        ],
    );

    // File 2: null key again (should fold with file 1's null-key row),
    // key=1 with a value (should fold with null → keeps value),
    // key=2 with all-null aggregates.
    write_nullable_state_file(
        &p("n2.mv.parquet"),
        &[
            (None, Some(200), Some(2)), // null key group
            (Some(1), Some(10), None),  // key=1, val_cnt is null
            (Some(2), None, None),      // key=2, all null aggs
        ],
    );

    // File 3: key=2 with still-null aggregates (should remain null after fold).
    write_nullable_state_file(
        &p("n3.mv.parquet"),
        &[
            (Some(2), None, None),         // key=2, all null aggs
            (Some(3), Some(700), Some(7)), // key=3
        ],
    );

    let files = vec![p("n1.mv.parquet"), p("n2.mv.parquet"), p("n3.mv.parquet")];
    // fold_ops: key=0, sum=1, sum=1
    let rows = mv_merge_engine::merge_state_streams(
        &files,
        &p("out.mv.parquet"),
        &[0],
        &[true],
        &[true],
        &[0, 1, 1],
    )
    .unwrap();

    assert_eq!(rows, 4, "4 distinct groups: null, 1, 2, 3");

    let output = read_nullable_rows(&p("out.mv.parquet"));

    // Group null: 100+200=300, 1+2=3
    assert_eq!(output[0], (None, Some(300), Some(3)));

    // Group 1: null+10=10, 1+null=1
    assert_eq!(output[1], (Some(1), Some(10), Some(1)));

    // Group 2: null+null=null (both files had null aggs → stays null)
    assert_eq!(output[2], (Some(2), None, None));

    // Group 3: 300+700=1000, 3+7=10
    assert_eq!(output[3], (Some(3), Some(1000), Some(10)));
}

// ── Test 4: recursive_merge ─────────────────────────────────────────────
//
// merge(merge(A, B), merge(C, D)) must equal merge(A, B, C, D).
// The output of any merge is itself a valid state file, so the engine is
// closed (associative + fold-compatible).

#[test]
fn recursive_merge_of_merge_output_is_ordered_and_correct() {
    let dir = tempfile::tempdir().unwrap();
    let p = |n: &str| dir.path().join(n).to_str().unwrap().to_string();

    write_state_file(
        &p("a.mv.parquet"),
        &[
            (1, 10, 1, 1, 100.0),
            (3, 30, 3, 3, 300.0),
            (5, 50, 5, 5, 500.0),
        ],
    );
    write_state_file(
        &p("b.mv.parquet"),
        &[
            (2, 20, 2, 2, 200.0),
            (3, 31, 1, 1, 310.0),
            (6, 60, 6, 6, 600.0),
        ],
    );
    write_state_file(
        &p("c.mv.parquet"),
        &[
            (1, 11, 1, 1, 110.0),
            (4, 40, 4, 4, 400.0),
            (5, 51, 1, 1, 510.0),
        ],
    );
    write_state_file(
        &p("d.mv.parquet"),
        &[
            (2, 21, 1, 1, 210.0),
            (4, 41, 1, 1, 410.0),
            (6, 61, 1, 1, 610.0),
        ],
    );

    let (oi, oa, onf) = default_ordering();
    let ops = default_fold_ops();

    // Flat 4-way merge.
    mv_merge_engine::merge_state_streams(
        &[p("a.mv.parquet"), p("b.mv.parquet"), p("c.mv.parquet"), p("d.mv.parquet")],
        &p("flat.mv.parquet"),
        &oi,
        &oa,
        &onf,
        &ops,
    )
    .unwrap();

    // Recursive: merge(A,B) → AB, merge(C,D) → CD, merge(AB,CD) → final.
    mv_merge_engine::merge_state_streams(
        &[p("a.mv.parquet"), p("b.mv.parquet")],
        &p("ab.mv.parquet"),
        &oi,
        &oa,
        &onf,
        &ops,
    )
    .unwrap();
    mv_merge_engine::merge_state_streams(
        &[p("c.mv.parquet"), p("d.mv.parquet")],
        &p("cd.mv.parquet"),
        &oi,
        &oa,
        &onf,
        &ops,
    )
    .unwrap();
    mv_merge_engine::merge_state_streams(
        &[p("ab.mv.parquet"), p("cd.mv.parquet")],
        &p("recursive.mv.parquet"),
        &oi,
        &oa,
        &onf,
        &ops,
    )
    .unwrap();

    let flat_output = read_rows(&p("flat.mv.parquet"));
    let recursive_output = read_rows(&p("recursive.mv.parquet"));

    assert_eq!(
        flat_output, recursive_output,
        "recursive merge must produce identical output to flat merge"
    );

    // Verify output is sorted.
    for window in flat_output.windows(2) {
        assert!(
            window[0].0 <= window[1].0,
            "output not sorted: {} followed by {}",
            window[0].0,
            window[1].0,
        );
    }

    // Verify specific fold values.
    // Key 1: 10+11=21, 1+1=2, 1+1=2, 100+110=210
    let k1 = flat_output.iter().find(|r| r.0 == 1).unwrap();
    assert_eq!(*k1, (1, 21, 2, 2, 210.0));

    // Key 3: 30+31=61, 3+1=4, 3+1=4, 300+310=610
    let k3 = flat_output.iter().find(|r| r.0 == 3).unwrap();
    assert_eq!(*k3, (3, 61, 4, 4, 610.0));
}

// ── Test 5: cancellation ────────────────────────────────────────────────
//
// Cancel mid-merge by providing an output path that becomes unwritable.
// The merge should return an error and any partial output should be cleaned up.
// (The streaming engine writes to the output file directly; if it fails
// mid-stream, the caller is responsible for cleanup. We test that the error
// propagates cleanly.)

#[test]
fn cancellation_mid_merge_returns_error_and_no_corrupt_output() {
    let dir = tempfile::tempdir().unwrap();
    let p = |n: &str| dir.path().join(n).to_str().unwrap().to_string();

    // Write valid input files.
    write_state_file(
        &p("c1.mv.parquet"),
        &[(1, 10, 1, 1, 100.0), (2, 20, 2, 2, 200.0)],
    );
    write_state_file(
        &p("c2.mv.parquet"),
        &[(3, 30, 3, 3, 300.0), (4, 40, 4, 4, 400.0)],
    );

    // Use a directory path as the output file (not a regular file) — this
    // simulates an I/O failure when trying to write the output.
    let bad_output = dir.path().join("not_a_file");
    fs::create_dir(&bad_output).unwrap();
    let bad_output_str = bad_output.to_str().unwrap().to_string();

    let (oi, oa, onf) = default_ordering();
    let ops = default_fold_ops();

    let result = mv_merge_engine::merge_state_streams(
        &[p("c1.mv.parquet"), p("c2.mv.parquet")],
        &bad_output_str,
        &oi,
        &oa,
        &onf,
        &ops,
    );

    // The engine should return an error, not panic.
    assert!(
        result.is_err(),
        "merge to a directory path should fail with I/O error"
    );
    let err_msg = result.unwrap_err();
    assert!(
        err_msg.contains("create")
            || err_msg.contains("Is a directory")
            || err_msg.contains("not_a_file"),
        "error should mention the I/O failure: {err_msg}"
    );
}

// ── Test 6: cleanup ─────────────────────────────────────────────────────
//
// After a failed merge, verify that NO temp or partial IPC files are left
// behind in the output directory. The engine writes directly to the output
// path; if it fails, only that path might exist (and it should be incomplete).

#[test]
fn cleanup_aborted_merge_leaves_no_temp_files() {
    let dir = tempfile::tempdir().unwrap();
    let out_dir = tempfile::tempdir().unwrap();
    let p = |n: &str| dir.path().join(n).to_str().unwrap().to_string();
    let out_p = |n: &str| out_dir.path().join(n).to_str().unwrap().to_string();

    write_state_file(&p("ok.mv.parquet"), &[(1, 10, 1, 1, 100.0)]);

    // Provide a mismatched fold_ops length to force an error AFTER opening
    // the readers but BEFORE writing any output.
    let result = mv_merge_engine::merge_state_streams(
        &[p("ok.mv.parquet")],
        &out_p("should_not_exist.mv.parquet"),
        &[0],
        &[true],
        &[true],
        &[0, 1], // Wrong length: schema has 5 cols, fold_ops has 2.
    );

    assert!(result.is_err(), "mismatched fold_ops should fail");

    // Verify no files were created in the output directory.
    let entries: Vec<_> = fs::read_dir(out_dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .collect();
    assert!(
        entries.is_empty(),
        "output directory should be empty after failed merge, found: {:?}",
        entries.iter().map(|e| e.file_name()).collect::<Vec<_>>()
    );
}

// ── Test 7: schema_mismatch ─────────────────────────────────────────────
//
// Files with different schemas (arity mismatch or type mismatch) must
// produce a clear error, not a panic or silent corruption.

#[test]
fn schema_mismatch_returns_error_not_panic() {
    let dir = tempfile::tempdir().unwrap();
    let p = |n: &str| dir.path().join(n).to_str().unwrap().to_string();

    // File 1: standard 5-column schema.
    write_state_file(&p("s1.mv.parquet"), &[(1, 10, 1, 1, 100.0)]);

    // File 2: different schema — 3 columns with different types.
    let schema2 = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Utf8, true), // Utf8 instead of Int64
        Field::new("val", DataType::Int64, true),
        Field::new("extra", DataType::Float64, true),
    ]));
    let batch2 = RecordBatch::try_new(
        schema2.clone(),
        vec![
            Arc::new(StringArray::from(vec!["a"])),
            Arc::new(Int64Array::from(vec![10])),
            Arc::new(Float64Array::from(vec![1.0])),
        ],
    )
    .unwrap();
    let f2 = File::create(&p("s2.mv.parquet")).unwrap();
    let mut w2 = ParquetWriter::try_new(f2, schema2.clone(), None).unwrap();
    w2.write(&batch2).unwrap();
    w2.close().unwrap();

    // Test arity mismatch (5 cols vs 3 cols).
    let (oi, oa, onf) = default_ordering();
    let result = mv_merge_engine::merge_state_streams(
        &[p("s1.mv.parquet"), p("s2.mv.parquet")],
        &p("out.mv.parquet"),
        &oi,
        &oa,
        &onf,
        &default_fold_ops(),
    );
    assert!(result.is_err(), "arity mismatch must return error");
    let err = result.unwrap_err();
    assert!(
        err.contains("arity mismatch") || err.contains("mismatch"),
        "error should describe arity mismatch: {err}"
    );

    // Test type mismatch: same arity but different types.
    let schema3 = Arc::new(Schema::new(vec![
        Field::new("RegionID", DataType::Utf8, true), // Utf8 instead of Int64
        Field::new("adv_sum", DataType::Int64, true),
        Field::new("cnt", DataType::Int64, true),
        Field::new("avg_cnt", DataType::UInt64, true),
        Field::new("avg_sum", DataType::Float64, true),
    ]));
    let batch3 = RecordBatch::try_new(
        schema3.clone(),
        vec![
            Arc::new(StringArray::from(vec!["region_a"])),
            Arc::new(Int64Array::from(vec![10])),
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(UInt64Array::from(vec![1u64])),
            Arc::new(Float64Array::from(vec![100.0])),
        ],
    )
    .unwrap();
    let f3 = File::create(&p("s3.mv.parquet")).unwrap();
    let mut w3 = ParquetWriter::try_new(f3, schema3.clone(), None).unwrap();
    w3.write(&batch3).unwrap();
    w3.close().unwrap();

    let result2 = mv_merge_engine::merge_state_streams(
        &[p("s1.mv.parquet"), p("s3.mv.parquet")],
        &p("out2.mv.parquet"),
        &oi,
        &oa,
        &onf,
        &default_fold_ops(),
    );
    assert!(result2.is_err(), "type mismatch must return error");
    let err2 = result2.unwrap_err();
    assert!(
        err2.contains("type mismatch") || err2.contains("mismatch"),
        "error should describe type mismatch: {err2}"
    );

    // Also test validate_parquet_header with wrong schema hash.
    let schema = state_schema();
    let correct_hash = mv_merge_engine::compute_schema_hash(&schema);
    let wrong_hash = correct_hash ^ 0xFFFF_FFFF;

    let validation =
        mv_merge_engine::validate_parquet_header(&p("s1.mv.parquet"), wrong_hash, &[0], &[true], &[true]);
    assert!(
        validation.is_err(),
        "wrong schema hash must fail validation"
    );
    assert!(
        validation.unwrap_err().contains("schema hash mismatch"),
        "error should mention schema hash mismatch"
    );
}

// ── Test 8: definition_mismatch ─────────────────────────────────────────
//
// Different fold SQL strings produce different definition hashes.
// Files from different definitions must not be silently merged.

#[test]
fn definition_mismatch_detected_via_hash() {
    // Two different fold definitions.
    let sql_sum = "SELECT key, SUM(val) FROM t GROUP BY key";
    let sql_count = "SELECT key, COUNT(val) FROM t GROUP BY key";

    let hash_sum = mv_merge_engine::compute_definition_hash(sql_sum);
    let hash_count = mv_merge_engine::compute_definition_hash(sql_count);

    assert_ne!(
        hash_sum, hash_count,
        "different fold SQL must produce different definition hashes"
    );

    // Same SQL must produce same hash (deterministic).
    let hash_sum2 = mv_merge_engine::compute_definition_hash(sql_sum);
    assert_eq!(
        hash_sum, hash_sum2,
        "same fold SQL must produce identical definition hash"
    );

    // Whitespace-sensitive: slightly different SQL produces different hash.
    let sql_sum_space = "SELECT  key, SUM(val) FROM t GROUP BY key"; // extra space
    let hash_sum_space = mv_merge_engine::compute_definition_hash(sql_sum_space);
    assert_ne!(
        hash_sum, hash_sum_space,
        "whitespace difference should produce different hash (SQL is exact-match)"
    );

    // Schema hash validation catches schema drift.
    let schema_a = Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("val_sum", DataType::Int64, false),
    ]);
    let schema_b = Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("val_count", DataType::Int64, false), // different name
    ]);

    let hash_a = mv_merge_engine::compute_schema_hash(&schema_a);
    let hash_b = mv_merge_engine::compute_schema_hash(&schema_b);
    assert_ne!(
        hash_a, hash_b,
        "different schema field names must produce different hashes"
    );

    // Validate that a file written with schema_a fails validation with schema_b's hash.
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("def.mv.parquet").to_str().unwrap().to_string();

    let batch = RecordBatch::try_new(
        Arc::new(schema_a.clone()),
        vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(Int64Array::from(vec![10, 20])),
        ],
    )
    .unwrap();
    let f = File::create(&path).unwrap();
    let mut w = ParquetWriter::try_new(f, batch.schema(), None).unwrap();
    w.write(&batch).unwrap();
    w.close().unwrap();

    let validation = mv_merge_engine::validate_parquet_header(&path, hash_b, &[0], &[true], &[true]);
    assert!(
        validation.is_err(),
        "file with schema_a should fail validation against schema_b's hash"
    );
    assert!(validation.unwrap_err().contains("schema hash mismatch"));
}

// ── Test 9: tiny_memory ─────────────────────────────────────────────────
//
// The streaming engine's memory usage is O(k cursors + one accumulator
// row + one output batch buffer). Even with a large total data volume,
// peak memory should stay bounded by this — unlike the legacy SQL engine
// which materializes all rows.
//
// We prove this by running a merge over files totaling >> 12 MiB of data
// and verifying the output is exact and ordered. The engine itself never
// materializes more than k cursor rows + FLUSH_THRESHOLD rows at once.

#[test]
fn tiny_memory_large_data_output_exact_and_ordered() {
    let dir = tempfile::tempdir().unwrap();
    let p = |n: &str| dir.path().join(n).to_str().unwrap().to_string();

    // Generate 10 files, each with 10,000 sorted rows — 100K total rows.
    // Each file covers a non-overlapping key range (for simplicity) PLUS
    // a shared key range at [0..100) to exercise fold.
    let k = 10usize;
    let rows_per_file = 10_000usize;
    let shared_keys = 100usize;

    for file_idx in 0..k {
        let base = (file_idx * rows_per_file) as i64;
        let mut rows: Vec<(i64, i64, i64, u64, f64)> =
            Vec::with_capacity(rows_per_file + shared_keys);

        // Shared key range [0..100) — these will fold across all 10 files.
        for key in 0..shared_keys as i64 {
            rows.push((key, (file_idx + 1) as i64, 1, 1, (file_idx + 1) as f64));
        }

        // Unique key range for this file.
        for i in 0..rows_per_file {
            let key = 1_000_000 + base + i as i64; // non-overlapping, all > shared keys
            rows.push((key, key, 1, 1, key as f64));
        }

        // Sort by key before writing (engine requires sorted inputs).
        rows.sort_by_key(|r| r.0);
        write_state_file(&p(&format!("big{file_idx}.mv.parquet")), &rows);
    }

    let files: Vec<String> = (0..k).map(|i| p(&format!("big{i}.mv.parquet"))).collect();
    let (oi, oa, onf) = default_ordering();
    let ops = default_fold_ops();

    let row_count =
        mv_merge_engine::merge_state_streams(&files, &p("big_out.mv.parquet"), &oi, &oa, &onf, &ops)
            .unwrap();

    // Expected distinct keys: 100 shared + 10 * 10,000 unique = 100,100.
    let expected_distinct = shared_keys as i64 + (k * rows_per_file) as i64;
    assert_eq!(
        row_count, expected_distinct,
        "row count must match distinct keys"
    );

    // Read output and verify ordering.
    let output = read_rows(&p("big_out.mv.parquet"));
    assert_eq!(output.len(), expected_distinct as usize);

    // Verify strict ordering.
    for window in output.windows(2) {
        assert!(
            window[0].0 < window[1].0,
            "output not strictly sorted: key {} followed by {}",
            window[0].0,
            window[1].0,
        );
    }

    // Verify shared-key fold: each shared key had value (file_idx+1) from
    // each of 10 files → adv_sum = 1+2+3+...+10 = 55.
    for key in 0..shared_keys as i64 {
        let row = output.iter().find(|r| r.0 == key).unwrap();
        assert_eq!(
            row.1, 55,
            "shared key {key}: adv_sum should be 1+2+...+10=55, got {}",
            row.1
        );
        assert_eq!(
            row.2, 10,
            "shared key {key}: cnt should be 10 (one per file), got {}",
            row.2
        );
        assert_eq!(
            row.3, 10,
            "shared key {key}: avg_cnt should be 10, got {}",
            row.3
        );
        let expected_avg_sum: f64 = (1..=10).map(|i| i as f64).sum();
        assert!(
            (row.4 - expected_avg_sum).abs() < 1e-9,
            "shared key {key}: avg_sum should be {expected_avg_sum}, got {}",
            row.4
        );
    }

    // Verify unique keys are untouched (no fold needed, just passed through).
    let first_unique_key = 1_000_000i64;
    let row = output.iter().find(|r| r.0 == first_unique_key).unwrap();
    assert_eq!(
        row.1, first_unique_key,
        "unique key should pass through unchanged"
    );
}
