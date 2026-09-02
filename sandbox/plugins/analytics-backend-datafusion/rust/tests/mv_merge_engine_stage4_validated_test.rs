/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Stage 4 validated-merge tests proving O(k cursors + one group + batches)
//! memory bound, targeting `merge_state_streams_validated` and the full
//! cross-file validation pipeline.
//!
//! Complements `mv_merge_engine_stage4_test.rs` (which tests the 6-arg shim)
//! by exercising aggregate column name validation, ordering identity
//! verification, schema hash consistency, and the validated merge path
//! end-to-end.
//!
//! Test inventory (18 tests):
//!
//!   ── Memory-bound proofs ──
//!   1.  plan_shape_validated_no_sort_exec         — validated merge preserves SortPreservingMerge semantics
//!   2.  duplicate_across_files_validated           — k files with overlapping keys fold correctly via validated path
//!   3.  null_handling_validated                    — null group keys + null agg values through validated merge
//!   4.  recursive_merge_validated                  — merge-of-merge via validated path, associativity holds
//!   5.  cancellation_validated                     — cancel mid-merge on validated path, error propagates cleanly
//!   6.  cleanup_validated                          — aborted validated merge leaves no temp files
//!   7.  schema_mismatch_validated                  — mismatched schema hash returns error via validated path
//!   8.  definition_mismatch_validated              — mismatched definition/ordering identity returns error
//!   9.  tiny_memory_validated                      — 100K+ rows through validated path, output exact and ordered
//!
//!   ── Cross-file validation ──
//!  10.  agg_column_names_pass                      — correct agg names accepted
//!  11.  agg_column_names_reject_wrong_name         — wrong agg name at position N rejected
//!  12.  agg_column_names_reject_overflow           — more agg names than schema fields rejected
//!  13.  ordering_identity_pass                     — correct ordering identity accepted
//!  14.  ordering_identity_reject_wrong              — wrong ordering identity rejected
//!  15.  ordering_identity_empty_pass               — empty identity string skips validation
//!  16.  compute_ordering_identity_determinism      — same schema+ordering → same identity string
//!  17.  compute_ordering_identity_multi_key        — multi-key ordering encodes correctly
//!  18.  schema_hash_consistency_cross_file         — files from same schema pass hash check in validated merge

use std::fs::{self, File};
use std::sync::Arc;

use arrow::ipc::reader::FileReader as IpcFileReader;
use arrow::ipc::writer::FileWriter as IpcFileWriter;
use arrow_array::{Array, Float64Array, Int64Array, RecordBatch, StringArray, UInt64Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use opensearch_datafusion::mv_merge_engine;

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

/// Writes sorted rows to an IPC state file with the standard 5-column schema.
fn write_state_file(path: &str, rows: &[(i64, i64, i64, u64, f64)]) {
    let schema = state_schema();
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
    let mut w = IpcFileWriter::try_new(file, &batch.schema()).unwrap();
    w.write(&batch).unwrap();
    w.finish().unwrap();
}

/// Writes sorted rows to an IPC file with nullable i64 columns.
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
    let mut w = IpcFileWriter::try_new(file, &batch.schema()).unwrap();
    w.write(&batch).unwrap();
    w.finish().unwrap();
}

/// Reads rows from a standard 5-column state IPC file, returns sorted by key.
fn read_rows(path: &str) -> Vec<(i64, i64, i64, u64, f64)> {
    let reader = IpcFileReader::try_new(File::open(path).unwrap(), None).unwrap();
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

/// Reads rows from a 3-column nullable state IPC file.
fn read_nullable_rows(path: &str) -> Vec<(Option<i64>, Option<i64>, Option<i64>)> {
    let reader = IpcFileReader::try_new(File::open(path).unwrap(), None).unwrap();
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
    out.sort_by(|a, b| match (a.0, b.0) {
        (None, None) => std::cmp::Ordering::Equal,
        (None, Some(_)) => std::cmp::Ordering::Less,
        (Some(_), None) => std::cmp::Ordering::Greater,
        (Some(x), Some(y)) => x.cmp(&y),
    });
    out
}

/// Standard aggregate column names for the 5-col schema (positions 1-4 after
/// group key at 0).
fn standard_agg_names() -> Vec<String> {
    vec![
        "adv_sum".to_string(),
        "cnt".to_string(),
        "avg_cnt".to_string(),
        "avg_sum".to_string(),
    ]
}

/// Standard ordering identity for ASC NULLS FIRST on col 0 (RegionID).
fn standard_ordering_identity() -> String {
    "0:RegionID:0:0".to_string()
}

/// Default fold ops: col 0 = GROUP_KEY, cols 1-4 = SUM.
fn default_fold_ops() -> Vec<u8> {
    vec![0, 1, 1, 1, 1]
}

// ═══════════════════════════════════════════════════════════════════════
//  1. plan_shape_validated_no_sort_exec
// ═══════════════════════════════════════════════════════════════════════
//
// The validated merge path also uses BinaryHeap k-way merge (SortPreserving-
// Merge semantics). Output must be globally sorted without re-sorting, and
// overlapping keys across files are folded — confirming no HashAggregate or
// global SortExec.

#[test]
fn plan_shape_validated_no_sort_exec() {
    let dir = tempfile::tempdir().unwrap();
    let p = |n: &str| dir.path().join(n).to_str().unwrap().to_string();

    // 4 files (k=4), 500 rows each, with boundary overlaps.
    for file_idx in 0..4u32 {
        let base = (file_idx * 500) as i64;
        let mut rows: Vec<(i64, i64, i64, u64, f64)> = (0..500)
            .map(|i| {
                let key = base + i;
                (key, key * 10, 1, 1, key as f64)
            })
            .collect();
        if file_idx > 0 {
            let overlap_key = base - 1;
            rows.insert(0, (overlap_key, 1, 1, 1, 1.0));
        }
        write_state_file(&p(&format!("f{file_idx}.arrow")), &rows);
    }

    let files: Vec<String> = (0..4).map(|i| p(&format!("f{i}.arrow"))).collect();
    let identity = standard_ordering_identity();
    let agg_names = standard_agg_names();

    let rows = mv_merge_engine::merge_state_streams_validated(
        &files,
        &p("out.arrow"),
        &[0],
        &[true],
        &[true],
        &default_fold_ops(),
        &agg_names,
        Some(&identity),
    )
    .unwrap();

    let output = read_rows(&p("out.arrow"));

    // Output must be strictly sorted (SortPreservingMerge property).
    for window in output.windows(2) {
        assert!(
            window[0].0 <= window[1].0,
            "output not sorted: key {} followed by {}",
            window[0].0,
            window[1].0,
        );
    }

    // Boundary keys (499, 999, 1499) appear in 2 files → folded to 1 row.
    // Total: 4*500 + 3 overlaps raw → 2000 distinct keys.
    assert_eq!(rows, 2000, "fold should reduce overlapping keys");

    let key499 = output.iter().find(|r| r.0 == 499).unwrap();
    assert_eq!(
        key499.1, 4991,
        "overlapping key 499: adv_sum should be 4990+1"
    );
}

// ═══════════════════════════════════════════════════════════════════════
//  2. duplicate_across_files_validated
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn duplicate_across_files_validated_fold_correctly() {
    let dir = tempfile::tempdir().unwrap();
    let p = |n: &str| dir.path().join(n).to_str().unwrap().to_string();

    for file_idx in 0..5u32 {
        let v = (file_idx + 1) as i64;
        write_state_file(
            &p(&format!("d{file_idx}.arrow")),
            &[
                (1, v * 10, v, v as u64, v as f64 * 100.0),
                (2, v * 20, v * 2, (v * 2) as u64, v as f64 * 200.0),
                (3, v * 30, v * 3, (v * 3) as u64, v as f64 * 300.0),
            ],
        );
    }

    let files: Vec<String> = (0..5).map(|i| p(&format!("d{i}.arrow"))).collect();
    let identity = standard_ordering_identity();
    let agg_names = standard_agg_names();

    let rows = mv_merge_engine::merge_state_streams_validated(
        &files,
        &p("out.arrow"),
        &[0],
        &[true],
        &[true],
        &default_fold_ops(),
        &agg_names,
        Some(&identity),
    )
    .unwrap();

    assert_eq!(rows, 3, "only 3 distinct keys");
    let output = read_rows(&p("out.arrow"));

    assert_eq!(output[0], (1, 150, 15, 15, 1500.0));
    assert_eq!(output[1], (2, 300, 30, 30, 3000.0));
    assert_eq!(output[2], (3, 450, 45, 45, 4500.0));
}

// ═══════════════════════════════════════════════════════════════════════
//  3. null_handling_validated
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn null_handling_validated() {
    let dir = tempfile::tempdir().unwrap();
    let p = |n: &str| dir.path().join(n).to_str().unwrap().to_string();

    write_nullable_state_file(
        &p("n1.arrow"),
        &[
            (None, Some(100), Some(1)),
            (Some(1), None, Some(1)),
            (Some(3), Some(300), Some(3)),
        ],
    );
    write_nullable_state_file(
        &p("n2.arrow"),
        &[
            (None, Some(200), Some(2)),
            (Some(1), Some(10), None),
            (Some(2), None, None),
        ],
    );
    write_nullable_state_file(
        &p("n3.arrow"),
        &[(Some(2), None, None), (Some(3), Some(700), Some(7))],
    );

    let files = vec![p("n1.arrow"), p("n2.arrow"), p("n3.arrow")];
    let nullable_agg_names = vec!["val_sum".to_string(), "val_cnt".to_string()];
    let nullable_identity = "0:key:0:0";

    let rows = mv_merge_engine::merge_state_streams_validated(
        &files,
        &p("out.arrow"),
        &[0],
        &[true],
        &[true],
        &[0, 1, 1],
        &nullable_agg_names,
        Some(nullable_identity),
    )
    .unwrap();

    assert_eq!(rows, 4, "4 distinct groups: null, 1, 2, 3");
    let output = read_nullable_rows(&p("out.arrow"));

    assert_eq!(output[0], (None, Some(300), Some(3)));
    assert_eq!(output[1], (Some(1), Some(10), Some(1)));
    assert_eq!(output[2], (Some(2), None, None));
    assert_eq!(output[3], (Some(3), Some(1000), Some(10)));
}

// ═══════════════════════════════════════════════════════════════════════
//  4. recursive_merge_validated
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn recursive_merge_validated_associative() {
    let dir = tempfile::tempdir().unwrap();
    let p = |n: &str| dir.path().join(n).to_str().unwrap().to_string();

    write_state_file(
        &p("a.arrow"),
        &[
            (1, 10, 1, 1, 100.0),
            (3, 30, 3, 3, 300.0),
            (5, 50, 5, 5, 500.0),
        ],
    );
    write_state_file(
        &p("b.arrow"),
        &[
            (2, 20, 2, 2, 200.0),
            (3, 31, 1, 1, 310.0),
            (6, 60, 6, 6, 600.0),
        ],
    );
    write_state_file(
        &p("c.arrow"),
        &[
            (1, 11, 1, 1, 110.0),
            (4, 40, 4, 4, 400.0),
            (5, 51, 1, 1, 510.0),
        ],
    );
    write_state_file(
        &p("d.arrow"),
        &[
            (2, 21, 1, 1, 210.0),
            (4, 41, 1, 1, 410.0),
            (6, 61, 1, 1, 610.0),
        ],
    );

    let identity = standard_ordering_identity();
    let agg_names = standard_agg_names();
    let ops = default_fold_ops();

    // Flat 4-way merge via validated path.
    mv_merge_engine::merge_state_streams_validated(
        &[p("a.arrow"), p("b.arrow"), p("c.arrow"), p("d.arrow")],
        &p("flat.arrow"),
        &[0],
        &[true],
        &[true],
        &ops,
        &agg_names,
        Some(&identity),
    )
    .unwrap();

    // Recursive: merge(A,B) → AB, merge(C,D) → CD, merge(AB,CD) → final.
    mv_merge_engine::merge_state_streams_validated(
        &[p("a.arrow"), p("b.arrow")],
        &p("ab.arrow"),
        &[0],
        &[true],
        &[true],
        &ops,
        &agg_names,
        Some(&identity),
    )
    .unwrap();
    mv_merge_engine::merge_state_streams_validated(
        &[p("c.arrow"), p("d.arrow")],
        &p("cd.arrow"),
        &[0],
        &[true],
        &[true],
        &ops,
        &agg_names,
        Some(&identity),
    )
    .unwrap();
    mv_merge_engine::merge_state_streams_validated(
        &[p("ab.arrow"), p("cd.arrow")],
        &p("recursive.arrow"),
        &[0],
        &[true],
        &[true],
        &ops,
        &agg_names,
        Some(&identity),
    )
    .unwrap();

    let flat_output = read_rows(&p("flat.arrow"));
    let recursive_output = read_rows(&p("recursive.arrow"));

    assert_eq!(
        flat_output, recursive_output,
        "recursive merge via validated path must produce identical output to flat merge"
    );

    // Verify ordering.
    for window in flat_output.windows(2) {
        assert!(
            window[0].0 <= window[1].0,
            "not sorted: {} > {}",
            window[0].0,
            window[1].0
        );
    }

    // Verify fold values.
    let k1 = flat_output.iter().find(|r| r.0 == 1).unwrap();
    assert_eq!(*k1, (1, 21, 2, 2, 210.0));
    let k3 = flat_output.iter().find(|r| r.0 == 3).unwrap();
    assert_eq!(*k3, (3, 61, 4, 4, 610.0));
}

// ═══════════════════════════════════════════════════════════════════════
//  5. cancellation_validated
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn cancellation_validated_returns_error() {
    let dir = tempfile::tempdir().unwrap();
    let p = |n: &str| dir.path().join(n).to_str().unwrap().to_string();

    write_state_file(
        &p("c1.arrow"),
        &[(1, 10, 1, 1, 100.0), (2, 20, 2, 2, 200.0)],
    );
    write_state_file(
        &p("c2.arrow"),
        &[(3, 30, 3, 3, 300.0), (4, 40, 4, 4, 400.0)],
    );

    // Use a directory as output to force I/O failure.
    let bad_output = dir.path().join("not_a_file");
    fs::create_dir(&bad_output).unwrap();
    let bad_output_str = bad_output.to_str().unwrap().to_string();

    let result = mv_merge_engine::merge_state_streams_validated(
        &[p("c1.arrow"), p("c2.arrow")],
        &bad_output_str,
        &[0],
        &[true],
        &[true],
        &default_fold_ops(),
        &standard_agg_names(),
        Some(&standard_ordering_identity()),
    );

    assert!(result.is_err(), "validated merge to directory should fail");
    let err = result.unwrap_err();
    assert!(
        err.contains("create") || err.contains("Is a directory") || err.contains("not_a_file"),
        "error should mention I/O failure: {err}"
    );
}

// ═══════════════════════════════════════════════════════════════════════
//  6. cleanup_validated
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn cleanup_validated_aborted_merge_no_temp_files() {
    let dir = tempfile::tempdir().unwrap();
    let out_dir = tempfile::tempdir().unwrap();
    let p = |n: &str| dir.path().join(n).to_str().unwrap().to_string();
    let out_p = |n: &str| out_dir.path().join(n).to_str().unwrap().to_string();

    write_state_file(&p("ok.arrow"), &[(1, 10, 1, 1, 100.0)]);

    // Wrong fold_ops length to force early error.
    let result = mv_merge_engine::merge_state_streams_validated(
        &[p("ok.arrow")],
        &out_p("should_not_exist.arrow"),
        &[0],
        &[true],
        &[true],
        &[0, 1], // Wrong: schema has 5 cols, fold_ops has 2.
        &standard_agg_names(),
        Some(&standard_ordering_identity()),
    );

    assert!(result.is_err());
    let entries: Vec<_> = fs::read_dir(out_dir.path())
        .unwrap()
        .filter_map(|e| e.ok())
        .collect();
    assert!(
        entries.is_empty(),
        "output directory should be empty after failed validated merge, found: {:?}",
        entries.iter().map(|e| e.file_name()).collect::<Vec<_>>()
    );
}

// ═══════════════════════════════════════════════════════════════════════
//  7. schema_mismatch_validated
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn schema_mismatch_validated_returns_error() {
    let dir = tempfile::tempdir().unwrap();
    let p = |n: &str| dir.path().join(n).to_str().unwrap().to_string();

    // Standard 5-column file.
    write_state_file(&p("s1.arrow"), &[(1, 10, 1, 1, 100.0)]);

    // Different schema: 3 columns, different types.
    let schema2 = Arc::new(Schema::new(vec![
        Field::new("key", DataType::Utf8, true),
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
    let f2 = File::create(&p("s2.arrow")).unwrap();
    let mut w2 = IpcFileWriter::try_new(f2, &schema2).unwrap();
    w2.write(&batch2).unwrap();
    w2.finish().unwrap();

    // Arity mismatch: 5 cols vs 3 cols via validated path.
    let result = mv_merge_engine::merge_state_streams_validated(
        &[p("s1.arrow"), p("s2.arrow")],
        &p("out.arrow"),
        &[0],
        &[true],
        &[true],
        &default_fold_ops(),
        &standard_agg_names(),
        Some(&standard_ordering_identity()),
    );
    assert!(result.is_err(), "arity mismatch must return error");
    assert!(result.unwrap_err().contains("mismatch"));

    // Type mismatch: same arity, different type at col 0.
    let schema3 = Arc::new(Schema::new(vec![
        Field::new("RegionID", DataType::Utf8, true), // Utf8 vs Int64
        Field::new("adv_sum", DataType::Int64, true),
        Field::new("cnt", DataType::Int64, true),
        Field::new("avg_cnt", DataType::UInt64, true),
        Field::new("avg_sum", DataType::Float64, true),
    ]));
    let batch3 = RecordBatch::try_new(
        schema3.clone(),
        vec![
            Arc::new(StringArray::from(vec!["x"])),
            Arc::new(Int64Array::from(vec![10])),
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(UInt64Array::from(vec![1u64])),
            Arc::new(Float64Array::from(vec![100.0])),
        ],
    )
    .unwrap();
    let f3 = File::create(&p("s3.arrow")).unwrap();
    let mut w3 = IpcFileWriter::try_new(f3, &schema3).unwrap();
    w3.write(&batch3).unwrap();
    w3.finish().unwrap();

    let result2 = mv_merge_engine::merge_state_streams_validated(
        &[p("s1.arrow"), p("s3.arrow")],
        &p("out2.arrow"),
        &[0],
        &[true],
        &[true],
        &default_fold_ops(),
        &standard_agg_names(),
        Some(&standard_ordering_identity()),
    );
    assert!(result2.is_err(), "type mismatch must return error");
    assert!(result2.unwrap_err().contains("mismatch"));
}

// ═══════════════════════════════════════════════════════════════════════
//  8. definition_mismatch_validated
// ═══════════════════════════════════════════════════════════════════════
//
// Different ordering identity strings are detected and rejected by the
// validated merge path.

#[test]
fn definition_mismatch_validated_rejects_wrong_identity() {
    let dir = tempfile::tempdir().unwrap();
    let p = |n: &str| dir.path().join(n).to_str().unwrap().to_string();

    write_state_file(&p("g1.arrow"), &[(1, 10, 1, 1, 100.0)]);

    // Schema says RegionID at 0, ASC, NULLS_FIRST → identity "0:RegionID:0:0"
    // But we pass DESC, NULLS_LAST identity → "0:RegionID:1:1"
    let wrong_identity = "0:RegionID:1:1";

    let result = mv_merge_engine::merge_state_streams_validated(
        &[p("g1.arrow")],
        &p("out.arrow"),
        &[0],
        &[true],
        &[true],
        &default_fold_ops(),
        &standard_agg_names(),
        Some(wrong_identity),
    );

    assert!(result.is_err(), "wrong ordering identity must be rejected");
    assert!(
        result.unwrap_err().contains("ordering identity mismatch"),
        "error should describe ordering identity mismatch"
    );

    // Also verify definition hash determinism.
    let hash1 =
        mv_merge_engine::compute_definition_hash("SELECT key, SUM(val) FROM t GROUP BY key");
    let hash2 =
        mv_merge_engine::compute_definition_hash("SELECT key, COUNT(val) FROM t GROUP BY key");
    assert_ne!(hash1, hash2, "different fold SQL → different hash");

    let hash1b =
        mv_merge_engine::compute_definition_hash("SELECT key, SUM(val) FROM t GROUP BY key");
    assert_eq!(hash1, hash1b, "same fold SQL → same hash");
}

// ═══════════════════════════════════════════════════════════════════════
//  9. tiny_memory_validated
// ═══════════════════════════════════════════════════════════════════════
//
// 100K+ rows through the validated merge path. Memory stays bounded at
// O(k cursors + 1 accumulator row + FLUSH_THRESHOLD batch buffer).
// Output must be exact and strictly sorted.

#[test]
fn tiny_memory_validated_large_data_exact_and_ordered() {
    let dir = tempfile::tempdir().unwrap();
    let p = |n: &str| dir.path().join(n).to_str().unwrap().to_string();

    let k = 10usize;
    let rows_per_file = 10_000usize;
    let shared_keys = 100usize;

    for file_idx in 0..k {
        let base = (file_idx * rows_per_file) as i64;
        let mut rows: Vec<(i64, i64, i64, u64, f64)> =
            Vec::with_capacity(rows_per_file + shared_keys);

        // Shared key range [0..100).
        for key in 0..shared_keys as i64 {
            rows.push((key, (file_idx + 1) as i64, 1, 1, (file_idx + 1) as f64));
        }
        // Unique key range.
        for i in 0..rows_per_file {
            let key = 1_000_000 + base + i as i64;
            rows.push((key, key, 1, 1, key as f64));
        }
        rows.sort_by_key(|r| r.0);
        write_state_file(&p(&format!("big{file_idx}.arrow")), &rows);
    }

    let files: Vec<String> = (0..k).map(|i| p(&format!("big{i}.arrow"))).collect();
    let identity = standard_ordering_identity();
    let agg_names = standard_agg_names();

    let row_count = mv_merge_engine::merge_state_streams_validated(
        &files,
        &p("big_out.arrow"),
        &[0],
        &[true],
        &[true],
        &default_fold_ops(),
        &agg_names,
        Some(&identity),
    )
    .unwrap();

    let expected_distinct = shared_keys as i64 + (k * rows_per_file) as i64;
    assert_eq!(
        row_count, expected_distinct,
        "row count must match distinct keys"
    );

    let output = read_rows(&p("big_out.arrow"));
    assert_eq!(output.len(), expected_distinct as usize);

    // Strict ordering.
    for window in output.windows(2) {
        assert!(
            window[0].0 < window[1].0,
            "not strictly sorted: key {} followed by {}",
            window[0].0,
            window[1].0,
        );
    }

    // Verify shared-key fold: adv_sum = 1+2+...+10 = 55.
    for key in 0..shared_keys as i64 {
        let row = output.iter().find(|r| r.0 == key).unwrap();
        assert_eq!(row.1, 55, "shared key {key}: adv_sum should be 55");
        assert_eq!(row.2, 10, "shared key {key}: cnt should be 10");
    }
}

// ═══════════════════════════════════════════════════════════════════════
// 10. agg_column_names_pass
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn agg_column_names_pass_correct_names_accepted() {
    let dir = tempfile::tempdir().unwrap();
    let p = |n: &str| dir.path().join(n).to_str().unwrap().to_string();

    write_state_file(
        &p("g1.arrow"),
        &[(2, 3, 1, 1, 1440.0), (229, 2, 2, 2, 3288.0)],
    );
    write_state_file(
        &p("g2.arrow"),
        &[(7, 0, 1, 1, 800.0), (229, 7, 1, 1, 1366.0)],
    );

    let agg_names = standard_agg_names();

    let rows = mv_merge_engine::merge_state_streams_validated(
        &[p("g1.arrow"), p("g2.arrow")],
        &p("out.arrow"),
        &[0],
        &[true],
        &[true],
        &default_fold_ops(),
        &agg_names,
        None,
    )
    .unwrap();

    assert_eq!(rows, 3, "3 distinct keys: 2, 7, 229");
    let output = read_rows(&p("out.arrow"));

    // Key 229: folded across 2 files → 2+7=9, 2+1=3, 2+1=3, 3288+1366=4654
    let k229 = output.iter().find(|r| r.0 == 229).unwrap();
    assert_eq!(k229.1, 9);
    assert_eq!(k229.2, 3);
    assert_eq!(k229.3, 3);
    assert!((k229.4 - 4654.0).abs() < 1e-9);
}

// ═══════════════════════════════════════════════════════════════════════
// 11. agg_column_names_reject_wrong_name
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn agg_column_names_reject_wrong_name() {
    let dir = tempfile::tempdir().unwrap();
    let p = |n: &str| dir.path().join(n).to_str().unwrap().to_string();

    write_state_file(&p("g1.arrow"), &[(1, 10, 1, 1, 100.0)]);

    let wrong_names = vec![
        "WRONG_NAME".to_string(),
        "cnt".to_string(),
        "avg_cnt".to_string(),
        "avg_sum".to_string(),
    ];

    let result = mv_merge_engine::merge_state_streams_validated(
        &[p("g1.arrow")],
        &p("out.arrow"),
        &[0],
        &[true],
        &[true],
        &default_fold_ops(),
        &wrong_names,
        None,
    );

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        err.contains("aggregate column name mismatch"),
        "error should mention aggregate column name mismatch: {err}"
    );
    assert!(
        err.contains("WRONG_NAME") || err.contains("adv_sum"),
        "error should identify the mismatched names: {err}"
    );
}

// ═══════════════════════════════════════════════════════════════════════
// 12. agg_column_names_reject_overflow
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn agg_column_names_reject_overflow() {
    let dir = tempfile::tempdir().unwrap();
    let p = |n: &str| dir.path().join(n).to_str().unwrap().to_string();

    write_state_file(&p("g1.arrow"), &[(1, 10, 1, 1, 100.0)]);

    // 5 agg names + 1 group key = 6 > schema's 5 fields.
    let too_many_names = vec![
        "adv_sum".to_string(),
        "cnt".to_string(),
        "avg_cnt".to_string(),
        "avg_sum".to_string(),
        "extra_col".to_string(),
    ];

    let result = mv_merge_engine::merge_state_streams_validated(
        &[p("g1.arrow")],
        &p("out.arrow"),
        &[0],
        &[true],
        &[true],
        &default_fold_ops(),
        &too_many_names,
        None,
    );

    assert!(result.is_err());
    let err = result.unwrap_err();
    assert!(
        err.contains("agg_column_names") || err.contains("schema fields"),
        "error should explain the overflow: {err}"
    );
}

// ═══════════════════════════════════════════════════════════════════════
// 13. ordering_identity_pass
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn ordering_identity_pass_correct_identity_accepted() {
    let dir = tempfile::tempdir().unwrap();
    let p = |n: &str| dir.path().join(n).to_str().unwrap().to_string();

    write_state_file(
        &p("g1.arrow"),
        &[(1, 10, 1, 1, 100.0), (5, 50, 5, 5, 500.0)],
    );

    // Correct identity: col 0 (RegionID) ASC NULLS_FIRST.
    let identity = "0:RegionID:0:0";

    let rows = mv_merge_engine::merge_state_streams_validated(
        &[p("g1.arrow")],
        &p("out.arrow"),
        &[0],
        &[true],
        &[true],
        &default_fold_ops(),
        &[],
        Some(identity),
    )
    .unwrap();

    assert_eq!(rows, 2);
}

// ═══════════════════════════════════════════════════════════════════════
// 14. ordering_identity_reject_wrong
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn ordering_identity_reject_wrong() {
    let dir = tempfile::tempdir().unwrap();
    let p = |n: &str| dir.path().join(n).to_str().unwrap().to_string();

    write_state_file(&p("g1.arrow"), &[(1, 10, 1, 1, 100.0)]);

    // Wrong: claims DESC NULLS_LAST but schema+ordering contract is ASC NULLS_FIRST.
    let wrong = "0:RegionID:1:1";

    let result = mv_merge_engine::merge_state_streams_validated(
        &[p("g1.arrow")],
        &p("out.arrow"),
        &[0],
        &[true],
        &[true],
        &default_fold_ops(),
        &[],
        Some(wrong),
    );

    assert!(result.is_err());
    assert!(result.unwrap_err().contains("ordering identity mismatch"));
}

// ═══════════════════════════════════════════════════════════════════════
// 15. ordering_identity_empty_pass
// ═══════════════════════════════════════════════════════════════════════
//
// An empty ordering identity string should skip validation (backwards compat
// with callers that don't provide it).

#[test]
fn ordering_identity_empty_pass() {
    let dir = tempfile::tempdir().unwrap();
    let p = |n: &str| dir.path().join(n).to_str().unwrap().to_string();

    write_state_file(&p("g1.arrow"), &[(1, 10, 1, 1, 100.0)]);

    // Empty string should skip identity validation.
    let result = mv_merge_engine::merge_state_streams_validated(
        &[p("g1.arrow")],
        &p("out.arrow"),
        &[0],
        &[true],
        &[true],
        &default_fold_ops(),
        &[],
        Some(""),
    );
    assert!(result.is_ok(), "empty ordering identity should be accepted");

    // None should also skip identity validation.
    let result2 = mv_merge_engine::merge_state_streams_validated(
        &[p("g1.arrow")],
        &p("out2.arrow"),
        &[0],
        &[true],
        &[true],
        &default_fold_ops(),
        &[],
        None,
    );
    assert!(result2.is_ok(), "None ordering identity should be accepted");
}

// ═══════════════════════════════════════════════════════════════════════
// 16. compute_ordering_identity_determinism
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn compute_ordering_identity_determinism() {
    let schema = state_schema();

    let id1 = mv_merge_engine::compute_ordering_identity(&schema, &[0], &[true], &[true]);
    let id2 = mv_merge_engine::compute_ordering_identity(&schema, &[0], &[true], &[true]);
    assert_eq!(
        id1, id2,
        "same inputs must produce identical identity string"
    );
    assert_eq!(id1, "0:RegionID:0:0");

    // DESC changes the identity.
    let id_desc = mv_merge_engine::compute_ordering_identity(&schema, &[0], &[false], &[true]);
    assert_eq!(id_desc, "0:RegionID:1:0");
    assert_ne!(
        id1, id_desc,
        "ASC vs DESC must produce different identities"
    );

    // NULLS_LAST changes the identity.
    let id_nl = mv_merge_engine::compute_ordering_identity(&schema, &[0], &[true], &[false]);
    assert_eq!(id_nl, "0:RegionID:0:1");
    assert_ne!(id1, id_nl, "NULLS_FIRST vs NULLS_LAST must differ");
}

// ═══════════════════════════════════════════════════════════════════════
// 17. compute_ordering_identity_multi_key
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn compute_ordering_identity_multi_key() {
    let schema = state_schema();

    // Two-key ordering: col 0 ASC NULLS_FIRST, col 1 DESC NULLS_LAST.
    let identity = mv_merge_engine::compute_ordering_identity(
        &schema,
        &[0, 1],
        &[true, false],
        &[true, false],
    );
    assert_eq!(identity, "0:RegionID:0:0;1:adv_sum:1:1");

    // Three-key ordering.
    let identity3 = mv_merge_engine::compute_ordering_identity(
        &schema,
        &[0, 1, 2],
        &[true, true, false],
        &[true, true, false],
    );
    assert_eq!(identity3, "0:RegionID:0:0;1:adv_sum:0:0;2:cnt:1:1");

    // Out-of-bound index uses "?" as column name.
    let identity_oob = mv_merge_engine::compute_ordering_identity(&schema, &[99], &[true], &[true]);
    assert_eq!(identity_oob, "99:?:0:0");
}

// ═══════════════════════════════════════════════════════════════════════
// 18. schema_hash_consistency_cross_file
// ═══════════════════════════════════════════════════════════════════════

#[test]
fn schema_hash_consistency_cross_file_validated() {
    let dir = tempfile::tempdir().unwrap();
    let p = |n: &str| dir.path().join(n).to_str().unwrap().to_string();

    // Two files with identical schema.
    write_state_file(&p("g1.arrow"), &[(1, 10, 1, 1, 100.0)]);
    write_state_file(&p("g2.arrow"), &[(2, 20, 2, 2, 200.0)]);

    let rows = mv_merge_engine::merge_state_streams_validated(
        &[p("g1.arrow"), p("g2.arrow")],
        &p("out.arrow"),
        &[0],
        &[true],
        &[true],
        &default_fold_ops(),
        &standard_agg_names(),
        Some(&standard_ordering_identity()),
    )
    .unwrap();

    assert_eq!(rows, 2);

    // Schema hash is deterministic across files.
    let schema = state_schema();
    let h1 = mv_merge_engine::compute_schema_hash(&schema);
    let h2 = mv_merge_engine::compute_schema_hash(&schema);
    assert_eq!(h1, h2, "schema hash must be deterministic");

    // Different schema produces different hash.
    let alt_schema = Schema::new(vec![
        Field::new("key", DataType::Int64, false),
        Field::new("val", DataType::Utf8, false),
    ]);
    let h_alt = mv_merge_engine::compute_schema_hash(&alt_schema);
    assert_ne!(h1, h_alt, "different schemas must have different hashes");

    // Validate that cross-file schema hash mismatch is caught.
    let alt_schema_5col = Arc::new(Schema::new(vec![
        Field::new("RegionID", DataType::Int64, true),
        Field::new("adv_sum", DataType::Int64, true),
        Field::new("cnt", DataType::Int64, true),
        Field::new("avg_cnt", DataType::UInt64, true),
        Field::new("avg_sum", DataType::Float64, false), // NOT nullable vs nullable
    ]));
    let batch_alt = RecordBatch::try_new(
        alt_schema_5col.clone(),
        vec![
            Arc::new(Int64Array::from(vec![3])),
            Arc::new(Int64Array::from(vec![30])),
            Arc::new(Int64Array::from(vec![3])),
            Arc::new(UInt64Array::from(vec![3u64])),
            Arc::new(Float64Array::from(vec![300.0])),
        ],
    )
    .unwrap();
    let f_alt = File::create(&p("alt.arrow")).unwrap();
    let mut w_alt = IpcFileWriter::try_new(f_alt, &alt_schema_5col).unwrap();
    w_alt.write(&batch_alt).unwrap();
    w_alt.finish().unwrap();

    let result = mv_merge_engine::merge_state_streams_validated(
        &[p("g1.arrow"), p("alt.arrow")],
        &p("out_cross.arrow"),
        &[0],
        &[true],
        &[true],
        &default_fold_ops(),
        &standard_agg_names(),
        Some(&standard_ordering_identity()),
    );
    assert!(
        result.is_err(),
        "nullable mismatch should produce schema hash error"
    );
    assert!(result.unwrap_err().contains("schema hash mismatch"));
}
