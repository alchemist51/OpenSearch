/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Cross-validation: Stage 4 streaming merge engine must produce identical
//! results to the legacy `mv_merge_state` (DataFusion SQL path) for all
//! SUM-only fold definitions.

use std::fs::File;
use std::sync::Arc;

use arrow::ipc::reader::FileReader as IpcFileReader;
use arrow::ipc::writer::FileWriter as IpcFileWriter;
use arrow_array::{Float64Array, Int64Array, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema, SchemaRef};
use opensearch_datafusion::mv_merge_engine;
use opensearch_datafusion::mv_poc::mv_merge_state;

const FOLD_SQL: &str = "SELECT \"RegionID\", SUM(adv_sum), SUM(cnt), \
     SUM(CAST(avg_cnt AS BIGINT UNSIGNED)), SUM(avg_sum) \
     FROM mv_input GROUP BY \"RegionID\"";

fn state_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new("RegionID", DataType::Int64, true),
        Field::new("adv_sum", DataType::Int64, true),
        Field::new("cnt", DataType::Int64, true),
        Field::new("avg_cnt", DataType::UInt64, true),
        Field::new("avg_sum", DataType::Float64, true),
    ]))
}

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

/// Cross-validates: the Stage 4 streaming engine must produce the same output
/// as the legacy DataFusion SQL engine for the standard test inputs.
#[test]
fn stage4_matches_legacy_merge() {
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
    write_state_file(&p("g3.arrow"), &[(229, 1, 4, 4, 5000.0)]);

    let files = vec![p("g1.arrow"), p("g2.arrow"), p("g3.arrow")];

    // Legacy merge
    let legacy_rows = mv_merge_state(&files, FOLD_SQL, &p("legacy.arrow")).unwrap();
    let legacy_output = read_rows(&p("legacy.arrow"));

    // Stage 4 merge
    let stage4_rows = mv_merge_engine::merge_state_streams(
        &files,
        &p("stage4.arrow"),
        &[0],             // ordering_indices: group key at col 0
        &[true],          // ordering_asc: ASC
        &[true],          // ordering_nulls_first: NULLS FIRST
        &[0, 1, 1, 1, 1], // fold_ops: key, sum, sum, sum, sum
    )
    .unwrap();

    let stage4_output = read_rows(&p("stage4.arrow"));

    assert_eq!(legacy_rows, stage4_rows, "row counts must match");
    assert_eq!(
        legacy_output, stage4_output,
        "Stage 4 output must match legacy for SUM-only fold"
    );
}

/// Validates that the Stage 4 engine's schema hash is deterministic and
/// that validate_ipc_header accepts valid files.
#[test]
fn stage4_validation_roundtrip() {
    let dir = tempfile::tempdir().unwrap();
    let p = dir.path().join("valid.arrow").to_str().unwrap().to_string();

    write_state_file(&p, &[(1, 10, 1, 1, 100.0), (5, 50, 5, 5, 500.0)]);

    let schema = state_schema();
    let hash = mv_merge_engine::compute_schema_hash(&schema);

    let result = mv_merge_engine::validate_ipc_header(&p, hash, &[0], &[true], &[true]);
    assert!(
        result.is_ok(),
        "valid file should pass validation: {:?}",
        result
    );
}
