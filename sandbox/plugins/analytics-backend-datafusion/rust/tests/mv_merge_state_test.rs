/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! State⊕state merge: folding N state files must equal building the fold
//! over all rows at once, and the merge must be CLOSED (its output merges
//! again to the same answer). Mirrors the q9-native fold schema:
//! [RegionID i64, adv_sum i64, cnt i64, avg_cnt u64, avg_sum f64].

use std::fs::File;
use std::sync::Arc;

use arrow_array::{Float64Array, Int64Array, RecordBatch, UInt64Array};
use arrow_schema::{DataType, Field, Schema};
use opensearch_datafusion::mv_poc::mv_merge_state;

const FOLD_SQL: &str = "SELECT \"RegionID\", SUM(adv_sum), SUM(cnt), \
     SUM(CAST(avg_cnt AS BIGINT UNSIGNED)), SUM(avg_sum) \
     FROM mv_input GROUP BY \"RegionID\"";

fn state_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("RegionID", DataType::Int64, true),
        Field::new("adv_sum", DataType::Int64, true),
        Field::new("cnt", DataType::Int64, true),
        Field::new("avg_cnt", DataType::UInt64, true),
        Field::new("avg_sum", DataType::Float64, true),
    ]))
}

fn write_state_file(path: &str, rows: &[(i64, i64, i64, u64, f64)]) {
    let batch = RecordBatch::try_new(
        state_schema(),
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
    let mut w = arrow::ipc::writer::FileWriter::try_new(file, &batch.schema()).unwrap();
    w.write(&batch).unwrap();
    w.finish().unwrap();
}

fn read_rows(path: &str) -> Vec<(i64, i64, i64, u64, f64)> {
    let reader = arrow::ipc::reader::FileReader::try_new(File::open(path).unwrap(), None).unwrap();
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

#[test]
fn merge_folds_and_is_closed() {
    let dir = tempfile::tempdir().unwrap();
    let p = |n: &str| dir.path().join(n).to_str().unwrap().to_string();

    // Gen 1: regions 229 and 2. Gen 2: regions 229 (again) and 7.
    write_state_file(
        &p("g1.arrow"),
        &[(2, 3, 1, 1, 1440.0), (229, 2, 2, 2, 3288.0)],
    );
    write_state_file(
        &p("g2.arrow"),
        &[(7, 0, 1, 1, 800.0), (229, 7, 1, 1, 1366.0)],
    );
    // Gen 3: region 229 once more — three-way merge exercises >2 inputs.
    write_state_file(&p("g3.arrow"), &[(229, 1, 4, 4, 5000.0)]);

    // Merge all three.
    let rows = mv_merge_state(
        &[p("g1.arrow"), p("g2.arrow"), p("g3.arrow")],
        FOLD_SQL,
        &p("merged.arrow"),
    )
    .unwrap();
    assert_eq!(rows, 3, "three distinct regions");
    let merged = read_rows(&p("merged.arrow"));
    assert_eq!(
        merged,
        vec![
            (2, 3, 1, 1, 1440.0),
            (7, 0, 1, 1, 800.0),
            (229, 10, 7, 7, 9654.0), // sums fold; avg state pair folds componentwise
        ]
    );
    // avg finishes exactly: 9654 / 7
    assert!((merged[2].4 / merged[2].3 as f64 - 1379.142857142857).abs() < 1e-9);

    // CLOSURE: merging incrementally (g1⊕g2, then ⊕g3) equals the 3-way merge.
    mv_merge_state(&[p("g1.arrow"), p("g2.arrow")], FOLD_SQL, &p("m12.arrow")).unwrap();
    mv_merge_state(&[p("m12.arrow"), p("g3.arrow")], FOLD_SQL, &p("m123.arrow")).unwrap();
    assert_eq!(
        read_rows(&p("m123.arrow")),
        merged,
        "merge must be associative-in-effect"
    );

    // Single-input merge = compaction no-op (already folded input folds to itself).
    mv_merge_state(&[p("merged.arrow")], FOLD_SQL, &p("m_self.arrow")).unwrap();
    assert_eq!(
        read_rows(&p("m_self.arrow")),
        merged,
        "idempotent on folded input"
    );
}
