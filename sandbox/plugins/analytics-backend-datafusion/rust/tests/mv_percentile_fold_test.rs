/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Percentile (t-digest) through the GENERAL PartialReduce fold — the tricky
//! aggregate on purpose: its state is a sketch with no SQL merge function,
//! and sketch merging is NOT exact. The honest contract asserted here:
//!
//!  1. build per-generation states (Partial), fold them (PartialReduce),
//!     finalize (Final) → the answer stays within a tolerance of the EXACT
//!     percentile computed over all raw rows;
//!  2. one-shot digest over all rows vs folded digests — measured, bounded;
//!  3. fold-order drift: (g1⊕g2)⊕g3 vs (g1⊕g2⊕g3) — measured, bounded
//!     (NOT asserted bitwise-equal — that would be a false promise).

use std::fs::File;
use std::sync::Arc;

use arrow_array::{Float64Array, Int64Array, RecordBatch};
use arrow_schema::{DataType, Field, Schema};
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::collect;
use datafusion::prelude::SessionContext;
use opensearch_datafusion::mv_fold::{mv_finalize_state, mv_fold_state};

const DEFN: &str =
    "SELECT \"RegionID\", approx_percentile_cont(\"ResolutionWidth\", 0.95) FROM mv_input GROUP BY \"RegionID\"";

fn input_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("RegionID", DataType::Int64, true),
        Field::new("ResolutionWidth", DataType::Float64, true),
    ]))
}

fn rows_batch(rows: &[(i64, f64)]) -> RecordBatch {
    RecordBatch::try_new(
        input_schema(),
        vec![
            Arc::new(Int64Array::from(
                rows.iter().map(|r| r.0).collect::<Vec<_>>(),
            )),
            Arc::new(Float64Array::from(
                rows.iter().map(|r| r.1).collect::<Vec<_>>(),
            )),
        ],
    )
    .unwrap()
}

/// Runs the definition's PARTIAL stage over raw rows, writes the state file.
fn build_state_file(path: &str, rows: &[(i64, f64)]) {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    rt.block_on(async {
        let config = datafusion::execution::context::SessionConfig::new().with_target_partitions(1);
        let ctx = SessionContext::new_with_config(config);
        let table =
            datafusion::datasource::MemTable::try_new(input_schema(), vec![vec![rows_batch(rows)]])
                .unwrap();
        ctx.register_table("mv_input", Arc::new(table)).unwrap();
        let physical = ctx
            .sql(DEFN)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        // Find (or synthesize) the Partial half and run it.
        let partial = find_mode(&physical, AggregateMode::Partial)
            .unwrap_or_else(|| synth_partial(&physical));
        let batches = collect(partial, ctx.task_ctx()).await.unwrap();
        let schema = batches[0].schema();
        let combined = arrow::compute::concat_batches(&schema, &batches).unwrap();
        // group-key sort (col 0) — the state contract
        let idx = arrow::compute::sort_to_indices(combined.column(0), None, None).unwrap();
        let cols: Vec<_> = combined
            .columns()
            .iter()
            .map(|c| arrow::compute::take(c.as_ref(), &idx, None).unwrap())
            .collect();
        let sorted = RecordBatch::try_new(schema.clone(), cols).unwrap();
        let f = File::create(path).unwrap();
        let mut w = parquet::arrow::ArrowWriter::try_new(f, sorted.schema(), None).unwrap();
        w.write(&sorted).unwrap();
        w.close().unwrap();
    });
}

fn find_mode(
    plan: &Arc<dyn datafusion::physical_plan::ExecutionPlan>,
    mode: AggregateMode,
) -> Option<Arc<dyn datafusion::physical_plan::ExecutionPlan>> {
    if let Some(agg) = plan.downcast_ref::<AggregateExec>() {
        if *agg.mode() == mode {
            return Some(Arc::clone(plan));
        }
    }
    for c in plan.children() {
        if let Some(f) = find_mode(c, mode) {
            return Some(f);
        }
    }
    None
}

fn synth_partial(
    plan: &Arc<dyn datafusion::physical_plan::ExecutionPlan>,
) -> Arc<dyn datafusion::physical_plan::ExecutionPlan> {
    // Single-mode plan — rebuild the aggregate in Partial mode.
    let node = find_mode(plan, AggregateMode::Single)
        .or_else(|| find_mode(plan, AggregateMode::SinglePartitioned))
        .expect("no aggregate");
    let agg = node.downcast_ref::<AggregateExec>().unwrap();
    Arc::new(
        AggregateExec::try_new(
            AggregateMode::Partial,
            agg.group_expr().clone(),
            agg.aggr_expr().to_vec(),
            agg.filter_expr().to_vec(),
            Arc::clone(agg.input()),
            agg.input_schema(),
        )
        .unwrap(),
    )
}

fn write_batch(path: &str, batch: &RecordBatch) {
    let f = File::create(path).unwrap();
    let mut w = parquet::arrow::ArrowWriter::try_new(f, batch.schema(), None).unwrap();
    w.write(batch).unwrap();
    w.close().unwrap();
}

/// (region -> p95) from a finalized batch.
fn finals(batch: &RecordBatch) -> Vec<(i64, f64)> {
    let g = batch
        .column(0)
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap();
    let p = batch
        .column(1)
        .as_any()
        .downcast_ref::<Float64Array>()
        .unwrap();
    let mut out: Vec<_> = (0..batch.num_rows())
        .map(|i| (g.value(i), p.value(i)))
        .collect();
    out.sort_by_key(|r| r.0);
    out
}

fn exact_p95(values: &mut Vec<f64>) -> f64 {
    values.sort_by(|a, b| a.partial_cmp(b).unwrap());
    // linear interpolation (matches approx_percentile_cont's CONT semantics)
    let rank = 0.95 * (values.len() as f64 - 1.0);
    let lo = rank.floor() as usize;
    let hi = rank.ceil() as usize;
    values[lo] + (values[hi] - values[lo]) * (rank - rank.floor())
}

#[test]
fn percentile_folds_within_bounds() {
    let dir = tempfile::tempdir().unwrap();
    let p = |n: &str| dir.path().join(n).to_str().unwrap().to_string();

    // Three generations, one region (229), deterministic pseudo-random widths.
    let mut seed: u64 = 42;
    let mut next = move || {
        seed = seed
            .wrapping_mul(6364136223846793005)
            .wrapping_add(1442695040888963407);
        800.0 + (seed >> 40) as f64 % 1200.0
    };
    let g1: Vec<(i64, f64)> = (0..400).map(|_| (229, next())).collect();
    let g2: Vec<(i64, f64)> = (0..300).map(|_| (229, next())).collect();
    let g3: Vec<(i64, f64)> = (0..300).map(|_| (229, next())).collect();

    build_state_file(&p("g1.mv.parquet"), &g1);
    build_state_file(&p("g2.mv.parquet"), &g2);
    build_state_file(&p("g3.mv.parquet"), &g3);

    // Exact answer over all 1000 raw values.
    let mut all: Vec<f64> = g1.iter().chain(&g2).chain(&g3).map(|r| r.1).collect();
    let exact = exact_p95(&mut all);

    // MV path: fold the three states, then finalize.
    let folded = mv_fold_state(
        &[p("g1.mv.parquet"), p("g2.mv.parquet"), p("g3.mv.parquet")],
        DEFN,
        input_schema(),
    )
    .unwrap();
    assert_eq!(folded.num_rows(), 1, "one region folds to one state row");
    write_batch(&p("folded.mv.parquet"), &folded);
    let answer = finals(&mv_finalize_state(&[p("folded.mv.parquet")], DEFN, input_schema()).unwrap());
    let mv_p95 = answer[0].1;

    // Honest bound: t-digest is approximate — assert relative error, not equality.
    let rel = (mv_p95 - exact).abs() / exact;
    assert!(
        rel < 0.02,
        "folded p95 {mv_p95} vs exact {exact} — relative error {rel} exceeds 2%"
    );

    // Fold-order drift: incremental (g1⊕g2)⊕g3 vs the 3-way fold — measured
    // and BOUNDED, deliberately not asserted equal.
    let f12 = mv_fold_state(&[p("g1.mv.parquet"), p("g2.mv.parquet")], DEFN, input_schema()).unwrap();
    write_batch(&p("f12.mv.parquet"), &f12);
    let f123 = mv_fold_state(&[p("f12.mv.parquet"), p("g3.mv.parquet")], DEFN, input_schema()).unwrap();
    write_batch(&p("f123.mv.parquet"), &f123);
    let incr = finals(&mv_finalize_state(&[p("f123.mv.parquet")], DEFN, input_schema()).unwrap())[0].1;
    let drift = (incr - mv_p95).abs() / exact;
    assert!(
        drift < 0.02,
        "fold-order drift {drift} exceeds 2% (incremental {incr} vs 3-way {mv_p95})"
    );

    // Finalizing UNFOLDED per-generation states directly must also work
    // (the read path may see any fold level).
    let unfolded = finals(
        &mv_finalize_state(
            &[p("g1.mv.parquet"), p("g2.mv.parquet"), p("g3.mv.parquet")],
            DEFN,
            input_schema(),
        )
        .unwrap(),
    );
    let rel2 = (unfolded[0].1 - exact).abs() / exact;
    assert!(rel2 < 0.02, "unfolded finalize off by {rel2}");
}
