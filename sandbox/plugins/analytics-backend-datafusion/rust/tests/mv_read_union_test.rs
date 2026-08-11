/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! MV read path — union surgery proof.
//!
//! Proves `mv_read::apply_mv_binding` end to end with plain DataFusion:
//! for `SELECT service, COUNT(*), SUM(latency_ms) FROM payments GROUP BY service`
//! over two raw segments where segment 1 is MV-covered:
//!
//! 1. The stripped Partial plan is rewritten to
//!    UNION(scan(mv state of seg1, positionally aliased), Partial over seg2 only).
//! 2. Final over the union's state rows == direct aggregation over ALL raw rows.
//!    (If narrowing failed, seg1 would be counted twice — api=9, not 5.)
//! 3. All-covered: raw branch narrowed to zero files; answer still correct.
//! 4. Schema mismatch (extra state column): binding refused, original plan
//!    returned untouched (fallback-first contract).
//!
//! The state file is written with a DIFFERENT table alias (`mv_input`) than the
//! query's (`payments`), so state column names differ — proving the positional
//! aliasing, not name matching, is what lines the schemas up.

use std::collections::HashSet;
use std::fs::File;
use std::sync::Arc;

use arrow_array::{Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use datafusion::datasource::MemTable;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::{collect, ExecutionPlan};
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use opensearch_datafusion::mv_read::{apply_mv_binding, MVBinding};
use parquet::arrow::ArrowWriter;
use tempfile::TempDir;

fn raw_schema() -> Arc<Schema> {
    Arc::new(Schema::new(vec![
        Field::new("service", DataType::Utf8, true),
        Field::new("latency_ms", DataType::Int64, true),
    ]))
}

fn raw_batch(rows: &[(&str, i64)]) -> RecordBatch {
    RecordBatch::try_new(
        raw_schema(),
        vec![
            Arc::new(StringArray::from(
                rows.iter().map(|(s, _)| *s).collect::<Vec<_>>(),
            )),
            Arc::new(Int64Array::from(
                rows.iter().map(|(_, l)| *l).collect::<Vec<_>>(),
            )),
        ],
    )
    .expect("raw batch")
}

/// seg1: api:4 (latencies 30+50+900+25=1005), web:1 (40)
fn segment1() -> Vec<(&'static str, i64)> {
    vec![
        ("api", 30),
        ("api", 50),
        ("web", 40),
        ("api", 900),
        ("api", 25),
    ]
}

/// seg2: api:1 (10), web:1 (80), batch:1 (60)
fn segment2() -> Vec<(&'static str, i64)> {
    vec![("api", 10), ("web", 80), ("batch", 60)]
}

const QUERY: &str = "SELECT service, COUNT(*), SUM(latency_ms) FROM payments GROUP BY service";

fn write_parquet(path: &std::path::Path, batches: &[RecordBatch]) {
    let schema = batches[0].schema();
    let file = File::create(path).expect("create parquet");
    let mut writer = ArrowWriter::try_new(file, schema, None).expect("writer");
    for b in batches {
        writer.write(b).expect("write batch");
    }
    writer.close().expect("close writer");
}

fn find_partial(plan: &Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
    if let Some(agg) = plan.downcast_ref::<AggregateExec>() {
        if *agg.mode() == AggregateMode::Partial {
            return Some(plan.clone());
        }
    }
    for child in plan.children() {
        if let Some(found) = find_partial(child) {
            return Some(found);
        }
    }
    None
}

/// Builds the MV state file for one segment: Partial-mode aggregation of the
/// definition query over the segment batch, registered under the WRITER'S
/// table alias `mv_input` (so state column names differ from the query's).
async fn write_state_file(dir: &std::path::Path, name: &str, segment: Vec<(&str, i64)>) -> String {
    let ctx = SessionContext::new();
    let mem = MemTable::try_new(raw_schema(), vec![vec![raw_batch(&segment)]]).expect("memtable");
    ctx.register_table("mv_input", Arc::new(mem))
        .expect("register mv_input");
    let df = ctx
        .sql("SELECT service, COUNT(*), SUM(latency_ms) FROM mv_input GROUP BY service")
        .await
        .expect("definition sql");
    let physical = df.create_physical_plan().await.expect("physical");
    let partial = find_partial(&physical).expect("partial subtree");
    let states = collect(partial, ctx.task_ctx())
        .await
        .expect("collect states");
    let path = dir.join(name);
    write_parquet(&path, &states);
    path.to_string_lossy().into_owned()
}

/// Registers both raw segment files as `payments` and returns the stripped
/// Partial subtree for QUERY.
async fn partial_over_raw(ctx: &SessionContext) -> Arc<dyn ExecutionPlan> {
    let df = ctx.sql(QUERY).await.expect("query sql");
    let physical = df.create_physical_plan().await.expect("physical");
    find_partial(&physical).expect("partial subtree")
}

/// Final fold over collected state rows: registers them as a MemTable and
/// SUMs/aggregates the state columns (names from the PARTIAL side schema).
async fn final_fold(states: Vec<RecordBatch>) -> Vec<(String, i64, i64)> {
    let ctx = SessionContext::new();
    let schema = states[0].schema();
    let mem = MemTable::try_new(schema, vec![states]).expect("state memtable");
    ctx.register_table("states", Arc::new(mem))
        .expect("register states");
    let batches = ctx
        .sql(
            "SELECT service, SUM(\"count(Int64(1))[count]\") AS cnt, \
             SUM(\"sum(payments.latency_ms)[sum]\") AS lat \
             FROM states GROUP BY service ORDER BY service",
        )
        .await
        .expect("final sql")
        .collect()
        .await
        .expect("final collect");
    let mut rows = Vec::new();
    for b in &batches {
        // DF54 may surface strings as Utf8View — normalize via cast.
        let svc_arr =
            datafusion::arrow::compute::cast(b.column(0), &DataType::Utf8).expect("cast svc");
        let svc = svc_arr
            .as_any()
            .downcast_ref::<StringArray>()
            .expect("svc col");
        let cnt_arr =
            datafusion::arrow::compute::cast(b.column(1), &DataType::Int64).expect("cast cnt");
        let cnt = cnt_arr
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("cnt col");
        let lat_arr =
            datafusion::arrow::compute::cast(b.column(2), &DataType::Int64).expect("cast lat");
        let lat = lat_arr
            .as_any()
            .downcast_ref::<Int64Array>()
            .expect("lat col");
        for i in 0..b.num_rows() {
            rows.push((svc.value(i).to_string(), cnt.value(i), lat.value(i)));
        }
    }
    rows
}

fn golden() -> Vec<(String, i64, i64)> {
    vec![
        ("api".to_string(), 5, 1015),
        ("batch".to_string(), 1, 60),
        ("web".to_string(), 2, 120),
    ]
}

async fn setup_raw(dir: &std::path::Path) -> SessionContext {
    write_parquet(&dir.join("seg1.parquet"), &[raw_batch(&segment1())]);
    write_parquet(&dir.join("seg2.parquet"), &[raw_batch(&segment2())]);
    let ctx = SessionContext::new();
    ctx.register_parquet(
        "payments",
        dir.to_str().unwrap(),
        ParquetReadOptions::default(),
    )
    .await
    .expect("register payments");
    ctx
}

#[tokio::test]
async fn union_of_state_and_uncovered_raw_matches_full_raw() {
    let tmp = TempDir::new().expect("tmpdir");
    let raw_dir = tmp.path().join("raw");
    let mv_dir = tmp.path().join("mv");
    std::fs::create_dir_all(&raw_dir).unwrap();
    std::fs::create_dir_all(&mv_dir).unwrap();

    let mv1 = write_state_file(&mv_dir, "seg1.mv.parquet", segment1()).await;

    let ctx = setup_raw(&raw_dir).await;
    let partial = partial_over_raw(&ctx).await;

    let binding = MVBinding {
        mv_file_paths: vec![mv1],
        covered_raw_file_names: HashSet::from(["seg1.parquet".to_string()]),
        strict: false,
    };
    let bound = apply_mv_binding(&ctx, Arc::clone(&partial), &binding)
        .await
        .expect("binding must not error");
    assert!(
        !Arc::ptr_eq(&bound, &partial),
        "binding must produce a rewritten plan, not fall back"
    );
    assert_eq!(
        bound.schema(),
        partial.schema(),
        "union output schema must equal the partial output schema"
    );

    let states = collect(bound, ctx.task_ctx()).await.expect("collect union");
    assert_eq!(final_fold(states).await, golden());
}

#[tokio::test]
async fn all_covered_serves_entirely_from_state_files() {
    let tmp = TempDir::new().expect("tmpdir");
    let raw_dir = tmp.path().join("raw");
    let mv_dir = tmp.path().join("mv");
    std::fs::create_dir_all(&raw_dir).unwrap();
    std::fs::create_dir_all(&mv_dir).unwrap();

    let mv1 = write_state_file(&mv_dir, "seg1.mv.parquet", segment1()).await;
    let mv2 = write_state_file(&mv_dir, "seg2.mv.parquet", segment2()).await;

    let ctx = setup_raw(&raw_dir).await;
    let partial = partial_over_raw(&ctx).await;

    let binding = MVBinding {
        mv_file_paths: vec![mv1, mv2],
        covered_raw_file_names: HashSet::from([
            "seg1.parquet".to_string(),
            "seg2.parquet".to_string(),
        ]),
        strict: false,
    };
    let bound = apply_mv_binding(&ctx, Arc::clone(&partial), &binding)
        .await
        .expect("binding must not error");
    assert!(!Arc::ptr_eq(&bound, &partial), "binding must apply");

    let states = collect(bound, ctx.task_ctx()).await.expect("collect union");
    assert_eq!(final_fold(states).await, golden());
}

#[tokio::test]
async fn mismatched_state_schema_falls_back_to_raw_plan() {
    let tmp = TempDir::new().expect("tmpdir");
    let raw_dir = tmp.path().join("raw");
    let mv_dir = tmp.path().join("mv");
    std::fs::create_dir_all(&raw_dir).unwrap();
    std::fs::create_dir_all(&mv_dir).unwrap();

    // State file with a DIFFERENT shape (raw rows, not states): field count differs.
    write_parquet(&mv_dir.join("bogus.mv.parquet"), &[raw_batch(&segment1())]);
    let bogus = mv_dir
        .join("bogus.mv.parquet")
        .to_string_lossy()
        .into_owned();

    let ctx = setup_raw(&raw_dir).await;
    let partial = partial_over_raw(&ctx).await;

    let binding = MVBinding {
        mv_file_paths: vec![bogus],
        covered_raw_file_names: HashSet::from(["seg1.parquet".to_string()]),
        strict: false,
    };
    let bound = apply_mv_binding(&ctx, Arc::clone(&partial), &binding)
        .await
        .expect("non-strict fallback must not error");
    assert!(
        Arc::ptr_eq(&bound, &partial),
        "schema mismatch must return the original plan untouched"
    );

    // And the fallback plan still answers correctly from raw.
    let states = collect(bound, ctx.task_ctx())
        .await
        .expect("collect raw partial");
    assert_eq!(final_fold(states).await, golden());
}

/// Strict MV-only mode, all covered: the plan must contain NO raw scan of the
/// payments directory (state files only) and still produce the golden answer.
#[tokio::test]
async fn strict_all_covered_serves_from_state_files_only() {
    let tmp = TempDir::new().expect("tmpdir");
    let raw_dir = tmp.path().join("raw");
    let mv_dir = tmp.path().join("mv");
    std::fs::create_dir_all(&raw_dir).unwrap();
    std::fs::create_dir_all(&mv_dir).unwrap();

    let mv1 = write_state_file(&mv_dir, "seg1.mv.parquet", segment1()).await;
    let mv2 = write_state_file(&mv_dir, "seg2.mv.parquet", segment2()).await;

    let ctx = setup_raw(&raw_dir).await;
    let partial = partial_over_raw(&ctx).await;

    let binding = MVBinding {
        mv_file_paths: vec![mv1, mv2],
        covered_raw_file_names: HashSet::from([
            "seg1.parquet".to_string(),
            "seg2.parquet".to_string(),
        ]),
        strict: true,
    };
    let bound = apply_mv_binding(&ctx, Arc::clone(&partial), &binding)
        .await
        .expect("strict all-covered must succeed");

    // The plan must not reference ANY raw file: displayable plan mentions the
    // mv dir but never the raw dir.
    let display = datafusion::physical_plan::displayable(bound.as_ref())
        .indent(true)
        .to_string();
    let raw_dir_str = raw_dir.to_string_lossy().into_owned();
    assert!(
        !display.contains(&raw_dir_str),
        "strict plan must contain no raw scan; plan:\n{display}"
    );
    assert!(
        display.contains("mv"),
        "strict plan should scan the state files; plan:\n{display}"
    );

    let states = collect(bound, ctx.task_ctx())
        .await
        .expect("collect strict");
    assert_eq!(final_fold(states).await, golden());
}

/// Strict mode with an uncovered raw file: hard error, not silent fallback.
#[tokio::test]
async fn strict_with_uncovered_file_errors() {
    let tmp = TempDir::new().expect("tmpdir");
    let raw_dir = tmp.path().join("raw");
    let mv_dir = tmp.path().join("mv");
    std::fs::create_dir_all(&raw_dir).unwrap();
    std::fs::create_dir_all(&mv_dir).unwrap();

    let mv1 = write_state_file(&mv_dir, "seg1.mv.parquet", segment1()).await;

    let ctx = setup_raw(&raw_dir).await;
    let partial = partial_over_raw(&ctx).await;

    let binding = MVBinding {
        mv_file_paths: vec![mv1],
        covered_raw_file_names: HashSet::from(["seg1.parquet".to_string()]), // seg2 uncovered
        strict: true,
    };
    let err = apply_mv_binding(&ctx, Arc::clone(&partial), &binding)
        .await
        .expect_err("strict with uncovered raw files must error");
    assert!(
        err.to_string().contains("not covered by MV state"),
        "unexpected error: {err}"
    );
}

/// Strict mode with a mismatched state schema: hard error, not silent fallback.
#[tokio::test]
async fn strict_schema_mismatch_errors() {
    let tmp = TempDir::new().expect("tmpdir");
    let raw_dir = tmp.path().join("raw");
    let mv_dir = tmp.path().join("mv");
    std::fs::create_dir_all(&raw_dir).unwrap();
    std::fs::create_dir_all(&mv_dir).unwrap();

    write_parquet(&mv_dir.join("bogus.mv.parquet"), &[raw_batch(&segment1())]);
    let bogus = mv_dir
        .join("bogus.mv.parquet")
        .to_string_lossy()
        .into_owned();

    let ctx = setup_raw(&raw_dir).await;
    let partial = partial_over_raw(&ctx).await;

    let binding = MVBinding {
        mv_file_paths: vec![bogus],
        covered_raw_file_names: HashSet::from([
            "seg1.parquet".to_string(),
            "seg2.parquet".to_string(),
        ]),
        strict: true,
    };
    let err = apply_mv_binding(&ctx, Arc::clone(&partial), &binding)
        .await
        .expect_err("strict with mismatched schema must error");
    assert!(
        err.to_string().contains("did not apply"),
        "unexpected error: {err}"
    );
}
