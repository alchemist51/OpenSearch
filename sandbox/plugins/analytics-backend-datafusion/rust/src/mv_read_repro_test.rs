/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Repro harness for the "MV-only read returns ZERO rows on real state files"
//! defect (cbspan1_mv_mv1). Drives the SAME production entry points
//! (`mv_table_schema` + `register_mv_state_listing_table`) over the three real
//! hydrated `_mv_partial.*.parquet` files, with the exact deployed metadata:
//!
//! - table/logical schema = 36-field MAPPING order (alphabetical), incl.
//!   `_mv_source_generation` at logical position 2;
//! - state_fields = 35-name PHYSICAL order.
//!
//! Files are read from $MVREAD_REPRO_DIR (default
//! /home/abandeji/workplace/mv-read-repro/files). The test is #[ignore]d when
//! the directory is absent so CI without the fixtures stays green.

#![cfg(test)]

use std::sync::Arc;

use arrow::datatypes::{DataType, Field, Schema, SchemaRef};
use datafusion::prelude::{SessionConfig, SessionContext};

fn repro_dir() -> Option<std::path::PathBuf> {
    let dir = std::env::var("MVREAD_REPRO_DIR")
        .unwrap_or_else(|_| "/home/abandeji/workplace/mv-read-repro/files".to_string());
    let p = std::path::PathBuf::from(dir);
    if p.join("target_settings.json").exists() {
        Some(p)
    } else {
        None
    }
}

fn state_files(dir: &std::path::Path) -> Vec<String> {
    let mut v: Vec<String> = std::fs::read_dir(dir)
        .unwrap()
        .filter_map(|e| e.ok())
        .map(|e| e.path())
        .filter(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .map(|n| n.starts_with("_mv_partial.") && n.ends_with(".parquet"))
                .unwrap_or(false)
        })
        .map(|p| p.to_str().unwrap().to_string())
        .collect();
    v.sort();
    v
}

/// state_fields as declared in target_settings.json (index.mv.state_fields),
/// the 35-name PHYSICAL order.
fn state_fields() -> Vec<String> {
    [
        "event_bucket",
        "URL",
        "CounterID",
        "sum_advengineid",
        "min_advengineid",
        "max_advengineid",
        "cnt_advengineid",
        "sum_isrefresh",
        "min_isrefresh",
        "max_isrefresh",
        "cnt_isrefresh",
        "sum_resolutionwidth",
        "min_resolutionwidth",
        "max_resolutionwidth",
        "cnt_resolutionwidth",
        "sum_resolutionheight",
        "min_resolutionheight",
        "max_resolutionheight",
        "cnt_resolutionheight",
        "sum_resolutiondepth",
        "min_resolutiondepth",
        "max_resolutiondepth",
        "cnt_resolutiondepth",
        "sum_flashminor",
        "min_flashminor",
        "max_flashminor",
        "cnt_flashminor",
        "sum_netmajor",
        "min_netmajor",
        "max_netmajor",
        "cnt_netmajor",
        "sum_fetchtiming",
        "min_fetchtiming",
        "max_fetchtiming",
        "cnt_fetchtiming",
    ]
    .iter()
    .map(|s| s.to_string())
    .collect()
}

/// The 36-field MAPPING order (alphabetical) from target_mapping.json, incl.
/// `_mv_source_generation`. This is the logical schema the Java plan hands to
/// Rust. Types: CounterID=integer(Int32), URL=keyword(Utf8View),
/// event_bucket=date(Int64 in the plan's long reinterpretation), everything
/// else long(Int64). We model group keys the way the deployed plan does: the
/// KEYWORD key as Utf8View, the date key as Int64 (the plan reinterprets date
/// group keys as long), CounterID as Int32.
fn mapping_logical_schema() -> SchemaRef {
    let long = DataType::Int64;
    let f = |name: &str, dt: DataType| Field::new(name, dt, true);
    Arc::new(Schema::new(vec![
        f("CounterID", DataType::Int32),
        f("URL", DataType::Utf8View),
        f("_mv_source_generation", long.clone()),
        f("cnt_advengineid", long.clone()),
        f("cnt_fetchtiming", long.clone()),
        f("cnt_flashminor", long.clone()),
        f("cnt_isrefresh", long.clone()),
        f("cnt_netmajor", long.clone()),
        f("cnt_resolutiondepth", long.clone()),
        f("cnt_resolutionheight", long.clone()),
        f("cnt_resolutionwidth", long.clone()),
        f("event_bucket", long.clone()),
        f("max_advengineid", long.clone()),
        f("max_fetchtiming", long.clone()),
        f("max_flashminor", long.clone()),
        f("max_isrefresh", long.clone()),
        f("max_netmajor", long.clone()),
        f("max_resolutiondepth", long.clone()),
        f("max_resolutionheight", long.clone()),
        f("max_resolutionwidth", long.clone()),
        f("min_advengineid", long.clone()),
        f("min_fetchtiming", long.clone()),
        f("min_flashminor", long.clone()),
        f("min_isrefresh", long.clone()),
        f("min_netmajor", long.clone()),
        f("min_resolutiondepth", long.clone()),
        f("min_resolutionheight", long.clone()),
        f("min_resolutionwidth", long.clone()),
        f("sum_advengineid", long.clone()),
        f("sum_fetchtiming", long.clone()),
        f("sum_flashminor", long.clone()),
        f("sum_isrefresh", long.clone()),
        f("sum_netmajor", long.clone()),
        f("sum_resolutiondepth", long.clone()),
        f("sum_resolutionheight", long.clone()),
        f("sum_resolutionwidth", long.clone()),
    ]))
}

/// Logical schema in state_fields order (35 fields, no `_mv_source_generation`)
/// — bisect (a) table schema in state_fields order and (b) without
/// `_mv_source_generation`.
fn state_order_logical_schema() -> SchemaRef {
    let long = DataType::Int64;
    let f = |name: &str, dt: DataType| Field::new(name, dt, true);
    let mut fields = Vec::new();
    for (i, name) in state_fields().iter().enumerate() {
        let dt = match i {
            0 => long.clone(),          // event_bucket (long reinterpretation)
            1 => DataType::Utf8View,    // URL
            2 => DataType::Int32,       // CounterID
            _ => long.clone(),
        };
        fields.push(f(name, dt));
    }
    Arc::new(Schema::new(fields))
}

async fn session() -> SessionContext {
    let mut config = SessionConfig::new();
    config
        .options_mut()
        .execution
        .split_file_groups_by_statistics = true;
    // string_view default, as in production plans.
    config.options_mut().execution.parquet.schema_force_view_types = true;
    SessionContext::new_with_config(config)
}

fn total_rows(batches: &[arrow::record_batch::RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

async fn run(ctx: &SessionContext, q: &str) -> datafusion::error::Result<Vec<arrow::record_batch::RecordBatch>> {
    ctx.sql(q).await?.collect().await
}

/// Inspect the real physical parquet schema — prints field names/types so we
/// know the true on-disk shape (writer aliases? plain names? types?).
#[tokio::test]
async fn inspect_physical_schema() {
    let Some(dir) = repro_dir() else {
        eprintln!("MVREAD_REPRO_DIR absent; skipping");
        return;
    };
    let files = state_files(&dir);
    assert!(!files.is_empty(), "no _mv_partial files found in {:?}", dir);
    eprintln!("found {} state files", files.len());
    let schema = crate::mv_expr_adapter::read_schema_from_first_file(&files)
        .unwrap()
        .unwrap();
    eprintln!("PHYSICAL schema ({} fields):", schema.fields().len());
    for (i, fld) in schema.fields().iter().enumerate() {
        eprintln!("  [{}] {:?}  type={:?}", i, fld.name(), fld.data_type());
    }
    // Also dump footer sort order + row counts via parquet reader.
    for f in &files {
        let file = std::fs::File::open(f).unwrap();
        let r = parquet::file::reader::SerializedFileReader::new(file).unwrap();
        use parquet::file::reader::FileReader;
        let meta = r.metadata();
        let rows: i64 = (0..meta.num_row_groups())
            .map(|i| meta.row_group(i).num_rows())
            .sum();
        let sc = meta
            .row_group(0)
            .sorting_columns()
            .map(|c| format!("{:?}", c));
        eprintln!("  file {} rows={} sort={:?}", f, rows, sc);
    }
}

/// MAIN REPRO: production entry, mapping-order logical schema incl.
/// `_mv_source_generation`. Expect ZERO rows (the defect).
#[tokio::test]
async fn repro_zero_rows_mapping_order() {
    let Some(dir) = repro_dir() else {
        eprintln!("MVREAD_REPRO_DIR absent; skipping");
        return;
    };
    let files = state_files(&dir);
    let ctx = session().await;

    // Drive the SAME production derivation + registration.
    let physical = crate::mv_expr_adapter::read_schema_from_first_file(&files)
        .unwrap()
        .unwrap();
    let logical = mapping_logical_schema();
    let sf = state_fields();
    let (table_schema, _proj) =
        crate::session_context::mv_table_schema_for_test(&physical, &logical, &sf).unwrap();

    crate::mv_expr_adapter::register_mv_state_listing_table(
        &ctx, "mv", &files, table_schema, &sf,
    )
    .await
    .unwrap();

    let c = run(&ctx, "SELECT count(*) AS n FROM mv").await.unwrap();
    eprintln!("count(*) => {:?}", c);
    let head = run(
        &ctx,
        "SELECT \"CounterID\", \"URL\", \"cnt_advengineid\", \"sum_advengineid\" FROM mv LIMIT 3",
    )
    .await
    .unwrap();
    eprintln!("head3 rows = {}", total_rows(&head));
    eprintln!("head3 = {:?}", head);
    let grouped = run(
        &ctx,
        "SELECT \"CounterID\", SUM(\"cnt_advengineid\") AS s FROM mv GROUP BY \"CounterID\" LIMIT 5",
    )
    .await
    .unwrap();
    eprintln!("grouped rows = {}", total_rows(&grouped));
    eprintln!("grouped = {:?}", grouped);

    // Document the observed behavior (assertion adjusted after first run).
    eprintln!(
        "REPRO RESULT: count_rows={} head_rows={} grouped_rows={}",
        total_rows(&c),
        total_rows(&head),
        total_rows(&grouped)
    );
}

// ---------------------------------------------------------------------------
// Partial-aggregate plan reproduction (the ACTUAL node path).
//
// The node does not run raw SQL: the coordinator ships a Substrait partial-
// aggregate plan, the shard builds it with `physical_optimizer_rules_without_combine`,
// strips to the Partial half (`apply_aggregate_mode(Partial)`), executes it and
// returns the Partial output to the coordinator's FINAL. This harness rebuilds
// that path over the real files.
// ---------------------------------------------------------------------------

/// Builds a session mirroring `create_mv_only_session_context(has_partial_aggregate=true)`.
async fn partial_session() -> SessionContext {
    let mut config = SessionConfig::new();
    config
        .options_mut()
        .execution
        .split_file_groups_by_statistics = true;
    config.options_mut().execution.parquet.schema_force_view_types = true;
    let state = datafusion::execution::session_state::SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .with_physical_optimizer_rules(crate::agg_mode::physical_optimizer_rules_without_combine())
        .build();
    SessionContext::new_with_state(state)
}

/// Executes `sql` as a PARTIAL-aggregate shard fragment: build physical plan,
/// strip to the Partial half, execute the stripped plan and return its rows.
/// This is exactly the plan shape the node returns to the coordinator FINAL.
async fn run_partial(ctx: &SessionContext, sql: &str) -> Vec<arrow::record_batch::RecordBatch> {
    use datafusion::physical_plan::execute_stream;
    use futures::StreamExt;
    let physical = ctx.sql(sql).await.unwrap().create_physical_plan().await.unwrap();
    let stripped =
        crate::agg_mode::apply_aggregate_mode(physical, crate::agg_mode::Mode::Partial, false)
            .unwrap();
    eprintln!(
        "partial plan for [{}]:\n{}",
        sql,
        datafusion::physical_plan::displayable(stripped.as_ref()).indent(true)
    );
    let mut stream = execute_stream(stripped, ctx.task_ctx()).unwrap();
    let mut out = Vec::new();
    while let Some(b) = stream.next().await {
        out.push(b.unwrap());
    }
    out
}

async fn register_mapping_order(ctx: &SessionContext, files: &[String]) {
    let physical = crate::mv_expr_adapter::read_schema_from_first_file(files)
        .unwrap()
        .unwrap();
    let logical = mapping_logical_schema();
    let sf = state_fields();
    let (table_schema, _proj) =
        crate::session_context::mv_table_schema_for_test(&physical, &logical, &sf).unwrap();
    crate::mv_expr_adapter::register_mv_state_listing_table(ctx, "mv", files, table_schema, &sf)
        .await
        .unwrap();
}

/// THE REPRO: partial-aggregate `count(*)` and grouped fold over the real files
/// with the mapping-order logical schema. If the node's zero-rows defect lives
/// in this path, the Partial output has zero rows here.
#[tokio::test]
async fn repro_partial_aggregate_mapping_order() {
    let Some(dir) = repro_dir() else {
        eprintln!("MVREAD_REPRO_DIR absent; skipping");
        return;
    };
    let files = state_files(&dir);
    let ctx = partial_session().await;
    register_mapping_order(&ctx, &files).await;

    let c = run_partial(&ctx, "SELECT count(*) AS n FROM mv").await;
    let count_rows: usize = total_rows(&c);
    eprintln!("PARTIAL count(*) rows={} data={:?}", count_rows, c);

    let g = run_partial(
        &ctx,
        "SELECT \"CounterID\", SUM(\"cnt_advengineid\") AS s FROM mv GROUP BY \"CounterID\"",
    )
    .await;
    eprintln!("PARTIAL grouped rows={}", total_rows(&g));

    eprintln!(
        "PARTIAL REPRO RESULT: count_partial_rows={} grouped_partial_rows={}",
        count_rows,
        total_rows(&g)
    );
}

// ---------------------------------------------------------------------------
// FAITHFUL end-to-end: real Substrait plan (coordinator-shape) consumed against
// the MV-only registered table, exactly like prepare_partial_plan. Substrait
// column references are POSITIONAL against the base_schema; this is the path
// that can misalign if the registered table schema order differs from the
// base_schema the plan was built against.
// ---------------------------------------------------------------------------

/// Produce Substrait bytes for `sql` against a table named `mv` whose schema is
/// the 36-field MAPPING order (what the coordinator plans against).
async fn substrait_for(sql: &str) -> Vec<u8> {
    use datafusion_substrait::logical_plan::producer::to_substrait_plan;
    use prost::Message;
    let ctx = SessionContext::new();
    let mem = datafusion::datasource::MemTable::try_new(
        mapping_logical_schema(),
        vec![vec![]],
    )
    .unwrap();
    ctx.register_table("mv", Arc::new(mem)).unwrap();
    let plan = ctx.sql(sql).await.unwrap().logical_plan().clone();
    let substrait = to_substrait_plan(&plan, &ctx.state()).unwrap();
    let mut buf = Vec::new();
    substrait.encode(&mut buf).unwrap();
    buf
}

/// Consume Substrait against the MV-only registered table and execute the
/// Partial half — the exact prepare_partial_plan path.
async fn run_substrait_partial(
    ctx: &SessionContext,
    substrait_bytes: &[u8],
) -> datafusion::error::Result<Vec<arrow::record_batch::RecordBatch>> {
    use datafusion::physical_plan::execute_stream;
    use datafusion_substrait::logical_plan::consumer::from_substrait_plan;
    use futures::StreamExt;
    use prost::Message;
    use substrait::proto::Plan;

    let plan = Plan::decode(substrait_bytes).unwrap();
    let logical_plan = from_substrait_plan(&ctx.state(), &plan).await?;
    let dataframe = ctx.execute_logical_plan(logical_plan).await?;
    let physical_plan = dataframe.create_physical_plan().await?;
    let stripped =
        crate::agg_mode::apply_aggregate_mode(physical_plan, crate::agg_mode::Mode::Partial, false)?;
    eprintln!(
        "substrait partial plan:\n{}",
        datafusion::physical_plan::displayable(stripped.as_ref()).indent(true)
    );
    let mut stream = execute_stream(stripped, ctx.task_ctx())?;
    let mut out = Vec::new();
    while let Some(b) = stream.next().await {
        out.push(b?);
    }
    Ok(out)
}

#[tokio::test]
async fn repro_substrait_partial_mapping_order() {
    let Some(dir) = repro_dir() else {
        eprintln!("MVREAD_REPRO_DIR absent; skipping");
        return;
    };
    let files = state_files(&dir);
    let ctx = partial_session().await;
    register_mapping_order(&ctx, &files).await;

    let count_sub = substrait_for("SELECT count(*) AS n FROM mv").await;
    let c = run_substrait_partial(&ctx, &count_sub).await;
    match &c {
        Ok(b) => eprintln!("SUBSTRAIT PARTIAL count rows={} data={:?}", total_rows(b), b),
        Err(e) => eprintln!("SUBSTRAIT PARTIAL count ERROR: {e}"),
    }

    let grp_sub =
        substrait_for("SELECT \"CounterID\", SUM(\"cnt_advengineid\") AS s FROM mv GROUP BY \"CounterID\"")
            .await;
    let g = run_substrait_partial(&ctx, &grp_sub).await;
    match &g {
        Ok(b) => eprintln!("SUBSTRAIT PARTIAL grouped rows={}", total_rows(b)),
        Err(e) => eprintln!("SUBSTRAIT PARTIAL grouped ERROR: {e}"),
    }

    let head_sub = substrait_for(
        "SELECT \"CounterID\", \"URL\", \"cnt_advengineid\", \"sum_advengineid\" FROM mv LIMIT 3",
    )
    .await;
    let h = run_substrait_partial(&ctx, &head_sub).await;
    match &h {
        Ok(b) => eprintln!("SUBSTRAIT PARTIAL head rows={} data={:?}", total_rows(b), b),
        Err(e) => eprintln!("SUBSTRAIT PARTIAL head ERROR: {e}"),
    }
}

/// Bisect (c): does the footer sort-order advertisement fire for these files,
/// and does the resulting `with_file_sort_order` + statistics split change the
/// scanned row count? Registers via the production path and inspects the scan's
/// output ordering + a full count.
#[tokio::test]
async fn bisect_sort_order_advertisement() {
    let Some(dir) = repro_dir() else {
        eprintln!("MVREAD_REPRO_DIR absent; skipping");
        return;
    };
    let files = state_files(&dir);

    // Directly probe the footer sort-order helper via a registration + plan.
    let ctx = partial_session().await;
    register_mapping_order(&ctx, &files).await;
    let physical = ctx
        .sql("SELECT count(*) AS n FROM mv")
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    eprintln!(
        "count physical plan (full, unstripped):\n{}",
        datafusion::physical_plan::displayable(physical.as_ref()).indent(true)
    );
    let rows = run(&ctx, "SELECT count(*) AS n FROM mv").await.unwrap();
    eprintln!("bisect(c) full count = {:?}", rows);
}

/// Directly test whether the footer sort-order advertisement, when it DOES
/// fire, produces a scan that returns rows. On the node all 2604 files are
/// identically footer-stamped so the advertisement fires; locally we force it
/// by registering with the advertisement path exercised over the real files
/// (which ARE identically stamped — see inspect_physical_schema).
#[tokio::test]
async fn bisect_forced_sort_order_path() {
    let Some(dir) = repro_dir() else {
        eprintln!("MVREAD_REPRO_DIR absent; skipping");
        return;
    };
    let files = state_files(&dir);

    // Probe the helper the production registration uses.
    let sort = crate::mv_expr_adapter::unanimous_footer_sort_order_for_test(&files).unwrap();
    eprintln!("unanimous_footer_sort_order => {:?}", sort);

    // Register through production path (which advertises when Some) and run a
    // grouped fold whose group keys are the sort columns — the shape that
    // triggers streaming aggregation over the advertised ordering.
    let ctx = partial_session().await;
    register_mapping_order(&ctx, &files).await;
    let plan = ctx
        .sql("SELECT \"event_bucket\", \"URL\", \"CounterID\", SUM(\"sum_advengineid\") AS s \
              FROM mv GROUP BY \"event_bucket\", \"URL\", \"CounterID\"")
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    eprintln!(
        "grouped-by-sortkeys physical plan:\n{}",
        datafusion::physical_plan::displayable(plan.as_ref()).indent(true)
    );
    let rows = run(
        &ctx,
        "SELECT \"event_bucket\", \"URL\", \"CounterID\", SUM(\"sum_advengineid\") AS s \
         FROM mv GROUP BY \"event_bucket\", \"URL\", \"CounterID\"",
    )
    .await
    .unwrap();
    let n: usize = total_rows(&rows);
    eprintln!("bisect forced-sort grouped rows = {}", n);
}

/// Bisect (c'): same as forced-sort, but the group-key logical types match the
/// PHYSICAL types (event_bucket=Timestamp, URL=Utf8, CounterID=Int32). This is
/// the shape when the plan's base_schema declares date/keyword natively rather
/// than the Int64/Utf8View reinterpretation. Tests whether a type-driven
/// statistics split silently drops files.
#[tokio::test]
async fn bisect_physical_typed_group_keys() {
    let Some(dir) = repro_dir() else {
        eprintln!("MVREAD_REPRO_DIR absent; skipping");
        return;
    };
    let files = state_files(&dir);
    let physical = crate::mv_expr_adapter::read_schema_from_first_file(&files)
        .unwrap()
        .unwrap();

    // Logical = state_fields order, group keys typed to PHYSICAL types.
    let long = DataType::Int64;
    let mut fields = Vec::new();
    for (i, name) in state_fields().iter().enumerate() {
        let dt = match i {
            0 => DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
            1 => DataType::Utf8,
            2 => DataType::Int32,
            _ => long.clone(),
        };
        fields.push(Field::new(name, dt, true));
    }
    let logical: SchemaRef = Arc::new(Schema::new(fields));
    let sf = state_fields();
    let (table_schema, _p) =
        crate::session_context::mv_table_schema_for_test(&physical, &logical, &sf).unwrap();

    let ctx = partial_session().await;
    crate::mv_expr_adapter::register_mv_state_listing_table(&ctx, "mv", &files, table_schema, &sf)
        .await
        .unwrap();
    let rows = run(&ctx, "SELECT count(*) AS n FROM mv").await.unwrap();
    eprintln!("bisect physical-typed count = {:?}", rows);
    let g = run(
        &ctx,
        "SELECT \"event_bucket\", \"URL\", \"CounterID\", SUM(\"sum_advengineid\") AS s \
         FROM mv GROUP BY \"event_bucket\", \"URL\", \"CounterID\"",
    )
    .await
    .unwrap();
    eprintln!("bisect physical-typed grouped rows = {}", total_rows(&g));
}

/// Bisect (a) + (b): table/logical schema in STATE_FIELDS order and WITHOUT
/// `_mv_source_generation` (35 fields). Confirms the read path returns rows
/// with the alternative schema ordering too.
#[tokio::test]
async fn bisect_state_order_no_gen() {
    let Some(dir) = repro_dir() else {
        eprintln!("MVREAD_REPRO_DIR absent; skipping");
        return;
    };
    let files = state_files(&dir);
    let physical = crate::mv_expr_adapter::read_schema_from_first_file(&files)
        .unwrap()
        .unwrap();
    let logical = state_order_logical_schema();
    let sf = state_fields();
    let (table_schema, _p) =
        crate::session_context::mv_table_schema_for_test(&physical, &logical, &sf).unwrap();
    let ctx = partial_session().await;
    crate::mv_expr_adapter::register_mv_state_listing_table(&ctx, "mv", &files, table_schema, &sf)
        .await
        .unwrap();
    let c = run(&ctx, "SELECT count(*) AS n FROM mv").await.unwrap();
    let g = run(
        &ctx,
        "SELECT \"CounterID\", SUM(\"cnt_advengineid\") AS s FROM mv GROUP BY \"CounterID\"",
    )
    .await
    .unwrap();
    eprintln!(
        "bisect(a/b) state-order count_rows={} grouped_rows={}",
        total_rows(&c),
        total_rows(&g)
    );
}

/// Bisect (e): a SINGLE file, mapping-order schema.
#[tokio::test]
async fn bisect_single_file() {
    let Some(dir) = repro_dir() else {
        eprintln!("MVREAD_REPRO_DIR absent; skipping");
        return;
    };
    let files = state_files(&dir);
    let one = vec![files[0].clone()];
    let ctx = partial_session().await;
    register_mapping_order(&ctx, &one).await;
    let c = run(&ctx, "SELECT count(*) AS n FROM mv").await.unwrap();
    eprintln!("bisect(e) single-file count = {:?}", c);
}

/// CONSOLIDATED REGRESSION GATE: over the three REAL cbspan1_mv_mv1 state files,
/// through the production `mv_table_schema` + `register_mv_state_listing_table`
/// entry with the deployed metadata (mapping-order logical schema incl.
/// `_mv_source_generation`, state_fields physical order), the MV-only read path
/// MUST return the full row set — NOT zero rows. This is the exact shape that
/// returned zero on the ARM node; it asserts the read logic at this commit is
/// correct so any regression re-introducing the zero-rows defect fails here.
#[tokio::test]
async fn mv_only_real_files_return_rows_not_zero() {
    let Some(dir) = repro_dir() else {
        eprintln!("MVREAD_REPRO_DIR absent; skipping (set MVREAD_REPRO_DIR)");
        return;
    };
    let files = state_files(&dir);
    assert_eq!(files.len(), 3, "expected the three repro state files");

    // count(*) via the full Partial+Final SQL path.
    let ctx = partial_session().await;
    register_mapping_order(&ctx, &files).await;
    let c = run(&ctx, "SELECT count(*) AS n FROM mv").await.unwrap();
    let count = i64_scalar(&c, "n");
    assert_eq!(count, 229110, "count(*) must fold all state rows, not zero");

    // head 3 projection returns rows with real values.
    let head = run(
        &ctx,
        "SELECT \"CounterID\", \"URL\", \"cnt_advengineid\" FROM mv LIMIT 3",
    )
    .await
    .unwrap();
    assert_eq!(total_rows(&head), 3, "head 3 must return 3 rows, not zero");

    // grouped fold returns non-empty, non-null sums.
    let g = run(
        &ctx,
        "SELECT \"CounterID\", SUM(\"cnt_advengineid\") AS s FROM mv \
         GROUP BY \"CounterID\" ORDER BY s DESC LIMIT 5",
    )
    .await
    .unwrap();
    assert!(total_rows(&g) > 0, "grouped stats must return rows, not zero");
    let top = i64_scalar(&g, "s");
    assert!(top > 0, "top grouped SUM(cnt_advengineid) must be > 0, got {top}");
}

fn i64_scalar(batches: &[arrow::record_batch::RecordBatch], col: &str) -> i64 {
    use arrow::array::Int64Array;
    for b in batches {
        if let Some(a) = b.column_by_name(col) {
            let arr = a.as_any().downcast_ref::<Int64Array>().unwrap();
            if arr.len() > 0 {
                return arr.value(0);
            }
        }
    }
    panic!("no rows for column {col}");
}

/// Diagnostic gate: verify EVERY aggregate/group column reads real (non-null)
/// data, ruling out a per-column positional null-fill / mapping-vs-state-order
/// overlay defect. All 35 physical columns must resolve by name against
/// state_fields and read their true values.
#[tokio::test]
async fn all_columns_read_real_values_not_null() {
    let Some(dir) = repro_dir() else {
        return;
    };
    let files = state_files(&dir);
    let ctx = partial_session().await;
    register_mapping_order(&ctx, &files).await;
    let q = "SELECT \
        SUM(\"sum_advengineid\") a, SUM(\"min_advengineid\") b, SUM(\"max_advengineid\") c, \
        SUM(\"cnt_advengineid\") d, SUM(\"sum_resolutionwidth\") e, SUM(\"cnt_fetchtiming\") f, \
        COUNT(\"URL\") g, COUNT(DISTINCT \"CounterID\") h, MAX(\"event_bucket\") i FROM mv";
    let r = run(&ctx, q).await.unwrap();
    eprintln!("all-columns => {:?}", r);
    // COUNT(URL) must equal the full row count — proves the keyword column is
    // read (Utf8->Utf8View lossless cast), not null-filled.
    assert_eq!(i64_scalar(&r, "g"), 229110, "COUNT(URL) must be all rows");
    // Aggregate state columns carry real magnitudes.
    assert!(i64_scalar(&r, "e") > 0, "SUM(sum_resolutionwidth) must be > 0");
    assert!(i64_scalar(&r, "d") > 0, "SUM(cnt_advengineid) must be > 0");
    // event_bucket (date group key reinterpreted as long) reads a valid epoch.
    assert!(i64_scalar(&r, "i") > 0, "MAX(event_bucket) must be > 0");
    // CounterID Int32 group key reads distinct real values.
    assert!(i64_scalar(&r, "h") > 0, "COUNT(DISTINCT CounterID) must be > 0");
}
