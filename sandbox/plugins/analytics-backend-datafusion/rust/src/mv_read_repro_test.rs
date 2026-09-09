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
            0 => long.clone(),       // event_bucket (long reinterpretation)
            1 => DataType::Utf8View, // URL
            2 => DataType::Int32,    // CounterID
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
    config
        .options_mut()
        .execution
        .parquet
        .schema_force_view_types = true;
    SessionContext::new_with_config(config)
}

fn total_rows(batches: &[arrow::record_batch::RecordBatch]) -> usize {
    batches.iter().map(|b| b.num_rows()).sum()
}

async fn run(
    ctx: &SessionContext,
    q: &str,
) -> datafusion::error::Result<Vec<arrow::record_batch::RecordBatch>> {
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

    crate::mv_expr_adapter::register_mv_state_listing_table(&ctx, "mv", &files, table_schema, &sf)
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
    partial_session_cfg(false, true, true).await
}

/// Parameterized partial session for bisection.
/// - `pushdown_filters`: production sets this from DatafusionQueryConfig; on the
///   node it is TRUE. The zero-rows defect only surfaces when filters are pushed
///   INTO the parquet reader (so pruning + row-level filtering run against the
///   physical Utf8 column via the MV expr adapter's CAST rewrite).
/// - `force_view`: string_view default in production plans.
/// - `split_stats`: statistics-based file grouping (production sets true).
async fn partial_session_cfg(
    pushdown_filters: bool,
    force_view: bool,
    split_stats: bool,
) -> SessionContext {
    let mut config = SessionConfig::new();
    config
        .options_mut()
        .execution
        .split_file_groups_by_statistics = split_stats;
    config
        .options_mut()
        .execution
        .parquet
        .schema_force_view_types = force_view;
    config.options_mut().execution.parquet.pushdown_filters = pushdown_filters;
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
    let physical = ctx
        .sql(sql)
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
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
    let mem =
        datafusion::datasource::MemTable::try_new(mapping_logical_schema(), vec![vec![]]).unwrap();
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
    let stripped = crate::agg_mode::apply_aggregate_mode(
        physical_plan,
        crate::agg_mode::Mode::Partial,
        false,
    )?;
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
        Ok(b) => eprintln!(
            "SUBSTRAIT PARTIAL count rows={} data={:?}",
            total_rows(b),
            b
        ),
        Err(e) => eprintln!("SUBSTRAIT PARTIAL count ERROR: {e}"),
    }

    let grp_sub = substrait_for(
        "SELECT \"CounterID\", SUM(\"cnt_advengineid\") AS s FROM mv GROUP BY \"CounterID\"",
    )
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
        .sql(
            "SELECT \"event_bucket\", \"URL\", \"CounterID\", SUM(\"sum_advengineid\") AS s \
              FROM mv GROUP BY \"event_bucket\", \"URL\", \"CounterID\"",
        )
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
    assert!(
        total_rows(&g) > 0,
        "grouped stats must return rows, not zero"
    );
    let top = i64_scalar(&g, "s");
    assert!(
        top > 0,
        "top grouped SUM(cnt_advengineid) must be > 0, got {top}"
    );
}

/// Pick a URL value that exists in the fixtures via a grouped fold, so the
/// filter repro uses a real key with a known non-zero row count.
async fn pick_existing_url(ctx: &SessionContext) -> (String, i64) {
    let r = run(
        ctx,
        "SELECT \"URL\" AS u, SUM(\"cnt_advengineid\") AS n FROM mv \
         GROUP BY \"URL\" ORDER BY n DESC LIMIT 5",
    )
    .await
    .unwrap();
    use arrow::array::{Array, Int64Array, StringViewArray};
    for b in &r {
        let ucol = b.column_by_name("u").unwrap();
        let ncol = b
            .column_by_name("n")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        // URL is Utf8View in the mapping-order logical schema.
        if let Some(arr) = ucol.as_any().downcast_ref::<StringViewArray>() {
            for i in 0..arr.len() {
                if !arr.is_null(i) {
                    return (arr.value(i).to_string(), ncol.value(i));
                }
            }
        }
    }
    panic!("no non-null URL found in fixtures");
}

/// THE FILTER REPRO: reproduce the node's zero-rows defect on single-literal
/// comparisons over the keyword group key, and confirm InList(>=2)/OR/LIKE and
/// the Int32 key work. Prints the rewritten physical predicate for each shape.
#[tokio::test]
async fn repro_single_literal_filter_zero_rows() {
    let Some(dir) = repro_dir() else {
        eprintln!("MVREAD_REPRO_DIR absent; skipping");
        return;
    };
    let files = state_files(&dir);
    let ctx = partial_session().await;
    register_mapping_order(&ctx, &files).await;

    let (url, expected) = pick_existing_url(&ctx).await;
    eprintln!("chosen URL={url:?} expected cnt_advengineid sum={expected}");

    let esc = url.replace('\'', "''");
    let cases: Vec<(&str, String)> = vec![
        ("eq", format!("\"URL\" = '{esc}'")),
        (
            "range",
            format!("\"URL\" >= '{esc}' AND \"URL\" <= '{esc}'"),
        ),
        ("in1", format!("\"URL\" IN ('{esc}')")),
        ("in2", format!("\"URL\" IN ('{esc}', 'zzz')")),
        ("or", format!("\"URL\" = '{esc}' OR \"URL\" = 'zzz'")),
        ("like", format!("\"URL\" LIKE '{esc}'")),
        ("counterid", "\"CounterID\" = 62".to_string()),
    ];

    for (label, pred) in &cases {
        let sql = format!("SELECT SUM(\"cnt_advengineid\") AS n FROM mv WHERE {pred}");
        // Print the physical plan (shows the rewritten predicate the scan uses).
        let plan = ctx
            .sql(&sql)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        eprintln!(
            "[{label}] physical plan:\n{}",
            datafusion::physical_plan::displayable(plan.as_ref()).indent(true)
        );
        let r = run(&ctx, &sql).await.unwrap();
        let n = if total_rows(&r) == 0 {
            0
        } else {
            // n may be null if no rows matched the aggregate.
            use arrow::array::{Array, Int64Array};
            r[0].column_by_name("n")
                .and_then(|a| a.as_any().downcast_ref::<Int64Array>())
                .filter(|a| a.len() > 0 && !a.is_null(0))
                .map(|a| a.value(0))
                .unwrap_or(0)
        };
        eprintln!("[{label}] pred=[{pred}] => n={n}");
    }

    // The node uses pushdown_filters=TRUE (from DatafusionQueryConfig). Bisect
    // (a): re-run with pushdown_filters=TRUE — filters are pushed INTO the
    // parquet reader, where pruning + the row-level filter run against the
    // physical Utf8 column through the MV expr adapter's CAST-to-Utf8View
    // rewrite. This is the config that reproduces the node's zero-rows defect.
    eprintln!("===== pushdown_filters=TRUE (node config) =====");
    let ctx = partial_session_cfg(true, true, true).await;
    register_mapping_order(&ctx, &files).await;
    for (label, pred) in &cases {
        let sql = format!("SELECT SUM(\"cnt_advengineid\") AS n FROM mv WHERE {pred}");
        let plan = ctx
            .sql(&sql)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        if *label == "eq" {
            eprintln!(
                "[pushdown][{label}] physical plan:\n{}",
                datafusion::physical_plan::displayable(plan.as_ref()).indent(true)
            );
        }
        let r = run(&ctx, &sql).await.unwrap();
        let n = if total_rows(&r) == 0 {
            0
        } else {
            use arrow::array::{Array, Int64Array};
            r[0].column_by_name("n")
                .and_then(|a| a.as_any().downcast_ref::<Int64Array>())
                .filter(|a| a.len() > 0 && !a.is_null(0))
                .map(|a| a.value(0))
                .unwrap_or(0)
        };
        eprintln!("[pushdown][{label}] pred=[{pred}] => n={n}");
    }
}

/// SUBSTRAIT FILTER REPRO: drive the single-literal filter forms through the
/// REAL Substrait partial path (the exact node path: PPL -> Substrait ->
/// prepare_partial_plan). Substrait column refs are positional against the
/// base_schema (URL = Utf8View). This is the path that models how the literal
/// and column types are reconstructed on the shard.
#[tokio::test]
async fn repro_substrait_filter_single_literal() {
    let Some(dir) = repro_dir() else {
        eprintln!("MVREAD_REPRO_DIR absent; skipping");
        return;
    };
    let files = state_files(&dir);

    // First discover an existing URL via a plain grouped fold.
    let probe = partial_session().await;
    register_mapping_order(&probe, &files).await;
    let (url, expected) = pick_existing_url(&probe).await;
    eprintln!("SUBSTRAIT chosen URL={url:?} expected={expected}");
    let esc = url.replace('\'', "''");

    let cases: Vec<(&str, String)> = vec![
        ("eq", format!("\"URL\" = '{esc}'")),
        (
            "range",
            format!("\"URL\" >= '{esc}' AND \"URL\" <= '{esc}'"),
        ),
        ("in1", format!("\"URL\" IN ('{esc}')")),
        ("in2", format!("\"URL\" IN ('{esc}', 'zzz')")),
        ("or", format!("\"URL\" = '{esc}' OR \"URL\" = 'zzz'")),
        ("like", format!("\"URL\" LIKE '{esc}'")),
        ("counterid", "\"CounterID\" = 62".to_string()),
    ];

    for pushdown in [false, true] {
        eprintln!("===== SUBSTRAIT pushdown_filters={pushdown} =====");
        for (label, pred) in &cases {
            let sql = format!("SELECT SUM(\"cnt_advengineid\") AS n FROM mv WHERE {pred}");
            let sub = substrait_for(&sql).await;
            let ctx = partial_session_cfg(pushdown, true, true).await;
            register_mapping_order(&ctx, &files).await;
            let r = run_substrait_partial(&ctx, &sub).await;
            match r {
                Ok(b) => eprintln!(
                    "[sub pushdown={pushdown}][{label}] partial_rows={} pred=[{pred}]",
                    total_rows(&b)
                ),
                Err(e) => eprintln!("[sub pushdown={pushdown}][{label}] ERROR pred=[{pred}]: {e}"),
            }
        }
    }
}

/// Diagnostic: does the URL column in the real state files carry a Bloom
/// filter and/or a page index? The node's failing-vs-working filter signature
/// (=, single-IN, range fail; multi-IN, OR, LIKE work) matches bloom-filter
/// pruning with a type-mismatched probe. Desktop files may lack a bloom filter,
/// which would explain why the desktop cannot reproduce.
#[tokio::test]
async fn inspect_bloom_and_page_index() {
    let Some(dir) = repro_dir() else {
        eprintln!("MVREAD_REPRO_DIR absent; skipping");
        return;
    };
    use parquet::file::reader::{FileReader, SerializedFileReader};
    for f in state_files(&dir) {
        let file = std::fs::File::open(&f).unwrap();
        let r = SerializedFileReader::new(file).unwrap();
        let meta = r.metadata();
        for rg in 0..meta.num_row_groups() {
            let rgm = meta.row_group(rg);
            // URL is physical column 1.
            let col = rgm.column(1);
            eprintln!(
                "file={} rg={} col=URL bloom_offset={:?} bloom_len={:?} col_index_offset={:?} offset_index_offset={:?} stats={:?}",
                std::path::Path::new(&f).file_name().unwrap().to_str().unwrap(),
                rg,
                col.bloom_filter_offset(),
                col.bloom_filter_length(),
                col.column_index_offset(),
                col.offset_index_offset(),
                col.statistics().map(|s| format!("{:?}", s)),
            );
        }
    }
}

/// SYNTHETIC BLOOM REPRO: write a Utf8 URL column WITH a Parquet bloom filter
/// (as the node's build/merge writers do), register through the production
/// MV path with a Utf8View logical schema, and probe equality with
/// bloom_filter_on_read enabled. If the bloom probe hashes the Utf8View literal
/// differently than the Utf8 values that populated the filter, the row group is
/// wrongly pruned and equality returns ZERO rows — the node's defect signature.
#[tokio::test]
async fn repro_bloom_filter_utf8view_equality() {
    use arrow::array::{Int64Array, RecordBatch, StringArray};
    use parquet::arrow::ArrowWriter;
    use parquet::basic::Encoding;
    use parquet::file::properties::WriterProperties;

    let dir = tempfile::TempDir::new().unwrap();

    // Physical schema: URL is Utf8 (writer alias), plus one agg column.
    let physical: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("URL", DataType::Utf8, false),
        Field::new("cnt_advengineid", DataType::Int64, true),
    ]));
    let urls: Vec<&str> = vec![
        "http://a.example/1",
        "http://kinopoisk.ru",
        "http://z.example/9",
        "http://kinopoisk.ru",
    ];
    let batch = RecordBatch::try_new(
        Arc::clone(&physical),
        vec![
            Arc::new(StringArray::from(urls)),
            Arc::new(Int64Array::from(vec![100_i64, 1_625_250, 5, 0])),
        ],
    )
    .unwrap();
    let path = dir.path().join("_mv_partial.s0.t1.g0.bloom.parquet");
    let props = WriterProperties::builder()
        .set_bloom_filter_enabled(true)
        .set_column_bloom_filter_enabled("URL".into(), true)
        .set_dictionary_enabled(false)
        .set_encoding(Encoding::PLAIN)
        .build();
    {
        let file = std::fs::File::create(&path).unwrap();
        let mut w = ArrowWriter::try_new(file, Arc::clone(&physical), Some(props)).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
    }
    let files = vec![path.to_str().unwrap().to_string()];

    // Logical schema: URL as Utf8View (the deployed plan's type).
    let logical: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("URL", DataType::Utf8View, true),
        Field::new("cnt_advengineid", DataType::Int64, true),
    ]));
    let sf = vec!["URL".to_string(), "cnt_advengineid".to_string()];

    for bloom_on in [false, true] {
        let mut config = SessionConfig::new();
        config.options_mut().execution.parquet.pushdown_filters = true;
        config.options_mut().execution.parquet.bloom_filter_on_read = bloom_on;
        config
            .options_mut()
            .execution
            .parquet
            .schema_force_view_types = true;
        let ctx = SessionContext::new_with_config(config);
        let (ts, _p) =
            crate::session_context::mv_table_schema_for_test(&physical, &logical, &sf).unwrap();
        crate::mv_expr_adapter::register_mv_state_listing_table(&ctx, "mv", &files, ts, &sf)
            .await
            .unwrap();
        let r = run(
            &ctx,
            "SELECT SUM(\"cnt_advengineid\") AS n FROM mv WHERE \"URL\" = 'http://kinopoisk.ru'",
        )
        .await
        .unwrap();
        let n = {
            use arrow::array::{Array, Int64Array};
            r.first()
                .and_then(|b| b.column_by_name("n"))
                .and_then(|a| a.as_any().downcast_ref::<Int64Array>())
                .filter(|a| a.len() > 0 && !a.is_null(0))
                .map(|a| a.value(0))
                .unwrap_or(0)
        };
        eprintln!("[bloom_on={bloom_on}] URL='http://kinopoisk.ru' => n={n} (expect 1625250)");
    }
}

/// SYNTHETIC PAGE-INDEX REPRO: many long URLs (statistics truncation kicks in)
/// across multiple small data pages, written Utf8, queried Utf8View with page
/// index pruning enabled and pushdown on. Truncated page column-index bounds
/// combined with the CAST(URL AS Utf8View) pruning predicate can wrongly prune
/// the page holding the search key — the node's zero-rows signature.
#[tokio::test]
async fn repro_page_index_truncated_stats_utf8view() {
    use arrow::array::{Int64Array, RecordBatch, StringArray};
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;

    let dir = tempfile::TempDir::new().unwrap();
    let physical: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("URL", DataType::Utf8, false),
        Field::new("cnt_advengineid", DataType::Int64, true),
    ]));

    // Build many long, sorted URLs sharing a long common prefix so truncated
    // (short) page boundaries collapse to identical values — the pruning
    // hazard. Insert the target key in the middle.
    let prefix = "http://very-long-common-prefix.example.com/path/segment/that/exceeds/truncation/limit?query=";
    let target = format!("{prefix}kinopoisk.ru");
    let mut rows: Vec<(String, i64)> = Vec::new();
    for i in 0..2000 {
        rows.push((format!("{prefix}{:05}", i), 1));
    }
    rows.push((target.clone(), 1_625_250));
    rows.sort_by(|a, b| a.0.cmp(&b.0));
    let urls: Vec<&str> = rows.iter().map(|r| r.0.as_str()).collect();
    let cnts: Vec<i64> = rows.iter().map(|r| r.1).collect();
    let batch = RecordBatch::try_new(
        Arc::clone(&physical),
        vec![
            Arc::new(StringArray::from(urls)),
            Arc::new(Int64Array::from(cnts)),
        ],
    )
    .unwrap();

    let path = dir.path().join("_mv_partial.s0.t1.g0.pageidx.parquet");
    let props = WriterProperties::builder()
        .set_statistics_enabled(parquet::file::properties::EnabledStatistics::Page)
        .set_statistics_truncate_length(Some(24)) // truncate row-group stats
        .set_column_index_truncate_length(Some(24)) // truncate page column-index
        .set_data_page_row_count_limit(128) // many small pages
        .set_write_batch_size(128)
        .build();
    {
        let file = std::fs::File::create(&path).unwrap();
        let mut w = ArrowWriter::try_new(file, Arc::clone(&physical), Some(props)).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
    }
    let files = vec![path.to_str().unwrap().to_string()];

    let logical: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("URL", DataType::Utf8View, true),
        Field::new("cnt_advengineid", DataType::Int64, true),
    ]));
    let sf = vec!["URL".to_string(), "cnt_advengineid".to_string()];

    let mut config = SessionConfig::new();
    config.options_mut().execution.parquet.pushdown_filters = true;
    config
        .options_mut()
        .execution
        .parquet
        .schema_force_view_types = true;
    config.options_mut().execution.parquet.enable_page_index = true;
    let ctx = SessionContext::new_with_config(config);
    let (ts, _p) =
        crate::session_context::mv_table_schema_for_test(&physical, &logical, &sf).unwrap();
    crate::mv_expr_adapter::register_mv_state_listing_table(&ctx, "mv", &files, ts, &sf)
        .await
        .unwrap();
    let sql = format!("SELECT SUM(\"cnt_advengineid\") AS n FROM mv WHERE \"URL\" = '{target}'");
    let r = run(&ctx, &sql).await.unwrap();
    let n = {
        use arrow::array::{Array, Int64Array};
        r.first()
            .and_then(|b| b.column_by_name("n"))
            .and_then(|a| a.as_any().downcast_ref::<Int64Array>())
            .filter(|a| a.len() > 0 && !a.is_null(0))
            .map(|a| a.value(0))
            .unwrap_or(0)
    };
    eprintln!("[page_index] URL=target => n={n} (expect 1625250)");
}

/// SYNTHETIC MULTI-ROW-GROUP REPRO: the desktop 3 files each have ONE row group
/// spanning the full URL range, so row-group pruning never eliminates anything.
/// The node's 2604 files have many row groups whose Utf8 min/max are narrow.
/// Force many small row groups so pruning actually runs, register with a
/// Utf8View logical schema + pushdown, and probe equality on a mid-range key.
#[tokio::test]
async fn repro_multi_row_group_pruning_utf8view() {
    use arrow::array::{Int64Array, RecordBatch, StringArray};
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;

    let dir = tempfile::TempDir::new().unwrap();
    let physical: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("URL", DataType::Utf8, false),
        Field::new("cnt_advengineid", DataType::Int64, true),
    ]));

    let target = "http://kinopoisk.ru";
    let mut rows: Vec<(String, i64)> = Vec::new();
    for i in 0..5000 {
        // Spread values around the target so it lands in a middle row group.
        rows.push((format!("http://site{:05}.example/page", i), 1));
    }
    rows.push((target.to_string(), 1_625_250));
    rows.sort_by(|a, b| a.0.cmp(&b.0));
    let urls: Vec<&str> = rows.iter().map(|r| r.0.as_str()).collect();
    let cnts: Vec<i64> = rows.iter().map(|r| r.1).collect();
    let batch = RecordBatch::try_new(
        Arc::clone(&physical),
        vec![
            Arc::new(StringArray::from(urls)),
            Arc::new(Int64Array::from(cnts)),
        ],
    )
    .unwrap();

    let path = dir.path().join("_mv_partial.s0.t1.g0.rgs.parquet");
    let props = WriterProperties::builder()
        .set_max_row_group_size(256) // many row groups
        .set_statistics_truncate_length(Some(16))
        .set_column_index_truncate_length(Some(16))
        .build();
    {
        let file = std::fs::File::create(&path).unwrap();
        let mut w = ArrowWriter::try_new(file, Arc::clone(&physical), Some(props)).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
    }
    let files = vec![path.to_str().unwrap().to_string()];

    let logical: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("URL", DataType::Utf8View, true),
        Field::new("cnt_advengineid", DataType::Int64, true),
    ]));
    let sf = vec!["URL".to_string(), "cnt_advengineid".to_string()];

    let mut config = SessionConfig::new();
    config.options_mut().execution.parquet.pushdown_filters = true;
    config
        .options_mut()
        .execution
        .parquet
        .schema_force_view_types = true;
    config.options_mut().execution.parquet.enable_page_index = true;
    let ctx = SessionContext::new_with_config(config);
    let (ts, _p) =
        crate::session_context::mv_table_schema_for_test(&physical, &logical, &sf).unwrap();
    crate::mv_expr_adapter::register_mv_state_listing_table(&ctx, "mv", &files, ts, &sf)
        .await
        .unwrap();
    let r = run(
        &ctx,
        "SELECT SUM(\"cnt_advengineid\") AS n FROM mv WHERE \"URL\" = 'http://kinopoisk.ru'",
    )
    .await
    .unwrap();
    let n = {
        use arrow::array::{Array, Int64Array};
        r.first()
            .and_then(|b| b.column_by_name("n"))
            .and_then(|a| a.as_any().downcast_ref::<Int64Array>())
            .filter(|a| a.len() > 0 && !a.is_null(0))
            .map(|a| a.value(0))
            .unwrap_or(0)
    };
    eprintln!("[multi_rg] URL='http://kinopoisk.ru' => n={n} (expect 1625250)");
}

/// Bisect the FULL production interaction over the real files: sort-order
/// advertisement (fires) + split_file_groups_by_statistics + pushdown, with the
/// single-equality filter on URL. This is the exact node config. Prints the
/// plan and the row count for each failing/working filter form.
#[tokio::test]
async fn bisect_real_files_equality_with_sort_order() {
    let Some(dir) = repro_dir() else {
        eprintln!("MVREAD_REPRO_DIR absent; skipping");
        return;
    };
    let files = state_files(&dir);
    let probe = partial_session().await;
    register_mapping_order(&probe, &files).await;
    let (url, expected) = pick_existing_url(&probe).await;
    eprintln!("chosen URL={url:?} expected={expected}");
    let esc = url.replace('\'', "''");

    // Exact node config: pushdown TRUE, force_view TRUE, split_stats TRUE.
    let ctx = partial_session_cfg(true, true, true).await;
    register_mapping_order(&ctx, &files).await;

    for (label, pred) in [
        ("eq", format!("\"URL\" = '{esc}'")),
        ("in1", format!("\"URL\" IN ('{esc}')")),
        ("or", format!("\"URL\" = '{esc}' OR \"URL\" = 'zzz'")),
    ] {
        let sql = format!(
            "SELECT \"URL\", SUM(\"cnt_advengineid\") AS n FROM mv WHERE {pred} GROUP BY \"URL\""
        );
        let plan = ctx
            .sql(&sql)
            .await
            .unwrap()
            .create_physical_plan()
            .await
            .unwrap();
        eprintln!(
            "[{label}] plan:\n{}",
            datafusion::physical_plan::displayable(plan.as_ref()).indent(true)
        );
        let r = run(&ctx, &sql).await.unwrap();
        eprintln!("[{label}] rows={} pred=[{pred}]", total_rows(&r));
    }
}

/// ROOT-CAUSE REPRO: two overlapping MV generations, each internally sorted by
/// (event_bucket, URL), forced into ONE file group. The footer advertises a
/// (event_bucket, URL) sort order, which DataFusion trusts as the SCAN's
/// output_ordering. But concatenating two independently-sorted generations is
/// NOT globally sorted by URL for a fixed event_bucket, so a single-equality
/// filter on URL (which drives `ordering_mode=Sorted` streaming aggregation and
/// sort-based row selection) can miss rows and return ZERO — the node's defect.
///
/// This is the shape the 3 desktop fixtures do NOT have (their event_bucket
/// ranges keep the target key inside one group), so it is the missing repro.
#[tokio::test]
async fn repro_overlapping_generations_false_sort_order() {
    use arrow::array::{Int64Array, RecordBatch, StringArray};
    use parquet::arrow::ArrowWriter;
    use parquet::file::metadata::SortingColumn;
    use parquet::file::properties::WriterProperties;

    let dir = tempfile::TempDir::new().unwrap();
    // Physical schema: (event_bucket Int64, URL Utf8, cnt Int64).
    let physical: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("event_bucket", DataType::Int64, false),
        Field::new("URL", DataType::Utf8, false),
        Field::new("cnt_advengineid", DataType::Int64, true),
    ]));

    // Both generations share the SAME event_bucket range [1..3] (overlapping),
    // each internally sorted by (event_bucket, URL). The target URL appears in
    // BOTH generations. Concatenating them is NOT globally sorted by URL within
    // a bucket.
    let write = |name: &str, buckets: Vec<i64>, urls: Vec<&str>, cnts: Vec<i64>| -> String {
        let batch = RecordBatch::try_new(
            Arc::clone(&physical),
            vec![
                Arc::new(Int64Array::from(buckets)),
                Arc::new(StringArray::from(urls)),
                Arc::new(Int64Array::from(cnts)),
            ],
        )
        .unwrap();
        let sorting = vec![
            SortingColumn {
                column_idx: 0,
                descending: false,
                nulls_first: false,
            },
            SortingColumn {
                column_idx: 1,
                descending: false,
                nulls_first: false,
            },
        ];
        let props = WriterProperties::builder()
            .set_sorting_columns(Some(sorting))
            .build();
        let path = dir.path().join(name);
        let file = std::fs::File::create(&path).unwrap();
        let mut w = ArrowWriter::try_new(file, Arc::clone(&physical), Some(props)).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        path.to_str().unwrap().to_string()
    };

    // Gen A: bucket 1 -> aaa, kinopoisk ; bucket 2 -> zzz
    let a = write(
        "_mv_partial.s0.t1.g0.aaaa.parquet",
        vec![1, 1, 2],
        vec!["aaa", "http://kinopoisk.ru", "zzz"],
        vec![1, 1_000_000, 1],
    );
    // Gen B: bucket 1 -> bbb, kinopoisk ; bucket 2 -> yyy  (also sorted internally)
    let b = write(
        "_mv_partial.s0.t1.g1.bbbb.parquet",
        vec![1, 1, 2],
        vec!["bbb", "http://kinopoisk.ru", "yyy"],
        vec![1, 625_250, 1],
    );
    let files = vec![a, b];

    let logical: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("event_bucket", DataType::Int64, true),
        Field::new("URL", DataType::Utf8View, true),
        Field::new("cnt_advengineid", DataType::Int64, true),
    ]));
    let sf = vec![
        "event_bucket".to_string(),
        "URL".to_string(),
        "cnt_advengineid".to_string(),
    ];

    // Node config: pushdown + force_view + split_stats + target_partitions=1 so
    // the two generations are CHAINED into one file group (the hazard).
    let mut config = SessionConfig::new();
    config.options_mut().execution.parquet.pushdown_filters = true;
    config
        .options_mut()
        .execution
        .parquet
        .schema_force_view_types = true;
    config
        .options_mut()
        .execution
        .split_file_groups_by_statistics = true;
    config.options_mut().execution.target_partitions = 1;
    let state = datafusion::execution::session_state::SessionStateBuilder::new()
        .with_config(config)
        .with_default_features()
        .with_physical_optimizer_rules(crate::agg_mode::physical_optimizer_rules_without_combine())
        .build();
    let ctx = SessionContext::new_with_state(state);
    let (ts, _p) =
        crate::session_context::mv_table_schema_for_test(&physical, &logical, &sf).unwrap();
    crate::mv_expr_adapter::register_mv_state_listing_table(&ctx, "mv", &files, ts, &sf)
        .await
        .unwrap();

    let plan = ctx
        .sql("SELECT SUM(\"cnt_advengineid\") AS n FROM mv WHERE \"URL\" = 'http://kinopoisk.ru'")
        .await
        .unwrap()
        .create_physical_plan()
        .await
        .unwrap();
    eprintln!(
        "[overlap] plan:\n{}",
        datafusion::physical_plan::displayable(plan.as_ref()).indent(true)
    );
    let r = run(
        &ctx,
        "SELECT SUM(\"cnt_advengineid\") AS n FROM mv WHERE \"URL\" = 'http://kinopoisk.ru'",
    )
    .await
    .unwrap();
    let n = {
        use arrow::array::{Array, Int64Array};
        r.first()
            .and_then(|b| b.column_by_name("n"))
            .and_then(|a| a.as_any().downcast_ref::<Int64Array>())
            .filter(|a| a.len() > 0 && !a.is_null(0))
            .map(|a| a.value(0))
            .unwrap_or(0)
    };
    eprintln!("[overlap] URL='http://kinopoisk.ru' => n={n} (expect 1625250)");
}

/// BISECT (e): the target key is the LARGEST value in its row group and long
/// enough that statistics truncation shortens the row-group max. Run the SAME
/// fixture with URL logical type Utf8 (no view) and Utf8View, with pushdown, to
/// isolate whether the Utf8->Utf8View cast changes pruning of a truncated max.
#[tokio::test]
async fn bisect_truncated_max_utf8_vs_utf8view() {
    use arrow::array::{Int64Array, RecordBatch, StringArray};
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;

    let dir = tempfile::TempDir::new().unwrap();
    let physical: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("URL", DataType::Utf8, false),
        Field::new("cnt_advengineid", DataType::Int64, true),
    ]));

    // Long shared prefix; the target sorts LAST and is long, so a short
    // truncated row-group max may fall BELOW the target's full value.
    let base = "http://common-prefix-that-is-quite-long.example.org/resource/item?id=";
    let target = format!("{base}zzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzzz-KINOPOISK");
    let mut rows: Vec<(String, i64)> = Vec::new();
    for i in 0..1000 {
        rows.push((format!("{base}{:04}", i), 1));
    }
    rows.push((target.clone(), 1_625_250));
    rows.sort_by(|a, b| a.0.cmp(&b.0));
    let urls: Vec<&str> = rows.iter().map(|r| r.0.as_str()).collect();
    let cnts: Vec<i64> = rows.iter().map(|r| r.1).collect();
    let batch = RecordBatch::try_new(
        Arc::clone(&physical),
        vec![
            Arc::new(StringArray::from(urls)),
            Arc::new(Int64Array::from(cnts)),
        ],
    )
    .unwrap();
    let path = dir.path().join("_mv_partial.s0.t1.g0.trunc.parquet");
    let props = WriterProperties::builder()
        .set_max_row_group_size(256)
        .set_statistics_truncate_length(Some(40))
        .set_column_index_truncate_length(Some(40))
        .build();
    {
        let file = std::fs::File::create(&path).unwrap();
        let mut w = ArrowWriter::try_new(file, Arc::clone(&physical), Some(props)).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
    }
    let files = vec![path.to_str().unwrap().to_string()];
    let sf = vec!["URL".to_string(), "cnt_advengineid".to_string()];

    for view in [false, true] {
        let url_ty = if view {
            DataType::Utf8View
        } else {
            DataType::Utf8
        };
        let logical: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("URL", url_ty, true),
            Field::new("cnt_advengineid", DataType::Int64, true),
        ]));
        let mut config = SessionConfig::new();
        config.options_mut().execution.parquet.pushdown_filters = true;
        config
            .options_mut()
            .execution
            .parquet
            .schema_force_view_types = view;
        config.options_mut().execution.parquet.enable_page_index = true;
        let ctx = SessionContext::new_with_config(config);
        let (ts, _p) =
            crate::session_context::mv_table_schema_for_test(&physical, &logical, &sf).unwrap();
        crate::mv_expr_adapter::register_mv_state_listing_table(&ctx, "mv", &files, ts, &sf)
            .await
            .unwrap();
        let sql =
            format!("SELECT SUM(\"cnt_advengineid\") AS n FROM mv WHERE \"URL\" = '{target}'");
        let r = run(&ctx, &sql).await.unwrap();
        let n = {
            use arrow::array::{Array, Int64Array};
            r.first()
                .and_then(|b| b.column_by_name("n"))
                .and_then(|a| a.as_any().downcast_ref::<Int64Array>())
                .filter(|a| a.len() > 0 && !a.is_null(0))
                .map(|a| a.value(0))
                .unwrap_or(0)
        };
        eprintln!("[trunc view={view}] => n={n} (expect 1625250)");
    }
}

/// Exhaustive real-file check: for MANY existing URLs (not just the top one),
/// assert single-equality equals the grouped-fold value. If ANY existing URL
/// returns zero under equality while its grouped value is non-zero, that is the
/// defect reproduced. Runs under the exact node config (pushdown+view+split).
#[tokio::test]
async fn real_files_equality_matches_grouped_for_many_urls() {
    let Some(dir) = repro_dir() else {
        eprintln!("MVREAD_REPRO_DIR absent; skipping");
        return;
    };
    let files = state_files(&dir);
    let ctx = partial_session_cfg(true, true, true).await;
    register_mapping_order(&ctx, &files).await;

    // Ground truth: grouped fold over URL.
    let g = run(
        &ctx,
        "SELECT \"URL\" AS u, SUM(\"cnt_advengineid\") AS n FROM mv \
         GROUP BY \"URL\" HAVING SUM(\"cnt_advengineid\") > 0 ORDER BY n DESC LIMIT 40",
    )
    .await
    .unwrap();
    use arrow::array::{Array, Int64Array, StringViewArray};
    let mut checked = 0;
    let mut mismatches = 0;
    for b in &g {
        let ucol = b.column_by_name("u").unwrap();
        let ncol = b
            .column_by_name("n")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let uarr = ucol.as_any().downcast_ref::<StringViewArray>().unwrap();
        for i in 0..uarr.len() {
            if uarr.is_null(i) {
                continue;
            }
            let url = uarr.value(i).to_string();
            let expected = ncol.value(i);
            let esc = url.replace('\'', "''");
            let r = run(
                &ctx,
                &format!("SELECT SUM(\"cnt_advengineid\") AS n FROM mv WHERE \"URL\" = '{esc}'"),
            )
            .await
            .unwrap();
            let got = r
                .first()
                .and_then(|b| b.column_by_name("n"))
                .and_then(|a| a.as_any().downcast_ref::<Int64Array>())
                .filter(|a| a.len() > 0 && !a.is_null(0))
                .map(|a| a.value(0))
                .unwrap_or(0);
            checked += 1;
            if got != expected {
                mismatches += 1;
                eprintln!("MISMATCH url={url:?} eq={got} grouped={expected}");
            }
        }
    }
    eprintln!("checked {checked} URLs, {mismatches} mismatches");
    assert_eq!(
        mismatches, 0,
        "single-equality must match grouped fold for every URL"
    );
}

/// BISECT (d): directly exercise `MvPhysicalExprAdapter::rewrite` on the exact
/// physical predicate DataFusion pushes for `URL = 'x'`, using the real physical
/// schema (URL Utf8 at pos 1) and the mapping-order logical schema (URL Utf8View
/// at logical index 1, `_mv_source_generation` at index 2). Confirms the rewrite
/// produces `CAST(URL@1 AS Utf8View) = Utf8View('x')` and NOT a null literal or
/// a wrong physical position.
#[tokio::test]
async fn bisect_adapter_rewrites_url_equality() {
    let Some(dir) = repro_dir() else {
        eprintln!("MVREAD_REPRO_DIR absent; skipping");
        return;
    };
    let files = state_files(&dir);
    let physical = crate::mv_expr_adapter::read_schema_from_first_file(&files)
        .unwrap()
        .unwrap();
    let logical = mapping_logical_schema();
    let sf = state_fields();

    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{lit, BinaryExpr, Column};
    use datafusion::physical_expr_adapter::PhysicalExprAdapterFactory;
    use datafusion::physical_plan::PhysicalExpr;

    let factory = crate::mv_expr_adapter::MvPhysicalExprAdapterFactory::new(sf.clone());
    let adapter = factory
        .create(Arc::clone(&logical), Arc::clone(&physical))
        .unwrap();

    // URL is logical index 1 in mapping order.
    let url_idx = logical.index_of("URL").unwrap();
    let expr: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
        Arc::new(Column::new("URL", url_idx)),
        Operator::Eq,
        lit("http://kinopoisk.ru"),
    ));
    let rewritten = adapter.rewrite(expr).unwrap();
    eprintln!("[adapter] URL={url_idx} rewritten = {rewritten:?}");

    // CounterID is logical index 0 in mapping order (Int32).
    let cid_idx = logical.index_of("CounterID").unwrap();
    let expr2: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
        Arc::new(Column::new("CounterID", cid_idx)),
        Operator::Eq,
        lit(62_i32),
    ));
    let rw2 = adapter.rewrite(expr2).unwrap();
    eprintln!("[adapter] CounterID={cid_idx} rewritten = {rw2:?}");
}

/// STRONGEST REPRO ATTEMPT: bloom filter + many small row groups + Utf8View
/// logical + pushdown — the node's exact URL column shape. The mismatched-type
/// predicate `CAST(URL AS Utf8View) = Utf8(lit)` drives a bloom probe per row
/// group; a type-confused probe prunes the row group holding the key.
#[tokio::test]
async fn repro_bloom_plus_many_row_groups() {
    use arrow::array::{Int64Array, RecordBatch, StringArray};
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;

    let dir = tempfile::TempDir::new().unwrap();
    let physical: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("URL", DataType::Utf8, false),
        Field::new("cnt_advengineid", DataType::Int64, true),
    ]));
    let target = "http://kinopoisk.ru";
    let mut rows: Vec<(String, i64)> = Vec::new();
    for i in 0..5000 {
        rows.push((format!("http://site{:05}.example/p", i), 1));
    }
    rows.push((target.to_string(), 1_625_250));
    rows.sort_by(|a, b| a.0.cmp(&b.0));
    let urls: Vec<&str> = rows.iter().map(|r| r.0.as_str()).collect();
    let cnts: Vec<i64> = rows.iter().map(|r| r.1).collect();
    let batch = RecordBatch::try_new(
        Arc::clone(&physical),
        vec![
            Arc::new(StringArray::from(urls)),
            Arc::new(Int64Array::from(cnts)),
        ],
    )
    .unwrap();
    let path = dir.path().join("_mv_partial.s0.t1.g0.bloomrg.parquet");
    let props = WriterProperties::builder()
        .set_max_row_group_size(200)
        .set_bloom_filter_enabled(true)
        .set_column_bloom_filter_enabled("URL".into(), true)
        .build();
    {
        let file = std::fs::File::create(&path).unwrap();
        let mut w = ArrowWriter::try_new(file, Arc::clone(&physical), Some(props)).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
    }
    let files = vec![path.to_str().unwrap().to_string()];
    let logical: SchemaRef = Arc::new(Schema::new(vec![
        Field::new("URL", DataType::Utf8View, true),
        Field::new("cnt_advengineid", DataType::Int64, true),
    ]));
    let sf = vec!["URL".to_string(), "cnt_advengineid".to_string()];

    let mut config = SessionConfig::new();
    config.options_mut().execution.parquet.pushdown_filters = true;
    config
        .options_mut()
        .execution
        .parquet
        .schema_force_view_types = true;
    config.options_mut().execution.parquet.bloom_filter_on_read = true;
    config.options_mut().execution.parquet.enable_page_index = true;
    let ctx = SessionContext::new_with_config(config);
    let (ts, _p) =
        crate::session_context::mv_table_schema_for_test(&physical, &logical, &sf).unwrap();
    crate::mv_expr_adapter::register_mv_state_listing_table(&ctx, "mv", &files, ts, &sf)
        .await
        .unwrap();
    let r = run(
        &ctx,
        "SELECT SUM(\"cnt_advengineid\") AS n FROM mv WHERE \"URL\" = 'http://kinopoisk.ru'",
    )
    .await
    .unwrap();
    let n = {
        use arrow::array::{Array, Int64Array};
        r.first()
            .and_then(|b| b.column_by_name("n"))
            .and_then(|a| a.as_any().downcast_ref::<Int64Array>())
            .filter(|a| a.len() > 0 && !a.is_null(0))
            .map(|a| a.value(0))
            .unwrap_or(0)
    };
    eprintln!("[bloom+rg] URL='http://kinopoisk.ru' => n={n} (expect 1625250)");
}

/// REGRESSION GATE (defect: single-literal filter on the MV keyword key returns
/// zero rows). Over the three REAL cbspan1_mv_mv1 state files, through the exact
/// node config (pushdown + string_view + statistics split + partial-agg optimizer),
/// every single-literal filter form on the keyword key (URL) MUST return the same
/// non-zero fold as the grouped-then-filtered ground truth — NOT zero rows:
///   =, range (>= AND <=), single-value IN, multi-value IN, LIKE (exact literal).
/// The Int32 key (CounterID) is covered too. This asserts the well-typed
/// predicate the MV expr adapter now emits (`URL_utf8 = CAST(lit AS Utf8)`, no
/// cast-wrapped column) prunes correctly on the native physical type.
#[tokio::test]
async fn mv_only_single_literal_filters_match_grouped_not_zero() {
    let Some(dir) = repro_dir() else {
        eprintln!("MVREAD_REPRO_DIR absent; skipping (set MVREAD_REPRO_DIR)");
        return;
    };
    let files = state_files(&dir);
    assert_eq!(files.len(), 3, "expected the three repro state files");

    // Exact node config.
    let ctx = partial_session_cfg(true, true, true).await;
    register_mapping_order(&ctx, &files).await;

    // ---- Keyword key (URL, physical Utf8 / logical Utf8View) ----
    // Ground truth: pick an existing URL and its exact grouped fold value.
    let (url, expected) = pick_existing_url(&ctx).await;
    assert!(expected > 0, "chosen URL must have a positive fold");
    let esc = url.replace('\'', "''");

    let scalar = |batches: &[arrow::record_batch::RecordBatch]| -> i64 {
        use arrow::array::{Array, Int64Array};
        batches
            .first()
            .and_then(|b| b.column_by_name("n"))
            .and_then(|a| a.as_any().downcast_ref::<Int64Array>())
            .filter(|a| a.len() > 0 && !a.is_null(0))
            .map(|a| a.value(0))
            .unwrap_or(0)
    };

    for (label, pred) in [
        ("eq", format!("\"URL\" = '{esc}'")),
        (
            "range",
            format!("\"URL\" >= '{esc}' AND \"URL\" <= '{esc}'"),
        ),
        ("in1", format!("\"URL\" IN ('{esc}')")),
        ("in2", format!("\"URL\" IN ('{esc}', 'zzz-nonexistent')")),
        ("like", format!("\"URL\" LIKE '{esc}'")),
    ] {
        let r = run(
            &ctx,
            &format!("SELECT SUM(\"cnt_advengineid\") AS n FROM mv WHERE {pred}"),
        )
        .await
        .unwrap();
        let got = scalar(&r);
        assert_eq!(
            got, expected,
            "[{label}] URL filter must fold to {expected}, got {got} (pred={pred})"
        );
    }

    // ---- Int32 key (CounterID) ----
    // Ground truth from a grouped fold; pick an existing CounterID.
    let g = run(
        &ctx,
        "SELECT \"CounterID\" AS c, SUM(\"cnt_advengineid\") AS n FROM mv \
         GROUP BY \"CounterID\" HAVING SUM(\"cnt_advengineid\") > 0 ORDER BY n DESC LIMIT 1",
    )
    .await
    .unwrap();
    use arrow::array::{Array, Int32Array, Int64Array};
    let cid = g[0]
        .column_by_name("c")
        .unwrap()
        .as_any()
        .downcast_ref::<Int32Array>()
        .unwrap()
        .value(0);
    let cid_expected = g[0]
        .column_by_name("n")
        .unwrap()
        .as_any()
        .downcast_ref::<Int64Array>()
        .unwrap()
        .value(0);
    assert!(cid_expected > 0);

    for (label, pred) in [
        ("eq", format!("\"CounterID\" = {cid}")),
        (
            "range",
            format!("\"CounterID\" >= {cid} AND \"CounterID\" <= {cid}"),
        ),
        ("in1", format!("\"CounterID\" IN ({cid})")),
        ("in2", format!("\"CounterID\" IN ({cid}, -999)")),
    ] {
        let r = run(
            &ctx,
            &format!("SELECT SUM(\"cnt_advengineid\") AS n FROM mv WHERE {pred}"),
        )
        .await
        .unwrap();
        let got = scalar(&r);
        assert_eq!(
            got, cid_expected,
            "[{label}] CounterID filter must fold to {cid_expected}, got {got} (pred={pred})"
        );
    }
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
    assert!(
        i64_scalar(&r, "e") > 0,
        "SUM(sum_resolutionwidth) must be > 0"
    );
    assert!(i64_scalar(&r, "d") > 0, "SUM(cnt_advengineid) must be > 0");
    // event_bucket (date group key reinterpreted as long) reads a valid epoch.
    assert!(i64_scalar(&r, "i") > 0, "MAX(event_bucket) must be > 0");
    // CounterID Int32 group key reads distinct real values.
    assert!(
        i64_scalar(&r, "h") > 0,
        "COUNT(DISTINCT CounterID) must be > 0"
    );
}
