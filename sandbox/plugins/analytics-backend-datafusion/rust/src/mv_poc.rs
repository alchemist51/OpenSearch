/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! POC(mv): self-contained materialized-view build.
//!
//! One blocking call: read a primary parquet file, run the hardcoded MV query
//! stopped at Partial mode, write the state batches to an MV parquet file.
//! Used by the mv-data-format plugin's writer at flush time.
//!
//! Deliberately independent of the runtime manager / ShardView machinery: a
//! private current-thread tokio runtime per call. This is POC-grade — the
//! production build path goes through the engine session + memory pools.

use std::fs::File;
use std::sync::Arc;

use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::{collect, ExecutionPlan};
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use parquet::arrow::ArrowWriter;

/// Shared core: run the definition's Partial stage over one parquet file and
/// sort the resulting state rows by the group key (col 0) so downstream merges
/// are streaming k-way folds (memory ∝ cursors, not groups). Sort cost ∝
/// #groups, never #docs.
fn build_sorted_state(
    input_file: &str,
    table_name: &str,
    sql: &str,
) -> Result<arrow_array::RecordBatch, String> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("mv_poc runtime: {e}"))?;

    rt.block_on(async {
        // Keep the definition at a real Partial aggregate. The default
        // CombinePartialFinalAggregate optimizer can collapse small inputs to
        // Single mode, whose answer-shaped field names differ from the stable
        // state schema consumed by refresh, merge, and finalization paths.
        let state = datafusion::execution::session_state::SessionStateBuilder::new()
            .with_default_features()
            .with_physical_optimizer_rules(
                crate::agg_mode::physical_optimizer_rules_without_combine(),
            )
            .build();
        let ctx = SessionContext::new_with_state(state);
        ctx.register_parquet(table_name, input_file, ParquetReadOptions::default())
            .await
            .map_err(|e| format!("mv_poc register_parquet({input_file}): {e}"))?;
        collect_sorted_partial(&ctx, sql).await
    })
}

/// The shared core: plan `sql` on `ctx`, run its PARTIAL stage, sort by the
/// group key (col 0). Callers differ only in what they registered as input.
async fn collect_sorted_partial(
    ctx: &SessionContext,
    sql: &str,
) -> Result<arrow_array::RecordBatch, String> {
    {
        let df = ctx
            .sql(sql)
            .await
            .map_err(|e| format!("mv_poc plan sql: {e}"))?;
        let physical = df
            .create_physical_plan()
            .await
            .map_err(|e| format!("mv_poc physical plan: {e}"))?;
        let partial = find_partial(&physical)
            .ok_or_else(|| "mv_poc: no Partial aggregate in plan".to_string())?;

        let batches = collect(partial, ctx.task_ctx())
            .await
            .map_err(|e| format!("mv_poc collect partial: {e}"))?;

        // Schema comes from the plan even when batches are empty — callers
        // rely on a well-formed (possibly zero-row) state batch.
        let schema = if batches.is_empty() {
            return Err(
                "mv_poc: partial produced no batches (expected at least an empty batch)"
                    .to_string(),
            );
        } else {
            batches[0].schema()
        };

        let concatenated = arrow::compute::concat_batches(&schema, &batches)
            .map_err(|e| format!("mv_poc concat: {e}"))?;
        let sort_indices = arrow::compute::sort_to_indices(concatenated.column(0), None, None)
            .map_err(|e| format!("mv_poc sort: {e}"))?;
        let sorted_columns: Result<Vec<_>, _> = concatenated
            .columns()
            .iter()
            .map(|c| arrow::compute::take(c.as_ref(), &sort_indices, None))
            .collect();
        arrow_array::RecordBatch::try_new(
            schema,
            sorted_columns.map_err(|e| format!("mv_poc take: {e}"))?,
        )
        .map_err(|e| format!("mv_poc sorted batch: {e}"))
    }
}

/// STATE⊕STATE MERGE (decision 18's "later optimization", code-complete but
/// gated OFF until the generation-watermark orphan sweep lands): folds N
/// group-key-sorted state files into ONE folded state file, without touching
/// the primary. The FOLD SQL runs in PARTIAL mode over the union of the
/// inputs, so the output is again a valid state file — the merge is CLOSED
/// over the state algebra (its output can be merged again, read by the
/// fold reader, everything a build-produced file can).
///
/// Correctness note (why the gate exists): merging state files BAKES IN any
/// orphaned generations (rows shipped for a source generation that later
/// rolled back). The recompute-from-parquet merger is immune (it reads the
/// post-merge document set); this variant may only become the default once
/// the sweep can prove no orphans precede the merge inputs.
pub fn mv_merge_state(
    state_files: &[String],
    fold_sql: &str,
    output_file: &str,
) -> Result<i64, String> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("mv_merge runtime: {e}"))?;

    rt.block_on(async {
        // Single partition + DECLARED per-file sort order: state files are
        // group-key-sorted by construction (the build sorts them for exactly
        // this moment), so registering them as ONE multi-path ListingTable
        // with file_sort_order = [group_key ASC] lets DataFusion plan the
        // streaming shape — SortPreservingMergeExec across the k file
        // streams feeding an ordered-input aggregate (memory ∝ k cursors,
        // not ∝ groups) — instead of a hash aggregate over the union.
        let config = datafusion::execution::context::SessionConfig::new().with_target_partitions(1);
        let ctx = SessionContext::new_with_config(config);
        use datafusion::datasource::file_format::arrow::ArrowFormat;
        use datafusion::datasource::listing::{
            ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
        };
        let urls: Vec<ListingTableUrl> = state_files
            .iter()
            .map(|f| ListingTableUrl::parse(f).map_err(|e| format!("mv_merge url {f}: {e}")))
            .collect::<Result<_, String>>()?;
        let mut listing_options =
            ListingOptions::new(std::sync::Arc::new(ArrowFormat)).with_file_extension(".arrow");
        let schema = listing_options
            .infer_schema(&ctx.state(), &urls[0])
            .await
            .map_err(|e| format!("mv_merge infer schema: {e}"))?;
        // Group key = column 0 by the state contract; sorted ascending,
        // nulls first (matches sort_to_indices defaults at build time).
        let group_key = schema.field(0).name().clone();
        if let Some(sort_exprs) =
            crate::session_context::build_file_sort_order(&[group_key], &["asc".to_string()])
        {
            listing_options = listing_options.with_file_sort_order(vec![sort_exprs]);
        }
        let table_config = ListingTableConfig::new_with_multi_paths(urls)
            .with_listing_options(listing_options)
            .with_schema(schema);
        let table =
            ListingTable::try_new(table_config).map_err(|e| format!("mv_merge table: {e}"))?;
        ctx.register_table("mv_input", std::sync::Arc::new(table))
            .map_err(|e| format!("mv_merge register: {e}"))?;

        // Run the fold to COMPLETION (not partial-only): fold definitions are
        // pure SUM/MIN/MAX by the mergeable-core rule, so their FINAL output
        // is numerically identical to fully-combined state — while a Partial
        // stage combines only within a partition and would leave one state
        // row per (file, group): valid, but zero compaction.
        let df = ctx
            .sql(fold_sql)
            .await
            .map_err(|e| format!("mv_merge fold sql: {e}"))?;
        let batches = df
            .collect()
            .await
            .map_err(|e| format!("mv_merge collect: {e}"))?;
        if batches.is_empty() {
            return Err("mv_merge: fold produced no batches".to_string());
        }
        let schema = batches[0].schema();
        let concatenated = arrow::compute::concat_batches(&schema, &batches)
            .map_err(|e| format!("mv_merge concat: {e}"))?;
        let sort_indices = arrow::compute::sort_to_indices(concatenated.column(0), None, None)
            .map_err(|e| format!("mv_merge sort: {e}"))?;
        let sorted_columns: Result<Vec<_>, _> = concatenated
            .columns()
            .iter()
            .map(|c| arrow::compute::take(c.as_ref(), &sort_indices, None))
            .collect();
        // CLOSURE over the schema: rename output columns positionally to the
        // input state names (SQL would name them "sum(mv_input.adv_sum)" —
        // the merged file must look exactly like any other state file, so
        // its output can be merged again and read by the same fold reader).
        let input_schema = ctx
            .table("mv_input")
            .await
            .map_err(|e| format!("mv_merge input schema: {e}"))?
            .schema()
            .as_arrow()
            .clone();
        if input_schema.fields().len() != schema.fields().len() {
            return Err(format!(
                "mv_merge: fold output arity {} != state arity {} — the fold definition must be the state's own fold",
                schema.fields().len(),
                input_schema.fields().len()
            ));
        }
        let renamed = Arc::new(arrow_schema::Schema::new(
            schema
                .fields()
                .iter()
                .zip(input_schema.fields().iter())
                .map(|(out_f, in_f)| {
                    arrow_schema::Field::new(in_f.name(), out_f.data_type().clone(), out_f.is_nullable())
                })
                .collect::<Vec<_>>(),
        ));
        let sorted = arrow_array::RecordBatch::try_new(
            renamed,
            sorted_columns.map_err(|e| format!("mv_merge take: {e}"))?,
        )
        .map_err(|e| format!("mv_merge sorted: {e}"))?;

        let file = File::create(output_file)
            .map_err(|e| format!("mv_merge create {output_file}: {e}"))?;
        let mut writer = arrow::ipc::writer::FileWriter::try_new(file, &sorted.schema())
            .map_err(|e| format!("mv_merge ipc writer: {e}"))?;
        writer
            .write(&sorted)
            .map_err(|e| format!("mv_merge write: {e}"))?;
        writer.finish().map_err(|e| format!("mv_merge finish: {e}"))?;
        Ok(sorted.num_rows() as i64)
    })
}

/// Builds the MV state file (Arrow IPC, decision 17 — the fold readers use
/// register_arrow). Returns the number of state rows written.
pub fn mv_build_poc(
    input_file: &str,
    table_name: &str,
    sql: &str,
    output_file: &str,
) -> Result<i64, String> {
    let sorted = build_sorted_state(input_file, table_name, sql)?;
    let file =
        File::create(output_file).map_err(|e| format!("mv_poc create {output_file}: {e}"))?;
    let mut writer = arrow::ipc::writer::FileWriter::try_new(file, &sorted.schema())
        .map_err(|e| format!("mv_poc ipc writer: {e}"))?;
    writer
        .write(&sorted)
        .map_err(|e| format!("mv_poc write batch: {e}"))?;
    writer.finish().map_err(|e| format!("mv_poc finish: {e}"))?;
    Ok(sorted.num_rows() as i64)
}

/// Refresh-time build for the ship path: same Partial build + group-key sort
/// as [`mv_build_poc`], but the sorted state batch is EXPORTED via Arrow
/// C-Data into caller-provided struct addresses (zero copy into the JVM)
/// instead of being written to a file. Returns the state row count.
pub fn mv_build_arrow(
    input_file: &str,
    table_name: &str,
    sql: &str,
    array_addr: i64,
    schema_addr: i64,
) -> Result<i64, String> {
    use arrow_array::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
    use arrow_array::Array; // into_data
    let sorted = build_sorted_state(input_file, table_name, sql)?;
    let rows = sorted.num_rows() as i64;
    let struct_array: arrow_array::StructArray = sorted.into();
    let data = struct_array.into_data();
    let ffi_schema = FFI_ArrowSchema::try_from(data.data_type())
        .map_err(|e| format!("mv_build_arrow schema export: {e}"))?;
    let ffi_array = FFI_ArrowArray::new(&data);
    unsafe {
        std::ptr::write(array_addr as *mut FFI_ArrowArray, ffi_array);
        std::ptr::write(schema_addr as *mut FFI_ArrowSchema, ffi_schema);
    }
    Ok(rows)
}

fn find_partial(plan: &Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
    if let Some(agg) = plan.downcast_ref::<AggregateExec>() {
        // Partial: the state-producing stage of a split plan. Single /
        // SinglePartitioned: small inputs (e.g. one tiny parquet) skip the
        // split — output schema EQUALS the Partial state schema because MV
        // definitions are pre-decomposed to their mergeable core (SUM/COUNT/
        // MIN/MAX only; no raw AVG), so accepting it is exact, not a fallback.
        match agg.mode() {
            AggregateMode::Partial | AggregateMode::Single | AggregateMode::SinglePartitioned => {
                return Some(plan.clone());
            }
            _ => {}
        }
    }
    for child in plan.children() {
        if let Some(found) = find_partial(child) {
            return Some(found);
        }
    }
    None
}

/// POC(mv) search: Final-style aggregation over MV state files. Returns rows
/// as "service\tcount" lines joined by newlines (POC-grade wire format).
pub fn mv_search_poc(
    state_files: &[String],
    group_key: &str,
    state_col: &str,
) -> Result<String, String> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("mv_poc search runtime: {e}"))?;

    rt.block_on(async {
        // Single partition: the PARTIAL stage combines per partition — with N
        // input files as N partitions each group would emit once per
        // partition (valid state, but zero compaction, which is the whole
        // point of the merge).
        let config = datafusion::execution::context::SessionConfig::new().with_target_partitions(1);
        let ctx = SessionContext::new_with_config(config);
        for (i, f) in state_files.iter().enumerate() {
            ctx.register_parquet(&format!("mv_{i}"), f.as_str(), ParquetReadOptions::default())
                .await
                .map_err(|e| format!("mv_poc search register {f}: {e}"))?;
        }
        let union_sql = (0..state_files.len())
            .map(|i| format!("SELECT * FROM mv_{i}"))
            .collect::<Vec<_>>()
            .join(" UNION ALL ");
        let sql = format!(
            "SELECT \"{group_key}\", SUM(\"{state_col}\") AS cnt FROM ({union_sql}) GROUP BY \"{group_key}\" ORDER BY \"{group_key}\""
        );
        let df = ctx.sql(&sql).await.map_err(|e| format!("mv_poc search sql: {e}"))?;
        let batches = df.collect().await.map_err(|e| format!("mv_poc search collect: {e}"))?;

        let mut out = String::new();
        for b in &batches {
            let svc = arrow::compute::cast(b.column(0), &arrow_schema::DataType::Utf8)
                .map_err(|e| format!("cast svc: {e}"))?;
            let cnt = arrow::compute::cast(b.column(1), &arrow_schema::DataType::Int64)
                .map_err(|e| format!("cast cnt: {e}"))?;
            let svc = svc.as_any().downcast_ref::<arrow_array::StringArray>().ok_or("svc downcast")?;
            let cnt = cnt.as_any().downcast_ref::<arrow_array::Int64Array>().ok_or("cnt downcast")?;
            for i in 0..b.num_rows() {
                out.push_str(&format!("{}\t{}\n", svc.value(i), cnt.value(i)));
            }
        }
        Ok(out)
    })
}
