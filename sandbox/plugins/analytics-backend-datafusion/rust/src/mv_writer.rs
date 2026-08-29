/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! POC(mv) v2: streaming MV writer with DataFusion-maintained state.
//!
//! Lifecycle (mirrors the parquet native writer): `create(definition SQL)` /
//! `feed(batch)` / `finalize(path)` / `abort`.
//!
//! Architecture (decided 2026-08-03): **hash aggregation until flush + one
//! sort at flush.**
//! - `feed`: run the definition's aggregation STOPPED AT `Partial` mode over
//!   the fed batch (raw rows → state rows), then fold the resulting mini
//!   state batch into the held state via `PartialReduce` (state ⊕ state) —
//!   DataFusion owns the entire state algebra; nothing is hand-rolled.
//! - `finalize`: drain the held state → single sort by group keys (cost ∝
//!   groups, never docs) → write the sorted state parquet.
//!
//! POC simplification: the held state is a Vec of state batches compacted via
//! PartialReduce whenever it grows past a threshold, rather than one live
//! GroupedHashAggregateStream fed by a channel. Same operator algebra, no
//! long-lived task per writer.
//!
//! TODO(mv, noted optimization — do not build yet): for high group
//! cardinality, replace hash-until-flush with sorted runs per rotation +
//! `SortPreservingMergeExec` + order-aware (`GroupOrdered`) fold at finalize:
//! spillable runs, O(1)-group fold memory. Trigger: all-groups hash state
//! memory ∝ G hurts. See mv-incremental-lld §3.4 / KB notes.

use std::fs::File;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::{Arc, Mutex};

use arrow_array::{Array, RecordBatch};
use arrow_schema::Schema;
use datafusion::catalog::TableProvider;
use datafusion::datasource::MemTable;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::{collect, ExecutionPlan};
use datafusion::prelude::{ParquetReadOptions, SessionContext};
use parquet::arrow::ArrowWriter;

/// Compact the held state batches via PartialReduce when the count exceeds this.
const COMPACT_THRESHOLD: usize = 8;

/// One live MV writer.
struct MvWriterState {
    /// The MV definition, e.g. "SELECT service, status, COUNT(*), ... FROM mv_input GROUP BY service, status".
    /// Table name inside the SQL must be `mv_input`.
    sql: String,
    /// Raw-input schema (captured from the first fed batch) for planning.
    input_schema: Option<Arc<Schema>>,
    /// Accumulated partial-state batches (each already state rows).
    state_batches: Vec<RecordBatch>,
    /// Number of group-by columns (leading columns of the state schema).
    num_group_cols: usize,
}

pub struct MvWriterHandle {
    state: Mutex<MvWriterState>,
}

static NEXT_ID: AtomicI64 = AtomicI64::new(1);

static WRITERS: std::sync::LazyLock<Mutex<std::collections::HashMap<i64, Arc<MvWriterHandle>>>> =
    std::sync::LazyLock::new(|| Mutex::new(std::collections::HashMap::new()));

pub fn mv_writer_create(sql: &str, num_group_cols: i64) -> i64 {
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    WRITERS.lock().unwrap().insert(
        id,
        Arc::new(MvWriterHandle {
            state: Mutex::new(MvWriterState {
                sql: sql.to_string(),
                input_schema: None,
                state_batches: Vec::new(),
                num_group_cols: num_group_cols as usize,
            }),
        }),
    );
    id
}

fn get_writer(id: i64) -> Result<Arc<MvWriterHandle>, String> {
    WRITERS
        .lock()
        .unwrap()
        .get(&id)
        .cloned()
        .ok_or_else(|| format!("mv_writer: unknown handle {id}"))
}

fn find_agg_with_mode(
    plan: &Arc<dyn ExecutionPlan>,
    mode: AggregateMode,
) -> Option<Arc<dyn ExecutionPlan>> {
    if let Some(agg) = plan.downcast_ref::<AggregateExec>() {
        if *agg.mode() == mode {
            return Some(plan.clone());
        }
    }
    for child in plan.children() {
        if let Some(found) = find_agg_with_mode(child, mode) {
            return Some(found);
        }
    }
    None
}

fn block_on<F: std::future::Future>(fut: F) -> Result<F::Output, String> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("mv_writer runtime: {e}"))?;
    Ok(rt.block_on(fut))
}

/// Runs the definition SQL over `mv_input` = the given batches, returning the
/// PARTIAL-mode output (state rows). DataFusion's Partial operator does the
/// raw→state lift; the same helper serves feed (raw input) because Partial
/// over raw rows IS the fold for a single batch.
fn partial_states_over(
    sql: &str,
    schema: Arc<Schema>,
    batches: Vec<RecordBatch>,
) -> Result<Vec<RecordBatch>, String> {
    block_on(async move {
        // Build the session WITHOUT CombinePartialFinalAggregate: on small
        // single-partition inputs that rule collapses the Partial/Final pair
        // into Single mode and the Partial node this writer depends on
        // disappears ("no Partial aggregate in plan" — the near-empty-batch
        // flush failure). Same rule removal the engine applies on every
        // execution path (agg_mode.rs).
        let state = datafusion::execution::session_state::SessionStateBuilder::new()
            .with_default_features()
            .with_physical_optimizer_rules(
                crate::agg_mode::physical_optimizer_rules_without_combine(),
            )
            .build();
        let ctx = SessionContext::new_with_state(state);
        let mem = MemTable::try_new(schema, vec![batches]).map_err(|e| format!("memtable: {e}"))?;
        ctx.register_table("mv_input", Arc::new(mem))
            .map_err(|e| format!("register: {e}"))?;
        let df = ctx.sql(sql).await.map_err(|e| format!("plan: {e}"))?;
        let physical = df
            .create_physical_plan()
            .await
            .map_err(|e| format!("physical: {e}"))?;
        let partial = find_agg_with_mode(&physical, AggregateMode::Partial)
            .ok_or("no Partial aggregate in plan")?;
        collect(partial, ctx.task_ctx())
            .await
            .map_err(|e| format!("collect: {e}"))
    })?
}

/// Folds N state batches into compacted state batches via PartialReduce:
/// state ⊕ state → state. Constructed the same way agg_mode.rs builds
/// PartialReduce nodes — by planning the definition and swapping the
/// Partial node's mode, with the state batches as input.
fn partial_reduce(
    sql: &str,
    input_schema: Arc<Schema>,
    state_batches: Vec<RecordBatch>,
) -> Result<Vec<RecordBatch>, String> {
    if state_batches.len() <= 1 {
        return Ok(state_batches);
    }
    let state_schema = state_batches[0].schema();
    block_on(async move {
        // Build the session WITHOUT CombinePartialFinalAggregate: on small
        // single-partition inputs that rule collapses the Partial/Final pair
        // into Single mode and the Partial node this writer depends on
        // disappears ("no Partial aggregate in plan" — the near-empty-batch
        // flush failure). Same rule removal the engine applies on every
        // execution path (agg_mode.rs).
        let state = datafusion::execution::session_state::SessionStateBuilder::new()
            .with_default_features()
            .with_physical_optimizer_rules(
                crate::agg_mode::physical_optimizer_rules_without_combine(),
            )
            .build();
        let ctx = SessionContext::new_with_state(state);
        // Plan the definition over a dummy raw table to obtain the aggregate
        // node shape (group exprs + aggregate exprs + schemas)...
        let dummy =
            MemTable::try_new(input_schema, vec![vec![]]).map_err(|e| format!("dummy: {e}"))?;
        ctx.register_table("mv_input", Arc::new(dummy))
            .map_err(|e| format!("register: {e}"))?;
        let df = ctx.sql(sql).await.map_err(|e| format!("plan: {e}"))?;
        let physical = df
            .create_physical_plan()
            .await
            .map_err(|e| format!("physical: {e}"))?;
        let partial = find_agg_with_mode(&physical, AggregateMode::Partial)
            .ok_or("no Partial aggregate in plan")?;
        let agg = partial
            .downcast_ref::<AggregateExec>()
            .ok_or("partial is not AggregateExec")?;

        // ...then rebuild it in PartialReduce mode over the accumulated state
        // batches (a MemoryExec scanning state rows).
        let states = MemTable::try_new(state_schema.clone(), vec![state_batches])
            .map_err(|e| format!("state memtable: {e}"))?;
        let state_scan = states
            .scan(&ctx.state(), None, &[], None)
            .await
            .map_err(|e| format!("state scan: {e}"))?;

        let reduce = AggregateExec::try_new(
            AggregateMode::PartialReduce,
            agg.group_expr().clone(),
            agg.aggr_expr().to_vec(),
            agg.filter_expr().to_vec(),
            state_scan,
            agg.input_schema(),
        )
        .map_err(|e| format!("PartialReduce construct: {e}"))?;

        collect(Arc::new(reduce), ctx.task_ctx())
            .await
            .map_err(|e| format!("collect reduce: {e}"))
    })?
}

pub fn mv_writer_feed(id: i64, batch: &RecordBatch) -> Result<(), String> {
    let handle = get_writer(id)?;
    let mut st = handle.state.lock().unwrap();
    if st.input_schema.is_none() {
        st.input_schema = Some(batch.schema());
    }
    let schema = st.input_schema.clone().unwrap();
    let sql = st.sql.clone();

    // Raw batch → state rows (DataFusion Partial does the lift + in-batch fold).
    let mini_states = partial_states_over(&sql, schema.clone(), vec![batch.clone()])?;
    st.state_batches.extend(mini_states);

    // Bound the held state: fold accumulated state batches via PartialReduce.
    if st.state_batches.len() > COMPACT_THRESHOLD {
        let folded = partial_reduce(&sql, schema, std::mem::take(&mut st.state_batches))?;
        st.state_batches = folded;
    }
    Ok(())
}

/// Shared finalize core: removes the writer, folds all accumulated state via
/// PartialReduce, and lexsorts by the group-key columns. Cost ∝ groups.
fn finalize_sorted_batch(id: i64) -> Result<RecordBatch, String> {
    let handle = {
        WRITERS
            .lock()
            .unwrap()
            .remove(&id)
            .ok_or_else(|| format!("mv_writer_finalize: unknown handle {id}"))?
    };
    let mut st = handle.state.lock().unwrap();
    if st.state_batches.is_empty() {
        return Err("mv_writer_finalize: no data fed".to_string());
    }
    let schema = st.input_schema.clone().ok_or("no input schema")?;
    let sql = st.sql.clone();

    // Final fold: all accumulated state → one state set (PartialReduce).
    let folded = partial_reduce(&sql, schema, std::mem::take(&mut st.state_batches))?;

    // Single sort by group keys (leading columns), cost ∝ groups.
    let state_schema = folded[0].schema();
    let concatenated = arrow::compute::concat_batches(&state_schema, &folded)
        .map_err(|e| format!("concat: {e}"))?;
    let sort_cols: Vec<arrow::compute::SortColumn> = (0..st.num_group_cols)
        .map(|i| arrow::compute::SortColumn {
            values: concatenated.column(i).clone(),
            options: None,
        })
        .collect();
    let indices = arrow::compute::lexsort_to_indices(&sort_cols, None)
        .map_err(|e| format!("lexsort: {e}"))?;
    let sorted_columns: Result<Vec<_>, _> = concatenated
        .columns()
        .iter()
        .map(|c| arrow::compute::take(c.as_ref(), &indices, None))
        .collect();
    RecordBatch::try_new(
        state_schema,
        sorted_columns.map_err(|e| format!("take: {e}"))?,
    )
    .map_err(|e| format!("sorted batch: {e}"))
}

pub fn mv_writer_finalize(id: i64, output_file: &str) -> Result<i64, String> {
    // State files are ARROW IPC (decision 17): they are small, whole-scanned,
    // and written INSIDE the refresh (on the ack path in ship mode) — IPC
    // write is framed buffer copy, no parquet encode tax, and the future
    // merger mmaps them back zero-copy. Compacted/merged output may revisit
    // parquet (compression + stats pruning) when the merger lands.
    let sorted = finalize_sorted_batch(id)?;
    let state_schema = sorted.schema();
    let file = File::create(output_file).map_err(|e| format!("create {output_file}: {e}"))?;
    let mut writer = arrow::ipc::writer::FileWriter::try_new(file, &state_schema)
        .map_err(|e| format!("ipc writer: {e}"))?;
    writer.write(&sorted).map_err(|e| format!("write: {e}"))?;
    writer.finish().map_err(|e| format!("finish: {e}"))?;
    Ok(sorted.num_rows() as i64)
}

/// Separate-index ship path: finalize and export the sorted state batch via
/// Arrow C-Data into caller-provided FFI struct addresses — ZERO COPY into
/// the JVM (the Java side imports the same buffers; the release callback
/// frees the Rust allocation when the JVM consumer closes). No scratch
/// parquet file, no row re-encoding.
pub fn mv_writer_finalize_arrow(id: i64, array_addr: i64, schema_addr: i64) -> Result<i64, String> {
    use arrow_array::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
    let sorted = finalize_sorted_batch(id)?;
    let rows = sorted.num_rows() as i64;
    let struct_array: arrow_array::StructArray = sorted.into();
    let data = struct_array.into_data();
    let ffi_schema = FFI_ArrowSchema::try_from(data.data_type())
        .map_err(|e| format!("finalize_arrow schema export: {e}"))?;
    let ffi_array = FFI_ArrowArray::new(&data);
    unsafe {
        std::ptr::write(array_addr as *mut FFI_ArrowArray, ffi_array);
        std::ptr::write(schema_addr as *mut FFI_ArrowSchema, ffi_schema);
    }
    Ok(rows)
}

pub fn mv_writer_abort(id: i64) {
    WRITERS.lock().unwrap().remove(&id);
}

/// POC(mv) v2 search: Final-fold over MV state files by re-running the
/// definition's aggregation in Final mode over the states. Kept SQL-based for
/// the POC: SUM the count/sum states, MIN/MAX the extrema states.
pub fn mv_search_v2(state_files: &[String], select_final_sql: &str) -> Result<String, String> {
    block_on(async move {
        // Build the session WITHOUT CombinePartialFinalAggregate: on small
        // single-partition inputs that rule collapses the Partial/Final pair
        // into Single mode and the Partial node this writer depends on
        // disappears ("no Partial aggregate in plan" — the near-empty-batch
        // flush failure). Same rule removal the engine applies on every
        // execution path (agg_mode.rs).
        let state = datafusion::execution::session_state::SessionStateBuilder::new()
            .with_default_features()
            .with_physical_optimizer_rules(
                crate::agg_mode::physical_optimizer_rules_without_combine(),
            )
            .build();
        let ctx = SessionContext::new_with_state(state);
        for (i, f) in state_files.iter().enumerate() {
            // State files are Arrow IPC (decision 17).
            ctx.register_arrow(
                &format!("mv_{i}"),
                f.as_str(),
                datafusion::execution::options::ArrowReadOptions::default(),
            )
            .await
            .map_err(|e| format!("register {f}: {e}"))?;
        }
        let union_sql = (0..state_files.len())
            .map(|i| format!("SELECT * FROM mv_{i}"))
            .collect::<Vec<_>>()
            .join(" UNION ALL ");
        let sql = select_final_sql.replace("__MV_STATES__", &format!("({union_sql})"));
        let df = ctx
            .sql(&sql)
            .await
            .map_err(|e| format!("search sql: {e}"))?;
        let batches = df
            .collect()
            .await
            .map_err(|e| format!("search collect: {e}"))?;

        let mut out = String::new();
        for b in &batches {
            for row in 0..b.num_rows() {
                let mut cells = Vec::with_capacity(b.num_columns());
                for col in 0..b.num_columns() {
                    let arr = arrow::compute::cast(b.column(col), &arrow_schema::DataType::Utf8)
                        .map_err(|e| format!("cast col {col}: {e}"))?;
                    let sa = arr
                        .as_any()
                        .downcast_ref::<arrow_array::StringArray>()
                        .ok_or("utf8 downcast")?;
                    cells.push(if sa.is_valid(row) {
                        sa.value(row).to_string()
                    } else {
                        "null".to_string()
                    });
                }
                out.push_str(&cells.join("\t"));
                out.push('\n');
            }
        }
        Ok(out)
    })?
}
