/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! POC(mv): self-contained materialized-view ship build.
//!
//! One blocking call: read a primary parquet file, run the definition stopped
//! at Partial mode, and EXPORT the sorted state batch via Arrow C-Data (an
//! in-memory handoff to the JVM). Nothing here persists Arrow: MV state at
//! rest is Parquet only (written by the managed build / streaming writer).
//!
//! Deliberately independent of the runtime manager / ShardView machinery: a
//! private current-thread tokio runtime per call. This is POC-grade — the
//! production build path goes through the engine session + memory pools.

use std::sync::Arc;

use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::{collect, ExecutionPlan};
use datafusion::prelude::{ParquetReadOptions, SessionContext};

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

