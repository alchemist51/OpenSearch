/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! The GENERAL state fold: `AggregateMode::PartialReduce` over state files.
//!
//! The SQL-expressible folds (SUM of sums, MIN of mins) only exist for
//! decomposable-to-arithmetic aggregates. Sketch aggregates (t-digest
//! percentiles, HLL distinct counts) have no SQL merge function — their fold
//! IS the accumulator's `merge_batch`, reachable only through an
//! `AggregateExec` in a state-consuming mode. `PartialReduce` is exactly
//! that: state in, state out — so ONE mechanism folds every aggregate the
//! engine supports, and the hand-written fold SQL becomes unnecessary.
//!
//! Recipe (mirrors the engine's own agg_mode machinery):
//!  1. plan the DEFINITION SQL against a schema-only table → physical plan;
//!  2. take the FINAL half's group/aggregate expressions (they are built to
//!     CONSUME state — the input contract PartialReduce shares);
//!  3. scan the state files (sorted multi-path listing), alias the columns
//!     positionally to the Partial half's output names;
//!  4. `AggregateExec::try_new(PartialReduce, …, aliased_scan)`;
//!  5. collect, sort by group key, rename back to the state names — the
//!     output is again a valid state file (closed over the algebra).
//!
//! ## Sketch-merge honesty
//! For arithmetic states the fold is EXACT. For sketches it is the sketch's
//! own merge: deterministic, but `merge(a,b,c)` need not equal
//! `merge(merge(a,b),c)` bit-for-bit, and repeated folding can accumulate
//! bounded drift (t-digest). Tests assert error bounds against exact
//! answers, not bitwise equality — that is the honest contract for
//! percentile MVs.

use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::Result as DfResult;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::collect;
use datafusion::physical_plan::expressions::Column;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::{ExecutionPlan, PhysicalExpr};
use datafusion::prelude::SessionContext;

/// Folds `state_files` (Arrow IPC, group-key-sorted, positionally matching
/// the definition's Partial output) into one folded state batch via
/// `PartialReduce`. `input_schema` is the ORIGINAL input schema the
/// definition SQL is written against (schema only — no data is read).
pub fn mv_fold_state(
    state_files: &[String],
    definition_sql: &str,
    input_schema: SchemaRef,
) -> Result<arrow_array::RecordBatch, String> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("mv_fold runtime: {e}"))?;

    rt.block_on(async {
        // Single partition: one PartialReduce must see ALL state rows or
        // groups split across partitions never combine.
        let config =
            datafusion::execution::context::SessionConfig::new().with_target_partitions(1);
        let ctx = SessionContext::new_with_config(config);

        // 1. Schema-only table so the definition SQL plans.
        let empty = datafusion::datasource::MemTable::try_new(
            Arc::clone(&input_schema),
            vec![vec![]],
        )
        .map_err(|e| format!("mv_fold schema table: {e}"))?;
        ctx.register_table("mv_input", Arc::new(empty))
            .map_err(|e| format!("mv_fold register: {e}"))?;
        let physical = ctx
            .sql(definition_sql)
            .await
            .map_err(|e| format!("mv_fold plan sql: {e}"))?
            .create_physical_plan()
            .await
            .map_err(|e| format!("mv_fold physical: {e}"))?;

        // 2. The Final half (its exprs consume state) + the Partial half
        //    (its output schema names the state columns).
        let final_node = find_agg(&physical, |m| {
            matches!(m, AggregateMode::Final | AggregateMode::FinalPartitioned | AggregateMode::Single | AggregateMode::SinglePartitioned)
        })
        .ok_or("mv_fold: no Final/Single aggregate in plan")?;
        let final_agg = final_node
            .downcast_ref::<AggregateExec>()
            .ok_or("mv_fold: downcast")?;
        // Single mode plans (small schema-only inputs) have no separate
        // Partial; its own state schema comes from state_fields either way —
        // use the Final's input_schema when a Partial exists, else derive.
        let partial_schema: SchemaRef = match find_agg(&physical, |m| matches!(m, AggregateMode::Partial)) {
            Some(p) => p.schema(),
            None => {
                // Single-mode plan: rebuild the same aggregate in Partial mode
                // over the schema table to obtain the state schema.
                let partial = AggregateExec::try_new(
                    AggregateMode::Partial,
                    final_agg.group_expr().clone(),
                    final_agg.aggr_expr().to_vec(),
                    final_agg.filter_expr().to_vec(),
                    Arc::clone(final_agg.input()),
                    final_agg.input_schema(),
                )
                .map_err(|e| format!("mv_fold partial probe: {e}"))?;
                partial.schema()
            }
        };

        // 3. State scan, aliased positionally to the partial names.
        let scan = crate::mv_read::build_mv_state_scan(&ctx, state_files)
            .await
            .map_err(|e| format!("mv_fold state scan: {e}"))?;
        let scan_schema = scan.schema();
        if scan_schema.fields().len() != partial_schema.fields().len() {
            return Err(format!(
                "mv_fold: state arity {} != partial arity {} — the files do not belong to this definition",
                scan_schema.fields().len(),
                partial_schema.fields().len()
            ));
        }
        for (i, (sf, pf)) in scan_schema.fields().iter().zip(partial_schema.fields().iter()).enumerate() {
            if sf.data_type() != pf.data_type() {
                return Err(format!(
                    "mv_fold: state col {i} type {:?} != partial type {:?} (crude strict mode: no coercion)",
                    sf.data_type(),
                    pf.data_type()
                ));
            }
        }
        let alias_exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = partial_schema
            .fields()
            .iter()
            .enumerate()
            .map(|(i, pf)| {
                (
                    Arc::new(Column::new(scan_schema.field(i).name(), i)) as Arc<dyn PhysicalExpr>,
                    pf.name().clone(),
                )
            })
            .collect();
        let aliased: Arc<dyn ExecutionPlan> = Arc::new(
            ProjectionExec::try_new(alias_exprs, scan).map_err(|e| format!("mv_fold alias: {e}"))?,
        );

        // 4. The fold itself.
        let reduce = AggregateExec::try_new(
            AggregateMode::PartialReduce,
            final_agg.group_expr().clone(),
            final_agg.aggr_expr().to_vec(),
            final_agg.filter_expr().to_vec(),
            aliased,
            final_agg.input_schema(),
        )
        .map_err(|e| format!("mv_fold PartialReduce: {e}"))?;

        // 5. Collect + sort by group key + rename back to state names.
        let batches = collect(Arc::new(reduce), ctx.task_ctx())
            .await
            .map_err(|e| format!("mv_fold collect: {e}"))?;
        let out_schema = if batches.is_empty() {
            return Err("mv_fold: no output batches".to_string());
        } else {
            batches[0].schema()
        };
        let concatenated = arrow::compute::concat_batches(&out_schema, &batches)
            .map_err(|e| format!("mv_fold concat: {e}"))?;
        let sort_indices = arrow::compute::sort_to_indices(concatenated.column(0), None, None)
            .map_err(|e| format!("mv_fold sort: {e}"))?;
        let sorted_columns: Result<Vec<_>, _> = concatenated
            .columns()
            .iter()
            .map(|c| arrow::compute::take(c.as_ref(), &sort_indices, None))
            .collect();
        let renamed = Arc::new(arrow_schema::Schema::new(
            out_schema
                .fields()
                .iter()
                .zip(scan_schema.fields().iter())
                .map(|(out_f, in_f)| {
                    arrow_schema::Field::new(in_f.name(), out_f.data_type().clone(), out_f.is_nullable())
                })
                .collect::<Vec<_>>(),
        ));
        arrow_array::RecordBatch::try_new(
            renamed,
            sorted_columns.map_err(|e| format!("mv_fold take: {e}"))?,
        )
        .map_err(|e| format!("mv_fold batch: {e}"))
    })
}

/// Finalizes state files: same plan surgery but with `AggregateMode::Final`,
/// producing the query's ANSWER rows (evaluate on merged accumulators).
pub fn mv_finalize_state(
    state_files: &[String],
    definition_sql: &str,
    input_schema: SchemaRef,
) -> Result<arrow_array::RecordBatch, String> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("mv_final runtime: {e}"))?;
    rt.block_on(async {
        let config = datafusion::execution::context::SessionConfig::new().with_target_partitions(1);
        let ctx = SessionContext::new_with_config(config);
        let empty =
            datafusion::datasource::MemTable::try_new(Arc::clone(&input_schema), vec![vec![]])
                .map_err(|e| format!("mv_final schema table: {e}"))?;
        ctx.register_table("mv_input", Arc::new(empty))
            .map_err(|e| format!("mv_final register: {e}"))?;
        let physical = ctx
            .sql(definition_sql)
            .await
            .map_err(|e| format!("mv_final plan: {e}"))?
            .create_physical_plan()
            .await
            .map_err(|e| format!("mv_final physical: {e}"))?;
        let final_node = find_agg(&physical, |m| {
            matches!(
                m,
                AggregateMode::Final
                    | AggregateMode::FinalPartitioned
                    | AggregateMode::Single
                    | AggregateMode::SinglePartitioned
            )
        })
        .ok_or("mv_final: no Final/Single aggregate")?;
        let final_agg = final_node
            .downcast_ref::<AggregateExec>()
            .ok_or("mv_final: downcast")?;
        let partial_schema: SchemaRef =
            match find_agg(&physical, |m| matches!(m, AggregateMode::Partial)) {
                Some(p) => p.schema(),
                None => {
                    let partial = AggregateExec::try_new(
                        AggregateMode::Partial,
                        final_agg.group_expr().clone(),
                        final_agg.aggr_expr().to_vec(),
                        final_agg.filter_expr().to_vec(),
                        Arc::clone(final_agg.input()),
                        final_agg.input_schema(),
                    )
                    .map_err(|e| format!("mv_final partial probe: {e}"))?;
                    partial.schema()
                }
            };
        let scan = crate::mv_read::build_mv_state_scan(&ctx, state_files)
            .await
            .map_err(|e| format!("mv_final scan: {e}"))?;
        let scan_schema = scan.schema();
        let alias_exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = partial_schema
            .fields()
            .iter()
            .enumerate()
            .map(|(i, pf)| {
                (
                    Arc::new(Column::new(scan_schema.field(i).name(), i)) as Arc<dyn PhysicalExpr>,
                    pf.name().clone(),
                )
            })
            .collect();
        let aliased: Arc<dyn ExecutionPlan> = Arc::new(
            ProjectionExec::try_new(alias_exprs, scan)
                .map_err(|e| format!("mv_final alias: {e}"))?,
        );
        let final_exec = AggregateExec::try_new(
            AggregateMode::Final,
            final_agg.group_expr().clone(),
            final_agg.aggr_expr().to_vec(),
            final_agg.filter_expr().to_vec(),
            aliased,
            final_agg.input_schema(),
        )
        .map_err(|e| format!("mv_final Final: {e}"))?;
        let batches = collect(Arc::new(final_exec), ctx.task_ctx())
            .await
            .map_err(|e| format!("mv_final collect: {e}"))?;
        let schema = batches
            .first()
            .map(|b| b.schema())
            .ok_or("mv_final: empty")?;
        arrow::compute::concat_batches(&schema, &batches)
            .map_err(|e| format!("mv_final concat: {e}"))
    })
}

/// Finds the first aggregate node matching `pred`; returns the PLAN NODE
/// (callers downcast_ref to read its expressions).
fn find_agg(
    plan: &Arc<dyn ExecutionPlan>,
    pred: impl Fn(&AggregateMode) -> bool + Copy,
) -> Option<Arc<dyn ExecutionPlan>> {
    if let Some(agg) = plan.downcast_ref::<AggregateExec>() {
        if pred(agg.mode()) {
            return Some(Arc::clone(plan));
        }
    }
    for child in plan.children() {
        if let Some(found) = find_agg(child, pred) {
            return Some(found);
        }
    }
    None
}
