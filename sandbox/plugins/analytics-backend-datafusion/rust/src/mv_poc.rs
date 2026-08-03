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

/// Builds the MV state file. Returns the number of state rows written.
pub fn mv_build_poc(input_file: &str, table_name: &str, sql: &str, output_file: &str) -> Result<i64, String> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("mv_poc runtime: {e}"))?;

    rt.block_on(async {
        let ctx = SessionContext::new();
        ctx.register_parquet(table_name, input_file, ParquetReadOptions::default())
            .await
            .map_err(|e| format!("mv_poc register_parquet({input_file}): {e}"))?;

        let df = ctx.sql(sql).await.map_err(|e| format!("mv_poc plan sql: {e}"))?;
        let physical = df
            .create_physical_plan()
            .await
            .map_err(|e| format!("mv_poc physical plan: {e}"))?;
        let partial =
            find_partial(&physical).ok_or_else(|| "mv_poc: no Partial aggregate in plan".to_string())?;

        let batches = collect(partial, ctx.task_ctx())
            .await
            .map_err(|e| format!("mv_poc collect partial: {e}"))?;

        // Always write a file, even for zero groups — the composite flush
        // contract requires "files for all formats or none" when the primary
        // flushed. Schema comes from the plan even when batches are empty.
        let schema = if batches.is_empty() {
            return Err("mv_poc: partial produced no batches (expected at least an empty batch)".to_string());
        } else {
            batches[0].schema()
        };

        // Sort states by the group key (col 0) so merges are streaming k-way
        // folds (memory ∝ cursors, not groups). Sort-after-aggregation: cost
        // ∝ #groups, never #docs.
        let concatenated = arrow::compute::concat_batches(&schema, &batches)
            .map_err(|e| format!("mv_poc concat: {e}"))?;
        let sort_indices = arrow::compute::sort_to_indices(concatenated.column(0), None, None)
            .map_err(|e| format!("mv_poc sort: {e}"))?;
        let sorted_columns: Result<Vec<_>, _> = concatenated
            .columns()
            .iter()
            .map(|c| arrow::compute::take(c.as_ref(), &sort_indices, None))
            .collect();
        let sorted = arrow_array::RecordBatch::try_new(
            schema.clone(),
            sorted_columns.map_err(|e| format!("mv_poc take: {e}"))?,
        )
        .map_err(|e| format!("mv_poc sorted batch: {e}"))?;

        let file = File::create(output_file).map_err(|e| format!("mv_poc create {output_file}: {e}"))?;
        let mut writer =
            ArrowWriter::try_new(file, schema, None).map_err(|e| format!("mv_poc writer: {e}"))?;
        writer.write(&sorted).map_err(|e| format!("mv_poc write batch: {e}"))?;
        writer.close().map_err(|e| format!("mv_poc close: {e}"))?;
        Ok(sorted.num_rows() as i64)
    })
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

/// POC(mv) search: Final-style aggregation over MV state files. Returns rows
/// as "service\tcount" lines joined by newlines (POC-grade wire format).
pub fn mv_search_poc(state_files: &[String], group_key: &str, state_col: &str) -> Result<String, String> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("mv_poc search runtime: {e}"))?;

    rt.block_on(async {
        let ctx = SessionContext::new();
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
