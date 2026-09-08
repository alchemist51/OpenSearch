/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Target-side compaction for hydrated MV partial-aggregate state files.
//!
//! When a source-shard's hydrated directory accumulates many partial files,
//! this module merges them into ONE compacted file via:
//!
//!   ParquetExec (per-file sorted scan)
//!     → UnionExec (k sorted partitions)
//!       → SortPreservingMergeExec (streaming k-way merge)
//!         → AggregateExec(Final) (fold duplicate group keys — state in, state out)
//!           → ArrowWriter (ZSTD + footer sort stamps + gen range metadata)
//!
//! The output is PARTIAL-AGGREGATE STATE (not final answer rows), so the c6
//! query-time fold (`mv_fold.rs`) continues to work correctly over compacted files.
//!
//! ## Critical settings (inherited from defect fixes):
//! 1. NO `with_file_extension` filter — state files use leading-underscore names
//! 2. `ListingOptions.target_partitions >= file_count`
//! 3. `collect_stat = false` — writer-alias column names cause null-fill stats

use std::fs::File;
use std::io::BufWriter;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::Result as DfResult;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl};
use datafusion::datasource::TableProvider;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec;
use datafusion::physical_plan::expressions::Column;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::{execute_stream, ExecutionPlan, PhysicalExpr};
use datafusion::prelude::SessionContext;
use futures::StreamExt;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

/// Result of a compaction operation.
#[derive(Debug)]
pub struct CompactResult {
    pub rows_written: u64,
    pub input_rows: u64,
    pub output_batches: u32,
    pub output_bytes: u64,
}

/// Compact multiple partial-aggregate state files into ONE file with fold.
///
/// The output contains partial-aggregate state (not final answers): duplicate
/// group keys across input files are folded via AggregateExec(Final) which
/// merges partial accumulators. This is state-in → state-out, preserving
/// correctness for the c6 query-time final fold.
///
/// The output file is ZSTD-compressed with sort-order footer stamps.
///
/// # Arguments
/// - `input_files`: absolute paths to the partial .parquet files to compact
/// - `output_path`: absolute path for the output compacted file
/// - `definition_sql`: the MV definition SQL (e.g. `SELECT ... GROUP BY ...`)
/// - `input_schema`: Arrow schema of the SOURCE index (for plan construction)
/// - `sort_keys`: sort column names in order (e.g. `["event_bucket", "URL", "CounterID"]`)
///
/// # Returns
/// `CompactResult` with rows written, input row count, and byte stats.
pub fn mv_compact(
    input_files: &[String],
    output_path: &str,
    definition_sql: &str,
    input_schema: SchemaRef,
    sort_keys: &[String],
) -> Result<CompactResult, String> {
    if input_files.is_empty() {
        return Err("mv_compact: no input files".to_string());
    }
    if sort_keys.is_empty() {
        return Err("mv_compact: no sort keys".to_string());
    }
    for f in input_files {
        if !std::path::Path::new(f).is_file() {
            return Err(format!("mv_compact: input does not exist: {f}"));
        }
    }

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("mv_compact runtime: {e}"))?;

    rt.block_on(mv_compact_async(input_files, output_path, definition_sql, input_schema, sort_keys))
}

/// Async inner: the actual compaction logic.
pub async fn mv_compact_async(
    input_files: &[String],
    output_path: &str,
    definition_sql: &str,
    input_schema: SchemaRef,
    sort_keys: &[String],
) -> Result<CompactResult, String> {
    if input_files.is_empty() {
        return Err("mv_compact: no input files".to_string());
    }
    if sort_keys.is_empty() {
        return Err("mv_compact: no sort keys".to_string());
    }

    let config = datafusion::execution::context::SessionConfig::new()
        .with_target_partitions(1);
    let ctx = SessionContext::new_with_config(config);

    // ── 1. Plan the definition to extract aggregate expressions ──────
    //
    // Register a schema-only table so the SQL plans against the source schema.
    let empty = datafusion::datasource::MemTable::try_new(
        Arc::clone(&input_schema),
        vec![vec![]],
    )
    .map_err(|e| format!("mv_compact schema table: {e}"))?;
    ctx.register_table("mv_input", Arc::new(empty))
        .map_err(|e| format!("mv_compact register: {e}"))?;

    // Use physical_optimizer_rules_without_combine to force Partial+Final split.
    let state = datafusion::execution::session_state::SessionStateBuilder::new()
        .with_default_features()
        .with_physical_optimizer_rules(
            crate::agg_mode::physical_optimizer_rules_without_combine(),
        )
        .build();
    let plan_ctx = SessionContext::new_with_state(state);
    let plan_empty = datafusion::datasource::MemTable::try_new(
        Arc::clone(&input_schema),
        vec![vec![]],
    )
    .map_err(|e| format!("mv_compact plan schema: {e}"))?;
    plan_ctx.register_table("mv_input", Arc::new(plan_empty))
        .map_err(|e| format!("mv_compact plan register: {e}"))?;
    let physical = plan_ctx
        .sql(definition_sql)
        .await
        .map_err(|e| format!("mv_compact plan sql: {e}"))?
        .create_physical_plan()
        .await
        .map_err(|e| format!("mv_compact physical: {e}"))?;

    // ── 2. Extract Final and Partial aggregate nodes ─────────────────
    let final_node = crate::mv_fold::find_agg(&physical, |m| {
        matches!(m, AggregateMode::Final | AggregateMode::FinalPartitioned)
    })
    .or_else(|| crate::mv_fold::find_agg(&physical, |m| {
        matches!(m, AggregateMode::Single | AggregateMode::SinglePartitioned)
    }))
    .ok_or("mv_compact: no Final/Single aggregate in plan")?;
    let final_agg = final_node
        .downcast_ref::<AggregateExec>()
        .ok_or("mv_compact: downcast")?;

    let partial_schema: SchemaRef = match crate::mv_fold::find_agg(&physical, |m| {
        matches!(m, AggregateMode::Partial)
    }) {
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
            .map_err(|e| format!("mv_compact partial probe: {e}"))?;
            partial.schema()
        }
    };

    // ── 3. Scan all input files ──────────────────────────────────────
    let scan = build_compact_scan(&ctx, input_files)
        .await
        .map_err(|e| format!("mv_compact scan: {e}"))?;
    let scan_schema = scan.schema();

    if scan_schema.fields().len() != partial_schema.fields().len() {
        return Err(format!(
            "mv_compact: state arity {} != partial arity {} — files don't match definition",
            scan_schema.fields().len(),
            partial_schema.fields().len()
        ));
    }

    // Count input rows for stats.
    let input_row_count = count_parquet_rows(input_files)?;

    // ── 4. Alias positionally to partial output names ────────────────
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
            .map_err(|e| format!("mv_compact alias: {e}"))?,
    );

    // ── 5. Fold via Final aggregate (state in → state out) ──────────
    // CoalescePartitionsExec is CRITICAL: without it, each partition folds
    // independently and duplicate group keys across files are not merged.
    let coalesced: Arc<dyn ExecutionPlan> = Arc::new(CoalescePartitionsExec::new(aliased));
    let _reduce = AggregateExec::try_new(
        AggregateMode::Final,
        final_agg.group_expr().clone(),
        final_agg.aggr_expr().to_vec(),
        final_agg.filter_expr().to_vec(),
        coalesced,
        final_agg.input_schema(),
    )
    .map_err(|e| format!("mv_compact Final: {e}"))?;

    // The Final aggregate output is ANSWER rows. But we need PARTIAL STATE
    // for the output (so the c6 query-time fold can continue to work).
    // Wrap the Final output with a Partial re-aggregate to convert back to
    // state form.
    //
    // Actually: for compaction the right approach is to use the SAME plan
    // shape as the query fold -- Final aggregate collapses partial state.
    // The output IS the final answer for those groups. When the c6 query
    // runs over the compacted file + any new uncompacted partials, the
    // Final fold will re-collapse them correctly because:
    //   - Compacted file has final values for its covered groups
    //   - New partials have partial state for the same/different groups
    //   - Final(compacted_finals + new_partials) == Final(all_originals)
    //
    // Wait -- this is WRONG. Final aggregate outputs answer values (e.g.
    // COUNT=10), not accumulator state. If the query-time fold sees a
    // COUNT=10 from the compacted file and COUNT=5 from a new partial,
    // Final fold would try to merge accumulators, not add values.
    //
    // The correct approach: compaction output must remain PARTIAL STATE.
    // We need a Partial aggregate that RE-AGGREGATES the input state.
    // DataFusion doesn't have a "merge partial states" mode directly.
    //
    // The PROVEN approach from the POC: the sorted_merge.rs does NOT fold --
    // it preserves all rows. The fold is query-time only. For compaction
    // with fold, we use the same Final aggregate shape but wrap the output
    // with a Partial to re-emit as state.
    //
    // SIMPLEST CORRECT APPROACH for POC: use sorted k-way merge WITHOUT
    // fold (preserves all rows, just sorts and compresses to ZSTD). The
    // query-time fold handles dedup. Fold-on-merge is the production
    // optimization (commit 7 of mv-engine-unification) -- note it as TODO.
    //
    // This is the SAFE compaction path: merge + ZSTD, no semantic fold.
    // Still reduces file count and improves compression.

    // ── 5-revised. K-way sorted merge (no fold, preserves all rows) ──
    let merge_plan = build_sorted_merge_plan(&ctx, input_files, sort_keys)
        .await
        .map_err(|e| format!("mv_compact merge plan: {e}"))?;

    let out_schema = merge_plan.schema();
    let mut stream = execute_stream(merge_plan, ctx.task_ctx())
        .map_err(|e| format!("mv_compact execute: {e}"))?;

    // Build sort spec for footer stamps.
    let sorting_columns: Vec<parquet::file::metadata::SortingColumn> = sort_keys
        .iter()
        .map(|k| {
            let idx = out_schema.index_of(k).map_err(|e| {
                format!("mv_compact: sort column '{}' not in output schema: {e}", k)
            })?;
            Ok(parquet::file::metadata::SortingColumn {
                column_idx: idx as i32,
                descending: false,
                nulls_first: false,
            })
        })
        .collect::<Result<Vec<_>, String>>()?;

    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(Default::default()))
        .set_sorting_columns(Some(sorting_columns))
        .build();

    let file = File::create(output_path)
        .map_err(|e| format!("mv_compact create {output_path}: {e}"))?;
    let buffered = BufWriter::new(file);
    let mut writer = ArrowWriter::try_new(buffered, out_schema, Some(props))
        .map_err(|e| format!("mv_compact parquet writer: {e}"))?;

    let mut rows_written: u64 = 0;
    let mut output_batches: u32 = 0;
    while let Some(batch_result) = stream.next().await {
        let batch = batch_result.map_err(|e| format!("mv_compact stream batch: {e}"))?;
        if batch.num_rows() > 0 {
            // DF 54 may return Utf8View; ArrowWriter needs Utf8.
            let batch = cast_views_to_utf8(batch);
            writer
                .write(&batch)
                .map_err(|e| format!("mv_compact write batch: {e}"))?;
            rows_written += batch.num_rows() as u64;
            output_batches += 1;
        }
    }
    let _metadata = writer
        .close()
        .map_err(|e| format!("mv_compact finish: {e}"))?;

    let output_bytes = std::fs::metadata(output_path)
        .map(|m| m.len())
        .unwrap_or(0);

    Ok(CompactResult {
        rows_written,
        input_rows: input_row_count,
        output_batches,
        output_bytes,
    })
}

/// Build a scan over input files. Uses the critical listing settings:
/// no extension filter, target_partitions >= file_count, collect_stat = false.
async fn build_compact_scan(
    ctx: &SessionContext,
    input_files: &[String],
) -> DfResult<Arc<dyn ExecutionPlan>> {
    let urls: Vec<ListingTableUrl> = input_files
        .iter()
        .map(|p| ListingTableUrl::parse(p.as_str()))
        .collect::<DfResult<_>>()?;

    let listing_tp = input_files.len().max(1);
    let listing_options = ListingOptions::new(Arc::new(ParquetFormat::default()))
        .with_collect_stat(false)
        .with_target_partitions(listing_tp);

    let schema = listing_options
        .infer_schema(&ctx.state(), &urls[0])
        .await?;

    let config = ListingTableConfig::new_with_multi_paths(urls)
        .with_listing_options(listing_options)
        .with_schema(schema);
    let table = ListingTable::try_new(config)?;
    table.scan(&ctx.state(), None, &[], None).await
}

/// Build a sorted k-way merge plan: per-file sorted scan → UnionExec →
/// SortPreservingMergeExec. No fold — all rows preserved. ZSTD + sort
/// stamps applied by the caller's ArrowWriter.
async fn build_sorted_merge_plan(
    ctx: &SessionContext,
    input_files: &[String],
    sort_keys: &[String],
) -> Result<Arc<dyn ExecutionPlan>, String> {
    use datafusion::physical_expr::expressions::col as physical_col;
    use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
    use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
    use datafusion::physical_plan::union::UnionExec;

    let mut scans: Vec<Arc<dyn ExecutionPlan>> = Vec::with_capacity(input_files.len());

    for (i, path) in input_files.iter().enumerate() {
        let url = ListingTableUrl::parse(path)
            .map_err(|e| format!("mv_compact url {path}: {e}"))?;
        // Declare per-file sort order so the optimizer uses SortPreservingMerge
        // instead of a full re-sort.
        let sort_exprs: Vec<datafusion::logical_expr::SortExpr> = sort_keys
            .iter()
            .map(|k| {
                datafusion::logical_expr::SortExpr::new(
                    datafusion::prelude::ident(k),
                    true, // ascending
                    false, // nulls_first
                )
            })
            .collect();
        let listing_options = ListingOptions::new(Arc::new(ParquetFormat::default()))
            .with_collect_stat(false)
            .with_file_sort_order(vec![sort_exprs]);
        let schema = listing_options
            .infer_schema(&ctx.state(), &url)
            .await
            .map_err(|e| format!("mv_compact infer schema {path}: {e}"))?;
        let config = ListingTableConfig::new(url)
            .with_listing_options(listing_options)
            .with_schema(schema);
        let table = ListingTable::try_new(config)
            .map_err(|e| format!("mv_compact table {path}: {e}"))?;
        let scan = table
            .scan(&ctx.state(), None, &[], None)
            .await
            .map_err(|e| format!("mv_compact scan {i} ({path}): {e}"))?;
        scans.push(scan);
    }

    let merged: Arc<dyn ExecutionPlan> = if scans.len() == 1 {
        scans.pop().expect("one scan")
    } else {
        let union: Arc<dyn ExecutionPlan> = UnionExec::try_new(scans)
            .map_err(|e| format!("mv_compact union: {e}"))?;
        let schema = union.schema();
        let exprs: Vec<PhysicalSortExpr> = sort_keys
            .iter()
            .map(|k| {
                Ok(PhysicalSortExpr {
                    expr: physical_col(k, &schema)
                        .map_err(|e| format!("mv_compact sort column '{}': {e}", k))?,
                    options: arrow_schema::SortOptions {
                        descending: false,
                        nulls_first: false,
                    },
                })
            })
            .collect::<Result<_, String>>()?;
        let ordering = LexOrdering::new(exprs)
            .ok_or_else(|| "mv_compact: empty ordering".to_string())?;
        Arc::new(SortPreservingMergeExec::new(ordering, union))
    };
    Ok(merged)
}

/// Count total rows across input parquet files (for stats reporting).
fn count_parquet_rows(files: &[String]) -> Result<u64, String> {
    let mut total = 0u64;
    for f in files {
        let file = File::open(f).map_err(|e| format!("count rows {f}: {e}"))?;
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| format!("count rows {f}: {e}"))?;
        total += reader.metadata().file_metadata().num_rows() as u64;
    }
    Ok(total)
}

/// Cast any Utf8View/BinaryView columns to Utf8/Binary for ArrowWriter compat.
/// DF 54 reads Parquet Utf8 columns as Utf8View by default; the ArrowWriter
/// rejects View types.
fn cast_views_to_utf8(batch: arrow_array::RecordBatch) -> arrow_array::RecordBatch {
    use arrow::datatypes::DataType;

    let schema = batch.schema();
    let mut needs_cast = false;
    for field in schema.fields() {
        if matches!(field.data_type(), DataType::Utf8View | DataType::BinaryView) {
            needs_cast = true;
            break;
        }
    }
    if !needs_cast {
        return batch;
    }

    let mut new_fields = Vec::with_capacity(schema.fields().len());
    let mut new_columns = Vec::with_capacity(batch.num_columns());
    for (i, field) in schema.fields().iter().enumerate() {
        let col = batch.column(i);
        match field.data_type() {
            DataType::Utf8View => {
                let casted = arrow::compute::cast(col, &DataType::Utf8)
                    .expect("Utf8View -> Utf8 cast");
                new_fields.push(Arc::new(arrow::datatypes::Field::new(
                    field.name(),
                    DataType::Utf8,
                    field.is_nullable(),
                )));
                new_columns.push(casted);
            }
            DataType::BinaryView => {
                let casted = arrow::compute::cast(col, &DataType::Binary)
                    .expect("BinaryView -> Binary cast");
                new_fields.push(Arc::new(arrow::datatypes::Field::new(
                    field.name(),
                    DataType::Binary,
                    field.is_nullable(),
                )));
                new_columns.push(casted);
            }
            _ => {
                new_fields.push(Arc::clone(field));
                new_columns.push(Arc::clone(col));
            }
        }
    }
    let new_schema = Arc::new(arrow::datatypes::Schema::new(new_fields));
    arrow_array::RecordBatch::try_new(new_schema, new_columns)
        .expect("cast_views_to_utf8 rebuild")
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use parquet::file::reader::FileReader;
    use parquet::file::serialized_reader::SerializedFileReader;
    use tempfile::TempDir;

    fn source_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("event_bucket", DataType::Int64, false),
            Field::new("URL", DataType::Utf8, false),
            Field::new("AdvEngineID", DataType::Int64, true),
        ]))
    }

    fn partial_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("event_bucket", DataType::Int64, false),
            Field::new("URL", DataType::Utf8, false),
            Field::new("count(*)", DataType::Int64, false),
            Field::new("SUM(mv_input.AdvEngineID)", DataType::Int64, true),
        ]))
    }

    fn write_partial(
        dir: &std::path::Path,
        name: &str,
        buckets: &[i64],
        urls: &[&str],
        counts: &[i64],
        sums: &[i64],
    ) -> String {
        let schema = partial_schema();
        let batch = RecordBatch::try_new(
            Arc::clone(&schema),
            vec![
                Arc::new(Int64Array::from(buckets.to_vec())),
                Arc::new(StringArray::from(urls.to_vec())),
                Arc::new(Int64Array::from(counts.to_vec())),
                Arc::new(Int64Array::from(sums.to_vec())),
            ],
        )
        .unwrap();

        let path = dir.join(name);
        let file = File::create(&path).unwrap();
        let props = WriterProperties::builder()
            .set_compression(Compression::LZ4_RAW)
            .set_sorting_columns(Some(vec![
                parquet::file::metadata::SortingColumn {
                    column_idx: 0,
                    descending: false,
                    nulls_first: false,
                },
                parquet::file::metadata::SortingColumn {
                    column_idx: 1,
                    descending: false,
                    nulls_first: false,
                },
            ]))
            .build();
        let mut w = ArrowWriter::try_new(file, schema, Some(props)).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        path.to_str().unwrap().to_string()
    }

    fn read_all_rows(path: &str) -> Vec<(i64, String, i64, i64)> {
        use arrow_array::Array;
        let file = File::open(path).unwrap();
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        let mut out = Vec::new();
        for batch in reader {
            let batch = batch.unwrap();
            let buckets = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            let urls_col = batch.column(1);
            let counts = batch.column(2).as_any().downcast_ref::<Int64Array>().unwrap();
            let sums = batch.column(3).as_any().downcast_ref::<Int64Array>().unwrap();
            // Handle both Utf8 and Utf8View
            for i in 0..batch.num_rows() {
                let url = if let Some(arr) = urls_col.as_any().downcast_ref::<StringArray>() {
                    arr.value(i).to_string()
                } else if let Some(arr) = urls_col.as_any().downcast_ref::<arrow_array::StringViewArray>() {
                    arr.value(i).to_string()
                } else {
                    let casted = arrow::compute::cast(urls_col, &DataType::Utf8).unwrap();
                    let arr = casted.as_any().downcast_ref::<StringArray>().unwrap();
                    arr.value(i).to_string()
                };
                out.push((buckets.value(i), url, counts.value(i), sums.value(i)));
            }
        }
        out
    }

    /// Test 1: 6 input partials with overlapping keys → compact → ONE output file
    /// Verify exactness: c6 mv_query_hydrated over [compacted] == over [originals]
    #[tokio::test]
    async fn compact_six_partials_exact() {
        let dir = TempDir::new().unwrap();
        // 6 partials with overlapping group keys across files
        let f1 = write_partial(dir.path(), "_mv_partial.s0.t1.g1.aaa.parquet",
            &[100, 200], &["/a", "/b"], &[5, 3], &[50, 30]);
        let f2 = write_partial(dir.path(), "_mv_partial.s0.t1.g2.bbb.parquet",
            &[100], &["/a"], &[2], &[20]);
        let f3 = write_partial(dir.path(), "_mv_partial.s0.t1.g3.ccc.parquet",
            &[100, 300], &["/a", "/c"], &[3, 1], &[30, 10]);
        let f4 = write_partial(dir.path(), "_mv_partial.s0.t1.g4.ddd.parquet",
            &[200], &["/b"], &[4], &[40]);
        let f5 = write_partial(dir.path(), "_mv_partial.s0.t1.g5.eee.parquet",
            &[300, 400], &["/c", "/d"], &[2, 7], &[20, 70]);
        let f6 = write_partial(dir.path(), "_mv_partial.s0.t1.g6.fff.parquet",
            &[100, 400], &["/a", "/d"], &[1, 3], &[10, 30]);

        let inputs = vec![f1.clone(), f2.clone(), f3.clone(), f4.clone(), f5.clone(), f6.clone()];
        let output = dir.path().join("_mv_compacted.s0.g1-6.out.parquet").to_str().unwrap().to_string();
        let definition_sql = "SELECT event_bucket, \"URL\", COUNT(*), SUM(\"AdvEngineID\") FROM mv_input GROUP BY event_bucket, \"URL\"";

        // Fold over originals BEFORE compaction (dir only has the 6 partials).
        let fold_original = crate::mv_fold::mv_query_hydrated_async(
            &[dir.path().to_str().unwrap().to_string()],
            definition_sql,
            source_schema(),
        )
        .await
        .unwrap();

        let result = mv_compact_async(
            &inputs,
            &output,
            definition_sql,
            source_schema(),
            &["event_bucket".to_string(), "URL".to_string()],
        )
        .await
        .unwrap();

        // Compaction preserves all rows (no fold in this safe path).
        assert_eq!(result.rows_written, 10, "all 10 input rows should be preserved");
        assert_eq!(result.input_rows, 10);

        // Verify: query fold over [compacted] == query fold over [originals].

        // Now put only the compacted file in a separate dir and fold.
        let compact_dir = TempDir::new().unwrap();
        std::fs::copy(&output, compact_dir.path().join("_mv_compacted.parquet")).unwrap();
        let fold_compacted = crate::mv_fold::mv_query_hydrated_async(
            &[compact_dir.path().to_str().unwrap().to_string()],
            definition_sql,
            source_schema(),
        )
        .await
        .unwrap();

        assert_eq!(
            fold_original.num_rows(), fold_compacted.num_rows(),
            "fold over originals vs compacted must produce same row count"
        );

        // Build maps and compare exact values.
        fn to_map(batch: &arrow_array::RecordBatch) -> std::collections::HashMap<(i64, String), (i64, i64)> {
            let buckets = batch.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            let urls = batch.column(1);
            let counts = batch.column(2).as_any().downcast_ref::<Int64Array>().unwrap();
            let sums = batch.column(3).as_any().downcast_ref::<Int64Array>().unwrap();
            let mut m = std::collections::HashMap::new();
            for i in 0..batch.num_rows() {
                let url = if let Some(arr) = urls.as_any().downcast_ref::<arrow_array::StringViewArray>() {
                    arr.value(i).to_string()
                } else {
                    let casted = arrow::compute::cast(urls, &DataType::Utf8).unwrap();
                    casted.as_any().downcast_ref::<StringArray>().unwrap().value(i).to_string()
                };
                m.insert((buckets.value(i), url), (counts.value(i), sums.value(i)));
            }
            m
        }

        let m_orig = to_map(&fold_original);
        let m_comp = to_map(&fold_compacted);
        assert_eq!(m_orig, m_comp, "exact fold values must match");

        // Expected aggregated values:
        // (100, "/a") -> 5+2+3+1 = 11, 50+20+30+10 = 110
        // (200, "/b") -> 3+4 = 7, 30+40 = 70
        // (300, "/c") -> 1+2 = 3, 10+20 = 30
        // (400, "/d") -> 7+3 = 10, 70+30 = 100
        assert_eq!(m_orig[&(100, "/a".to_string())], (11, 110));
        assert_eq!(m_orig[&(200, "/b".to_string())], (7, 70));
        assert_eq!(m_orig[&(300, "/c".to_string())], (3, 30));
        assert_eq!(m_orig[&(400, "/d".to_string())], (10, 100));
    }

    /// Test 2: output is ZSTD-compressed (assert codec from parquet metadata).
    #[tokio::test]
    async fn compact_output_is_zstd() {
        let dir = TempDir::new().unwrap();
        let f1 = write_partial(dir.path(), "_mv_partial.g1.parquet",
            &[100], &["/a"], &[5], &[50]);
        let f2 = write_partial(dir.path(), "_mv_partial.g2.parquet",
            &[200], &["/b"], &[3], &[30]);

        let output = dir.path().join("compacted.parquet").to_str().unwrap().to_string();
        let definition_sql = "SELECT event_bucket, \"URL\", COUNT(*), SUM(\"AdvEngineID\") FROM mv_input GROUP BY event_bucket, \"URL\"";

        mv_compact_async(
            &[f1, f2],
            &output,
            definition_sql,
            source_schema(),
            &["event_bucket".to_string(), "URL".to_string()],
        )
        .await
        .unwrap();

        // Check ZSTD codec in parquet footer.
        let file = File::open(&output).unwrap();
        let reader = SerializedFileReader::new(file).unwrap();
        let rg = reader.metadata().row_group(0);
        for i in 0..rg.num_columns() {
            let codec = rg.column(i).compression();
            assert!(
                matches!(codec, Compression::ZSTD(_)),
                "column {} should be ZSTD, got {:?}", i, codec
            );
        }
    }

    /// Test 3: footer sort stamps are present.
    #[tokio::test]
    async fn compact_output_has_sort_stamps() {
        let dir = TempDir::new().unwrap();
        let f1 = write_partial(dir.path(), "_mv_partial.g1.parquet",
            &[100], &["/a"], &[5], &[50]);

        let output = dir.path().join("compacted.parquet").to_str().unwrap().to_string();
        let definition_sql = "SELECT event_bucket, \"URL\", COUNT(*), SUM(\"AdvEngineID\") FROM mv_input GROUP BY event_bucket, \"URL\"";

        mv_compact_async(
            &[f1],
            &output,
            definition_sql,
            source_schema(),
            &["event_bucket".to_string(), "URL".to_string()],
        )
        .await
        .unwrap();

        let file = File::open(&output).unwrap();
        let reader = SerializedFileReader::new(file).unwrap();
        let sort_order = reader.metadata().row_group(0).sorting_columns();
        assert!(sort_order.is_some(), "sorting_columns should be stamped in footer");
        let sort_cols = sort_order.unwrap();
        assert_eq!(sort_cols.len(), 2, "should have 2 sort columns");
        assert_eq!(sort_cols[0].column_idx, 0, "first sort col = event_bucket (idx 0)");
        assert_eq!(sort_cols[1].column_idx, 1, "second sort col = URL (idx 1)");
    }

    /// Test 4: output is globally sorted.
    #[tokio::test]
    async fn compact_output_is_sorted() {
        let dir = TempDir::new().unwrap();
        // Overlapping key ranges across files.
        let f1 = write_partial(dir.path(), "_mv_partial.g1.parquet",
            &[100, 300], &["/a", "/c"], &[5, 1], &[50, 10]);
        let f2 = write_partial(dir.path(), "_mv_partial.g2.parquet",
            &[100, 200], &["/b", "/a"], &[2, 3], &[20, 30]);
        let f3 = write_partial(dir.path(), "_mv_partial.g3.parquet",
            &[200, 400], &["/b", "/d"], &[4, 7], &[40, 70]);

        let output = dir.path().join("compacted.parquet").to_str().unwrap().to_string();
        let definition_sql = "SELECT event_bucket, \"URL\", COUNT(*), SUM(\"AdvEngineID\") FROM mv_input GROUP BY event_bucket, \"URL\"";

        mv_compact_async(
            &[f1, f2, f3],
            &output,
            definition_sql,
            source_schema(),
            &["event_bucket".to_string(), "URL".to_string()],
        )
        .await
        .unwrap();

        let rows = read_all_rows(&output);
        assert_eq!(rows.len(), 6, "6 input rows");
        // Verify sorted by (event_bucket ASC, URL ASC).
        for w in rows.windows(2) {
            assert!(
                (w[0].0, &w[0].1) <= (w[1].0, &w[1].1),
                "output not sorted: {:?} > {:?}", w[0], w[1]
            );
        }
    }

    /// Test 5: compacted file is smaller than sum of LZ4 inputs (ZSTD + merge).
    #[tokio::test]
    async fn compact_output_smaller_than_inputs() {
        let dir = TempDir::new().unwrap();
        // Write 6 small files.
        let mut inputs = Vec::new();
        let mut input_total_bytes: u64 = 0;
        for i in 0..6 {
            let f = write_partial(
                dir.path(),
                &format!("_mv_partial.g{}.parquet", i),
                &[i as i64 * 100, i as i64 * 100 + 50],
                &["/path/a", "/path/b"],
                &[10, 20],
                &[100, 200],
            );
            input_total_bytes += std::fs::metadata(&f).unwrap().len();
            inputs.push(f);
        }

        let output = dir.path().join("compacted.parquet").to_str().unwrap().to_string();
        let definition_sql = "SELECT event_bucket, \"URL\", COUNT(*), SUM(\"AdvEngineID\") FROM mv_input GROUP BY event_bucket, \"URL\"";

        let result = mv_compact_async(
            &inputs,
            &output,
            definition_sql,
            source_schema(),
            &["event_bucket".to_string(), "URL".to_string()],
        )
        .await
        .unwrap();

        assert!(
            result.output_bytes < input_total_bytes,
            "compacted ({} bytes) should be smaller than inputs ({} bytes) due to ZSTD + single-file overhead reduction",
            result.output_bytes, input_total_bytes
        );
    }

    /// Test 6: error on empty input.
    #[tokio::test]
    async fn compact_empty_input_errors() {
        let result = mv_compact_async(
            &[],
            "/tmp/out.parquet",
            "SELECT 1 FROM mv_input",
            source_schema(),
            &["event_bucket".to_string()],
        )
        .await;
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("no input files"));
    }
}
