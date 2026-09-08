/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Query-time fold over hydrated MV partial-aggregate state files.
//!
//! Partials are produced by the source-side build (c2: `mv_partial.rs` seal at
//! rotation/refresh), uploaded to remote store (c3), and downloaded by the
//! target hydrator (c5) into `<targetShardData>/mv_hydrated/<sourceShard>/`.
//!
//! This module applies AggregateExec(Final) over ALL partial files from all
//! source shards + all generations, collapsing duplicated group keys across
//! partials into exact final aggregate values. The approach mirrors the
//! pull-based-mv-poc `mv_fold.rs` but:
//!   - Scans a DIRECTORY tree (hydrated layout) rather than individual file paths
//!   - Uses `register_mv_state_listing_table` pattern adapted for the hydrated dir
//!   - Returns the final answer rows (not intermediate state)
//!
//! ## Three critical settings (from defect fixes, MUST be preserved):
//! 1. NO `with_file_extension` filter — state files use leading-underscore names
//! 2. `ListingOptions.target_partitions >= file_count` — overlapping files need
//!    separate groups for the sorted path
//! 3. `collect_stat = false` — writer-alias column names cause null-fill stats

use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::common::Result as DfResult;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl};
use datafusion::datasource::TableProvider;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::collect;
use datafusion::physical_plan::expressions::Column;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::{ExecutionPlan, PhysicalExpr};
use datafusion::prelude::SessionContext;

/// Query the hydrated MV partials via Final aggregation. Returns the answer
/// rows (not intermediate state). `hydrated_dirs` is a list of absolute paths
/// to per-source-shard directories, each containing .parquet partial files.
/// `definition_sql` is the definition written against `mv_input`.
/// `input_schema` is the Arrow schema of the source index (for plan surgery).
///
/// Returns (schema_json, rows_as_json_arrays) for FFI transport.
pub fn mv_query_hydrated(
    hydrated_dirs: &[String],
    definition_sql: &str,
    input_schema: SchemaRef,
) -> Result<arrow_array::RecordBatch, String> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("mv_query runtime: {e}"))?;

    rt.block_on(mv_query_hydrated_async(hydrated_dirs, definition_sql, input_schema))
}

/// Async inner: the actual query logic, callable from both the FFI sync
/// wrapper (via block_on) and tests (via #[tokio::test]).
pub async fn mv_query_hydrated_async(
    hydrated_dirs: &[String],
    definition_sql: &str,
    input_schema: SchemaRef,
) -> Result<arrow_array::RecordBatch, String> {
        let config = datafusion::execution::context::SessionConfig::new()
            .with_target_partitions(1);
        let ctx = SessionContext::new_with_config(config);

        // 1. Schema-only table so the definition SQL plans.
        let empty = datafusion::datasource::MemTable::try_new(
            Arc::clone(&input_schema),
            vec![vec![]],
        )
        .map_err(|e| format!("mv_query schema table: {e}"))?;
        ctx.register_table("mv_input", Arc::new(empty))
            .map_err(|e| format!("mv_query register: {e}"))?;

        // Plan the definition to extract the Final aggregate's expressions.
        // Use physical_optimizer_rules_without_combine to FORCE a Partial+Final
        // split — with the default optimizer, small inputs collapse to Single
        // mode and the Final aggregate's accumulator format differs.
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
        .map_err(|e| format!("mv_query plan schema: {e}"))?;
        plan_ctx.register_table("mv_input", Arc::new(plan_empty))
            .map_err(|e| format!("mv_query plan register: {e}"))?;
        let physical = plan_ctx
            .sql(definition_sql)
            .await
            .map_err(|e| format!("mv_query plan sql: {e}"))?
            .create_physical_plan()
            .await
            .map_err(|e| format!("mv_query physical: {e}"))?;

        // 2. Find the Final and Partial aggregate nodes.
        // First try to find a proper Final (from Partial+Final split).
        // Fall back to Single only if no Final exists.
        let final_node = find_agg(&physical, |m| {
            matches!(m, AggregateMode::Final | AggregateMode::FinalPartitioned)
        })
        .or_else(|| find_agg(&physical, |m| {
            matches!(m, AggregateMode::Single | AggregateMode::SinglePartitioned)
        }))
        .ok_or("mv_query: no Final/Single aggregate in plan")?;
        let final_agg = final_node
            .downcast_ref::<AggregateExec>()
            .ok_or("mv_query: downcast")?;

        let partial_schema: SchemaRef = match find_agg(&physical, |m| {
            matches!(m, AggregateMode::Partial)
        }) {
            Some(p) => p.schema(),
            None => {
                // Single-mode plan: rebuild Partial to get state schema.
                let partial = AggregateExec::try_new(
                    AggregateMode::Partial,
                    final_agg.group_expr().clone(),
                    final_agg.aggr_expr().to_vec(),
                    final_agg.filter_expr().to_vec(),
                    Arc::clone(final_agg.input()),
                    final_agg.input_schema(),
                )
                .map_err(|e| format!("mv_query partial probe: {e}"))?;
                partial.schema()
            }
        };

        // 3. Scan all hydrated directories (each is a source-shard subdir).
        let scan = build_hydrated_scan(&ctx, hydrated_dirs)
            .await
            .map_err(|e| format!("mv_query scan: {e}"))?;
        let scan_schema = scan.schema();

        if scan_schema.fields().len() != partial_schema.fields().len() {
            return Err(format!(
                "mv_query: state arity {} != partial arity {} — files don't match definition",
                scan_schema.fields().len(),
                partial_schema.fields().len()
            ));
        }

        // 4. Alias positionally to partial output names.
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
                .map_err(|e| format!("mv_query alias: {e}"))?,
        );

        // 5. Merge partial state rows by group key with a Final aggregate
        // (state in -> answer out, the proven mv_finalize_state shape).
        // CRITICAL: AggregateMode::Final requires a SINGLE input partition.
        // The hydrated scan yields one partition per file (target_partitions
        // >= file_count), and building AggregateExec manually skips the
        // planner's automatic CoalescePartitionsExec insertion -- without it
        // each partition folds independently and duplicate group keys leak
        // through (observed: 6 groups instead of 3).
        let coalesced: Arc<dyn ExecutionPlan> =
            Arc::new(datafusion::physical_plan::coalesce_partitions::CoalescePartitionsExec::new(aliased));
        let reduce = AggregateExec::try_new(
            AggregateMode::Final,
            final_agg.group_expr().clone(),
            final_agg.aggr_expr().to_vec(),
            final_agg.filter_expr().to_vec(),
            coalesced,
            final_agg.input_schema(),
        )
        .map_err(|e| format!("mv_query Final: {e}"))?;

        let batches = collect(Arc::new(reduce), ctx.task_ctx())
            .await
            .map_err(|e| format!("mv_query collect: {e}"))?;

        if batches.is_empty() {
            return Err("mv_query: no output batches".to_string());
        }
        let schema = batches[0].schema();
        arrow::compute::concat_batches(&schema, &batches)
            .map_err(|e| format!("mv_query concat: {e}"))
}

/// Build a scan plan over hydrated directories. Each directory is a source
/// shard's hydrated partials. Uses ListingTable with:
/// - NO file_extension filter (critical: state files have leading underscores)
/// - target_partitions >= file_count
/// - collect_stat = false (writer-alias column names)
async fn build_hydrated_scan(
    ctx: &SessionContext,
    hydrated_dirs: &[String],
) -> DfResult<Arc<dyn ExecutionPlan>> {
    // Collect all .parquet files from all subdirectories.
    let mut all_files: Vec<String> = Vec::new();
    for dir in hydrated_dirs {
        let dir_path = std::path::Path::new(dir);
        if !dir_path.is_dir() {
            continue;
        }
        for entry in std::fs::read_dir(dir_path).map_err(|e| {
            datafusion::common::DataFusionError::Execution(format!(
                "mv_query: cannot read hydrated dir '{}': {}", dir, e
            ))
        })? {
            let entry = entry.map_err(|e| {
                datafusion::common::DataFusionError::Execution(format!("mv_query: dir entry: {e}"))
            })?;
            let p = entry.path();
            if p.extension().map_or(false, |e| e == "parquet") {
                all_files.push(p.to_string_lossy().to_string());
            }
        }
    }

    if all_files.is_empty() {
        return Err(datafusion::common::DataFusionError::Execution(
            "mv_query: no parquet files found in hydrated directories".to_string(),
        ));
    }

    // Parse as ListingTableUrl (file:// scheme for local files).
    let urls: Vec<ListingTableUrl> = all_files
        .iter()
        .map(|p| ListingTableUrl::parse(p.as_str()))
        .collect::<DfResult<_>>()?;

    // CRITICAL SETTINGS (defect fixes from POC):
    // 1. No with_file_extension — state files use leading underscores
    // 2. target_partitions >= file_count
    // 3. collect_stat = false
    let listing_tp = all_files.len().max(1);
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

/// Finds the first aggregate node matching `pred`.
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

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int64Array, RecordBatch, StringArray};
    use arrow::datatypes::{DataType, Field, Schema};
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use tempfile::TempDir;

    fn source_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("event_bucket", DataType::Int64, false),
            Field::new("URL", DataType::Utf8, false),
            Field::new("AdvEngineID", DataType::Int64, true),
        ]))
    }

    fn partial_schema() -> SchemaRef {
        // The partial state schema produced by:
        // SELECT event_bucket, "URL", COUNT(*), SUM("AdvEngineID") FROM mv_input GROUP BY event_bucket, "URL"
        Arc::new(Schema::new(vec![
            Field::new("event_bucket", DataType::Int64, false),
            Field::new("URL", DataType::Utf8, false),
            Field::new("count(*)", DataType::Int64, false),
            Field::new("SUM(mv_input.AdvEngineID)", DataType::Int64, true),
        ]))
    }

    fn write_partial(dir: &std::path::Path, name: &str, buckets: &[i64], urls: &[&str], counts: &[i64], sums: &[i64]) -> String {
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
        let file = std::fs::File::create(&path).unwrap();
        let mut w = ArrowWriter::try_new(file, schema, Some(WriterProperties::builder().build())).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        path.to_str().unwrap().to_string()
    }

    /// Core test: 2 source shards × 2 generations with overlapping group keys.
    /// Final fold must collapse partial aggregates exactly.
    #[tokio::test]
    async fn fold_across_shards_and_generations() {
        let dir = TempDir::new().unwrap();
        // Source shard 0
        let shard0 = dir.path().join("0");
        std::fs::create_dir_all(&shard0).unwrap();
        // Gen 0: bucket=100/URL="/a" -> count=5, sum=50
        write_partial(&shard0, "_mv_partial.g0.parquet", &[100, 200], &["/a", "/b"], &[5, 3], &[50, 30]);
        // Gen 1: bucket=100/URL="/a" -> count=2, sum=20 (overlapping key)
        write_partial(&shard0, "_mv_partial.g1.parquet", &[100], &["/a"], &[2], &[20]);

        // Source shard 1
        let shard1 = dir.path().join("1");
        std::fs::create_dir_all(&shard1).unwrap();
        // Gen 0: bucket=100/URL="/a" -> count=3, sum=30 (overlapping key from different shard)
        write_partial(&shard1, "_mv_partial.g0.parquet", &[100, 300], &["/a", "/c"], &[3, 1], &[30, 10]);
        // Gen 1: bucket=200/URL="/b" -> count=4, sum=40 (overlapping with shard0 gen0)
        write_partial(&shard1, "_mv_partial.g1.parquet", &[200], &["/b"], &[4], &[40]);

        let definition_sql = "SELECT event_bucket, \"URL\", COUNT(*), SUM(\"AdvEngineID\") FROM mv_input GROUP BY event_bucket, \"URL\"";

        let result = mv_query_hydrated_async(
            &[shard0.to_str().unwrap().to_string(), shard1.to_str().unwrap().to_string()],
            definition_sql,
            source_schema(),
        )
        .await
        .unwrap();

        // Expected (group key -> count, sum):
        // (100, "/a") -> 5+2+3 = 10, 50+20+30 = 100
        // (200, "/b") -> 3+4 = 7, 30+40 = 70
        // (300, "/c") -> 1, 10
        assert_eq!(result.num_rows(), 3, "expected 3 distinct groups, got {}", result.num_rows());

        // Verify by collecting into a map
        let buckets = result.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
        // DF 54 scans parquet Utf8 as Utf8View (schema_force_view_types default)
        let urls = result.column(1).as_any().downcast_ref::<arrow_array::StringViewArray>().unwrap();
        let counts = result.column(2).as_any().downcast_ref::<Int64Array>().unwrap();
        let sums = result.column(3).as_any().downcast_ref::<Int64Array>().unwrap();

        let mut found: std::collections::HashMap<(i64, String), (i64, i64)> = std::collections::HashMap::new();
        for i in 0..result.num_rows() {
            found.insert(
                (buckets.value(i), urls.value(i).to_string()),
                (counts.value(i), sums.value(i)),
            );
        }

        assert_eq!(found[&(100, "/a".to_string())], (10, 100), "group (100, /a) exact");
        assert_eq!(found[&(200, "/b".to_string())], (7, 70), "group (200, /b) exact");
        assert_eq!(found[&(300, "/c".to_string())], (1, 10), "group (300, /c) exact");
    }

    /// Empty directory returns an error (not silent empty result).
    #[tokio::test]
    async fn empty_hydrated_dir_errors() {
        let dir = TempDir::new().unwrap();
        let shard0 = dir.path().join("0");
        std::fs::create_dir_all(&shard0).unwrap();

        let result = mv_query_hydrated_async(
            &[shard0.to_str().unwrap().to_string()],
            "SELECT event_bucket, COUNT(*) FROM mv_input GROUP BY event_bucket",
            source_schema(),
        )
        .await;
        assert!(result.is_err(), "expected error for empty hydrated dir");
    }
}
