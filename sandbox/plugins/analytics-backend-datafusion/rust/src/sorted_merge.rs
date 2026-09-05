/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Generic sorted-parquet merge: N sorted files in, ONE sorted file out.
//!
//! This is deliberately format- and definition-agnostic (the Lucene merge
//! idiom: consolidate files, never combine rows). Rows are streamed in
//! declared sort order and written verbatim — row count out equals the sum
//! of the inputs. Any semantic folding of equal keys remains a QUERY-time
//! concern, which already handles duplicate keys across files today.
//!
//! Memory model: the plan executes on the shared [`DataFusionRuntime`]'s
//! memory pool and disk manager. Each input file is its own sorted
//! partition, so the optimizer satisfies the ORDER BY with a streaming
//! `SortPreservingMergeExec` over k partitions (~k in-flight batches). If
//! the optimizer ever falls back to a full sort, the pool + spill still
//! bound memory — slower, never bigger.

use std::fs::File;
use std::io::BufWriter;
use std::sync::Arc;

use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::datasource::TableProvider;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_expr::expressions::col as physical_col;
use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
use datafusion::physical_plan::{execute_stream, ExecutionPlan};
use datafusion::prelude::{SessionConfig, SessionContext};
use futures::StreamExt;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;

use crate::api::DataFusionRuntime;

/// Outcome of a merge: rows written and output batch count.
#[derive(Debug)]
pub struct SortedMergeResult {
    pub rows_written: u64,
    pub output_batches: u32,
}

/// Per-column sort specification, mirroring the contract of the original
/// `parquet_merge_files` (plain config — column names plus direction and
/// null-placement flags; no schema or definition knowledge).
#[derive(Debug, Clone)]
pub struct SortSpec {
    pub column: String,
    /// true = descending.
    pub descending: bool,
    /// true = nulls first.
    pub nulls_first: bool,
}

/// Merges `input_files` (each individually sorted by `sort`) into one
/// sorted parquet file at `output_file`, executing on `runtime`'s shared
/// memory pool and disk manager.
pub fn merge_sorted_parquet_files(
    runtime: &DataFusionRuntime,
    input_files: &[String],
    sort: &[SortSpec],
    output_file: &str,
) -> Result<SortedMergeResult, String> {
    if input_files.is_empty() {
        return Err("sorted_merge: no input files".to_string());
    }
    if sort.is_empty() {
        return Err("sorted_merge: no sort columns".to_string());
    }
    for f in input_files {
        if !std::path::Path::new(f).is_file() {
            return Err(format!("sorted_merge: input does not exist: {f}"));
        }
    }

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("sorted_merge runtime: {e}"))?;

    rt.block_on(async {
        let ctx = merge_session(runtime);
        let plan = sorted_merge_plan(&ctx, input_files, sort).await?;

        let schema = plan.schema();
        let mut stream = execute_stream(plan, ctx.task_ctx())
            .map_err(|e| format!("sorted_merge execute: {e}"))?;
        let file = File::create(output_file)
            .map_err(|e| format!("sorted_merge create {output_file}: {e}"))?;
        let buffered = BufWriter::new(file);
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(Default::default()))
            .build();
        let mut writer = ArrowWriter::try_new(buffered, schema, Some(props))
            .map_err(|e| format!("sorted_merge parquet writer: {e}"))?;

        let mut rows_written: u64 = 0;
        let mut output_batches: u32 = 0;
        while let Some(batch_result) = stream.next().await {
            let batch = batch_result.map_err(|e| format!("sorted_merge stream batch: {e}"))?;
            if batch.num_rows() > 0 {
                let batch = crate::helper::compact_view_arrays(batch);
                writer
                    .write(&batch)
                    .map_err(|e| format!("sorted_merge write batch: {e}"))?;
                rows_written += batch.num_rows() as u64;
                output_batches += 1;
            }
        }
        writer
            .close()
            .map_err(|e| format!("sorted_merge finish: {e}"))?;

        Ok(SortedMergeResult { rows_written, output_batches })
    })
}

/// Session sharing the global runtime's memory pool + disk manager.
fn merge_session(runtime: &DataFusionRuntime) -> SessionContext {
    let config = SessionConfig::new().with_target_partitions(1);
    let state = SessionStateBuilder::new()
        .with_config(config)
        .with_runtime_env(Arc::new(runtime.runtime_env.clone()))
        .with_default_features()
        .build();
    SessionContext::new_with_state(state)
}

/// Builds the merge plan DIRECTLY as physical operators:
/// one parquet scan per input file (each individually sorted) → UnionExec
/// (k sorted partitions) → SortPreservingMergeExec (streaming k-way merge
/// into one sorted partition). Constructed explicitly because the logical
/// layer cannot express "k sorted files with overlapping ranges" — its only
/// fallback is a full re-sort, exactly what this module exists to avoid.
async fn sorted_merge_plan(
    ctx: &SessionContext,
    input_files: &[String],
    sort: &[SortSpec],
) -> Result<Arc<dyn ExecutionPlan>, String> {
    use datafusion::physical_plan::sorts::sort_preserving_merge::SortPreservingMergeExec;
    use datafusion::physical_plan::union::UnionExec;

    // One single-file scan per input, registered as its own table so each
    // scan claims the declared per-file sort order.
    let mut scans: Vec<Arc<dyn ExecutionPlan>> = Vec::with_capacity(input_files.len());
    let mut schema_ref = None;
    for (i, path) in input_files.iter().enumerate() {
        let url = ListingTableUrl::parse(path).map_err(|e| format!("sorted_merge url {path}: {e}"))?;
        let sort_exprs: Vec<datafusion::logical_expr::SortExpr> = sort
            .iter()
            .map(|s| {
                datafusion::logical_expr::SortExpr::new(
                    datafusion::prelude::ident(&s.column),
                    !s.descending,
                    s.nulls_first,
                )
            })
            .collect();
        let listing_options = ListingOptions::new(Arc::new(ParquetFormat::default()))
            .with_file_extension(".parquet")
            .with_file_sort_order(vec![sort_exprs]);
        let schema = listing_options
            .infer_schema(&ctx.state(), &url)
            .await
            .map_err(|e| format!("sorted_merge infer schema {path}: {e}"))?;
        schema_ref.get_or_insert_with(|| schema.clone());
        let config = ListingTableConfig::new(url)
            .with_listing_options(listing_options)
            .with_schema(schema);
        let table =
            ListingTable::try_new(config).map_err(|e| format!("sorted_merge table {path}: {e}"))?;
        let scan = table
            .scan(&ctx.state(), None, &[], None)
            .await
            .map_err(|e| format!("sorted_merge scan {i} ({path}): {e}"))?;
        scans.push(scan);
    }

    let merged: Arc<dyn ExecutionPlan> = if scans.len() == 1 {
        scans.pop().expect("one scan")
    } else {
        let union: Arc<dyn ExecutionPlan> =
            UnionExec::try_new(scans).map_err(|e| format!("sorted_merge union: {e}"))?;
        let schema = union.schema();
        let exprs: Vec<PhysicalSortExpr> = sort
            .iter()
            .map(|s| {
                Ok(PhysicalSortExpr {
                    expr: physical_col(&s.column, &schema)
                        .map_err(|e| format!("sorted_merge sort column '{}': {e}", s.column))?,
                    options: arrow_schema::SortOptions {
                        descending: s.descending,
                        nulls_first: s.nulls_first,
                    },
                })
            })
            .collect::<Result<_, String>>()?;
        let ordering = LexOrdering::new(exprs)
            .ok_or_else(|| "sorted_merge: empty ordering".to_string())?;
        Arc::new(SortPreservingMergeExec::new(ordering, union))
    };
    Ok(merged)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int64Array, RecordBatch, StringArray};
    use arrow_schema::{DataType, Field, Schema};
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;

    fn test_runtime(pool_bytes: usize) -> DataFusionRuntime {
        let env = RuntimeEnvBuilder::new()
            .with_memory_limit(pool_bytes, 1.0)
            .build()
            .expect("test runtime env");
        DataFusionRuntime::new_for_bench(env)
    }

    /// Writes one sorted parquet file with rows (k, v) sorted by k ASC.
    fn write_sorted_file(dir: &std::path::Path, name: &str, keys: &[&str], vals: &[i64]) -> String {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Utf8, false),
            Field::new("v", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(StringArray::from(keys.to_vec())),
                Arc::new(Int64Array::from(vals.to_vec())),
            ],
        )
        .unwrap();
        let path = dir.join(name);
        let file = File::create(&path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        path.to_string_lossy().to_string()
    }

    fn read_column_k(path: &str) -> Vec<String> {
        use arrow_array::Array;
        use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
        let file = File::open(path).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        let mut out = Vec::new();
        for batch in reader {
            let batch = batch.unwrap();
            let raw = batch.column(batch.schema().index_of("k").unwrap()).clone();
            // The merge session reads strings as Utf8View; normalize for assertion.
            let casted = arrow::compute::cast(&raw, &DataType::Utf8).unwrap();
            let col = casted.as_any().downcast_ref::<StringArray>().unwrap().clone();
            for i in 0..col.len() {
                out.push(col.value(i).to_string());
            }
        }
        out
    }

    fn asc_sort() -> Vec<SortSpec> {
        vec![SortSpec { column: "k".to_string(), descending: false, nulls_first: true }]
    }

    #[test]
    fn merge_preserves_rows_and_produces_global_order() {
        let dir = tempfile::tempdir().unwrap();
        // Overlapping key ranges across files — the interesting case.
        let f1 = write_sorted_file(dir.path(), "g1.parquet", &["a", "c", "e", "g"], &[1, 2, 3, 4]);
        let f2 = write_sorted_file(dir.path(), "g2.parquet", &["b", "c", "f"], &[5, 6, 7]);
        let f3 = write_sorted_file(dir.path(), "g3.parquet", &["a", "d", "g", "h"], &[8, 9, 10, 11]);
        let out = dir.path().join("merged.parquet").to_string_lossy().to_string();

        let runtime = test_runtime(32 * 1024 * 1024);
        let result = merge_sorted_parquet_files(
            &runtime,
            &[f1, f2, f3],
            &asc_sort(),
            &out,
        )
        .expect("merge must succeed");

        // No fold: every input row survives.
        assert_eq!(result.rows_written, 11);
        let keys = read_column_k(&out);
        assert_eq!(keys.len(), 11);
        let mut sorted = keys.clone();
        sorted.sort();
        assert_eq!(keys, sorted, "output must be globally sorted");
        // Duplicate keys from different files are preserved, not combined.
        assert_eq!(keys.iter().filter(|k| k.as_str() == "a").count(), 2);
        assert_eq!(keys.iter().filter(|k| k.as_str() == "c").count(), 2);
    }

    #[test]
    fn merge_plan_uses_sort_preserving_merge() {
        let dir = tempfile::tempdir().unwrap();
        let f1 = write_sorted_file(dir.path(), "p1.parquet", &["a", "b"], &[1, 2]);
        let f2 = write_sorted_file(dir.path(), "p2.parquet", &["a", "c"], &[3, 4]);

        let runtime = test_runtime(32 * 1024 * 1024);
        let rt = tokio::runtime::Builder::new_current_thread().enable_all().build().unwrap();
        let plan = rt.block_on(async {
            let ctx = merge_session(&runtime);
            let physical = sorted_merge_plan(&ctx, &[f1, f2], &asc_sort()).await.unwrap();
            let rendered = datafusion::physical_plan::displayable(physical.as_ref())
                .indent(false)
                .to_string();
            rendered
        });
        assert!(
            plan.contains("SortPreservingMergeExec"),
            "expected streaming k-way merge, got plan:\n{plan}"
        );
        assert!(
            !plan.contains("SortExec"),
            "declared per-file order must elide the full sort, got plan:\n{plan}"
        );
    }

    #[test]
    fn merge_succeeds_under_small_pool() {
        // Streaming merge must work with a deliberately tiny pool — memory
        // is bounded by in-flight batches, not input size.
        let dir = tempfile::tempdir().unwrap();
        let keys1: Vec<String> = (0..5000).map(|i| format!("k{:06}", i * 2)).collect();
        let keys2: Vec<String> = (0..5000).map(|i| format!("k{:06}", i * 2 + 1)).collect();
        let k1: Vec<&str> = keys1.iter().map(|s| s.as_str()).collect();
        let k2: Vec<&str> = keys2.iter().map(|s| s.as_str()).collect();
        let v: Vec<i64> = (0..5000).collect();
        let f1 = write_sorted_file(dir.path(), "s1.parquet", &k1, &v);
        let f2 = write_sorted_file(dir.path(), "s2.parquet", &k2, &v);
        let out = dir.path().join("small_pool.parquet").to_string_lossy().to_string();

        let runtime = test_runtime(4 * 1024 * 1024);
        let result =
            merge_sorted_parquet_files(&runtime, &[f1, f2], &asc_sort(), &out).unwrap();
        assert_eq!(result.rows_written, 10_000);
    }

    #[test]
    fn merge_rejects_bad_input() {
        let runtime = test_runtime(8 * 1024 * 1024);
        assert!(merge_sorted_parquet_files(&runtime, &[], &asc_sort(), "/tmp/x").is_err());
        assert!(merge_sorted_parquet_files(
            &runtime,
            &["/nonexistent/gen.parquet".to_string()],
            &asc_sort(),
            "/tmp/x"
        )
        .is_err());
        assert!(merge_sorted_parquet_files(
            &runtime,
            &["/tmp/whatever.parquet".to_string()],
            &[],
            "/tmp/x"
        )
        .is_err());
    }
}
