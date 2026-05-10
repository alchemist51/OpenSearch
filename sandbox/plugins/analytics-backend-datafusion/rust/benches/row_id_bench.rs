//! Benchmark: row ID fetch — all four approaches at three selectivities.
//!
//! Approaches:
//!   - Baseline: SELECT ___row_id (reads from disk, no row_base, no optimizer)
//!   - Approach 1 (ListingTable): ProjectRowIdOptimizer (___row_id + row_base)
//!   - Approach 2 (IndexedPredicateOnly): Indexed pipeline, compute from position
//!   - Approach 3 (IndexedPassthrough): ComputeRowIdOptimizer (___row_id + row_base, conceptual position)
//!
//! Files:
//!   - /Users/abandeji/Downloads/generation-1.parquet (8.5M rows)
//!   - /Users/abandeji/Downloads/generation-2.parquet (5.7M rows)
//!
//! Usage:
//!   cargo bench --bench row_id_bench

use criterion::{criterion_group, criterion_main, BenchmarkId, Criterion};
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::execution::memory_pool::GreedyMemoryPool;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use futures::TryStreamExt;
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use opensearch_datafusion::api::DataFusionRuntime;
use opensearch_datafusion::datafusion_query_config::{DatafusionQueryConfig, RowIdStrategy};
use opensearch_datafusion::memory::DynamicLimitPool;
use opensearch_datafusion::query_executor;
use opensearch_datafusion::runtime_manager::RuntimeManager;
use std::sync::Arc;

const FILE1: &str = "/Users/abandeji/Downloads/generation-1.parquet";
const FILE2: &str = "/Users/abandeji/Downloads/generation-2.parquet";
const DIR: &str = "/Users/abandeji/Downloads/";

fn setup() -> (RuntimeManager, DataFusionRuntime) {
    let mgr = RuntimeManager::new(4);
    let runtime_env = RuntimeEnvBuilder::new()
        .with_memory_pool(Arc::new(GreedyMemoryPool::new(1024 * 1024 * 1024)))
        .build()
        .unwrap();
    let (_, handle) = DynamicLimitPool::new(1024 * 1024 * 1024);
    let df_runtime = DataFusionRuntime {
        runtime_env,
        custom_cache_manager: None,
        dynamic_limit_handle: handle,
    };
    (mgr, df_runtime)
}

fn get_metas(mgr: &RuntimeManager, files: &[&str]) -> Arc<Vec<object_store::ObjectMeta>> {
    let store = Arc::new(LocalFileSystem::new());
    let metas: Vec<object_store::ObjectMeta> = files
        .iter()
        .map(|f| {
            let path = object_store::path::Path::from(*f);
            mgr.io_runtime.block_on(store.head(&path)).unwrap()
        })
        .collect();
    Arc::new(metas)
}

fn get_substrait(mgr: &RuntimeManager, file_path: &str, sql: &str) -> Vec<u8> {
    use datafusion::datasource::file_format::parquet::ParquetFormat;
    use datafusion::datasource::listing::{ListingOptions, ListingTable, ListingTableConfig};
    use datafusion_substrait::logical_plan::producer::to_substrait_plan;
    use prost::Message;

    mgr.io_runtime.block_on(async {
        let ctx = datafusion::prelude::SessionContext::new();
        let url = ListingTableUrl::parse(file_path).unwrap();
        let opts = ListingOptions::new(Arc::new(ParquetFormat::new()))
            .with_file_extension(".parquet")
            .with_collect_stat(true);
        let schema = opts.infer_schema(&ctx.state(), &url).await.unwrap();
        let cfg = ListingTableConfig::new(url)
            .with_listing_options(opts)
            .with_schema(schema);
        ctx.register_table("t", Arc::new(ListingTable::try_new(cfg).unwrap()))
            .unwrap();
        let plan = ctx.sql(sql).await.unwrap().logical_plan().clone();
        let sub = to_substrait_plan(&plan, &ctx.state()).unwrap();
        let mut buf = Vec::new();
        sub.encode(&mut buf).unwrap();
        buf
    })
}

fn bench_all_approaches(c: &mut Criterion) {
    let (mgr, df_runtime) = setup();

    let metas_single = get_metas(&mgr, &[FILE1]);
    let metas_multi = get_metas(&mgr, &[FILE1, FILE2]);
    let url_single = ListingTableUrl::parse(FILE1).unwrap();
    let url_multi = ListingTableUrl::parse(DIR).unwrap();

    // Substrait plans for three selectivities
    let plan_200 = get_substrait(&mgr, FILE1, "SELECT \"___row_id\" FROM t WHERE target_status_code = 200");
    let plan_404 = get_substrait(&mgr, FILE1, "SELECT \"___row_id\" FROM t WHERE target_status_code = 404");
    let plan_500 = get_substrait(&mgr, FILE1, "SELECT \"___row_id\" FROM t WHERE target_status_code = 500");

    let selectivities = vec![
        ("200_70pct", &plan_200),
        ("404_5pct", &plan_404),
        ("500_2pct", &plan_500),
    ];

    let approaches: Vec<(&str, DatafusionQueryConfig)> = vec![
        ("baseline", DatafusionQueryConfig { target_partitions: 10, ..Default::default() }),
        ("listing_table", DatafusionQueryConfig { target_partitions: 10, row_id_strategy: RowIdStrategy::ListingTable, ..Default::default() }),
        ("indexed_pred", DatafusionQueryConfig { target_partitions: 10, row_id_strategy: RowIdStrategy::IndexedPredicateOnly, ..Default::default() }),
        ("indexed_pass", DatafusionQueryConfig { target_partitions: 10, row_id_strategy: RowIdStrategy::IndexedPassthrough, ..Default::default() }),
    ];

    let mut group = c.benchmark_group("row_id");
    group.sample_size(10);
    group.warm_up_time(std::time::Duration::from_secs(2));
    group.measurement_time(std::time::Duration::from_secs(5));

    // === Single segment ===
    for (sel_label, plan) in &selectivities {
        for (approach_label, config) in &approaches {
            let id = BenchmarkId::new(
                format!("1seg/{}", approach_label),
                sel_label,
            );
            group.bench_with_input(id, plan, |b, plan| {
                let config = config.clone();
                let df_rt = &df_runtime;
                b.to_async(mgr.io_runtime.as_ref()).iter(|| {
                    let url = url_single.clone();
                    let metas = metas_single.clone();
                    let plan = (*plan).clone();
                    let exec = mgr.cpu_executor();
                    let config = config.clone();
                    async move {
                        let ptr = query_executor::execute_query(
                            url, metas, "t".into(), plan, df_rt, exec, None, &config,
                        ).await.unwrap();
                        let mut stream = unsafe {
                            Box::from_raw(ptr as *mut datafusion::physical_plan::stream::RecordBatchStreamAdapter<
                                opensearch_datafusion::cross_rt_stream::CrossRtStream,
                            >)
                        };
                        let mut rows = 0u64;
                        while let Some(batch) = stream.try_next().await.unwrap() {
                            rows += batch.num_rows() as u64;
                        }
                        rows
                    }
                });
            });
        }
    }

    // === Multi segment ===
    for (sel_label, plan) in &selectivities {
        for (approach_label, config) in &approaches {
            let id = BenchmarkId::new(
                format!("2seg/{}", approach_label),
                sel_label,
            );
            group.bench_with_input(id, plan, |b, plan| {
                let config = config.clone();
                let df_rt = &df_runtime;
                b.to_async(mgr.io_runtime.as_ref()).iter(|| {
                    let url = url_multi.clone();
                    let metas = metas_multi.clone();
                    let plan = (*plan).clone();
                    let exec = mgr.cpu_executor();
                    let config = config.clone();
                    async move {
                        let ptr = query_executor::execute_query(
                            url, metas, "t".into(), plan, df_rt, exec, None, &config,
                        ).await.unwrap();
                        let mut stream = unsafe {
                            Box::from_raw(ptr as *mut datafusion::physical_plan::stream::RecordBatchStreamAdapter<
                                opensearch_datafusion::cross_rt_stream::CrossRtStream,
                            >)
                        };
                        let mut rows = 0u64;
                        while let Some(batch) = stream.try_next().await.unwrap() {
                            rows += batch.num_rows() as u64;
                        }
                        rows
                    }
                });
            });
        }
    }

    group.finish();
    mgr.cpu_executor.shutdown();
    std::mem::forget(mgr);
}

/// Correctness check: all four approaches must return the same row count
/// for the same query. Runs once per selectivity, asserts equality.
fn verify_correctness(c: &mut Criterion) {
    let (mgr, df_runtime) = setup();

    let metas = get_metas(&mgr, &[FILE1]);
    let url = ListingTableUrl::parse(FILE1).unwrap();

    let queries = vec![
        ("200", "SELECT \"___row_id\" FROM t WHERE target_status_code = 200"),
        ("404", "SELECT \"___row_id\" FROM t WHERE target_status_code = 404"),
        ("500", "SELECT \"___row_id\" FROM t WHERE target_status_code = 500"),
    ];

    let strategies = vec![
        ("baseline", RowIdStrategy::ListingTable),
        ("listing_table", RowIdStrategy::ListingTable),
        ("indexed_pred", RowIdStrategy::IndexedPredicateOnly),
        ("indexed_pass", RowIdStrategy::IndexedPassthrough),
    ];

    println!("\n=== Correctness verification ===");
    for (q_label, sql) in &queries {
        let plan = get_substrait(&mgr, FILE1, sql);
        let mut row_counts: Vec<(&str, u64)> = Vec::new();

        for (s_label, strategy) in &strategies {
            let config = DatafusionQueryConfig {
                target_partitions: 10,
                row_id_strategy: *strategy,
                ..Default::default()
            };
            let rows = mgr.io_runtime.block_on(async {
                let ptr = query_executor::execute_query(
                    url.clone(),
                    metas.clone(),
                    "t".into(),
                    plan.clone(),
                    &df_runtime,
                    mgr.cpu_executor(),
                    None,
                    &config,
                )
                .await
                .unwrap();
                let mut stream = unsafe {
                    Box::from_raw(
                        ptr as *mut datafusion::physical_plan::stream::RecordBatchStreamAdapter<
                            opensearch_datafusion::cross_rt_stream::CrossRtStream,
                        >,
                    )
                };
                let mut rows = 0u64;
                while let Some(batch) = stream.try_next().await.unwrap() {
                    rows += batch.num_rows() as u64;
                }
                rows
            });
            row_counts.push((s_label, rows));
        }

        let first = row_counts[0].1;
        let all_match = row_counts.iter().all(|(_, r)| *r == first);
        println!(
            "  filter={}: rows={} | all_match={}",
            q_label, first, all_match
        );
        for (label, count) in &row_counts {
            if *count != first {
                println!("    MISMATCH: {} returned {} (expected {})", label, count, first);
            }
        }
        assert!(
            all_match,
            "Row counts differ for filter={}: {:?}",
            q_label, row_counts
        );
    }
    println!("  ✓ All approaches return identical row counts\n");

    // Dummy bench so criterion doesn't complain about empty group
    let mut group = c.benchmark_group("correctness");
    group.sample_size(10);
    group.bench_function("verify", |b| {
        b.iter(|| 1 + 1);
    });
    group.finish();

    mgr.cpu_executor.shutdown();
    std::mem::forget(mgr);
}

criterion_group!(benches, bench_all_approaches, verify_correctness);
criterion_main!(benches);
