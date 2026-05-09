//! Benchmark comparing row ID emission strategies using two generation parquet files.
//!
//! Hardcoded to use:
//!   - /Users/abandeji/Downloads/generation-1.parquet (8.5M rows, ___row_id Int32)
//!   - /Users/abandeji/Downloads/generation-2.parquet (5.7M rows, ___row_id Int32)
//!
//! Usage:
//!   cargo bench --bench row_id_bench

use criterion::{criterion_group, criterion_main, Criterion};
use datafusion::datasource::listing::ListingTableUrl;
use datafusion::execution::memory_pool::GreedyMemoryPool;
use datafusion::execution::runtime_env::RuntimeEnvBuilder;
use futures::TryStreamExt;
use object_store::local::LocalFileSystem;
use object_store::ObjectStore;
use opensearch_datafusion::api::DataFusionRuntime;
use opensearch_datafusion::memory::DynamicLimitPool;
use opensearch_datafusion::query_executor;
use opensearch_datafusion::runtime_manager::RuntimeManager;
use std::sync::Arc;

const FILE1: &str = "/Users/abandeji/Downloads/generation-2.parquet";
const DIR: &str = "/Users/abandeji/Downloads/generation-2.parquet";

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

fn get_metas_for_files(mgr: &RuntimeManager, files: &[&str]) -> Arc<Vec<object_store::ObjectMeta>> {
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

/// Build a substrait plan by registering a ListingTable over the given file.
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

fn bench_row_id_strategies(c: &mut Criterion) {
    let (mgr, df_runtime) = setup();

    // Use single file
    let metas = get_metas_for_files(&mgr, &[FILE1]);
    let url = ListingTableUrl::parse(DIR).unwrap();
    let config = opensearch_datafusion::datafusion_query_config::DatafusionQueryConfig::default();

    // Build substrait plans
    let plan_row_id = get_substrait(&mgr, DIR, "SELECT ___row_id FROM t");
    let plan_count = get_substrait(&mgr, DIR, "SELECT COUNT(*) FROM t");
    let plan_port = get_substrait(&mgr, DIR, "SELECT backend_port FROM t");
    let plan_both = get_substrait(&mgr, DIR, "SELECT ___row_id, backend_port FROM t");
    // Filtered queries: target_status_code filter + ___row_id projection
    let plan_filter_500 = get_substrait(&mgr, DIR, "SELECT ___row_id FROM t WHERE target_status_code = 500");
    let plan_filter_404 = get_substrait(&mgr, DIR, "SELECT ___row_id FROM t WHERE target_status_code = 404");
    let plan_filter_200 = get_substrait(&mgr, DIR, "SELECT ___row_id FROM t WHERE target_status_code = 200");

    let mut group = c.benchmark_group("row_id_strategies");
    group.sample_size(10);
    group.warm_up_time(std::time::Duration::from_secs(1));
    group.measurement_time(std::time::Duration::from_secs(5));

    // Baseline: COUNT(*) — metadata only, no column reads
    group.bench_function("count_star", |b| {
        b.to_async(mgr.io_runtime.as_ref()).iter(|| {
            let url = url.clone();
            let metas = metas.clone();
            let plan = plan_count.clone();
            let exec = mgr.cpu_executor();
            async {
                let ptr = query_executor::execute_query(
                    url, metas, "t".into(), plan, &df_runtime, exec, None, &config,
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
            }
        });
    });

    // Baseline: SELECT ___row_id — reads the column from disk
    group.bench_function("select_row_id_from_disk", |b| {
        b.to_async(mgr.io_runtime.as_ref()).iter(|| {
            let url = url.clone();
            let metas = metas.clone();
            let plan = plan_row_id.clone();
            let exec = mgr.cpu_executor();
            async {
                let ptr = query_executor::execute_query(
                    url, metas, "t".into(), plan, &df_runtime, exec, None, &config,
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
            }
        });
    });

    // SELECT backend_port — reads a different Int32 column (comparison)
    group.bench_function("select_backend_port", |b| {
        b.to_async(mgr.io_runtime.as_ref()).iter(|| {
            let url = url.clone();
            let metas = metas.clone();
            let plan = plan_port.clone();
            let exec = mgr.cpu_executor();
            async {
                let ptr = query_executor::execute_query(
                    url, metas, "t".into(), plan, &df_runtime, exec, None, &config,
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
            }
        });
    });

    // SELECT ___row_id, backend_port — two columns
    group.bench_function("select_row_id_and_port", |b| {
        b.to_async(mgr.io_runtime.as_ref()).iter(|| {
            let url = url.clone();
            let metas = metas.clone();
            let plan = plan_both.clone();
            let exec = mgr.cpu_executor();
            async {
                let ptr = query_executor::execute_query(
                    url, metas, "t".into(), plan, &df_runtime, exec, None, &config,
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
            }
        });
    });

    // Filter: target_status_code = 500 (~2% selectivity, 113K rows)
    group.bench_function("filter_500_select_row_id", |b| {
        b.to_async(mgr.io_runtime.as_ref()).iter(|| {
            let url = url.clone();
            let metas = metas.clone();
            let plan = plan_filter_500.clone();
            let exec = mgr.cpu_executor();
            async {
                let ptr = query_executor::execute_query(
                    url, metas, "t".into(), plan, &df_runtime, exec, None, &config,
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
            }
        });
    });

    // Filter: target_status_code = 404 (~5% selectivity, 286K rows)
    group.bench_function("filter_404_select_row_id", |b| {
        b.to_async(mgr.io_runtime.as_ref()).iter(|| {
            let url = url.clone();
            let metas = metas.clone();
            let plan = plan_filter_404.clone();
            let exec = mgr.cpu_executor();
            async {
                let ptr = query_executor::execute_query(
                    url, metas, "t".into(), plan, &df_runtime, exec, None, &config,
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
            }
        });
    });

    // Filter: target_status_code = 200 (~70% selectivity, 4M rows)
    group.bench_function("filter_200_select_row_id", |b| {
        b.to_async(mgr.io_runtime.as_ref()).iter(|| {
            let url = url.clone();
            let metas = metas.clone();
            let plan = plan_filter_200.clone();
            let exec = mgr.cpu_executor();
            async {
                let ptr = query_executor::execute_query(
                    url, metas, "t".into(), plan, &df_runtime, exec, None, &config,
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
            }
        });
    });

    group.finish();
    mgr.cpu_executor.shutdown();
    std::mem::forget(mgr);
}

criterion_group!(benches, bench_row_id_strategies);
criterion_main!(benches);
