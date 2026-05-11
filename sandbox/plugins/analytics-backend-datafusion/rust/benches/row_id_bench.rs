//! Benchmark: row ID fetch — baseline vs ShardTableProvider+optimizer.
//!
//! Approaches:
//!   - Baseline: ListingTable, reads ___row_id as plain column (local per-file IDs)
//!   - ListingTable: ShardTableProvider + ProjectRowIdOptimizer (___row_id + row_base = absolute IDs)
//!
//! IndexedPredicateOnly (compute from position, zero ___row_id I/O) requires the
//! indexed executor path and is tested separately in tests_e2e/row_id_emission.rs.
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

    // === IndexedPredicateOnly (single segment): direct IndexedTableProvider ===
    // Exercises the true position-based row ID computation path.
    {
        use opensearch_datafusion::indexed_table::table_provider::{
            IndexedTableConfig, IndexedTableProvider, SegmentFileInfo,
        };
        use opensearch_datafusion::indexed_table::eval::RowGroupBitsetSource;
        use opensearch_datafusion::indexed_table::eval::select_all::SelectAllBitsetSource;
        use opensearch_datafusion::indexed_table::stream::RowGroupInfo;
        use datafusion::parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};

        let path = std::path::Path::new(FILE1);
        let size = std::fs::metadata(path).unwrap().len();
        let file = std::fs::File::open(path).unwrap();
        let meta = ArrowReaderMetadata::load(&file, ArrowReaderOptions::new().with_page_index(true)).unwrap();
        let schema = meta.schema().clone();
        let parquet_meta = meta.metadata().clone();
        let mut rgs = Vec::new();
        let mut offset = 0i64;
        for i in 0..parquet_meta.num_row_groups() {
            let n = parquet_meta.row_group(i).num_rows();
            rgs.push(RowGroupInfo { index: i, first_row: offset, num_rows: n });
            offset += n;
        }
        let total_rows = offset;

        let object_path = object_store::path::Path::from(path.to_string_lossy().as_ref());
        let segment = SegmentFileInfo {
            segment_ord: 0,
            max_doc: total_rows,
            object_path,
            parquet_size: size,
            row_groups: rgs,
            metadata: Arc::clone(&parquet_meta),
            global_base: 0,
        };

        // Single bench: all rows (no filter), emit_row_ids from position
        let segment_clone = segment.clone();
        let schema_clone = schema.clone();
        let id = BenchmarkId::new("1seg/indexed_pred", "all_rows");
        group.bench_function(id, |b| {
            b.to_async(mgr.io_runtime.as_ref()).iter(|| {
                let segment = segment_clone.clone();
                let schema = schema_clone.clone();
                async move {
                    let factory: opensearch_datafusion::indexed_table::table_provider::EvaluatorFactory =
                        Arc::new(move |_seg, _chunk, _sm| {
                            Ok(Arc::new(SelectAllBitsetSource) as Arc<dyn RowGroupBitsetSource>)
                        });
                    let store: Arc<dyn object_store::ObjectStore> = Arc::new(LocalFileSystem::new());
                    let store_url = datafusion::execution::object_store::ObjectStoreUrl::local_filesystem();
                    let provider = Arc::new(IndexedTableProvider::new(IndexedTableConfig {
                        schema,
                        segments: vec![segment],
                        store,
                        store_url,
                        evaluator_factory: factory,
                        target_partitions: 10,
                        force_strategy: None,
                        force_pushdown: Some(false),
                        pushdown_predicate: None,
                        query_config: Arc::new(opensearch_datafusion::datafusion_query_config::DatafusionQueryConfig {
                            target_partitions: 10,
                            ..Default::default()
                        }),
                        predicate_columns: vec![],
                        emit_row_ids: true,
                    }));
                    let ctx = datafusion::prelude::SessionContext::new();
                    ctx.register_table("t", provider).unwrap();
                    let df = ctx.sql("SELECT * FROM t").await.unwrap();
                    let mut stream = df.execute_stream().await.unwrap();
                    let mut rows = 0u64;
                    while let Some(batch) = stream.try_next().await.unwrap() {
                        rows += batch.num_rows() as u64;
                    }
                    rows
                }
            });
        });
    }

    group.finish();
    mgr.cpu_executor.shutdown();
    std::mem::forget(mgr);
}

/// Correctness check: all approaches must return the same row count
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
        ("baseline", RowIdStrategy::None),
        ("listing_table", RowIdStrategy::ListingTable),
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
