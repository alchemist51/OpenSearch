/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! POC: end-to-end tests for `emit_row_ids` mode.
//! Verifies that the indexed query path can return global row IDs
//! instead of actual data columns.

use std::sync::Arc;

use datafusion::arrow::array::UInt64Array;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::execution::context::SessionContext;
use datafusion::logical_expr::Operator;
use datafusion::parquet::arrow::arrow_reader::{ArrowReaderMetadata, ArrowReaderOptions};
use futures::StreamExt;

use super::*;

/// Helper: run a tree with `emit_row_ids: true` and return the collected row IDs.
async fn run_tree_row_ids(tree: BoolNode) -> Vec<u64> {
    let tmp = write_fixture_parquet();
    let path = tmp.path().to_path_buf();
    let size = std::fs::metadata(&path).unwrap().len();

    let file = std::fs::File::open(&path).unwrap();
    let meta =
        ArrowReaderMetadata::load(&file, ArrowReaderOptions::new().with_page_index(true)).unwrap();
    let schema = meta.schema().clone();
    let parquet_meta = meta.metadata().clone();
    let mut rgs = Vec::new();
    let mut offset = 0i64;
    for i in 0..parquet_meta.num_row_groups() {
        let n = parquet_meta.row_group(i).num_rows();
        rgs.push(RowGroupInfo {
            index: i,
            first_row: offset,
            num_rows: n,
        });
        offset += n;
    }

    let object_path = object_store::path::Path::from(path.to_string_lossy().as_ref());
    let segment = SegmentFileInfo {
        segment_ord: 0,
        max_doc: 16,
        object_path,
        parquet_size: size,
        row_groups: rgs,
        metadata: Arc::clone(&parquet_meta),
        global_base: 0,
    };

    let tree = tree.push_not_down();
    let collectors = wire_collectors(&tree);
    let per_leaf: Vec<(i32, Arc<dyn RowGroupDocsCollector>)> = collectors
        .into_iter()
        .enumerate()
        .map(|(i, c)| (i as i32, c))
        .collect();
    let tree = Arc::new(tree);
    let factory: super::super::table_provider::EvaluatorFactory = {
        let per_leaf = per_leaf.clone();
        let tree = Arc::clone(&tree);
        let schema = schema.clone();
        Arc::new(move |segment, _chunk, _stream_metrics| {
            let resolved = tree.resolve(&per_leaf)?;
            let pruner = Arc::new(PagePruner::new(&schema, Arc::clone(&segment.metadata)));
            let eval: Arc<dyn RowGroupBitsetSource> = Arc::new(TreeBitsetSource {
                tree: Arc::new(resolved),
                evaluator: Arc::new(BitmapTreeEvaluator),
                leaves: Arc::new(
                    crate::indexed_table::eval::bitmap_tree::CollectorLeafBitmaps {
                        ffm_collector_calls: _stream_metrics.ffm_collector_calls.clone(),
                    },
                ),
                page_pruner: pruner,
                cost_predicate: 1,
                cost_collector: 10,
                max_collector_parallelism: 1,
                pruning_predicates: Arc::new(std::collections::HashMap::new()),
                page_prune_metrics: Some(
                    crate::indexed_table::page_pruner::PagePruneMetrics::from_stream_metrics(
                        _stream_metrics,
                    ),
                ),
                collector_strategy:
                    crate::indexed_table::eval::CollectorCallStrategy::TightenOuterBounds,
            });
            Ok(eval)
        })
    };

    let store: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::local::LocalFileSystem::new());
    let store_url = datafusion::execution::object_store::ObjectStoreUrl::local_filesystem();
    let provider = Arc::new(IndexedTableProvider::new(IndexedTableConfig {
        schema: schema.clone(),
        segments: vec![segment],
        store,
        store_url,
        evaluator_factory: factory,
        target_partitions: 1,
        force_strategy: Some(FilterStrategy::BooleanMask),
        force_pushdown: Some(false),
        pushdown_predicate: None,
        query_config: Arc::new(crate::datafusion_query_config::DatafusionQueryConfig::default()),
        predicate_columns: vec![],
        emit_row_ids: true, // <-- POC flag
    }));

    let ctx = SessionContext::new();
    ctx.register_table("t", provider).unwrap();
    // SELECT * — the schema is overridden to [_row_id: UInt64] by the flag
    let df = ctx.sql("SELECT * FROM t").await.unwrap();
    let plan = df.create_physical_plan().await.unwrap();
    let task_ctx = ctx.task_ctx();
    let mut stream = datafusion::physical_plan::execute_stream(plan, task_ctx).unwrap();
    let mut row_ids: Vec<u64> = Vec::new();
    while let Some(batch) = stream.next().await {
        let b = batch.unwrap();
        assert_eq!(b.num_columns(), 1, "should have only _row_id column");
        assert_eq!(b.schema().field(0).name(), "_row_id");
        let col = b.column(0).as_any().downcast_ref::<UInt64Array>().unwrap();
        for i in 0..b.num_rows() {
            row_ids.push(col.value(i));
        }
    }
    row_ids.sort();
    row_ids
}

// ── Tests ────────────────────────────────────────────────────────────

/// brand="amazon" matches rows 0,1,2,3,12 — verify we get those row IDs back.
#[tokio::test]
async fn test_emit_row_ids_single_collector_amazon() {
    // Collector tag 0 = brand_eq("amazon")
    let tree = BoolNode::And(vec![index_leaf(0)]);
    let ids = run_tree_row_ids(tree).await;
    assert_eq!(ids, vec![0, 1, 2, 3, 12]);
}

/// brand="apple" matches rows 4,5,6,7,13.
#[tokio::test]
async fn test_emit_row_ids_single_collector_apple() {
    let tree = BoolNode::And(vec![index_leaf(1)]);
    let ids = run_tree_row_ids(tree).await;
    assert_eq!(ids, vec![4, 5, 6, 7, 13]);
}

/// AND(brand="amazon", price > 100) matches rows where brand=amazon AND price>100.
/// amazon rows: 0(50), 1(150), 2(80), 3(120), 12(30) → price>100: rows 1, 3.
#[tokio::test]
async fn test_emit_row_ids_collector_and_predicate() {
    let tree = BoolNode::And(vec![
        index_leaf(0), // amazon
        pred_int("price", Operator::Gt, 100),
    ]);
    let ids = run_tree_row_ids(tree).await;
    assert_eq!(ids, vec![1, 3]);
}

/// OR(brand="amazon", brand="apple") matches rows 0-7, 12, 13.
#[tokio::test]
async fn test_emit_row_ids_or_two_collectors() {
    let tree = BoolNode::Or(vec![index_leaf(0), index_leaf(1)]);
    let ids = run_tree_row_ids(tree).await;
    assert_eq!(ids, vec![0, 1, 2, 3, 4, 5, 6, 7, 12, 13]);
}

/// AND(brand="apple", status="archived") — apple rows: 4,5,6,7,13;
/// archived rows: 1,5,9,12,13. Intersection: 5, 13.
#[tokio::test]
async fn test_emit_row_ids_two_collectors_and() {
    let tree = BoolNode::And(vec![index_leaf(1), index_leaf(2)]);
    let ids = run_tree_row_ids(tree).await;
    assert_eq!(ids, vec![5, 13]);
}

/// Verify global_base offset works: set global_base=1000 and check IDs are shifted.
async fn run_tree_row_ids_with_global_base(tree: BoolNode, global_base: u64) -> Vec<u64> {
    let tmp = write_fixture_parquet();
    let path = tmp.path().to_path_buf();
    let size = std::fs::metadata(&path).unwrap().len();

    let file = std::fs::File::open(&path).unwrap();
    let meta =
        ArrowReaderMetadata::load(&file, ArrowReaderOptions::new().with_page_index(true)).unwrap();
    let schema = meta.schema().clone();
    let parquet_meta = meta.metadata().clone();
    let mut rgs = Vec::new();
    let mut offset = 0i64;
    for i in 0..parquet_meta.num_row_groups() {
        let n = parquet_meta.row_group(i).num_rows();
        rgs.push(RowGroupInfo {
            index: i,
            first_row: offset,
            num_rows: n,
        });
        offset += n;
    }

    let object_path = object_store::path::Path::from(path.to_string_lossy().as_ref());
    let segment = SegmentFileInfo {
        segment_ord: 0,
        max_doc: 16,
        object_path,
        parquet_size: size,
        row_groups: rgs,
        metadata: Arc::clone(&parquet_meta),
        global_base,
    };

    let tree = tree.push_not_down();
    let collectors = wire_collectors(&tree);
    let per_leaf: Vec<(i32, Arc<dyn RowGroupDocsCollector>)> = collectors
        .into_iter()
        .enumerate()
        .map(|(i, c)| (i as i32, c))
        .collect();
    let tree = Arc::new(tree);
    let factory: super::super::table_provider::EvaluatorFactory = {
        let per_leaf = per_leaf.clone();
        let tree = Arc::clone(&tree);
        let schema = schema.clone();
        Arc::new(move |segment, _chunk, _stream_metrics| {
            let resolved = tree.resolve(&per_leaf)?;
            let pruner = Arc::new(PagePruner::new(&schema, Arc::clone(&segment.metadata)));
            let eval: Arc<dyn RowGroupBitsetSource> = Arc::new(TreeBitsetSource {
                tree: Arc::new(resolved),
                evaluator: Arc::new(BitmapTreeEvaluator),
                leaves: Arc::new(
                    crate::indexed_table::eval::bitmap_tree::CollectorLeafBitmaps {
                        ffm_collector_calls: _stream_metrics.ffm_collector_calls.clone(),
                    },
                ),
                page_pruner: pruner,
                cost_predicate: 1,
                cost_collector: 10,
                max_collector_parallelism: 1,
                pruning_predicates: Arc::new(std::collections::HashMap::new()),
                page_prune_metrics: Some(
                    crate::indexed_table::page_pruner::PagePruneMetrics::from_stream_metrics(
                        _stream_metrics,
                    ),
                ),
                collector_strategy:
                    crate::indexed_table::eval::CollectorCallStrategy::TightenOuterBounds,
            });
            Ok(eval)
        })
    };

    let store: Arc<dyn object_store::ObjectStore> =
        Arc::new(object_store::local::LocalFileSystem::new());
    let store_url = datafusion::execution::object_store::ObjectStoreUrl::local_filesystem();
    let provider = Arc::new(IndexedTableProvider::new(IndexedTableConfig {
        schema: schema.clone(),
        segments: vec![segment],
        store,
        store_url,
        evaluator_factory: factory,
        target_partitions: 1,
        force_strategy: Some(FilterStrategy::BooleanMask),
        force_pushdown: Some(false),
        pushdown_predicate: None,
        query_config: Arc::new(crate::datafusion_query_config::DatafusionQueryConfig::default()),
        predicate_columns: vec![],
        emit_row_ids: true,
    }));

    let ctx = SessionContext::new();
    ctx.register_table("t", provider).unwrap();
    let df = ctx.sql("SELECT * FROM t").await.unwrap();
    let plan = df.create_physical_plan().await.unwrap();
    let task_ctx = ctx.task_ctx();
    let mut stream = datafusion::physical_plan::execute_stream(plan, task_ctx).unwrap();
    let mut row_ids: Vec<u64> = Vec::new();
    while let Some(batch) = stream.next().await {
        let b = batch.unwrap();
        let col = b.column(0).as_any().downcast_ref::<UInt64Array>().unwrap();
        for i in 0..b.num_rows() {
            row_ids.push(col.value(i));
        }
    }
    row_ids.sort();
    row_ids
}

/// With global_base=1000, brand="amazon" (rows 0,1,2,3,12) should give 1000,1001,1002,1003,1012.
#[tokio::test]
async fn test_emit_row_ids_with_global_base_offset() {
    let tree = BoolNode::And(vec![index_leaf(0)]);
    let ids = run_tree_row_ids_with_global_base(tree, 1000).await;
    assert_eq!(ids, vec![1000, 1001, 1002, 1003, 1012]);
}
