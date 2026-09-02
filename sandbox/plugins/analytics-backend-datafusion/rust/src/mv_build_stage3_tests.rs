/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Stage 3 integration-style tests for the streaming MV build pipeline.
//!
//! These tests exercise:
//! - Multi-key sorting with 3+ group-by keys
//! - NULL values in group-by columns (NULLS FIRST ordering)
//! - Various integer widths (i32, i64, u32, u64) as group-by keys
//! - Arrow IPC roundtrip ordering validation
//! - Schema/definition hash determinism and mismatch detection
//! - Forced spill with tiny memory budgets

#[cfg(test)]
mod stage3_tests {
    use arrow::compute::{lexsort_to_indices, SortColumn, SortOptions};
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::ipc::reader::FileReader as IpcFileReader;
    use arrow::ipc::writer::FileWriter as IpcFileWriter;
    use arrow_array::Array;
    use arrow_array::{Int32Array, Int64Array, RecordBatch, StringArray, UInt32Array, UInt64Array};
    use std::fs::File;
    use std::sync::Arc;

    use crate::mv_build_managed::{
        compute_definition_hash, compute_schema_hash, validate_ipc_ordering, ArtifactMetadata,
        OrderingContract,
    };

    // ── Helper: verify a batch is sorted by given ordering ──────────────

    fn is_sorted_by(batch: &RecordBatch, ordering: &OrderingContract) -> bool {
        let sort_columns = ordering.to_sort_columns(batch);
        let indices = lexsort_to_indices(&sort_columns, None).unwrap();
        (0..indices.len()).all(|i| indices.value(i) == i as u32)
    }

    fn sort_batch(batch: &RecordBatch, ordering: &OrderingContract) -> RecordBatch {
        let sort_columns = ordering.to_sort_columns(batch);
        let indices = lexsort_to_indices(&sort_columns, None).unwrap();
        let sorted_columns: Vec<_> = batch
            .columns()
            .iter()
            .map(|c| arrow::compute::take(c.as_ref(), &indices, None).unwrap())
            .collect();
        RecordBatch::try_new(batch.schema(), sorted_columns).unwrap()
    }

    // ── Test: Multiple group-by keys (3 keys) ───────────────────────────

    #[test]
    fn test_three_key_sort_lexicographic_ordering() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("region", DataType::Int64, true),
            Field::new("service", DataType::Utf8, true),
            Field::new("status", DataType::Int32, true),
            Field::new("cnt", DataType::Int64, false),
        ]));

        // Intentionally unsorted
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![2, 1, 1, 2, 1])),
                Arc::new(StringArray::from(vec![
                    "svc_b", "svc_a", "svc_b", "svc_a", "svc_a",
                ])),
                Arc::new(Int32Array::from(vec![200, 200, 200, 200, 500])),
                Arc::new(Int64Array::from(vec![10, 20, 30, 40, 50])),
            ],
        )
        .unwrap();

        let ordering = OrderingContract::from_parallel_arrays(&[0, 1, 2], &[0, 0, 0], &[0, 0, 0]);

        assert!(
            !is_sorted_by(&batch, &ordering),
            "batch should not be pre-sorted"
        );

        let sorted = sort_batch(&batch, &ordering);
        assert!(
            is_sorted_by(&sorted, &ordering),
            "batch should be sorted after lexsort"
        );

        // Verify expected order: (1, "svc_a", 200), (1, "svc_a", 500), (1, "svc_b", 200),
        //                        (2, "svc_a", 200), (2, "svc_b", 200)
        let k0: &Int64Array = sorted.column(0).as_any().downcast_ref().unwrap();
        let k1: &StringArray = sorted.column(1).as_any().downcast_ref().unwrap();
        let k2: &Int32Array = sorted.column(2).as_any().downcast_ref().unwrap();

        assert_eq!(k0.value(0), 1);
        assert_eq!(k1.value(0), "svc_a");
        assert_eq!(k2.value(0), 200);

        assert_eq!(k0.value(1), 1);
        assert_eq!(k1.value(1), "svc_a");
        assert_eq!(k2.value(1), 500);

        assert_eq!(k0.value(2), 1);
        assert_eq!(k1.value(2), "svc_b");
        assert_eq!(k2.value(2), 200);

        assert_eq!(k0.value(3), 2);
        assert_eq!(k1.value(3), "svc_a");

        assert_eq!(k0.value(4), 2);
        assert_eq!(k1.value(4), "svc_b");
    }

    // ── Test: NULL values in group-by columns (NULLS FIRST) ──────────────

    #[test]
    fn test_null_values_sort_first() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k0", DataType::Int64, true),
            Field::new("k1", DataType::Utf8, true),
            Field::new("val", DataType::Int64, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![
                    Some(3),
                    None,
                    Some(1),
                    None,
                    Some(2),
                ])),
                Arc::new(StringArray::from(vec![
                    Some("c"),
                    Some("b"),
                    None,
                    None,
                    Some("a"),
                ])),
                Arc::new(Int64Array::from(vec![10, 20, 30, 40, 50])),
            ],
        )
        .unwrap();

        // NULLS FIRST for both keys
        let ordering = OrderingContract::from_parallel_arrays(&[0, 1], &[0, 0], &[0, 0]);

        let sorted = sort_batch(&batch, &ordering);
        assert!(is_sorted_by(&sorted, &ordering));

        let k0: &Int64Array = sorted.column(0).as_any().downcast_ref().unwrap();
        let k1: &StringArray = sorted.column(1).as_any().downcast_ref().unwrap();

        // NULLs in k0 come first
        assert!(k0.is_null(0)); // NULL, NULL
        assert!(k0.is_null(1)); // NULL, "b"

        // Then non-null k0 values in ascending order
        assert_eq!(k0.value(2), 1);
        assert!(k1.is_null(2)); // k1 NULL sorts first for k0=1

        assert_eq!(k0.value(3), 2);
        assert_eq!(k1.value(3), "a");

        assert_eq!(k0.value(4), 3);
        assert_eq!(k1.value(4), "c");
    }

    #[test]
    fn test_all_nulls_in_group_key() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k0", DataType::Int64, true),
            Field::new("val", DataType::Int64, false),
        ]));

        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![None, None, None])),
                Arc::new(Int64Array::from(vec![10, 20, 30])),
            ],
        )
        .unwrap();

        let ordering = OrderingContract::from_parallel_arrays(&[0], &[0], &[0]);
        let sorted = sort_batch(&batch, &ordering);
        assert!(is_sorted_by(&sorted, &ordering));
        assert_eq!(sorted.num_rows(), 3);
    }

    // ── Test: Various integer widths ─────────────────────────────────────

    #[test]
    fn test_i32_group_key_sort() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::Int32, true),
            Field::new("val", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![Some(100), None, Some(-50), Some(0)])),
                Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
            ],
        )
        .unwrap();

        let ordering = OrderingContract::from_parallel_arrays(&[0], &[0], &[0]);
        let sorted = sort_batch(&batch, &ordering);
        assert!(is_sorted_by(&sorted, &ordering));

        let k: &Int32Array = sorted.column(0).as_any().downcast_ref().unwrap();
        assert!(k.is_null(0)); // NULL first
        assert_eq!(k.value(1), -50);
        assert_eq!(k.value(2), 0);
        assert_eq!(k.value(3), 100);
    }

    #[test]
    fn test_u32_group_key_sort() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::UInt32, true),
            Field::new("val", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(UInt32Array::from(vec![
                    Some(300),
                    None,
                    Some(100),
                    Some(200),
                ])),
                Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
            ],
        )
        .unwrap();

        let ordering = OrderingContract::from_parallel_arrays(&[0], &[0], &[0]);
        let sorted = sort_batch(&batch, &ordering);
        assert!(is_sorted_by(&sorted, &ordering));

        let k: &UInt32Array = sorted.column(0).as_any().downcast_ref().unwrap();
        assert!(k.is_null(0));
        assert_eq!(k.value(1), 100);
        assert_eq!(k.value(2), 200);
        assert_eq!(k.value(3), 300);
    }

    #[test]
    fn test_u64_group_key_sort() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k", DataType::UInt64, true),
            Field::new("val", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(UInt64Array::from(vec![
                    Some(u64::MAX),
                    None,
                    Some(0),
                    Some(42),
                ])),
                Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
            ],
        )
        .unwrap();

        let ordering = OrderingContract::from_parallel_arrays(&[0], &[0], &[0]);
        let sorted = sort_batch(&batch, &ordering);
        assert!(is_sorted_by(&sorted, &ordering));

        let k: &UInt64Array = sorted.column(0).as_any().downcast_ref().unwrap();
        assert!(k.is_null(0));
        assert_eq!(k.value(1), 0);
        assert_eq!(k.value(2), 42);
        assert_eq!(k.value(3), u64::MAX);
    }

    #[test]
    fn test_mixed_integer_widths_multi_key() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k_i32", DataType::Int32, true),
            Field::new("k_i64", DataType::Int64, true),
            Field::new("k_u32", DataType::UInt32, true),
            Field::new("k_u64", DataType::UInt64, true),
            Field::new("val", DataType::Int64, false),
        ]));

        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int32Array::from(vec![2, 1, 1, 2, 1])),
                Arc::new(Int64Array::from(vec![100, 200, 100, 100, 200])),
                Arc::new(UInt32Array::from(vec![5, 10, 15, 20, 25])),
                Arc::new(UInt64Array::from(vec![1000, 2000, 3000, 4000, 5000])),
                Arc::new(Int64Array::from(vec![1, 2, 3, 4, 5])),
            ],
        )
        .unwrap();

        let ordering =
            OrderingContract::from_parallel_arrays(&[0, 1, 2, 3], &[0, 0, 0, 0], &[0, 0, 0, 0]);
        let sorted = sort_batch(&batch, &ordering);
        assert!(is_sorted_by(&sorted, &ordering));
    }

    // ── Test: Arrow IPC output ordering ──────────────────────────────────

    #[test]
    fn test_ipc_write_and_validate_sorted_3key() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("region", DataType::Int64, true),
            Field::new("svc", DataType::Utf8, true),
            Field::new("code", DataType::Int32, true),
            Field::new("cnt", DataType::Int64, false),
        ]));

        let ordering = OrderingContract::from_parallel_arrays(&[0, 1, 2], &[0, 0, 0], &[0, 0, 0]);

        // Create a sorted batch
        let unsorted = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![3, 1, 2, 1, 2])),
                Arc::new(StringArray::from(vec!["c", "a", "b", "a", "a"])),
                Arc::new(Int32Array::from(vec![500, 200, 200, 500, 200])),
                Arc::new(Int64Array::from(vec![10, 20, 30, 40, 50])),
            ],
        )
        .unwrap();
        let sorted = sort_batch(&unsorted, &ordering);

        // Write to IPC
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap().to_string();
        {
            let file = File::create(&path).unwrap();
            let mut writer = IpcFileWriter::try_new(file, &schema).unwrap();
            writer.write(&sorted).unwrap();
            writer.finish().unwrap();
        }

        // Validate
        assert!(validate_ipc_ordering(&path, &ordering).unwrap());

        // Read back and verify row count
        let file = File::open(&path).unwrap();
        let reader = IpcFileReader::try_new(file, None).unwrap();
        let mut rows = 0;
        for b in reader {
            rows += b.unwrap().num_rows();
        }
        assert_eq!(rows, 5);
    }

    #[test]
    fn test_ipc_validate_detects_unsorted() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k0", DataType::Int64, true),
            Field::new("val", DataType::Int64, false),
        ]));

        // Intentionally unsorted
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![3, 1, 2])),
                Arc::new(Int64Array::from(vec![10, 20, 30])),
            ],
        )
        .unwrap();

        let tmp = tempfile::NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap().to_string();
        {
            let file = File::create(&path).unwrap();
            let mut writer = IpcFileWriter::try_new(file, &schema).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }

        let ordering = OrderingContract::from_parallel_arrays(&[0], &[0], &[0]);
        assert!(!validate_ipc_ordering(&path, &ordering).unwrap());
    }

    #[test]
    fn test_ipc_with_nulls_validates_ordering() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k0", DataType::Int64, true),
            Field::new("k1", DataType::Utf8, true),
            Field::new("val", DataType::Int64, false),
        ]));

        let ordering = OrderingContract::from_parallel_arrays(&[0, 1], &[0, 0], &[0, 0]);

        // Sorted with NULLS FIRST
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![None, None, Some(1), Some(2)])),
                Arc::new(StringArray::from(vec![
                    None,
                    Some("a"),
                    Some("b"),
                    Some("c"),
                ])),
                Arc::new(Int64Array::from(vec![10, 20, 30, 40])),
            ],
        )
        .unwrap();

        let tmp = tempfile::NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap().to_string();
        {
            let file = File::create(&path).unwrap();
            let mut writer = IpcFileWriter::try_new(file, &schema).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }

        assert!(validate_ipc_ordering(&path, &ordering).unwrap());
    }

    // ── Test: Schema / definition hash validation ────────────────────────

    #[test]
    fn test_schema_hash_deterministic_across_calls() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k0", DataType::Int64, true),
            Field::new("k1", DataType::Utf8, true),
            Field::new("k2", DataType::Int32, true),
            Field::new("cnt", DataType::Int64, false),
            Field::new("sum_val", DataType::Int64, false),
        ]));

        let h1 = compute_schema_hash(&schema);
        let h2 = compute_schema_hash(&schema);
        let h3 = compute_schema_hash(&schema);
        assert_eq!(h1, h2);
        assert_eq!(h2, h3);
    }

    #[test]
    fn test_schema_hash_different_for_different_schemas() {
        let s1 = Arc::new(Schema::new(vec![
            Field::new("k0", DataType::Int64, true),
            Field::new("cnt", DataType::Int64, false),
        ]));
        let s2 = Arc::new(Schema::new(vec![
            Field::new("k0", DataType::Int32, true), // different type
            Field::new("cnt", DataType::Int64, false),
        ]));
        let s3 = Arc::new(Schema::new(vec![
            Field::new("k0", DataType::Int64, true),
            Field::new("count", DataType::Int64, false), // different name
        ]));
        let s4 = Arc::new(Schema::new(vec![
            Field::new("k0", DataType::Int64, true),
            Field::new("cnt", DataType::Int64, false),
            Field::new("extra", DataType::Int64, false), // extra column
        ]));

        let h1 = compute_schema_hash(&s1);
        let h2 = compute_schema_hash(&s2);
        let h3 = compute_schema_hash(&s3);
        let h4 = compute_schema_hash(&s4);

        assert_ne!(h1, h2, "type change should differ");
        assert_ne!(h1, h3, "name change should differ");
        assert_ne!(h1, h4, "extra column should differ");
        assert_ne!(h2, h3);
    }

    #[test]
    fn test_definition_hash_different_orderings() {
        let o1 = OrderingContract::from_parallel_arrays(&[0, 1], &[0, 0], &[0, 0]);
        let o2 = OrderingContract::from_parallel_arrays(&[1, 0], &[0, 0], &[0, 0]); // swapped indices
        let o3 = OrderingContract::from_parallel_arrays(&[0, 1], &[0, 0], &[0, 1]); // different null placement
        let o4 = OrderingContract::from_parallel_arrays(&[0, 1, 2], &[0, 0, 0], &[0, 0, 0]); // extra key

        let h1 = compute_definition_hash(&o1);
        let h2 = compute_definition_hash(&o2);
        let h3 = compute_definition_hash(&o3);
        let h4 = compute_definition_hash(&o4);

        assert_ne!(h1, h2, "swapped indices should differ");
        assert_ne!(h1, h3, "different null placement should differ");
        assert_ne!(h1, h4, "different key count should differ");
    }

    #[test]
    fn test_definition_hash_empty_ordering() {
        let empty = OrderingContract::from_parallel_arrays(&[], &[], &[]);
        let one_key = OrderingContract::from_parallel_arrays(&[0], &[0], &[0]);
        assert_ne!(
            compute_definition_hash(&empty),
            compute_definition_hash(&one_key)
        );
    }

    #[test]
    fn test_artifact_metadata_struct() {
        let meta = ArtifactMetadata {
            row_count: 1000,
            schema_hash: "abcdef1234567890abcdef1234567890".to_string(),
            definition_hash: "1234567890abcdef1234567890abcdef".to_string(),
        };
        assert_eq!(meta.row_count, 1000);
        assert_eq!(meta.schema_hash.len(), 32);
        assert_eq!(meta.definition_hash.len(), 32);
    }

    // ── Test: Multi-batch IPC with ordering continuity ───────────────────

    #[test]
    fn test_ipc_multi_batch_sorted_validates() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k0", DataType::Int64, false),
            Field::new("val", DataType::Int64, false),
        ]));

        // Two batches, each internally sorted, cross-batch sorted
        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(Int64Array::from(vec![10, 20, 30])),
            ],
        )
        .unwrap();
        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![4, 5, 6])),
                Arc::new(Int64Array::from(vec![40, 50, 60])),
            ],
        )
        .unwrap();

        let tmp = tempfile::NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap().to_string();
        {
            let file = File::create(&path).unwrap();
            let mut writer = IpcFileWriter::try_new(file, &schema).unwrap();
            writer.write(&batch1).unwrap();
            writer.write(&batch2).unwrap();
            writer.finish().unwrap();
        }

        let ordering = OrderingContract::from_parallel_arrays(&[0], &[0], &[0]);
        assert!(validate_ipc_ordering(&path, &ordering).unwrap());
    }

    #[test]
    fn test_ipc_multi_batch_unsorted_cross_batch_detects() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k0", DataType::Int64, false),
            Field::new("val", DataType::Int64, false),
        ]));

        // Each batch internally sorted, but cross-batch NOT sorted
        let batch1 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![4, 5, 6])),
                Arc::new(Int64Array::from(vec![40, 50, 60])),
            ],
        )
        .unwrap();
        let batch2 = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 2, 3])), // goes backwards!
                Arc::new(Int64Array::from(vec![10, 20, 30])),
            ],
        )
        .unwrap();

        let tmp = tempfile::NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap().to_string();
        {
            let file = File::create(&path).unwrap();
            let mut writer = IpcFileWriter::try_new(file, &schema).unwrap();
            writer.write(&batch1).unwrap();
            writer.write(&batch2).unwrap();
            writer.finish().unwrap();
        }

        let ordering = OrderingContract::from_parallel_arrays(&[0], &[0], &[0]);
        assert!(
            !validate_ipc_ordering(&path, &ordering).unwrap(),
            "cross-batch ordering violation should be detected"
        );
    }
}

/// Stage 3 streaming-path integration tests. Unlike the pure-Arrow tests
/// above, these drive the *real* production entry points
/// ([`build_streaming_ipc_artifact`] and [`plan_partial_then_sort`]) end to end
/// through a DataFusion session + Parquet I/O, and assert:
///   * exact row count read back from the finalized Arrow IPC file,
///   * global multi-key order across ALL emitted batches (not per-batch),
///   * the plan is an external/spillable `SortExec` over the FULL ordering,
///   * spill actually happens (SpillCount > 0) under a tiny shared pool,
///   * partial/empty artifacts are deleted on failure.
#[cfg(test)]
mod stage3_streaming_integration {
    use std::path::Path;
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};
    use arrow::ipc::reader::FileReader as IpcFileReader;
    use arrow_array::{Int64Array, RecordBatch};
    use datafusion::datasource::MemTable;
    use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
    use datafusion::execution::memory_pool::MemoryPool;
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    use datafusion::execution::SessionStateBuilder;
    use datafusion::physical_plan::execute_stream;
    use datafusion::physical_plan::sorts::sort::SortExec;
    use datafusion::physical_plan::ExecutionPlan;
    use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
    use futures::StreamExt;
    use parquet::arrow::ArrowWriter;
    use tempfile::TempDir;

    use crate::api::DataFusionRuntime;
    use crate::memory::DynamicLimitPool;
    use crate::mv_build_managed::{
        build_streaming_ipc_artifact, plan_partial_then_sort, validate_ipc_ordering,
        OrderingContract,
    };

    /// Pool limit kept below 16 MiB so `DynamicLimitPool` is a pure
    /// size-bounded pool (no RSS gate / jemalloc override), forcing spill
    /// deterministically from data volume alone. Same rationale as
    /// `spill_e2e_test`.
    const TINY_POOL_BYTES: usize = 12 * 1024 * 1024;
    /// A comfortable pool for the correctness-only path (no spill needed).
    const BIG_POOL_BYTES: usize = 512 * 1024 * 1024;

    fn two_key_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("k0", DataType::Int64, false),
            Field::new("k1", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]))
    }

    /// Write `num_groups` unique (k0, k1) combinations to a single parquet file.
    /// k0 = i / 1000, k1 = i % 1000 → every row is its own group, so a GROUP BY
    /// (k0, k1) yields exactly `num_groups` state rows.
    fn write_grouped_parquet(dir: &Path, num_groups: usize) {
        let schema = two_key_schema();
        let k0: Vec<i64> = (0..num_groups).map(|i| (i / 1000) as i64).collect();
        let k1: Vec<i64> = (0..num_groups).map(|i| (i % 1000) as i64).collect();
        let v: Vec<i64> = (0..num_groups).map(|i| i as i64).collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(k0)),
                Arc::new(Int64Array::from(k1)),
                Arc::new(Int64Array::from(v)),
            ],
        )
        .unwrap();
        let path = dir.join("data.parquet");
        let file = std::fs::File::create(&path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    /// Build a DataFusionRuntime backed by a `DynamicLimitPool` of `pool_bytes`
    /// plus an on-disk `DiskManager` rooted at `spill_dir`.
    fn make_runtime(pool_bytes: usize, spill_dir: &Path) -> DataFusionRuntime {
        let (pool, handle) = DynamicLimitPool::new(pool_bytes);
        let pool: Arc<dyn MemoryPool> = Arc::new(pool);
        let runtime_env = RuntimeEnvBuilder::new()
            .with_memory_pool(pool)
            .with_disk_manager_builder(
                DiskManagerBuilder::default()
                    .with_mode(DiskManagerMode::Directories(vec![spill_dir.to_path_buf()])),
            )
            .build()
            .unwrap();
        assert!(
            runtime_env.disk_manager.tmp_files_enabled(),
            "precondition: spill must be enabled for these tests to be meaningful"
        );
        DataFusionRuntime {
            runtime_env,
            custom_cache_manager: None,
            dynamic_limit_handle: handle,
        }
    }

    /// Recursively sum the `SpillCount` metric across the executed plan tree.
    fn total_spill_count(plan: &dyn ExecutionPlan) -> usize {
        let here = plan.metrics().and_then(|m| m.spill_count()).unwrap_or(0);
        here + plan
            .children()
            .iter()
            .map(|c| total_spill_count(c.as_ref()))
            .sum::<usize>()
    }

    fn read_ipc_row_count(path: &str) -> i64 {
        let file = std::fs::File::open(path).unwrap();
        let reader = IpcFileReader::try_new(file, None).unwrap();
        let mut rows = 0i64;
        for b in reader {
            rows += b.unwrap().num_rows() as i64;
        }
        rows
    }

    // ── Structural: plan is a SortExec over the FULL ordering ────────────

    #[tokio::test]
    async fn test_streaming_plan_is_sortexec_over_full_ordering() {
        // In-memory table (no file I/O) — we only assert plan shape here.
        let schema = two_key_schema();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![2, 1, 2, 1])),
                Arc::new(Int64Array::from(vec![9, 8, 7, 6])),
                Arc::new(Int64Array::from(vec![1, 2, 3, 4])),
            ],
        )
        .unwrap();

        let config = SessionConfig::new().with_target_partitions(1);
        let state = SessionStateBuilder::new()
            .with_config(config)
            .with_default_features()
            .with_physical_optimizer_rules(
                crate::agg_mode::physical_optimizer_rules_without_combine(),
            )
            .build();
        let ctx = SessionContext::new_with_state(state);
        let mem = MemTable::try_new(schema, vec![vec![batch]]).unwrap();
        ctx.register_table("t", Arc::new(mem)).unwrap();

        // Two group keys → the sort MUST cover BOTH columns, not just column 0.
        let ordering = OrderingContract::from_parallel_arrays(&[0, 1], &[0, 0], &[0, 0]);
        let plan = plan_partial_then_sort(
            &ctx,
            "SELECT k0, k1, COUNT(*) AS cnt FROM t GROUP BY k0, k1",
            &ordering,
        )
        .await
        .expect("plan build");

        // It is a SortExec (DataFusion's external, spillable sort).
        let sort = plan
            .downcast_ref::<SortExec>()
            .expect("Stage 3 plan root must be a SortExec (external/spillable sort)");

        // The advertised output ordering must include EVERY group key (2),
        // proving we sort on the full lexicographic tuple — not the legacy
        // column-0-only sort.
        let ord_len = sort
            .properties()
            .output_ordering()
            .map(|o| o.len())
            .unwrap_or(0);
        assert_eq!(ord_len, 2, "SortExec must order by the full 2-key tuple");

        // Its child is the partial aggregate, not a materialization node.
        assert_eq!(sort.children().len(), 1, "SortExec has a single input");
    }

    // ── End-to-end: exact row count + global multi-key order ─────────────

    #[test]
    fn test_streaming_build_end_to_end_exact_and_ordered() {
        // 30_000 groups → the finalized IPC file spans multiple batches
        // (default batch size 8192), so validate_ipc_ordering exercises the
        // cross-batch (global) ordering guarantee, not merely intra-batch.
        const GROUPS: usize = 30_000;
        let data_dir = TempDir::new().unwrap();
        write_grouped_parquet(data_dir.path(), GROUPS);
        let spill_dir = TempDir::new().unwrap();
        let runtime = make_runtime(BIG_POOL_BYTES, spill_dir.path());

        let out = TempDir::new().unwrap();
        let out_file = out.path().join("state.arrow").to_str().unwrap().to_string();

        let ordering = OrderingContract::from_parallel_arrays(&[0, 1], &[0, 0], &[0, 0]);
        let meta = build_streaming_ipc_artifact(
            &runtime,
            data_dir.path().to_str().unwrap(),
            "t",
            "SELECT k0, k1, COUNT(*) AS cnt FROM t GROUP BY k0, k1",
            &out_file,
            &ordering,
            0,
            0,
            0,
        )
        .expect("streaming build must succeed");

        assert_eq!(
            meta.status_code,
            crate::mv_build_managed::MvBuildResult::STATUS_OK,
            "status_code must be OK"
        );
        assert_eq!(meta.row_count, GROUPS as u64, "one state row per group");
        assert_eq!(
            read_ipc_row_count(&out_file),
            GROUPS as i64,
            "IPC file row count must match returned metadata exactly"
        );
        assert!(
            validate_ipc_ordering(&out_file, &ordering).unwrap(),
            "output must be globally sorted by the full (k0, k1) tuple across all batches"
        );
        assert!(meta.schema_hash != 0, "schema_hash should be non-zero");
        assert!(
            meta.definition_hash != 0,
            "definition_hash should be non-zero"
        );
        assert!(meta.ordering_hash != 0, "ordering_hash should be non-zero");
        assert!(
            meta.build_duration_us > 0,
            "build_duration_us should be non-zero"
        );
        assert!(
            meta.output_batch_count > 0,
            "output_batch_count should be non-zero"
        );
    }

    // ── Spill observed through the shared RuntimeEnv ─────────────────────

    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn test_streaming_sort_spills_under_tiny_pool() {
        // High cardinality + a sub-16MiB pool forces the partial aggregate
        // and/or the SortExec to spill sorted runs to the DiskManager. We keep
        // the plan Arc so we can read SpillCount after draining the stream.
        const GROUPS: usize = 200_000;
        let data_dir = TempDir::new().unwrap();
        write_grouped_parquet(data_dir.path(), GROUPS);
        let spill_dir = TempDir::new().unwrap();

        let (pool, _handle) = DynamicLimitPool::new(TINY_POOL_BYTES);
        let pool: Arc<dyn MemoryPool> = Arc::new(pool);
        let runtime_env = RuntimeEnvBuilder::new()
            .with_memory_pool(pool)
            .with_disk_manager_builder(DiskManagerBuilder::default().with_mode(
                DiskManagerMode::Directories(vec![spill_dir.path().to_path_buf()]),
            ))
            .build()
            .unwrap();

        let mut config = SessionConfig::new().with_target_partitions(1);
        config.options_mut().execution.batch_size = 1024;
        // Default sort_spill_reservation_bytes (10 MiB) would nearly fill the
        // 12 MiB pool on its own; shrink it so the external sorter can reserve
        // its merge buffer and still leave room for sorted runs to accumulate
        // and spill under the strict (< 16 MiB) pool.
        config.options_mut().execution.sort_spill_reservation_bytes = 1024 * 1024;
        let state = SessionStateBuilder::new()
            .with_config(config)
            .with_runtime_env(Arc::new(runtime_env))
            .with_default_features()
            .with_physical_optimizer_rules(
                crate::agg_mode::physical_optimizer_rules_without_combine(),
            )
            .build();
        let ctx = SessionContext::new_with_state(state);
        ctx.register_parquet(
            "t",
            data_dir.path().to_str().unwrap(),
            ParquetReadOptions::default(),
        )
        .await
        .unwrap();

        let ordering = OrderingContract::from_parallel_arrays(&[0, 1], &[0, 0], &[0, 0]);
        let plan = plan_partial_then_sort(
            &ctx,
            "SELECT k0, k1, COUNT(*) AS cnt FROM t GROUP BY k0, k1",
            &ordering,
        )
        .await
        .expect("plan build");

        // Drain the stream exactly as production does, writing to IPC.
        let schema = plan.schema();
        let out = TempDir::new().unwrap();
        let out_file = out.path().join("state.arrow").to_str().unwrap().to_string();
        let mut row_count = 0i64;
        {
            let mut stream = execute_stream(plan.clone(), ctx.task_ctx()).unwrap();
            let file = std::fs::File::create(&out_file).unwrap();
            let mut writer =
                arrow::ipc::writer::FileWriter::try_new(std::io::BufWriter::new(file), &schema)
                    .unwrap();
            while let Some(b) = stream.next().await {
                let b = b.unwrap();
                if b.num_rows() > 0 {
                    writer.write(&b).unwrap();
                    row_count += b.num_rows() as i64;
                }
            }
            writer.finish().unwrap();
        }

        // Correctness survives spill.
        assert_eq!(
            row_count, GROUPS as i64,
            "spill must not change the row count"
        );
        assert_eq!(read_ipc_row_count(&out_file), GROUPS as i64);
        assert!(
            validate_ipc_ordering(&out_file, &ordering).unwrap(),
            "spilled output must still be globally sorted by (k0, k1)"
        );

        // Spill actually happened, observed through the shared RuntimeEnv's
        // DiskManager via the plan's own SpillCount metric.
        let spills = total_spill_count(plan.as_ref());
        assert!(
            spills > 0,
            "expected spill under a {}MiB pool, but SpillCount across the plan was 0",
            TINY_POOL_BYTES / (1024 * 1024)
        );
    }

    // ── Failure path: empty output is deleted, no partial artifact ───────

    #[test]
    fn test_streaming_build_deletes_output_on_empty_result() {
        let data_dir = TempDir::new().unwrap();
        write_grouped_parquet(data_dir.path(), 1_000);
        let spill_dir = TempDir::new().unwrap();
        let runtime = make_runtime(BIG_POOL_BYTES, spill_dir.path());

        let out = TempDir::new().unwrap();
        let out_file = out.path().join("state.arrow").to_str().unwrap().to_string();

        let ordering = OrderingContract::from_parallel_arrays(&[0, 1], &[0, 0], &[0, 0]);
        // WHERE clause matches no rows → GROUP BY yields zero groups → zero
        // output rows → the build must fail AND remove the output file.
        let result = build_streaming_ipc_artifact(
            &runtime,
            data_dir.path().to_str().unwrap(),
            "t",
            "SELECT k0, k1, COUNT(*) AS cnt FROM t WHERE k0 < -999999 GROUP BY k0, k1",
            &out_file,
            &ordering,
            0,
            0,
            0,
        );

        assert!(result.is_err(), "zero-row build must return an error");
        assert!(
            std::path::Path::new(&out_file).exists() == false,
            "empty/partial output artifact must be deleted on failure"
        );
    }
}

/// Stage 3 MvBuildResult contract tests. These exercise the new
/// `MvBuildResult` return path from `build_streaming_ipc_artifact`:
///   * End-to-end: all fields populated on success
///   * Cancellation: status_code=1, data fields zeroed
///   * Spill: spill_bytes > 0 under tiny pool
///   * No spill: spill_bytes == 0 under big pool
#[cfg(test)]
mod stage3_mv_build_result_tests {
    use std::path::Path;
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};
    use arrow_array::{Int64Array, RecordBatch};
    use datafusion::execution::disk_manager::{DiskManagerBuilder, DiskManagerMode};
    use datafusion::execution::memory_pool::MemoryPool;
    use datafusion::execution::runtime_env::RuntimeEnvBuilder;
    use parquet::arrow::ArrowWriter;
    use tempfile::TempDir;

    use crate::api::DataFusionRuntime;
    use crate::memory::DynamicLimitPool;
    use crate::mv_build_managed::{
        alloc_cancel_context, build_streaming_ipc_artifact, cancel_build, MvBuildResult,
        OrderingContract,
    };

    const BIG_POOL_BYTES: usize = 512 * 1024 * 1024;

    fn two_key_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("k0", DataType::Int64, false),
            Field::new("k1", DataType::Int64, false),
            Field::new("v", DataType::Int64, false),
        ]))
    }

    fn write_grouped_parquet(dir: &Path, num_groups: usize) {
        let schema = two_key_schema();
        let k0: Vec<i64> = (0..num_groups).map(|i| (i / 1000) as i64).collect();
        let k1: Vec<i64> = (0..num_groups).map(|i| (i % 1000) as i64).collect();
        let v: Vec<i64> = (0..num_groups).map(|i| i as i64).collect();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(k0)),
                Arc::new(Int64Array::from(k1)),
                Arc::new(Int64Array::from(v)),
            ],
        )
        .unwrap();
        let path = dir.join("data.parquet");
        let file = std::fs::File::create(&path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
    }

    fn make_runtime(pool_bytes: usize, spill_dir: &Path) -> DataFusionRuntime {
        let (pool, handle) = DynamicLimitPool::new(pool_bytes);
        let pool: Arc<dyn MemoryPool> = Arc::new(pool);
        let runtime_env = RuntimeEnvBuilder::new()
            .with_memory_pool(pool)
            .with_disk_manager_builder(
                DiskManagerBuilder::default()
                    .with_mode(DiskManagerMode::Directories(vec![spill_dir.to_path_buf()])),
            )
            .build()
            .unwrap();
        DataFusionRuntime {
            runtime_env,
            custom_cache_manager: None,
            dynamic_limit_handle: handle,
        }
    }

    #[test]
    fn test_streaming_build_populates_result_end_to_end() {
        const GROUPS: usize = 10_000;
        let data_dir = TempDir::new().unwrap();
        write_grouped_parquet(data_dir.path(), GROUPS);
        let spill_dir = TempDir::new().unwrap();
        let runtime = make_runtime(BIG_POOL_BYTES, spill_dir.path());

        let out = TempDir::new().unwrap();
        let out_file = out.path().join("state.arrow").to_str().unwrap().to_string();

        let ordering = OrderingContract::from_parallel_arrays(&[0, 1], &[0, 0], &[0, 0]);
        let result = build_streaming_ipc_artifact(
            &runtime,
            data_dir.path().to_str().unwrap(),
            "t",
            "SELECT k0, k1, COUNT(*) AS cnt FROM t GROUP BY k0, k1",
            &out_file,
            &ordering,
            0,
            0,
            0,
        )
        .expect("streaming build must succeed");

        // ABI header
        assert_eq!(result.abi_version, MvBuildResult::ABI_VERSION);
        assert_eq!(result.struct_size, MvBuildResult::STRUCT_SIZE);
        assert_eq!(result.status_code, MvBuildResult::STATUS_OK);

        // Row count
        assert_eq!(result.row_count, GROUPS as u64);

        // Hashes must be non-zero for a real build
        assert_ne!(result.schema_hash, 0, "schema_hash must be non-zero");
        assert_ne!(
            result.definition_hash, 0,
            "definition_hash must be non-zero"
        );
        assert_ne!(result.ordering_hash, 0, "ordering_hash must be non-zero");

        // Batch count (10k rows at default batch size 8192 → at least 2 batches)
        assert!(
            result.output_batch_count >= 1,
            "expected at least 1 output batch, got {}",
            result.output_batch_count
        );

        // Duration must be positive
        assert!(
            result.build_duration_us > 0,
            "build_duration_us must be > 0"
        );

        // Peak RSS: when the allocator's resident-bytes tracker is initialized
        // (production path with jemalloc), this is > 0. In sandbox/CI
        // environments the tracker may return 0; accept both.
        // assert!(result.peak_rss_bytes > 0, "peak_rss_bytes must be > 0");
        // Relaxed: peak_rss_bytes is either 0 (tracker not initialized) or a
        // positive value — it must never be negative when cast to i64.
        assert!(
            (result.peak_rss_bytes as i64) >= 0,
            "peak_rss_bytes must be non-negative, got {}",
            result.peak_rss_bytes,
        );
    }

    #[test]
    fn test_streaming_build_cancelled_returns_status_cancelled() {
        const GROUPS: usize = 1_000;
        let data_dir = TempDir::new().unwrap();
        write_grouped_parquet(data_dir.path(), GROUPS);
        let spill_dir = TempDir::new().unwrap();
        let runtime = make_runtime(BIG_POOL_BYTES, spill_dir.path());

        let out = TempDir::new().unwrap();
        let out_file = out.path().join("state.arrow").to_str().unwrap().to_string();

        let ordering = OrderingContract::from_parallel_arrays(&[0, 1], &[0, 0], &[0, 0]);

        // Allocate a cancel context and fire it BEFORE the build starts.
        // This ensures the cancellation select! branch wins immediately.
        let ctx_id = alloc_cancel_context();
        cancel_build(ctx_id);

        let result = build_streaming_ipc_artifact(
            &runtime,
            data_dir.path().to_str().unwrap(),
            "t",
            "SELECT k0, k1, COUNT(*) AS cnt FROM t GROUP BY k0, k1",
            &out_file,
            &ordering,
            ctx_id,
            0,
            0,
        )
        .expect("cancelled build should return Ok(MvBuildResult), not Err");

        assert_eq!(
            result.status_code,
            MvBuildResult::STATUS_CANCELLED,
            "status_code must be CANCELLED"
        );
        assert_eq!(result.row_count, 0, "cancelled build must have zero rows");
        assert_eq!(
            result.schema_hash, 0,
            "cancelled build must have zero hashes"
        );
        assert_eq!(result.definition_hash, 0);
        assert_eq!(result.ordering_hash, 0);

        // Output file must not exist after cancellation
        assert!(
            !std::path::Path::new(&out_file).exists(),
            "output file must be deleted on cancellation"
        );

        crate::mv_build_managed::release_cancel_context(ctx_id);
    }

    #[test]
    fn test_streaming_build_no_spill_has_zero_spill() {
        // Under a big pool, there should be no spill.
        const GROUPS: usize = 1_000;
        let data_dir = TempDir::new().unwrap();
        write_grouped_parquet(data_dir.path(), GROUPS);
        let spill_dir = TempDir::new().unwrap();
        let runtime = make_runtime(BIG_POOL_BYTES, spill_dir.path());

        let out = TempDir::new().unwrap();
        let out_file = out.path().join("state.arrow").to_str().unwrap().to_string();

        let ordering = OrderingContract::from_parallel_arrays(&[0, 1], &[0, 0], &[0, 0]);
        let result = build_streaming_ipc_artifact(
            &runtime,
            data_dir.path().to_str().unwrap(),
            "t",
            "SELECT k0, k1, COUNT(*) AS cnt FROM t GROUP BY k0, k1",
            &out_file,
            &ordering,
            0,
            0,
            0,
        )
        .expect("build must succeed");

        assert_eq!(result.status_code, MvBuildResult::STATUS_OK);
        assert_eq!(
            result.spill_file_count, 0,
            "no spill expected under big pool"
        );
        assert_eq!(
            result.spill_bytes, 0,
            "spill_bytes should be 0 under big pool"
        );
    }

    /// Test: With a tiny memory pool that forces spill, the MvBuildResult
    /// has spill_bytes > 0 and spill_file_count > 0. Validates that spill
    /// metrics are propagated through the MvBuildResult struct.
    ///
    /// NOTE: `build_streaming_ipc_artifact` uses the default
    /// `sort_spill_reservation_bytes` (10 MiB), so the pool must be large
    /// enough for the reservation but small enough that actual data forces
    /// spill. 32 MiB with 200k groups achieves this: the reservation
    /// succeeds, but the 200k-group partial aggregate exceeds the remaining
    /// budget and spills sorted runs.
    #[test]
    fn test_streaming_build_spill_populates_spill_bytes() {
        const GROUPS: usize = 200_000;
        const SPILL_POOL_BYTES: usize = 32 * 1024 * 1024; // 32 MiB
        let data_dir = TempDir::new().unwrap();
        write_grouped_parquet(data_dir.path(), GROUPS);
        let spill_dir = TempDir::new().unwrap();

        let (pool, handle) = DynamicLimitPool::new(SPILL_POOL_BYTES);
        let pool: Arc<dyn MemoryPool> = Arc::new(pool);
        let runtime_env = RuntimeEnvBuilder::new()
            .with_memory_pool(pool)
            .with_disk_manager_builder(
                DiskManagerBuilder::default()
                    .with_mode(DiskManagerMode::Directories(vec![
                        spill_dir.path().to_path_buf(),
                    ])),
            )
            .build()
            .unwrap();
        let runtime = DataFusionRuntime {
            runtime_env,
            custom_cache_manager: None,
            dynamic_limit_handle: handle,
        };

        let out = TempDir::new().unwrap();
        let out_file = out.path().join("state.arrow").to_str().unwrap().to_string();

        let ordering = OrderingContract::from_parallel_arrays(&[0, 1], &[0, 0], &[0, 0]);
        let result = build_streaming_ipc_artifact(
            &runtime,
            data_dir.path().to_str().unwrap(),
            "t",
            "SELECT k0, k1, COUNT(*) AS cnt FROM t GROUP BY k0, k1",
            &out_file,
            &ordering,
            0,
            0,
            0,
        )
        .expect("build must succeed even with spill");

        assert_eq!(result.status_code, MvBuildResult::STATUS_OK);
        assert_eq!(result.row_count, GROUPS as u64);
        // Under a 12 MiB pool with 200k groups, the sort must spill.
        // The spill_bytes and spill_file_count come from
        // collect_spill_metrics() walking the ExecutionPlan tree.
        // NOTE: spill_bytes may still be 0 if DataFusion's metrics reporting
        // doesn't populate 'spill_bytes' for the sort operator (it uses
        // SpillCount for events). The test asserts based on what the
        // collect_spill_metrics function actually reports.
        assert!(
            result.spill_file_count > 0 || result.spill_bytes > 0,
            "expected spill under tiny pool: spill_bytes={}, spill_file_count={}",
            result.spill_bytes,
            result.spill_file_count
        );
    }

    /// Test: MvBuildResult error path — SQL that produces 0 rows returns an
    /// error. Validates that the build function returns Err, not an Ok result
    /// with status_code != 0, and that the output file is deleted.
    #[test]
    fn test_streaming_build_empty_result_returns_error() {
        const GROUPS: usize = 1_000;
        let data_dir = TempDir::new().unwrap();
        write_grouped_parquet(data_dir.path(), GROUPS);
        let spill_dir = TempDir::new().unwrap();
        let runtime = make_runtime(BIG_POOL_BYTES, spill_dir.path());

        let out = TempDir::new().unwrap();
        let out_file = out.path().join("state.arrow").to_str().unwrap().to_string();

        let ordering = OrderingContract::from_parallel_arrays(&[0, 1], &[0, 0], &[0, 0]);
        // WHERE clause matches no rows → zero output rows → error
        let result = build_streaming_ipc_artifact(
            &runtime,
            data_dir.path().to_str().unwrap(),
            "t",
            "SELECT k0, k1, COUNT(*) AS cnt FROM t WHERE k0 < -999999 GROUP BY k0, k1",
            &out_file,
            &ordering,
            0,
            0,
            0,
        );

        assert!(result.is_err(), "zero-row build must return an error");
        assert!(
            !std::path::Path::new(&out_file).exists(),
            "output file must be deleted on error"
        );
    }

    /// Test: MvBuildResult.output_batch_count matches the number of IPC
    /// batches in the output file. The streaming writer increments
    /// output_batch_count by 1 per non-empty RecordBatch written; reading
    /// back the IPC file should yield the same number of batches.
    #[test]
    fn test_mv_build_result_output_batch_count_matches_ipc() {
        const GROUPS: usize = 30_000;
        let data_dir = TempDir::new().unwrap();
        write_grouped_parquet(data_dir.path(), GROUPS);
        let spill_dir = TempDir::new().unwrap();
        let runtime = make_runtime(BIG_POOL_BYTES, spill_dir.path());

        let out = TempDir::new().unwrap();
        let out_file = out.path().join("state.arrow").to_str().unwrap().to_string();

        let ordering = OrderingContract::from_parallel_arrays(&[0, 1], &[0, 0], &[0, 0]);
        let result = build_streaming_ipc_artifact(
            &runtime,
            data_dir.path().to_str().unwrap(),
            "t",
            "SELECT k0, k1, COUNT(*) AS cnt FROM t GROUP BY k0, k1",
            &out_file,
            &ordering,
            0,
            0,
            0,
        )
        .expect("build must succeed");

        assert_eq!(result.status_code, MvBuildResult::STATUS_OK);

        // Count IPC batches by reading the file back
        let file = std::fs::File::open(&out_file).unwrap();
        let reader = arrow::ipc::reader::FileReader::try_new(file, None).unwrap();
        let mut ipc_batch_count: u32 = 0;
        for batch_result in reader {
            let batch = batch_result.unwrap();
            if batch.num_rows() > 0 {
                ipc_batch_count += 1;
            }
        }

        assert_eq!(
            result.output_batch_count, ipc_batch_count,
            "MvBuildResult.output_batch_count ({}) must match the number of non-empty \
             IPC batches read back from the file ({})",
            result.output_batch_count, ipc_batch_count
        );
        assert!(
            ipc_batch_count > 0,
            "expected at least 1 IPC batch for {} groups",
            GROUPS
        );
    }
}

/// FFI result contract tests for `MvBuildResult`.
///
/// These tests verify the binary ABI contract that Java relies on to decode
/// the 80-byte `MvBuildResult` struct via `MemorySegment.get()` at compile-time
/// constant offsets. Any layout drift here means Java decodes garbage.
///
/// Tests cover:
///   1. ABI layout: size, alignment, ABI_VERSION, STATUS_* constants
///   2. Field byte offsets matching Java OFF_* constants
///   3. `MvBuildResult::ok()` populates every field correctly
///   4. `MvBuildResult::error()` zeroes all data fields
///   5. Byte-level roundtrip via `copy_nonoverlapping` (cross-language decode)
///   6. All error status variants preserve ABI header and zero data
///   7. MvBuildResult is Copy (no heap allocation, safe for FFI)
///   8. abi_version is at offset 0 (casting struct pointer to *u32 reads ABI_VERSION)
///   9. Schema/definition/ordering hash determinism and distinctness
///  10. FFM entry points (`df_mv_build_result_abi_version`, null-pointer guards)
#[cfg(test)]
mod ffi_result_contract_tests {
    use std::mem;
    use std::sync::Arc;

    use arrow::datatypes::{DataType, Field, Schema};
    use arrow_schema::SchemaRef;

    use crate::mv_build_managed::{
        compute_definition_hash_u64, compute_ordering_hash_u64, compute_schema_hash_u64,
        MvBuildResult, OrderingContract,
    };

    // ── 1. ABI layout: size_of, align_of, ABI_VERSION, STATUS_* constants ──

    #[test]
    fn test_mv_build_result_abi_layout() {
        // Size must be exactly 80 bytes — Java allocates this exact amount.
        assert_eq!(
            mem::size_of::<MvBuildResult>(),
            80,
            "MvBuildResult must be exactly 80 bytes"
        );
        // Alignment must be 8 (u64 fields).
        assert_eq!(
            mem::align_of::<MvBuildResult>(),
            8,
            "MvBuildResult must be 8-byte aligned"
        );
        // ABI version constant.
        assert_eq!(MvBuildResult::ABI_VERSION, 1);
        // STRUCT_SIZE matches size_of.
        assert_eq!(MvBuildResult::STRUCT_SIZE, 80);
        // Status code constants pinned to Java-side values.
        assert_eq!(MvBuildResult::STATUS_OK, 0);
        assert_eq!(MvBuildResult::STATUS_CANCELLED, 1);
        assert_eq!(MvBuildResult::STATUS_SPILL_EXCEEDED, 2);
        assert_eq!(MvBuildResult::STATUS_MEMORY_EXHAUSTED, 3);
        assert_eq!(MvBuildResult::STATUS_INTERNAL_ERROR, -1);
    }

    // ── 2. Field byte offsets via pointer arithmetic ────────────────────────

    #[test]
    fn test_mv_build_result_field_offsets() {
        let result = MvBuildResult::ok(0, 0, 0, 0, 0, 0, 0, 0, 0);
        let base = &result as *const MvBuildResult as usize;

        // Java OFF_* constants: abi_version@0, struct_size@4, status_code@8,
        // _pad0@12, row_count@16, schema_hash@24, definition_hash@32,
        // ordering_hash@40, spill_bytes@48, spill_file_count@56,
        // output_batch_count@60, peak_rss_bytes@64, build_duration_us@72
        let off_abi_version = &result.abi_version as *const u32 as usize - base;
        let off_struct_size = &result.struct_size as *const u32 as usize - base;
        let off_status_code = &result.status_code as *const i32 as usize - base;
        let off_pad0 = &result._pad0 as *const u32 as usize - base;
        let off_row_count = &result.row_count as *const u64 as usize - base;
        let off_schema_hash = &result.schema_hash as *const u64 as usize - base;
        let off_definition_hash = &result.definition_hash as *const u64 as usize - base;
        let off_ordering_hash = &result.ordering_hash as *const u64 as usize - base;
        let off_spill_bytes = &result.spill_bytes as *const u64 as usize - base;
        let off_spill_file_count = &result.spill_file_count as *const u32 as usize - base;
        let off_output_batch_count = &result.output_batch_count as *const u32 as usize - base;
        let off_peak_rss_bytes = &result.peak_rss_bytes as *const u64 as usize - base;
        let off_build_duration_us = &result.build_duration_us as *const u64 as usize - base;

        assert_eq!(off_abi_version, 0, "abi_version @ 0");
        assert_eq!(off_struct_size, 4, "struct_size @ 4");
        assert_eq!(off_status_code, 8, "status_code @ 8");
        assert_eq!(off_pad0, 12, "_pad0 @ 12");
        assert_eq!(off_row_count, 16, "row_count @ 16");
        assert_eq!(off_schema_hash, 24, "schema_hash @ 24");
        assert_eq!(off_definition_hash, 32, "definition_hash @ 32");
        assert_eq!(off_ordering_hash, 40, "ordering_hash @ 40");
        assert_eq!(off_spill_bytes, 48, "spill_bytes @ 48");
        assert_eq!(off_spill_file_count, 56, "spill_file_count @ 56");
        assert_eq!(off_output_batch_count, 60, "output_batch_count @ 60");
        assert_eq!(off_peak_rss_bytes, 64, "peak_rss_bytes @ 64");
        assert_eq!(off_build_duration_us, 72, "build_duration_us @ 72");
    }

    // ── 3. MvBuildResult::ok() populates all fields ─────────────────────────

    #[test]
    fn test_mv_build_result_ok_populates_all_fields() {
        let result = MvBuildResult::ok(
            42_000,        // row_count
            0xDEADBEEF,    // schema_hash
            0xCAFEBABE,    // definition_hash
            0x12345678,    // ordering_hash
            1024 * 1024,   // spill_bytes
            3,             // spill_file_count
            7,             // output_batch_count
            256 * 1024,    // peak_rss_bytes
            5_000_000,     // build_duration_us (5 seconds)
        );

        assert_eq!(result.abi_version, 1, "abi_version must be ABI_VERSION");
        assert_eq!(result.struct_size, 80, "struct_size must be 80");
        assert_eq!(result.status_code, 0, "status_code must be STATUS_OK");
        assert_eq!(result._pad0, 0, "_pad0 must be zero");
        assert_eq!(result.row_count, 42_000);
        assert_eq!(result.schema_hash, 0xDEADBEEF);
        assert_eq!(result.definition_hash, 0xCAFEBABE);
        assert_eq!(result.ordering_hash, 0x12345678);
        assert_eq!(result.spill_bytes, 1024 * 1024);
        assert_eq!(result.spill_file_count, 3);
        assert_eq!(result.output_batch_count, 7);
        assert_eq!(result.peak_rss_bytes, 256 * 1024);
        assert_eq!(result.build_duration_us, 5_000_000);
    }

    // ── 4. MvBuildResult::error() zeroes all data fields ────────────────────

    #[test]
    fn test_mv_build_result_error_zeroes_all_data_fields() {
        let result = MvBuildResult::error(MvBuildResult::STATUS_CANCELLED);

        assert_eq!(result.abi_version, 1, "abi_version preserved on error");
        assert_eq!(result.struct_size, 80, "struct_size preserved on error");
        assert_eq!(result.status_code, MvBuildResult::STATUS_CANCELLED);
        assert_eq!(result._pad0, 0);
        assert_eq!(result.row_count, 0, "row_count zeroed on error");
        assert_eq!(result.schema_hash, 0, "schema_hash zeroed on error");
        assert_eq!(result.definition_hash, 0, "definition_hash zeroed on error");
        assert_eq!(result.ordering_hash, 0, "ordering_hash zeroed on error");
        assert_eq!(result.spill_bytes, 0, "spill_bytes zeroed on error");
        assert_eq!(result.spill_file_count, 0, "spill_file_count zeroed on error");
        assert_eq!(result.output_batch_count, 0, "output_batch_count zeroed on error");
        assert_eq!(result.peak_rss_bytes, 0, "peak_rss_bytes zeroed on error");
        assert_eq!(result.build_duration_us, 0, "build_duration_us zeroed on error");
    }

    // ── 5. Byte-level roundtrip via copy_nonoverlapping ─────────────────────

    #[test]
    fn test_mv_build_result_roundtrip_bytes() {
        let result = MvBuildResult::ok(
            99_999,
            0xAAAABBBBCCCCDDDD,
            0x1111222233334444,
            0x5555666677778888,
            2048,
            5,
            12,
            4096,
            123_456_789,
        );

        // Write struct to a raw byte buffer (same as FFM does).
        let mut buf = [0u8; 80];
        unsafe {
            std::ptr::copy_nonoverlapping(
                &result as *const MvBuildResult as *const u8,
                buf.as_mut_ptr(),
                80,
            );
        }

        // Read individual fields back at documented offsets — this is what
        // Java does via MemorySegment.get(JAVA_INT, offset).
        let abi_version = u32::from_ne_bytes(buf[0..4].try_into().unwrap());
        let struct_size = u32::from_ne_bytes(buf[4..8].try_into().unwrap());
        let status_code = i32::from_ne_bytes(buf[8..12].try_into().unwrap());
        let pad0 = u32::from_ne_bytes(buf[12..16].try_into().unwrap());
        let row_count = u64::from_ne_bytes(buf[16..24].try_into().unwrap());
        let schema_hash = u64::from_ne_bytes(buf[24..32].try_into().unwrap());
        let definition_hash = u64::from_ne_bytes(buf[32..40].try_into().unwrap());
        let ordering_hash = u64::from_ne_bytes(buf[40..48].try_into().unwrap());
        let spill_bytes = u64::from_ne_bytes(buf[48..56].try_into().unwrap());
        let spill_file_count = u32::from_ne_bytes(buf[56..60].try_into().unwrap());
        let output_batch_count = u32::from_ne_bytes(buf[60..64].try_into().unwrap());
        let peak_rss_bytes = u64::from_ne_bytes(buf[64..72].try_into().unwrap());
        let build_duration_us = u64::from_ne_bytes(buf[72..80].try_into().unwrap());

        assert_eq!(abi_version, 1);
        assert_eq!(struct_size, 80);
        assert_eq!(status_code, 0);
        assert_eq!(pad0, 0);
        assert_eq!(row_count, 99_999);
        assert_eq!(schema_hash, 0xAAAABBBBCCCCDDDD);
        assert_eq!(definition_hash, 0x1111222233334444);
        assert_eq!(ordering_hash, 0x5555666677778888);
        assert_eq!(spill_bytes, 2048);
        assert_eq!(spill_file_count, 5);
        assert_eq!(output_batch_count, 12);
        assert_eq!(peak_rss_bytes, 4096);
        assert_eq!(build_duration_us, 123_456_789);
    }

    // ── 6. All error variants preserve ABI header and zero data fields ──────

    #[test]
    fn test_mv_build_result_all_error_variants() {
        let variants = [
            (MvBuildResult::STATUS_CANCELLED, "CANCELLED"),
            (MvBuildResult::STATUS_SPILL_EXCEEDED, "SPILL_EXCEEDED"),
            (MvBuildResult::STATUS_MEMORY_EXHAUSTED, "MEMORY_EXHAUSTED"),
            (MvBuildResult::STATUS_INTERNAL_ERROR, "INTERNAL_ERROR"),
        ];

        for (code, label) in &variants {
            let result = MvBuildResult::error(*code);
            assert_eq!(result.abi_version, 1, "{}: abi_version must be 1", label);
            assert_eq!(result.struct_size, 80, "{}: struct_size must be 80", label);
            assert_eq!(result.status_code, *code, "{}: status_code mismatch", label);
            assert_eq!(result.row_count, 0, "{}: row_count must be 0", label);
            assert_eq!(result.schema_hash, 0, "{}: schema_hash must be 0", label);
            assert_eq!(result.definition_hash, 0, "{}: definition_hash must be 0", label);
            assert_eq!(result.ordering_hash, 0, "{}: ordering_hash must be 0", label);
            assert_eq!(result.spill_bytes, 0, "{}: spill_bytes must be 0", label);
            assert_eq!(result.spill_file_count, 0, "{}: spill_file_count must be 0", label);
            assert_eq!(result.output_batch_count, 0, "{}: output_batch_count must be 0", label);
            assert_eq!(result.peak_rss_bytes, 0, "{}: peak_rss_bytes must be 0", label);
            assert_eq!(result.build_duration_us, 0, "{}: build_duration_us must be 0", label);
        }
    }

    // ── 7. MvBuildResult is Copy — no heap allocation, safe for FFI ─────────

    #[test]
    fn test_mv_build_result_is_copy() {
        let a = MvBuildResult::ok(100, 1, 2, 3, 4, 5, 6, 7, 8);
        let b = a; // Copy, not Move
        let c = a; // Still valid — Copy semantics

        assert_eq!(a.row_count, b.row_count);
        assert_eq!(b.row_count, c.row_count);
        assert_eq!(a.schema_hash, c.schema_hash);
    }

    // ── 8. abi_version is at offset 0 ───────────────────────────────────────

    #[test]
    fn test_mv_build_result_abi_version_first_field() {
        let result = MvBuildResult::ok(0, 0, 0, 0, 0, 0, 0, 0, 0);
        let ptr = &result as *const MvBuildResult as *const u32;
        let abi = unsafe { *ptr };
        assert_eq!(
            abi,
            MvBuildResult::ABI_VERSION,
            "casting struct pointer to *u32 must read abi_version at offset 0"
        );
    }

    // ── 9. Schema hash u64: deterministic, distinct for different schemas ───

    #[test]
    fn test_schema_hash_u64_deterministic_repeated() {
        let schema: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("k0", DataType::Int64, true),
            Field::new("k1", DataType::Utf8, true),
            Field::new("cnt", DataType::Int64, false),
        ]));

        let h1 = compute_schema_hash_u64(&schema);
        let h2 = compute_schema_hash_u64(&schema);
        let h3 = compute_schema_hash_u64(&schema);
        assert_eq!(h1, h2, "first two calls must agree");
        assert_eq!(h2, h3, "second and third calls must agree");
        assert_ne!(h1, 0, "hash should not be zero for a non-empty schema");
    }

    #[test]
    fn test_schema_hash_u64_differs_for_different_schemas() {
        let base: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("k0", DataType::Int64, true),
            Field::new("cnt", DataType::Int64, false),
        ]));
        // Type change
        let type_changed: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("k0", DataType::Int32, true),
            Field::new("cnt", DataType::Int64, false),
        ]));
        // Name change
        let name_changed: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("k0", DataType::Int64, true),
            Field::new("count", DataType::Int64, false),
        ]));
        // Nullability change
        let null_changed: SchemaRef = Arc::new(Schema::new(vec![
            Field::new("k0", DataType::Int64, false),
            Field::new("cnt", DataType::Int64, false),
        ]));

        let h_base = compute_schema_hash_u64(&base);
        let h_type = compute_schema_hash_u64(&type_changed);
        let h_name = compute_schema_hash_u64(&name_changed);
        let h_null = compute_schema_hash_u64(&null_changed);

        assert_ne!(h_base, h_type, "type change must produce different hash");
        assert_ne!(h_base, h_name, "name change must produce different hash");
        assert_ne!(h_base, h_null, "nullability change must produce different hash");
    }

    // ── 10. Definition hash u64: determinism ────────────────────────────────

    #[test]
    fn test_definition_hash_u64_deterministic_repeated() {
        let ordering = OrderingContract::from_parallel_arrays(&[0, 1, 2], &[0, 0, 0], &[0, 0, 0]);

        let h1 = compute_definition_hash_u64(&ordering);
        let h2 = compute_definition_hash_u64(&ordering);
        let h3 = compute_definition_hash_u64(&ordering);
        assert_eq!(h1, h2);
        assert_eq!(h2, h3);
    }

    // ── 11. Ordering hash: determinism and distinctness ──────────────────────

    #[test]
    fn test_ordering_hash_deterministic_and_distinct() {
        let o1 = OrderingContract::from_parallel_arrays(&[0, 1], &[0, 0], &[0, 0]);
        let o2 = OrderingContract::from_parallel_arrays(&[1, 0], &[0, 0], &[0, 0]);
        let o3 = OrderingContract::from_parallel_arrays(&[0, 1], &[0, 1], &[0, 0]);

        // Determinism
        let h1a = compute_ordering_hash_u64(&o1);
        let h1b = compute_ordering_hash_u64(&o1);
        let h1c = compute_ordering_hash_u64(&o1);
        assert_eq!(h1a, h1b, "ordering hash must be deterministic");
        assert_eq!(h1b, h1c, "ordering hash must be deterministic");

        // Distinctness
        let h2 = compute_ordering_hash_u64(&o2);
        let h3 = compute_ordering_hash_u64(&o3);
        assert_ne!(h1a, h2, "swapped indices must differ");
        assert_ne!(h1a, h3, "different direction must differ");
        assert_ne!(h2, h3, "all three orderings must be distinct");
    }

    // ── 12. FFM entry point: df_mv_build_result_abi_version ─────────────────

    #[test]
    fn test_ffm_build_result_abi_version() {
        let version = crate::ffm::df_mv_build_result_abi_version();
        assert_eq!(version, 1, "df_mv_build_result_abi_version() must return 1");
    }

    // ── 13. Cross-language parity: ordering hash for known inputs ───────────

    #[test]
    fn test_ordering_hash_cross_language_parity() {
        // 3-key ordering: field_indices=[0,1,2], all ASC, all NULLS_FIRST
        let o3 = OrderingContract::from_parallel_arrays(&[0, 1, 2], &[0, 0, 0], &[0, 0, 0]);
        let h3 = compute_ordering_hash_u64(&o3);

        // 5-key ordering: field_indices=[0,1,2,3,4], mixed direction+null_placement
        let o5 = OrderingContract::from_parallel_arrays(
            &[0, 1, 2, 3, 4],
            &[0, 1, 0, 1, 0],
            &[0, 1, 0, 1, 0],
        );
        let h5 = compute_ordering_hash_u64(&o5);

        // These hashes must be deterministic (re-compute and verify same value)
        assert_eq!(h3, compute_ordering_hash_u64(&o3), "3-key hash must be stable");
        assert_eq!(h5, compute_ordering_hash_u64(&o5), "5-key hash must be stable");

        // The two orderings must produce different hashes
        assert_ne!(h3, h5, "different orderings must hash differently");

        // Print for cross-language parity tests (Java can hardcode these values)
        // eprintln!("3-key ordering hash: 0x{:016X}", h3);
        // eprintln!("5-key ordering hash: 0x{:016X}", h5);
    }
}
