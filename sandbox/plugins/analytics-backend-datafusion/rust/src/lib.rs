/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! DataFusion native execution engine for OpenSearch.
//!
//! The bridge-agnostic API lives in [`api`]. The FFM bridge (`ffm.rs`) exports
//! `extern "C"` functions for JDK FFM.

/// Column name for the shard-global row identifier used by Query-Then-Fetch.
/// Stored as Int64 in parquet, computed from position in the indexed path.
///
/// TODO: source this from Java — the canonical column name should be defined
/// once on the coordinator side and passed across FFM rather than hardcoded
/// here. Today both sides hardcode "__row_id__" and rely on the strings
/// matching by convention.
pub const ROW_ID_COLUMN_NAME: &str = "__row_id__";

pub(crate) mod agg_mode;
pub mod api;
pub mod cache;
pub mod cancellation;
pub mod cross_rt_stream;
pub mod datafusion_query_config;
pub mod executor;
pub mod ffm;
pub mod helper;
pub mod indexed_executor;
pub mod indexed_table;
pub mod local_executor;
pub mod memory;
pub mod memory_guard;
pub mod mv_build_managed;
pub mod mv_fold;
pub mod mv_merge_engine;
pub mod mv_poc;
pub mod mv_pull_metadata;
pub mod mv_read;
pub mod mv_state_table_provider;
pub mod mv_writer;
pub mod native_error;
pub mod partition_stream;
pub mod patterns;
pub mod phantom_corrector;
pub mod project_row_id_analyzer;
pub mod project_row_id_optimizer;
pub mod query_budget;
pub mod query_executor;
pub mod query_tracker;
pub mod relabel_exec;
pub mod runtime_manager;
pub mod schema_coerce;
pub mod session_context;
pub mod shard_table_provider;

pub mod native_node_stats;
pub mod scoped_index_optimizer;
pub mod scoped_page_index_reader;
pub mod search_stats;
pub mod stats;
pub mod task_monitors;
pub mod udaf;
pub mod udf;
pub mod udwf;

// Path aliases — old module names still resolve unchanged.
pub use cache::custom_cache_manager;
pub use cache::eviction_policy;
pub use cache::page_index as parquet_page_cache;
pub use cache::statistics_cache;

#[cfg(test)]
mod spill_e2e_test;

// Stage 3 streaming MV build regression tests (multi-key sort, NULLS FIRST
// ordering, IPC roundtrip, schema/definition hash, structural plan shape,
// forced-spill observation). Declared under `cfg(test)` so it only compiles
// for the test build; without this declaration the file is orphaned and its
// tests never run.
#[cfg(test)]
mod mv_build_stage3_tests;

// End-to-end TieredObjectStore + TieredBlockCache integration tests. Located here
// (not in the lower-level `opensearch-tiered-storage` crate) because they drive a
// real DataFusion session + Parquet I/O, and DataFusion/Parquet/Arrow are already
// normal dependencies of this crate — keeping the storage-primitive crate's test
// build free of the DataFusion stack.
#[cfg(test)]
mod tiered_storage_integration_tests;
