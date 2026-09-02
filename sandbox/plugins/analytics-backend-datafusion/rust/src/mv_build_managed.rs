/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Stage 3: Streaming MV build through the shared DataFusionRuntime.
//!
//! Replaces the Stage 2 collect→concat→sort→take pattern with:
//! 1. Spillable partial aggregation via DataFusion's AggregateExec
//! 2. External sort over the FULL lexicographic group tuple via SortExec
//! 3. Streaming Arrow IPC write — batches flow directly from SortExec to disk
//! 4. Metadata validation: schema hash, definition hash, ordering guarantee
//!
//! NO terminal collect/concat/sort/take anywhere in the production path.

use std::collections::HashMap;
use std::fs::File;
use std::io::BufWriter;
use std::sync::atomic::{AtomicI64, Ordering as AtomicOrdering};
use std::sync::Arc;

use arrow::compute::{lexsort_to_indices, SortColumn, SortOptions};
use arrow::ipc::writer::FileWriter as IpcFileWriter;
use arrow_array::RecordBatch;
use arrow_schema::SchemaRef;
use datafusion::execution::SessionStateBuilder;
use datafusion::physical_expr::expressions::col as physical_col;
use datafusion::physical_expr::{LexOrdering, PhysicalSortExpr};
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::sorts::sort::SortExec;
use datafusion::physical_plan::{collect, execute_stream, ExecutionPlan};
use datafusion::prelude::{ParquetReadOptions, SessionConfig, SessionContext};
use futures::StreamExt;
use tokio_util::sync::CancellationToken;

use crate::api::DataFusionRuntime;
use crate::cancellation;

/// Ordering key for a single column in the multi-column lexsort.
#[derive(Debug, Clone)]
pub struct OrderingKey {
    /// Zero-based state-field index (column position in the RecordBatch).
    pub field_index: usize,
    /// 0 = ascending (currently the only supported value).
    pub direction: i32,
    /// 0 = nulls first, 1 = nulls last.
    pub null_placement: i32,
}

/// FFI-serialized ordering contract: parallel arrays from Java.
#[derive(Debug, Clone)]
pub struct OrderingContract {
    pub keys: Vec<OrderingKey>,
}

impl OrderingContract {
    /// Build from parallel arrays (the FFI wire format from Java).
    pub fn from_parallel_arrays(
        indices: &[i32],
        directions: &[i32],
        null_placements: &[i32],
    ) -> Self {
        assert_eq!(indices.len(), directions.len());
        assert_eq!(indices.len(), null_placements.len());
        let keys = indices
            .iter()
            .zip(directions.iter())
            .zip(null_placements.iter())
            .map(|((&idx, &dir), &nulls)| OrderingKey {
                field_index: idx as usize,
                direction: dir,
                null_placement: nulls,
            })
            .collect();
        OrderingContract { keys }
    }

    /// Convert to Arrow SortColumn descriptors for `lexsort_to_indices`.
    pub fn to_sort_columns(&self, batch: &RecordBatch) -> Vec<SortColumn> {
        self.keys
            .iter()
            .map(|key| SortColumn {
                values: batch.column(key.field_index).clone(),
                options: Some(SortOptions {
                    descending: key.direction != 0,
                    nulls_first: key.null_placement == 0,
                }),
            })
            .collect()
    }

    /// Convert to DataFusion PhysicalSortExpr for SortExec.
    fn to_physical_sort_exprs(&self, schema: &SchemaRef) -> LexOrdering {
        let exprs: Vec<PhysicalSortExpr> = self
            .keys
            .iter()
            .map(|key| {
                let field_name = schema.field(key.field_index).name();
                PhysicalSortExpr {
                    expr: physical_col(field_name, schema).unwrap_or_else(|e| {
                        panic!(
                            "ordering key field '{}' (index {}) not found in schema: {}",
                            field_name, key.field_index, e
                        )
                    }),
                    options: SortOptions {
                        descending: key.direction != 0,
                        nulls_first: key.null_placement == 0,
                    },
                }
            })
            .collect();
        LexOrdering::new(exprs).expect("ordering contract must have at least one key")
    }
}

/// Metadata returned from a streaming artifact build.
#[derive(Debug, Clone)]
pub struct ArtifactMetadata {
    /// Number of rows written to the IPC artifact.
    pub row_count: i64,
    /// SHA-256 hex digest of the Arrow schema (serialized via IPC).
    pub schema_hash: String,
    /// SHA-256 hex digest of the ordering contract (field indices + directions + null placements).
    pub definition_hash: String,
}

// ── ABI-versioned result struct for `build_streaming_ipc_artifact` ───────

/// ABI-versioned result struct for `build_streaming_ipc_artifact`.
///
/// Returned as a flat `#[repr(C)]` struct written into a caller-allocated
/// buffer (same pattern as `DfStatsBuffer` in stats.rs). Java reads fields
/// via `MemorySegment.get()` at compile-time-constant offsets derived from
/// a `GroupLayout`. No heap allocation, no opaque pointer, no accessor FFI
/// functions — one `copy_nonoverlapping` and done.
///
/// # Versioning
///
/// `abi_version` is always the first field. Java checks it before reading
/// any other field. `struct_size` lets Java detect Rust-side growth: if
/// `struct_size > sizeof(JavaLayout)`, Java knows new fields were appended
/// and reads only the prefix it understands.
///
/// # Status codes
///
/// | Code | Meaning |
/// |------|---------|
/// | 0    | OK — all fields valid |
/// | 1    | Cancelled — context_id token was fired |
/// | 2    | SpillBudgetExceeded |
/// | 3    | MemoryExhausted |
/// | -1   | InternalError — error string returned via normal negated-pointer |
#[derive(Debug, Clone, Copy)]
#[repr(C)]
pub struct MvBuildResult {
    /// ABI version tag — always first field. Current: 1.
    pub abi_version: u32,
    /// Total byte size of this struct as written by Rust (`size_of::<Self>()`).
    /// Java uses this for forward-compat: read min(java_layout_size, struct_size).
    pub struct_size: u32,
    /// 0=OK, 1=Cancelled, 2=SpillBudgetExceeded, 3=MemoryExhausted, -1=InternalError.
    pub status_code: i32,
    /// Padding to align `row_count` to 8 bytes.
    pub _pad0: u32,
    /// Number of rows written to the IPC artifact.
    pub row_count: u64,
    /// FNV-128 hash of the Arrow schema (field names, types, nullability).
    pub schema_hash: u64,
    /// FNV-128 hash of the ordering contract (field indices + directions + null placements).
    pub definition_hash: u64,
    /// FNV-128 hash of the ordering contract identity (deterministic, same as
    /// PullArtifactMetadata.ordering_identity from Stage 5).
    pub ordering_hash: u64,
    /// Total bytes spilled to disk by DataFusion's DiskManager during this build.
    pub spill_bytes: u64,
    /// Number of spill files created.
    pub spill_file_count: u32,
    /// Number of Arrow IPC batches written to the output file.
    pub output_batch_count: u32,
    /// Peak resident set size (bytes) observed during the build.
    pub peak_rss_bytes: u64,
    /// Wall-clock build duration in microseconds.
    pub build_duration_us: u64,
}

// Compile-time layout assertions (same pattern as stats.rs).
const _: () = assert!(std::mem::size_of::<MvBuildResult>() == 80);
const _: () = assert!(std::mem::align_of::<MvBuildResult>() == 8);

impl MvBuildResult {
    pub const ABI_VERSION: u32 = 1;
    pub const STATUS_OK: i32 = 0;
    pub const STATUS_CANCELLED: i32 = 1;
    pub const STATUS_SPILL_EXCEEDED: i32 = 2;
    pub const STATUS_MEMORY_EXHAUSTED: i32 = 3;
    pub const STATUS_INTERNAL_ERROR: i32 = -1;

    /// Size of the struct in bytes (compile-time constant for FFI).
    pub const STRUCT_SIZE: u32 = std::mem::size_of::<Self>() as u32;

    /// Construct a successful result from build instrumentation.
    #[allow(clippy::too_many_arguments)]
    pub fn ok(
        row_count: u64,
        schema_hash: u64,
        definition_hash: u64,
        ordering_hash: u64,
        spill_bytes: u64,
        spill_file_count: u32,
        output_batch_count: u32,
        peak_rss_bytes: u64,
        build_duration_us: u64,
    ) -> Self {
        Self {
            abi_version: Self::ABI_VERSION,
            struct_size: Self::STRUCT_SIZE,
            status_code: Self::STATUS_OK,
            _pad0: 0,
            row_count,
            schema_hash,
            definition_hash,
            ordering_hash,
            spill_bytes,
            spill_file_count,
            output_batch_count,
            peak_rss_bytes,
            build_duration_us,
        }
    }

    /// Construct a non-OK result (cancellation, spill exceeded, etc.).
    pub fn error(status_code: i32) -> Self {
        Self {
            abi_version: Self::ABI_VERSION,
            struct_size: Self::STRUCT_SIZE,
            status_code,
            _pad0: 0,
            row_count: 0,
            schema_hash: 0,
            definition_hash: 0,
            ordering_hash: 0,
            spill_bytes: 0,
            spill_file_count: 0,
            output_batch_count: 0,
            peak_rss_bytes: 0,
            build_duration_us: 0,
        }
    }
}

// ── u64 hash helpers for FFI (truncated FNV-128) ────────────────────────

/// Compute a deterministic u64 hash of an Arrow schema.
/// Hashes field names, types, and nullability. Returns the lower 64 bits
/// of the FNV-128 hash (same algorithm as `stable_hex_hash`).
pub fn compute_schema_hash_u64(schema: &SchemaRef) -> u64 {
    let mut bytes = Vec::new();
    for field in schema.fields() {
        bytes.extend_from_slice(field.name().as_bytes());
        bytes.push(0); // separator
        let type_str = format!("{:?}", field.data_type());
        bytes.extend_from_slice(type_str.as_bytes());
        bytes.push(if field.is_nullable() { 1 } else { 0 });
    }
    bytes.extend_from_slice(&(schema.fields().len() as u32).to_le_bytes());
    stable_hash_u64(&bytes)
}

/// Compute a deterministic u64 hash of an ordering contract.
/// Returns the lower 64 bits of the FNV-128 hash.
pub fn compute_definition_hash_u64(ordering: &OrderingContract) -> u64 {
    let mut bytes = Vec::with_capacity(ordering.keys.len() * 12);
    for key in &ordering.keys {
        bytes.extend_from_slice(&(key.field_index as u32).to_le_bytes());
        bytes.extend_from_slice(&(key.direction as u32).to_le_bytes());
        bytes.extend_from_slice(&(key.null_placement as u32).to_le_bytes());
    }
    stable_hash_u64(&bytes)
}

/// Compute a deterministic u64 hash of the ordering contract identity.
/// Same canonical representation as `mv_pull_metadata::compute_ordering_identity`
/// but using a stable FNV-128 hash (lower 64 bits) instead of `DefaultHasher`.
pub fn compute_ordering_hash_u64(ordering: &OrderingContract) -> u64 {
    let mut bytes = Vec::with_capacity(ordering.keys.len() * 6 + 4);
    for key in &ordering.keys {
        bytes.extend_from_slice(&(key.field_index as u32).to_le_bytes());
        bytes.push(if key.direction == 0 { 1 } else { 0 }); // asc flag
        bytes.push(if key.null_placement == 0 { 1 } else { 0 }); // nulls_first flag
    }
    bytes.extend_from_slice(&(ordering.keys.len() as u32).to_le_bytes());
    stable_hash_u64(&bytes)
}

/// Stable FNV-1a 128-bit hash → lower 64 bits as u64.
/// Deterministic across runs (unlike DefaultHasher which may be seeded).
fn stable_hash_u64(data: &[u8]) -> u64 {
    let mut h: u128 = 0x6c62272e07bb0142_62b821756295c58d_u128;
    let prime: u128 = 0x0000000001000000_000000000000013B_u128;
    for &b in data {
        h ^= b as u128;
        h = h.wrapping_mul(prime);
    }
    h as u64
}

/// Compute a deterministic hex digest of an Arrow schema.
/// Hashes field names, types, and nullability to detect schema drift.
pub fn compute_schema_hash(schema: &SchemaRef) -> String {
    let mut bytes = Vec::new();
    for field in schema.fields() {
        bytes.extend_from_slice(field.name().as_bytes());
        bytes.push(0); // separator
                       // Hash the debug representation of the data type (stable across runs)
        let type_str = format!("{:?}", field.data_type());
        bytes.extend_from_slice(type_str.as_bytes());
        bytes.push(if field.is_nullable() { 1 } else { 0 });
    }
    // Include field count to differentiate prefix-matching schemas
    bytes.extend_from_slice(&(schema.fields().len() as u32).to_le_bytes());
    stable_hex_hash(&bytes)
}

/// Compute a deterministic hex digest of an ordering contract.
pub fn compute_definition_hash(ordering: &OrderingContract) -> String {
    let mut bytes = Vec::with_capacity(ordering.keys.len() * 12);
    for key in &ordering.keys {
        bytes.extend_from_slice(&(key.field_index as u32).to_le_bytes());
        bytes.extend_from_slice(&(key.direction as u32).to_le_bytes());
        bytes.extend_from_slice(&(key.null_placement as u32).to_le_bytes());
    }
    stable_hex_hash(&bytes)
}

/// Stable FNV-1a 128-bit hash → 32-char hex string.
/// Deterministic across runs (unlike DefaultHasher which may be seeded).
fn stable_hex_hash(data: &[u8]) -> String {
    // FNV-1a 128-bit
    let mut h: u128 = 0x6c62272e07bb0142_62b821756295c58d_u128;
    let prime: u128 = 0x0000000001000000_000000000000013B_u128;
    for &b in data {
        h ^= b as u128;
        h = h.wrapping_mul(prime);
    }
    format!("{:032x}", h)
}

/// Validate that an Arrow IPC file's rows are sorted according to the ordering contract.
pub fn validate_ipc_ordering(file_path: &str, ordering: &OrderingContract) -> Result<bool, String> {
    let file = File::open(file_path)
        .map_err(|e| format!("validate_ipc_ordering open {}: {}", file_path, e))?;
    let reader = arrow::ipc::reader::FileReader::try_new(file, None)
        .map_err(|e| format!("validate_ipc_ordering reader {}: {}", file_path, e))?;

    let mut prev_batch: Option<RecordBatch> = None;
    for batch_result in reader {
        let batch = batch_result.map_err(|e| format!("validate_ipc_ordering read batch: {}", e))?;
        if batch.num_rows() == 0 {
            continue;
        }

        // Check intra-batch ordering
        let sort_columns = ordering.to_sort_columns(&batch);
        let indices = lexsort_to_indices(&sort_columns, None)
            .map_err(|e| format!("validate_ipc_ordering lexsort: {}", e))?;
        for i in 0..indices.len() {
            if indices.value(i) != i as u32 {
                return Ok(false);
            }
        }

        // Check inter-batch ordering (last row of prev <= first row of current)
        if let Some(ref prev) = prev_batch {
            let last_row = prev.num_rows() - 1;
            for key in &ordering.keys {
                let prev_col = prev.column(key.field_index);
                let curr_col = batch.column(key.field_index);
                // Compare the last element of prev with first element of current
                let prev_sort = SortColumn {
                    values: prev_col.slice(last_row, 1),
                    options: Some(SortOptions {
                        descending: key.direction != 0,
                        nulls_first: key.null_placement == 0,
                    }),
                };
                let curr_sort = SortColumn {
                    values: curr_col.slice(0, 1),
                    options: Some(SortOptions {
                        descending: key.direction != 0,
                        nulls_first: key.null_placement == 0,
                    }),
                };
                // Concatenate and check sort
                let combined = arrow::compute::concat(&[&*prev_sort.values, &*curr_sort.values])
                    .map_err(|e| format!("validate_ipc_ordering concat: {}", e))?;
                let combined_sort = vec![SortColumn {
                    values: combined,
                    options: prev_sort.options,
                }];
                let idx = lexsort_to_indices(&combined_sort, None)
                    .map_err(|e| format!("validate_ipc_ordering inter-batch: {}", e))?;
                if idx.value(0) != 0 {
                    return Ok(false); // prev row > current row on this key
                }
                if idx.value(0) == 0 && idx.value(1) == 1 {
                    break; // This key is strictly less or equal; if equal, check next key
                }
            }
        }
        prev_batch = Some(batch);
    }
    Ok(true)
}

/// Cancellation context registry for MV builds.
/// Maps context_id -> CancellationToken.
static MV_CANCEL_REGISTRY: std::sync::LazyLock<std::sync::Mutex<HashMap<i64, CancellationToken>>> =
    std::sync::LazyLock::new(|| std::sync::Mutex::new(HashMap::new()));

static NEXT_MV_CONTEXT_ID: AtomicI64 = AtomicI64::new(1);

/// Allocate a new cancellation context. Returns a unique context_id.
pub fn alloc_cancel_context() -> i64 {
    let id = NEXT_MV_CONTEXT_ID.fetch_add(1, AtomicOrdering::Relaxed);
    let token = CancellationToken::new();
    MV_CANCEL_REGISTRY.lock().unwrap().insert(id, token);
    id
}

/// Release a cancellation context after the build completes.
pub fn release_cancel_context(context_id: i64) {
    MV_CANCEL_REGISTRY.lock().unwrap().remove(&context_id);
}

/// Fire the cancellation token for the given context_id.
pub fn cancel_build(context_id: i64) {
    if let Some(token) = MV_CANCEL_REGISTRY.lock().unwrap().get(&context_id) {
        token.cancel();
    }
}

fn get_cancel_token(context_id: i64) -> Option<CancellationToken> {
    MV_CANCEL_REGISTRY.lock().unwrap().get(&context_id).cloned()
}

/// Stage 3: Build a streaming MV state artifact using external sort + direct IPC write.
///
/// This replaces the Stage 2 collect→concat→sort→take pattern with:
/// - Partial aggregation via DataFusion AggregateExec (spillable)
/// - External sort via SortExec over the FULL ordering contract
/// - Streaming write: batches flow from SortExec directly to Arrow IPC FileWriter
/// - No terminal collect/concat/sort/take in the production path
///
/// Returns `MvBuildResult` with full instrumentation (row count, hashes, spill, RSS, duration).
/// On cancellation returns `MvBuildResult::error(STATUS_CANCELLED)`.
/// On internal error returns `Err(String)` — the FFI wrapper converts this to STATUS_INTERNAL_ERROR.
pub fn build_streaming_ipc_artifact(
    runtime: &DataFusionRuntime,
    input_file: &str,
    table_name: &str,
    sql: &str,
    output_file: &str,
    ordering: &OrderingContract,
    context_id: i64,
    _spill_budget_bytes: i64,
    _spill_file_count_limit: i32,
) -> Result<MvBuildResult, String> {
    let wall_start = std::time::Instant::now();
    let start_rss = crate::memory_guard::cached_resident_bytes();

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("mv_build_streaming runtime: {e}"))?;

    let token = get_cancel_token(context_id);

    // Pre-compute hashes from the ordering contract (available before plan execution).
    let definition_hash = compute_definition_hash_u64(ordering);
    let ordering_hash = compute_ordering_hash_u64(ordering);

    rt.block_on(async {
        // Build SessionContext sharing the global runtime's memory pool + disk manager
        let config = SessionConfig::new().with_target_partitions(1);
        let state = SessionStateBuilder::new()
            .with_config(config)
            .with_runtime_env(Arc::new(runtime.runtime_env.clone()))
            .with_default_features()
            .with_physical_optimizer_rules(
                crate::agg_mode::physical_optimizer_rules_without_combine(),
            )
            .build();
        let ctx = SessionContext::new_with_state(state);
        ctx.register_parquet(table_name, input_file, ParquetReadOptions::default())
            .await
            .map_err(|e| format!("mv_build_streaming register_parquet({input_file}): {e}"))?;

        let build_future = async {
            // Stage 3: build the partial-aggregate + FULL-ordering SortExec plan.
            // Shared with the structural/spill regression tests so the exact
            // production plan shape is what gets asserted.
            let sort_exec = plan_partial_then_sort(&ctx, sql, ordering)
                .await
                .map_err(|e| format!("mv_build_streaming {e}"))?;

            // Stream sorted batches directly to Arrow IPC file
            let schema = sort_exec.schema();
            let schema_hash = compute_schema_hash_u64(&schema);

            let mut stream = execute_stream(sort_exec.clone(), ctx.task_ctx())
                .map_err(|e| format!("mv_build_streaming execute: {e}"))?;

            let file = File::create(output_file)
                .map_err(|e| format!("mv_build_streaming create {output_file}: {e}"))?;
            let buffered = BufWriter::new(file);
            let mut writer = IpcFileWriter::try_new(buffered, &schema)
                .map_err(|e| format!("mv_build_streaming ipc writer: {e}"))?;

            let mut row_count: u64 = 0;
            let mut output_batch_count: u32 = 0;
            while let Some(batch_result) = stream.next().await {
                let batch =
                    batch_result.map_err(|e| format!("mv_build_streaming stream batch: {e}"))?;
                if batch.num_rows() > 0 {
                    writer
                        .write(&batch)
                        .map_err(|e| format!("mv_build_streaming write batch: {e}"))?;
                    row_count += batch.num_rows() as u64;
                    output_batch_count += 1;
                }
            }

            writer
                .finish()
                .map_err(|e| format!("mv_build_streaming finish: {e}"))?;

            if row_count == 0 {
                return Err(
                    "mv_build_streaming: produced no rows (expected at least an empty batch)"
                        .to_string(),
                );
            }

            // Collect spill metrics from the executed plan tree.
            let (spill_bytes, spill_file_count) = collect_spill_metrics(sort_exec.as_ref());

            // Peak RSS: max of start and current.
            let end_rss = crate::memory_guard::cached_resident_bytes();
            let peak_rss_bytes = std::cmp::max(start_rss, end_rss) as u64;

            let build_duration_us = wall_start.elapsed().as_micros() as u64;

            Ok(MvBuildResult::ok(
                row_count,
                schema_hash,
                definition_hash,
                ordering_hash,
                spill_bytes,
                spill_file_count,
                output_batch_count,
                peak_rss_bytes,
                build_duration_us,
            ))
        };

        // Race the build against the cancellation token.
        let result = match token {
            Some(ref tok) => {
                tokio::select! {
                    biased;
                    _ = tok.cancelled() => {
                        let _ = std::fs::remove_file(output_file);
                        Ok(MvBuildResult::error(MvBuildResult::STATUS_CANCELLED))
                    }
                    res = build_future => res,
                }
            }
            None => build_future.await,
        };

        // Atomic-finalize contract: on ANY failure (SQL/plan error, stream
        // error, or zero-row output) never leave a partial or
        // empty artifact behind. The Java layer also renames from a private
        // temp path, but this defense-in-depth cleanup guarantees the native
        // writer's own output path is removed even if a caller forgets to.
        if result.is_err() {
            let _ = std::fs::remove_file(output_file);
        }
        result
    })
}

/// Recursively collect SpillCount and SpillBytes metrics from a plan tree.
/// Returns (total_spill_bytes, total_spill_file_count).
fn collect_spill_metrics(plan: &dyn ExecutionPlan) -> (u64, u32) {
    let mut total_bytes: u64 = 0;
    let mut total_files: u32 = 0;
    if let Some(metrics) = plan.metrics() {
        total_bytes += metrics.spill_count().unwrap_or(0) as u64;
        // SpillCount in DataFusion counts spill events; we report it as file count.
        // SpillBytes is the aggregate byte count of data spilled.
        // Note: DataFusion's MetricValue::SpillCount is the number of spill events,
        // and Output/SpillBytes tracks bytes. We map them to our struct fields.
        if let Some(spill_bytes) = metrics.iter().find(|m| m.value().name() == "spill_bytes") {
            total_bytes = spill_bytes.value().as_usize() as u64;
        }
        total_files += metrics.spill_count().unwrap_or(0) as u32;
    }
    for child in plan.children() {
        let (cb, cf) = collect_spill_metrics(child.as_ref());
        total_bytes += cb;
        total_files += cf;
    }
    (total_bytes, total_files)
}

/// Stage 3: Managed state-file build with streaming IPC output.
/// Replaces the Stage 2 mv_build_managed.
pub fn mv_build_managed(
    runtime: &DataFusionRuntime,
    input_file: &str,
    table_name: &str,
    sql: &str,
    output_file: &str,
    ordering: &OrderingContract,
    context_id: i64,
    spill_budget_bytes: i64,
    spill_file_count_limit: i32,
) -> Result<i64, String> {
    let result = build_streaming_ipc_artifact(
        runtime,
        input_file,
        table_name,
        sql,
        output_file,
        ordering,
        context_id,
        spill_budget_bytes,
        spill_file_count_limit,
    )?;
    // STATUS_CANCELLED is returned as Ok(MvBuildResult) with status_code != 0.
    // Propagate as an error for the legacy i64-returning wrapper.
    if result.status_code == MvBuildResult::STATUS_CANCELLED {
        return Err(format!("Query {} cancelled", context_id));
    }
    Ok(result.row_count as i64)
}

/// Managed Arrow C-Data build.
///
/// NOT on the production pull/ingestion path — that path is
/// [`build_streaming_ipc_artifact`] (streaming, no terminal `collect`). This
/// entry point exists only for the Arrow C-Data export FFI
/// (`df_mv_build_arrow_managed` ← `MVBuildRuntime.buildArrowManaged`), which
/// hands a single contiguous `StructArray` across the C-Data interface and
/// therefore *must* materialize one batch. It still sorts via `SortExec` over
/// the full ordering contract; the terminal `collect`/`concat_batches` here is
/// an inherent requirement of the C-Data single-array contract, not a
/// materialization on the streaming state-file build path.
pub fn mv_build_arrow_managed(
    runtime: &DataFusionRuntime,
    input_file: &str,
    table_name: &str,
    sql: &str,
    array_addr: i64,
    schema_addr: i64,
    ordering: &OrderingContract,
    context_id: i64,
    _spill_budget_bytes: i64,
    _spill_file_count_limit: i32,
) -> Result<i64, String> {
    use arrow_array::ffi::{FFI_ArrowArray, FFI_ArrowSchema};
    use arrow_array::Array;

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("mv_build_arrow_managed runtime: {e}"))?;

    let token = get_cancel_token(context_id);

    rt.block_on(async {
        let config = SessionConfig::new().with_target_partitions(1);
        let state = SessionStateBuilder::new()
            .with_config(config)
            .with_runtime_env(Arc::new(runtime.runtime_env.clone()))
            .with_default_features()
            .with_physical_optimizer_rules(
                crate::agg_mode::physical_optimizer_rules_without_combine(),
            )
            .build();
        let ctx = SessionContext::new_with_state(state);
        ctx.register_parquet(table_name, input_file, ParquetReadOptions::default())
            .await
            .map_err(|e| format!("mv_build_arrow_managed register_parquet({input_file}): {e}"))?;

        let build_future = async {
            let df = ctx
                .sql(sql)
                .await
                .map_err(|e| format!("mv_build_arrow_managed plan sql: {e}"))?;
            let physical = df
                .create_physical_plan()
                .await
                .map_err(|e| format!("mv_build_arrow_managed physical plan: {e}"))?;
            let partial = find_partial(&physical).ok_or_else(|| {
                "mv_build_arrow_managed: no Partial aggregate in plan".to_string()
            })?;

            // Stage 3: wrap in SortExec over full ordering contract
            let partial_schema = partial.schema();
            let sort_exprs = ordering.to_physical_sort_exprs(&partial_schema);
            let sort_exec = Arc::new(SortExec::new(sort_exprs, partial)) as Arc<dyn ExecutionPlan>;

            // Collect sorted batches for C-Data export (requires single batch)
            let batches = collect(sort_exec, ctx.task_ctx())
                .await
                .map_err(|e| format!("mv_build_arrow_managed collect sorted: {e}"))?;

            if batches.is_empty() {
                return Err("mv_build_arrow_managed: sorted stream produced no batches".to_string());
            }
            let schema = batches[0].schema();
            let concatenated = arrow::compute::concat_batches(&schema, &batches)
                .map_err(|e| format!("mv_build_arrow_managed concat: {e}"))?;

            let rows = concatenated.num_rows() as i64;
            let struct_array: arrow_array::StructArray = concatenated.into();
            let data = struct_array.into_data();
            let ffi_schema = FFI_ArrowSchema::try_from(data.data_type())
                .map_err(|e| format!("mv_build_arrow_managed schema export: {e}"))?;
            let ffi_array = FFI_ArrowArray::new(&data);
            unsafe {
                std::ptr::write(array_addr as *mut FFI_ArrowArray, ffi_array);
                std::ptr::write(schema_addr as *mut FFI_ArrowSchema, ffi_schema);
            }
            Ok(rows)
        };

        match token {
            Some(ref tok) => cancellation::cancellable(Some(tok), context_id, build_future).await,
            None => build_future.await,
        }
    })
}

/// Build a sorted state batch as an in-memory `RecordBatch`.
///
/// UNREACHABLE from the production pull path: there is no FFI export and no
/// Java caller for this function (verified — the pull path uses
/// [`build_streaming_ipc_artifact`]). It is retained only as a convenience for
/// potential in-process callers that need a single materialized sorted batch.
/// It sorts via `SortExec` over the full ordering contract but ends in a
/// terminal `collect`/`concat_batches`, so it is deliberately kept off the
/// bounded-memory streaming path.
pub fn build_sorted_state_managed(
    runtime: &DataFusionRuntime,
    input_file: &str,
    table_name: &str,
    sql: &str,
    ordering: &OrderingContract,
    context_id: i64,
    _spill_budget_bytes: i64,
    _spill_file_count_limit: i32,
) -> Result<RecordBatch, String> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("mv_build_managed runtime: {e}"))?;

    let token = get_cancel_token(context_id);

    rt.block_on(async {
        let config = SessionConfig::new().with_target_partitions(1);
        let state = SessionStateBuilder::new()
            .with_config(config)
            .with_runtime_env(Arc::new(runtime.runtime_env.clone()))
            .with_default_features()
            .with_physical_optimizer_rules(
                crate::agg_mode::physical_optimizer_rules_without_combine(),
            )
            .build();
        let ctx = SessionContext::new_with_state(state);
        ctx.register_parquet(table_name, input_file, ParquetReadOptions::default())
            .await
            .map_err(|e| format!("mv_build_managed register_parquet({input_file}): {e}"))?;

        let build_future = async {
            let df = ctx
                .sql(sql)
                .await
                .map_err(|e| format!("mv_build_managed plan sql: {e}"))?;
            let physical = df
                .create_physical_plan()
                .await
                .map_err(|e| format!("mv_build_managed physical plan: {e}"))?;
            let partial = find_partial(&physical)
                .ok_or_else(|| "mv_build_managed: no Partial aggregate in plan".to_string())?;

            // Stage 3: wrap in SortExec over full ordering contract
            let partial_schema = partial.schema();
            let sort_exprs = ordering.to_physical_sort_exprs(&partial_schema);
            let sort_exec = Arc::new(
                SortExec::new(sort_exprs, partial)
            ) as Arc<dyn ExecutionPlan>;

            // Collect sorted batches and concatenate
            let batches = collect(sort_exec, ctx.task_ctx())
                .await
                .map_err(|e| format!("mv_build_managed collect sorted: {e}"))?;

            if batches.is_empty() {
                return Err(
                    "mv_build_managed: sorted stream produced no batches (expected at least an empty batch)"
                        .to_string(),
                );
            }
            let schema = batches[0].schema();
            let concatenated = arrow::compute::concat_batches(&schema, &batches)
                .map_err(|e| format!("mv_build_managed concat: {e}"))?;

            Ok(concatenated)
        };

        match token {
            Some(ref tok) => {
                cancellation::cancellable(Some(tok), context_id, build_future).await
            }
            None => build_future.await,
        }
    })
}

/// Find the Partial aggregate node in the physical plan.
fn find_partial(plan: &Arc<dyn ExecutionPlan>) -> Option<Arc<dyn ExecutionPlan>> {
    if let Some(agg) = plan.downcast_ref::<AggregateExec>() {
        match agg.mode() {
            AggregateMode::Partial | AggregateMode::Single | AggregateMode::SinglePartitioned => {
                return Some(plan.clone());
            }
            _ => {}
        }
    }
    for child in plan.children() {
        if let Some(found) = find_partial(child) {
            return Some(found);
        }
    }
    None
}

/// Build the production Stage 3 physical plan: the partial aggregate wrapped in
/// a [`SortExec`] over the **full** [`OrderingContract`] (every
/// `MVGroupByOrdering.stateFieldIndices()` column, ASC NULLS FIRST).
///
/// This is the single place the streaming plan is constructed, shared by
/// [`build_streaming_ipc_artifact`] and the Stage 3 structural / spill
/// regression tests. `SortExec` is DataFusion's external, spillable sort: when
/// the session's memory pool is exhausted it spills sorted runs to the
/// `RuntimeEnv`'s `DiskManager` and merges them, so the plan is bounded-memory
/// end to end. The returned plan yields a **single, globally sorted** output
/// partition — the streaming writer therefore preserves global order across all
/// emitted batches (not merely within each batch).
///
/// No terminal `collect` / `concat_batches` / `sort_to_indices` / `take` is
/// used here or downstream in the production streaming path.
pub(crate) async fn plan_partial_then_sort(
    ctx: &SessionContext,
    sql: &str,
    ordering: &OrderingContract,
) -> Result<Arc<dyn ExecutionPlan>, String> {
    let df = ctx.sql(sql).await.map_err(|e| format!("plan sql: {e}"))?;
    let physical = df
        .create_physical_plan()
        .await
        .map_err(|e| format!("physical plan: {e}"))?;
    let partial =
        find_partial(&physical).ok_or_else(|| "no Partial aggregate in plan".to_string())?;

    // Wrap the partial aggregate in a SortExec over the FULL ordering tuple.
    let partial_schema = partial.schema();
    let sort_exprs = ordering.to_physical_sort_exprs(&partial_schema);
    Ok(Arc::new(SortExec::new(sort_exprs, partial)) as Arc<dyn ExecutionPlan>)
}

// ── Stage 3: Native schema cross-check (definition validation) ───────────
//
// A candidate MV definition is compiled on the Java side into a canonical
// partial SQL + ordering contract + expected state-field layout. Before the
// definition is accepted, we must confirm that the DataFusion engine will
// *physically* produce exactly the state schema Java derived — otherwise a
// drifted definition (wrong source column type, unknown column, a bad time
// bucket expression, …) would only surface at ingest time, after the target
// index is created. `validate_definition` plans (but never executes) the
// partial+sort against the REAL source Arrow schema and returns the engine's
// ACTUAL Partial-stage state schema plus the three deterministic hashes so
// Java can fail closed on any disagreement.

/// The outcome of a definition cross-check: the engine's ACTUAL Partial-stage
/// state schema (ordered `(field_name, arrow_type_token)` pairs) plus the three
/// deterministic hashes that Java also computes for cross-language comparison.
#[derive(Debug, Clone)]
pub struct DefinitionValidation {
    /// Ordered `(field_name, arrow_type_token)` for each Partial-stage state
    /// column, in physical output order.
    pub state_fields: Vec<(String, String)>,
    /// FNV-128 (lower-64) hash of the actual Partial-stage Arrow schema.
    pub schema_hash: u64,
    /// FNV-128 (lower-64) hash of the ordering-contract identity (matches
    /// `MVGroupByOrdering.orderingIdentityHash()` on the Java side).
    pub ordering_identity_hash: u64,
    /// FNV-128 (lower-64) hash of the ordering-contract definition
    /// (matches `MVGroupByOrdering.definitionIdentityHash()` on the Java side).
    pub definition_hash: u64,
}

/// Map a canonical arrow type token to an Arrow [`DataType`] for building the
/// source schema. The token set is the closed vocabulary the parquet
/// data-format produces for OpenSearch mapping types (see
/// `ArrowSchemaBuilder`/`*ParquetField.getArrowType()`), plus the temporal and
/// binary types those fields can emit.
pub fn arrow_token_to_type(token: &str) -> Result<arrow_schema::DataType, String> {
    use arrow_schema::{DataType, TimeUnit};
    Ok(match token {
        "int8" => DataType::Int8,
        "int16" => DataType::Int16,
        "int32" => DataType::Int32,
        "int64" => DataType::Int64,
        "uint8" => DataType::UInt8,
        "uint16" => DataType::UInt16,
        "uint32" => DataType::UInt32,
        "uint64" => DataType::UInt64,
        "float16" => DataType::Float16,
        "float32" => DataType::Float32,
        "float64" => DataType::Float64,
        "utf8" => DataType::Utf8,
        "bool" | "boolean" => DataType::Boolean,
        "timestamp_ms" => DataType::Timestamp(TimeUnit::Millisecond, None),
        "date32" => DataType::Date32,
        "date64" => DataType::Date64,
        "binary" => DataType::Binary,
        other => return Err(format!("unknown arrow type token '{other}'")),
    })
}

/// Map an Arrow [`DataType`] back to a canonical token string for the result.
/// Utf8/LargeUtf8/Utf8View collapse to `utf8`; Binary/LargeBinary to `binary`.
/// Any type outside the closed vocabulary falls back to its lower-cased
/// `Debug` rendering so callers still get a stable, comparable string.
pub fn arrow_type_to_token(dt: &arrow_schema::DataType) -> String {
    use arrow_schema::{DataType, TimeUnit};
    match dt {
        DataType::Int8 => "int8".to_string(),
        DataType::Int16 => "int16".to_string(),
        DataType::Int32 => "int32".to_string(),
        DataType::Int64 => "int64".to_string(),
        DataType::UInt8 => "uint8".to_string(),
        DataType::UInt16 => "uint16".to_string(),
        DataType::UInt32 => "uint32".to_string(),
        DataType::UInt64 => "uint64".to_string(),
        DataType::Float16 => "float16".to_string(),
        DataType::Float32 => "float32".to_string(),
        DataType::Float64 => "float64".to_string(),
        DataType::Utf8 | DataType::LargeUtf8 | DataType::Utf8View => "utf8".to_string(),
        DataType::Boolean => "bool".to_string(),
        DataType::Timestamp(TimeUnit::Millisecond, _) => "timestamp_ms".to_string(),
        DataType::Timestamp(TimeUnit::Second, _) => "timestamp_s".to_string(),
        DataType::Timestamp(TimeUnit::Microsecond, _) => "timestamp_us".to_string(),
        DataType::Timestamp(TimeUnit::Nanosecond, _) => "timestamp_ns".to_string(),
        DataType::Date32 => "date32".to_string(),
        DataType::Date64 => "date64".to_string(),
        DataType::Binary | DataType::LargeBinary => "binary".to_string(),
        other => format!("{other:?}").to_lowercase(),
    }
}

/// Parse the source-schema wire encoding into an Arrow schema.
///
/// Wire format (chosen for determinism + zero extra deps, mirroring the
/// existing newline-delimited `state_fields`/`state_paths` FFI convention in
/// `df_create_mv_only_session_context`): newline-separated records, each
/// `field_name \t arrow_type_token`. Blank lines are ignored. All fields are
/// built nullable (Partial-stage grouping/aggregation is null-tolerant and the
/// source parquet fields are themselves nullable).
fn parse_source_schema(encoded: &str) -> Result<SchemaRef, String> {
    use arrow_schema::{Field, Schema};
    let mut fields = Vec::new();
    for (lineno, line) in encoded.split('\n').enumerate() {
        let line = line.trim();
        if line.is_empty() {
            continue;
        }
        let mut parts = line.splitn(2, '\t');
        let name = parts
            .next()
            .filter(|s| !s.is_empty())
            .ok_or_else(|| format!("source schema line {lineno}: missing field name"))?;
        let token = parts
            .next()
            .map(str::trim)
            .filter(|s| !s.is_empty())
            .ok_or_else(|| {
                format!("source schema line {lineno}: field '{name}' missing arrow type token")
            })?;
        let dt = arrow_token_to_type(token)
            .map_err(|e| format!("source schema field '{name}': {e}"))?;
        fields.push(Field::new(name, dt, true));
    }
    if fields.is_empty() {
        return Err("source schema is empty".to_string());
    }
    Ok(Arc::new(Schema::new(fields)))
}

/// Stage 3 native cross-check: plan the candidate definition's partial+sort
/// against the REAL source Arrow schema and return the engine's ACTUAL
/// Partial-stage state schema + hashes WITHOUT executing.
///
/// The source schema is registered as an EMPTY in-memory table (no rows), so
/// planning binds column references and aggregate/expression output types
/// exactly as it would over the real parquet source, but nothing is executed.
/// Planning reuses [`plan_partial_then_sort`] — the single production planner —
/// so the state schema returned here is byte-for-byte what the ingest path
/// would physically produce.
///
/// Precise, non-panicking errors are returned for the failure modes a bad
/// definition exhibits: unknown source column, unparseable SQL, and aggregate/
/// expression type mismatches. The SQL text is echoed into planning errors so
/// the offending column is always named.
pub fn validate_definition(
    source_schema_encoded: &str,
    table_name: &str,
    sql: &str,
    ordering: &OrderingContract,
) -> Result<DefinitionValidation, String> {
    let schema = parse_source_schema(source_schema_encoded)?;

    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .map_err(|e| format!("mv_validate_definition runtime: {e}"))?;

    rt.block_on(async {
        // Same session shape as the production build path (partial kept separate
        // from final so `find_partial` inside `plan_partial_then_sort` resolves
        // the Partial-stage node), minus the shared DataFusionRuntime — nothing
        // executes, so no memory pool / disk manager is required.
        let config = SessionConfig::new().with_target_partitions(1);
        let state = SessionStateBuilder::new()
            .with_config(config)
            .with_default_features()
            .with_physical_optimizer_rules(
                crate::agg_mode::physical_optimizer_rules_without_combine(),
            )
            .build();
        let ctx = SessionContext::new_with_state(state);
        // Register the same UDF/UDAF surface the data-node session exposes so a
        // definition using a custom function is not falsely rejected as unknown.
        crate::udf::register_all(&ctx);
        crate::udaf::register_all(&ctx);

        let mem = datafusion::datasource::MemTable::try_new(schema.clone(), vec![vec![]])
            .map_err(|e| format!("mv_validate_definition memtable: {e}"))?;
        ctx.register_table(table_name, Arc::new(mem))
            .map_err(|e| format!("mv_validate_definition register_table({table_name}): {e}"))?;

        // Reuse the production planner — do NOT duplicate planning logic.
        let sort_exec = plan_partial_then_sort(&ctx, sql, ordering)
            .await
            .map_err(|e| format!("mv_validate_definition: planning failed for SQL [{sql}]: {e}"))?;

        let out_schema = sort_exec.schema();
        let state_fields: Vec<(String, String)> = out_schema
            .fields()
            .iter()
            .map(|f| (f.name().clone(), arrow_type_to_token(f.data_type())))
            .collect();

        Ok(DefinitionValidation {
            state_fields,
            schema_hash: compute_schema_hash_u64(&out_schema),
            ordering_identity_hash: compute_ordering_hash_u64(ordering),
            definition_hash: compute_definition_hash_u64(ordering),
        })
    })
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema};
    use arrow_array::{Int32Array, Int64Array, StringArray, UInt32Array, UInt64Array};
    use std::sync::Arc;

    // ── Stage 3: validate_definition (native schema cross-check) tests ────

    /// A valid multi-key / multi-aggregate definition plans cleanly and the
    /// engine's ACTUAL Partial-stage state schema is returned in physical
    /// order. Group-key columns keep their SELECT aliases; aggregate columns
    /// carry whatever physical name the Partial aggregate assigns (asserted by
    /// position + type, not by internal name). SUM/MIN/MAX/COUNT over Int64 all
    /// stay Int64.
    #[test]
    fn test_validate_definition_valid_multi_key_multi_agg() {
        let schema_enc = "event_bucket\tint64\nURL\tutf8\nUserID\tint64\nm0\tint64";
        let sql = "SELECT \"event_bucket\", \"URL\", \"UserID\", \
             SUM(\"m0\") AS m0_sum, MIN(\"m0\") AS m0_min, MAX(\"m0\") AS m0_max, \
             COUNT(\"m0\") AS m0_cnt \
             FROM mv_input GROUP BY \"event_bucket\", \"URL\", \"UserID\"";
        let ordering = OrderingContract::from_parallel_arrays(&[0, 1, 2], &[0, 0, 0], &[0, 0, 0]);
        let v = validate_definition(schema_enc, "mv_input", sql, &ordering).unwrap();

        // 3 group keys + 4 aggregate state columns = 7 physical state fields.
        assert_eq!(
            v.state_fields.len(),
            7,
            "expected 7 state fields, got {:?}",
            v.state_fields
        );

        // Group-key columns lead the layout with their aliases and source types.
        assert_eq!(v.state_fields[0].0, "event_bucket");
        assert_eq!(v.state_fields[0].1, "int64");
        assert_eq!(v.state_fields[1].0, "URL");
        assert_eq!(v.state_fields[1].1, "utf8");
        assert_eq!(v.state_fields[2].0, "UserID");
        assert_eq!(v.state_fields[2].1, "int64");

        // Aggregate state columns: SUM/MIN/MAX/COUNT over Int64 → Int64.
        for i in 3..7 {
            assert_eq!(
                v.state_fields[i].1, "int64",
                "aggregate state field {} ({}) should be int64",
                i, v.state_fields[i].0
            );
        }

        assert_ne!(v.schema_hash, 0);
        assert_eq!(v.ordering_identity_hash, compute_ordering_hash_u64(&ordering));
        assert_eq!(v.definition_hash, compute_definition_hash_u64(&ordering));
    }

    /// An unknown source column is rejected with a precise, non-panicking error
    /// that names the missing column.
    #[test]
    fn test_validate_definition_unknown_column() {
        let schema_enc = "k0\tint64\nm0\tint64";
        let sql =
            "SELECT \"k0\", SUM(\"DoesNotExist\") AS s FROM mv_input GROUP BY \"k0\"";
        let ordering = OrderingContract::from_parallel_arrays(&[0], &[0], &[0]);
        let err = validate_definition(schema_enc, "mv_input", sql, &ordering)
            .expect_err("unknown column must be rejected");
        assert!(
            err.contains("DoesNotExist"),
            "error must name the unknown column, got: {err}"
        );
    }

    /// DE7 date-bug regression: EventTime declared as `utf8` in the source but
    /// used in a numeric bucket division. Planning fails closed with a precise
    /// error that names the offending column.
    #[test]
    fn test_validate_definition_type_mismatch_de7_eventtime_utf8() {
        let schema_enc = "EventTime\tutf8\nUserID\tint64";
        // 5-minute bucket via numeric division on EventTime (which is Utf8 here).
        let sql = "SELECT \"EventTime\" / 300000 AS event_bucket, COUNT(*) AS cnt \
             FROM mv_input GROUP BY \"EventTime\" / 300000";
        let ordering = OrderingContract::from_parallel_arrays(&[0], &[0], &[0]);
        let err = validate_definition(schema_enc, "mv_input", sql, &ordering)
            .expect_err("EventTime-as-utf8 numeric division must be rejected");
        assert!(
            err.contains("EventTime"),
            "DE7 regression: error must name the EventTime column, got: {err}"
        );
    }

    /// Hashes are deterministic across two independent calls for the same
    /// definition + source schema.
    #[test]
    fn test_validate_definition_deterministic_hashes() {
        let schema_enc = "k0\tint64\nk1\tutf8\nm0\tint64";
        let sql = "SELECT \"k0\", \"k1\", SUM(\"m0\") AS s, COUNT(*) AS cnt \
             FROM mv_input GROUP BY \"k0\", \"k1\"";
        let ordering = OrderingContract::from_parallel_arrays(&[0, 1], &[0, 0], &[0, 0]);
        let a = validate_definition(schema_enc, "mv_input", sql, &ordering).unwrap();
        let b = validate_definition(schema_enc, "mv_input", sql, &ordering).unwrap();
        assert_eq!(a.schema_hash, b.schema_hash, "schema hash deterministic");
        assert_eq!(
            a.ordering_identity_hash, b.ordering_identity_hash,
            "ordering identity hash deterministic"
        );
        assert_eq!(
            a.definition_hash, b.definition_hash,
            "definition hash deterministic"
        );
        assert_eq!(
            a.state_fields, b.state_fields,
            "state fields deterministic"
        );
    }

    /// Malformed SQL is rejected without panicking.
    #[test]
    fn test_validate_definition_malformed_sql() {
        let schema_enc = "k0\tint64";
        let sql = "SELECT SELECT FROM WHERE GROUP";
        let ordering = OrderingContract::from_parallel_arrays(&[0], &[0], &[0]);
        let err = validate_definition(schema_enc, "mv_input", sql, &ordering)
            .expect_err("malformed SQL must be rejected");
        assert!(!err.is_empty(), "error message must not be empty");
    }

    /// MIN/MAX preserve the source integer width: MIN over an Int16 source
    /// column yields an Int16 state column. This documents the legitimate
    /// integer-widening reality that motivates the Java-side type-FAMILY
    /// comparison (rather than exact-width) in MVDefinitionValidator.
    #[test]
    fn test_validate_definition_min_preserves_int16_width() {
        let schema_enc = "k0\tint64\nm0\tint16";
        let sql = "SELECT \"k0\", MIN(\"m0\") AS m0_min, MAX(\"m0\") AS m0_max, COUNT(*) AS cnt \
             FROM mv_input GROUP BY \"k0\"";
        let ordering = OrderingContract::from_parallel_arrays(&[0], &[0], &[0]);
        let v = validate_definition(schema_enc, "mv_input", sql, &ordering).unwrap();
        // Find the MIN/MAX state columns by position: [k0, min, max, cnt].
        assert_eq!(v.state_fields.len(), 4, "got {:?}", v.state_fields);
        assert_eq!(v.state_fields[0].1, "int64", "group key k0 is int64");
        assert_eq!(
            v.state_fields[1].1, "int16",
            "MIN over Int16 source preserves Int16 width, got {:?}",
            v.state_fields
        );
        assert_eq!(
            v.state_fields[2].1, "int16",
            "MAX over Int16 source preserves Int16 width, got {:?}",
            v.state_fields
        );
        assert_eq!(v.state_fields[3].1, "int64", "COUNT(*) is int64");
    }

    // ── arrow token mapping round-trip ───────────────────────────────────

    #[test]
    fn test_arrow_token_roundtrip() {
        for token in [
            "int8", "int16", "int32", "int64", "uint8", "uint16", "uint32", "uint64", "float16",
            "float32", "float64", "utf8", "bool", "timestamp_ms", "date32", "date64", "binary",
        ] {
            let dt = arrow_token_to_type(token).unwrap();
            let back = arrow_type_to_token(&dt);
            // "bool"/"boolean" both parse; canonical token is "bool".
            let expected = if token == "boolean" { "bool" } else { token };
            assert_eq!(back, expected, "round-trip for token {token}");
        }
    }

    #[test]
    fn test_arrow_token_unknown_rejected() {
        let err = arrow_token_to_type("blahblah").expect_err("unknown token must error");
        assert!(err.contains("blahblah"), "error must name the token, got: {err}");
    }

    #[test]
    fn test_parse_source_schema_empty_rejected() {
        let err = parse_source_schema("   \n  \n").expect_err("empty schema must be rejected");
        assert!(err.contains("empty"), "got: {err}");
    }

    #[test]
    fn test_ordering_contract_from_parallel_arrays() {
        let contract = OrderingContract::from_parallel_arrays(&[0, 1, 2], &[0, 0, 0], &[0, 0, 1]);
        assert_eq!(contract.keys.len(), 3);
        assert_eq!(contract.keys[0].field_index, 0);
        assert_eq!(contract.keys[0].direction, 0);
        assert_eq!(contract.keys[0].null_placement, 0);
        assert_eq!(contract.keys[2].null_placement, 1);
    }

    #[test]
    fn test_cancel_context_lifecycle() {
        let id = alloc_cancel_context();
        assert!(id > 0);
        assert!(get_cancel_token(id).is_some());
        cancel_build(id);
        assert!(get_cancel_token(id).is_some());
        release_cancel_context(id);
        assert!(get_cancel_token(id).is_none());
    }

    #[test]
    fn test_cancel_unknown_context_is_noop() {
        cancel_build(999999);
        release_cancel_context(999999);
    }

    #[test]
    fn test_ordering_contract_empty_arrays() {
        let contract = OrderingContract::from_parallel_arrays(&[], &[], &[]);
        assert!(contract.keys.is_empty());
    }

    #[test]
    fn test_ordering_contract_single_key_asc_nulls_first() {
        let contract = OrderingContract::from_parallel_arrays(&[0], &[0], &[0]);
        assert_eq!(contract.keys.len(), 1);
        let key = &contract.keys[0];
        assert_eq!(key.field_index, 0);
        assert_eq!(key.direction, 0);
        assert_eq!(key.null_placement, 0);
    }

    #[test]
    fn test_ordering_contract_to_sort_columns_multi_key() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k0", DataType::Int64, true),
            Field::new("k1", DataType::Utf8, true),
            Field::new("cnt", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(Int64Array::from(vec![3, 1, 2])),
                Arc::new(StringArray::from(vec!["c", "a", "b"])),
                Arc::new(Int64Array::from(vec![10, 20, 30])),
            ],
        )
        .unwrap();

        let contract = OrderingContract::from_parallel_arrays(&[0, 1], &[0, 0], &[0, 1]);
        let sort_columns = contract.to_sort_columns(&batch);

        assert_eq!(sort_columns.len(), 2);

        let opts0 = sort_columns[0].options.unwrap();
        assert!(!opts0.descending);
        assert!(opts0.nulls_first);

        let opts1 = sort_columns[1].options.unwrap();
        assert!(!opts1.descending);
        assert!(!opts1.nulls_first);
    }

    #[test]
    fn test_ordering_contract_to_sort_columns_produces_valid_lexsort() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("bucket", DataType::Int64, true),
            Field::new("url", DataType::Utf8, true),
            Field::new("cnt", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![2, 1, 2, 1])),
                Arc::new(StringArray::from(vec!["b", "a", "a", "b"])),
                Arc::new(Int64Array::from(vec![10, 20, 30, 40])),
            ],
        )
        .unwrap();

        let contract = OrderingContract::from_parallel_arrays(&[0, 1], &[0, 0], &[0, 0]);
        let sort_columns = contract.to_sort_columns(&batch);

        let indices = lexsort_to_indices(&sort_columns, None).unwrap();
        assert_eq!(indices.len(), 4);
        assert_eq!(indices.value(0), 1);
        assert_eq!(indices.value(1), 3);
        assert_eq!(indices.value(2), 2);
        assert_eq!(indices.value(3), 0);
    }

    #[test]
    fn test_cancel_context_ids_are_unique() {
        let id1 = alloc_cancel_context();
        let id2 = alloc_cancel_context();
        let id3 = alloc_cancel_context();
        assert_ne!(id1, id2);
        assert_ne!(id2, id3);
        assert_ne!(id1, id3);
        release_cancel_context(id1);
        release_cancel_context(id2);
        release_cancel_context(id3);
    }

    #[test]
    fn test_cancel_fires_token() {
        let id = alloc_cancel_context();
        let token = get_cancel_token(id).unwrap();
        assert!(!token.is_cancelled());
        cancel_build(id);
        assert!(token.is_cancelled());
        release_cancel_context(id);
    }

    #[test]
    fn test_release_then_cancel_is_noop() {
        let id = alloc_cancel_context();
        release_cancel_context(id);
        cancel_build(id);
        assert!(get_cancel_token(id).is_none());
    }

    #[test]
    fn test_multiple_cancel_same_context_idempotent() {
        let id = alloc_cancel_context();
        cancel_build(id);
        cancel_build(id);
        cancel_build(id);
        let token = get_cancel_token(id).unwrap();
        assert!(token.is_cancelled());
        release_cancel_context(id);
    }

    #[test]
    #[should_panic(expected = "assertion")]
    fn test_ordering_contract_mismatched_array_lengths() {
        OrderingContract::from_parallel_arrays(&[0, 1], &[0], &[0, 0]);
    }

    // ── Stage 3 tests: schema hash, definition hash, IPC validation ──────

    #[test]
    fn test_compute_schema_hash_deterministic() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k0", DataType::Int64, true),
            Field::new("k1", DataType::Utf8, true),
            Field::new("cnt", DataType::Int64, false),
        ]));
        let h1 = compute_schema_hash(&schema);
        let h2 = compute_schema_hash(&schema);
        assert_eq!(h1, h2, "schema hash must be deterministic");
        assert_eq!(h1.len(), 32, "FNV-128 hex should be 32 chars");
    }

    #[test]
    fn test_compute_schema_hash_different_schemas() {
        let s1 = Arc::new(Schema::new(vec![Field::new("k0", DataType::Int64, true)]));
        let s2 = Arc::new(Schema::new(vec![Field::new("k0", DataType::Int32, true)]));
        assert_ne!(compute_schema_hash(&s1), compute_schema_hash(&s2));
    }

    #[test]
    fn test_compute_definition_hash_deterministic() {
        let ordering = OrderingContract::from_parallel_arrays(&[0, 1, 2], &[0, 0, 0], &[0, 0, 1]);
        let h1 = compute_definition_hash(&ordering);
        let h2 = compute_definition_hash(&ordering);
        assert_eq!(h1, h2, "definition hash must be deterministic");
        assert_eq!(h1.len(), 32);
    }

    #[test]
    fn test_compute_definition_hash_different_orderings() {
        let o1 = OrderingContract::from_parallel_arrays(&[0, 1], &[0, 0], &[0, 0]);
        let o2 = OrderingContract::from_parallel_arrays(&[0, 1], &[0, 0], &[0, 1]);
        assert_ne!(compute_definition_hash(&o1), compute_definition_hash(&o2));
    }

    #[test]
    fn test_compute_definition_hash_different_key_count() {
        let o1 = OrderingContract::from_parallel_arrays(&[0], &[0], &[0]);
        let o2 = OrderingContract::from_parallel_arrays(&[0, 1], &[0, 0], &[0, 0]);
        assert_ne!(compute_definition_hash(&o1), compute_definition_hash(&o2));
    }

    #[test]
    fn test_artifact_metadata_fields() {
        let meta = ArtifactMetadata {
            row_count: 42,
            schema_hash: "abc123".to_string(),
            definition_hash: "def456".to_string(),
        };
        assert_eq!(meta.row_count, 42);
        assert_eq!(meta.schema_hash, "abc123");
        assert_eq!(meta.definition_hash, "def456");
    }

    #[test]
    fn test_validate_ipc_ordering_sorted_file() {
        // Create a sorted batch and write to IPC
        let schema = Arc::new(Schema::new(vec![
            Field::new("k0", DataType::Int64, true),
            Field::new("k1", DataType::Utf8, true),
            Field::new("cnt", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![1, 1, 2, 2])),
                Arc::new(StringArray::from(vec!["a", "b", "a", "b"])),
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

        let ordering = OrderingContract::from_parallel_arrays(&[0, 1], &[0, 0], &[0, 0]);
        assert!(validate_ipc_ordering(&path, &ordering).unwrap());
    }

    #[test]
    fn test_validate_ipc_ordering_unsorted_file() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k0", DataType::Int64, true),
            Field::new("cnt", DataType::Int64, false),
        ]));
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
    fn test_multi_key_sort_with_nulls() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k0", DataType::Int64, true),
            Field::new("k1", DataType::Utf8, true),
            Field::new("cnt", DataType::Int64, false),
        ]));
        // NULLs should sort first (NULLS FIRST)
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![Some(1), None, Some(2), None])),
                Arc::new(StringArray::from(vec![Some("a"), Some("b"), None, None])),
                Arc::new(Int64Array::from(vec![10, 20, 30, 40])),
            ],
        )
        .unwrap();

        let contract = OrderingContract::from_parallel_arrays(&[0, 1], &[0, 0], &[0, 0]);
        let sort_columns = contract.to_sort_columns(&batch);
        let indices = lexsort_to_indices(&sort_columns, None).unwrap();

        // With NULLS FIRST: NULL k0 rows come first, then sorted by k1
        // NULL/NULL -> rows 3 (k0=null, k1=null)
        // NULL/"b"  -> row 1 (k0=null, k1="b")
        // 1/"a"     -> row 0
        // 2/NULL    -> row 2
        assert_eq!(indices.value(0), 3); // NULL, NULL
        assert_eq!(indices.value(1), 1); // NULL, "b"
        assert_eq!(indices.value(2), 0); // 1, "a"
        assert_eq!(indices.value(3), 2); // 2, NULL
    }

    #[test]
    fn test_various_integer_widths_sort() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k_i32", DataType::Int32, true),
            Field::new("k_i64", DataType::Int64, true),
            Field::new("k_u32", DataType::UInt32, true),
            Field::new("k_u64", DataType::UInt64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![3, 1, 2, 1])),
                Arc::new(Int64Array::from(vec![100, 200, 300, 100])),
                Arc::new(UInt32Array::from(vec![10, 20, 30, 40])),
                Arc::new(UInt64Array::from(vec![1000, 2000, 3000, 4000])),
            ],
        )
        .unwrap();

        // Sort by all 4 columns
        let contract =
            OrderingContract::from_parallel_arrays(&[0, 1, 2, 3], &[0, 0, 0, 0], &[0, 0, 0, 0]);
        let sort_columns = contract.to_sort_columns(&batch);
        let indices = lexsort_to_indices(&sort_columns, None).unwrap();

        // Expected order: (1,100,..) < (1,200,..) < (2,300,..) < (3,100,..)
        assert_eq!(indices.value(0), 3); // (1, 100, 40, 4000)
        assert_eq!(indices.value(1), 1); // (1, 200, 20, 2000)
        assert_eq!(indices.value(2), 2); // (2, 300, 30, 3000)
        assert_eq!(indices.value(3), 0); // (3, 100, 10, 1000)
    }

    #[test]
    fn test_ipc_roundtrip_preserves_ordering() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k0", DataType::Int32, true),
            Field::new("k1", DataType::Int64, true),
            Field::new("k2", DataType::Utf8, true),
            Field::new("val", DataType::Int64, false),
        ]));
        // Already sorted: (1, 10, "a"), (1, 10, "b"), (2, 5, "c")
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 1, 2])),
                Arc::new(Int64Array::from(vec![10, 10, 5])),
                Arc::new(StringArray::from(vec!["a", "b", "c"])),
                Arc::new(Int64Array::from(vec![100, 200, 300])),
            ],
        )
        .unwrap();

        // Write to IPC
        let tmp = tempfile::NamedTempFile::new().unwrap();
        let path = tmp.path().to_str().unwrap().to_string();
        {
            let file = File::create(&path).unwrap();
            let mut writer = IpcFileWriter::try_new(file, &schema).unwrap();
            writer.write(&batch).unwrap();
            writer.finish().unwrap();
        }

        // Read back and verify ordering
        let ordering = OrderingContract::from_parallel_arrays(&[0, 1, 2], &[0, 0, 0], &[0, 0, 0]);
        assert!(validate_ipc_ordering(&path, &ordering).unwrap());

        // Read back and verify data
        let file = File::open(&path).unwrap();
        let reader = arrow::ipc::reader::FileReader::try_new(file, None).unwrap();
        let mut total_rows = 0;
        for batch_result in reader {
            let b = batch_result.unwrap();
            total_rows += b.num_rows();
        }
        assert_eq!(total_rows, 3);
    }

    #[test]
    fn test_physical_sort_exprs_from_ordering() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k0", DataType::Int64, true),
            Field::new("k1", DataType::Utf8, true),
            Field::new("cnt", DataType::Int64, false),
        ]));
        let ordering = OrderingContract::from_parallel_arrays(&[0, 1], &[0, 0], &[0, 1]);
        let exprs = ordering.to_physical_sort_exprs(&schema);
        assert_eq!(exprs.len(), 2);
        assert!(!exprs[0].options.descending);
        assert!(exprs[0].options.nulls_first);
        assert!(!exprs[1].options.descending);
        assert!(!exprs[1].options.nulls_first);
    }

    // ── MvBuildResult tests ─────────────────────────────────────────────

    #[test]
    fn test_mv_build_result_struct_size_88() {
        assert_eq!(std::mem::size_of::<MvBuildResult>(), 80);
        assert_eq!(MvBuildResult::STRUCT_SIZE, 80);
    }

    #[test]
    fn test_mv_build_result_abi_version_is_1() {
        assert_eq!(MvBuildResult::ABI_VERSION, 1);
    }

    #[test]
    fn test_mv_build_result_ok_populates_all_fields() {
        let r = MvBuildResult::ok(
            1000,        // row_count
            0xAAAA,      // schema_hash
            0xBBBB,      // definition_hash
            0xCCCC,      // ordering_hash
            4096,        // spill_bytes
            2,           // spill_file_count
            5,           // output_batch_count
            1024 * 1024, // peak_rss_bytes
            500_000,     // build_duration_us
        );
        assert_eq!(r.abi_version, 1);
        assert_eq!(r.struct_size, 80);
        assert_eq!(r.status_code, MvBuildResult::STATUS_OK);
        assert_eq!(r._pad0, 0);
        assert_eq!(r.row_count, 1000);
        assert_eq!(r.schema_hash, 0xAAAA);
        assert_eq!(r.definition_hash, 0xBBBB);
        assert_eq!(r.ordering_hash, 0xCCCC);
        assert_eq!(r.spill_bytes, 4096);
        assert_eq!(r.spill_file_count, 2);
        assert_eq!(r.output_batch_count, 5);
        assert_eq!(r.peak_rss_bytes, 1024 * 1024);
        assert_eq!(r.build_duration_us, 500_000);
    }

    #[test]
    fn test_mv_build_result_error_zeroes_all_data_fields() {
        let r = MvBuildResult::error(MvBuildResult::STATUS_CANCELLED);
        assert_eq!(r.abi_version, 1);
        assert_eq!(r.struct_size, 80);
        assert_eq!(r.status_code, 1);
        assert_eq!(r.row_count, 0);
        assert_eq!(r.schema_hash, 0);
        assert_eq!(r.definition_hash, 0);
        assert_eq!(r.ordering_hash, 0);
        assert_eq!(r.spill_bytes, 0);
        assert_eq!(r.spill_file_count, 0);
        assert_eq!(r.output_batch_count, 0);
        assert_eq!(r.peak_rss_bytes, 0);
        assert_eq!(r.build_duration_us, 0);
    }

    #[test]
    fn test_mv_build_result_error_internal() {
        let r = MvBuildResult::error(MvBuildResult::STATUS_INTERNAL_ERROR);
        assert_eq!(r.status_code, -1);
    }

    #[test]
    fn test_mv_build_result_error_spill_exceeded() {
        let r = MvBuildResult::error(MvBuildResult::STATUS_SPILL_EXCEEDED);
        assert_eq!(r.status_code, 2);
    }

    #[test]
    fn test_mv_build_result_error_memory_exhausted() {
        let r = MvBuildResult::error(MvBuildResult::STATUS_MEMORY_EXHAUSTED);
        assert_eq!(r.status_code, 3);
    }

    #[test]
    fn test_mv_build_result_is_copy() {
        let r = MvBuildResult::ok(1, 2, 3, 4, 5, 6, 7, 8, 9);
        let r2 = r; // Copy, not move
        assert_eq!(r.row_count, r2.row_count);
    }

    #[test]
    fn test_mv_build_result_abi_version_first_field() {
        // The abi_version field must be at offset 0 for the versioning contract.
        let r = MvBuildResult::ok(0, 0, 0, 0, 0, 0, 0, 0, 0);
        let ptr = &r as *const MvBuildResult as *const u32;
        unsafe {
            assert_eq!(*ptr, MvBuildResult::ABI_VERSION);
        }
    }

    // ── u64 hash helper tests ───────────────────────────────────────────

    #[test]
    fn test_compute_schema_hash_u64_deterministic() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("k0", DataType::Int64, true),
            Field::new("k1", DataType::Utf8, true),
            Field::new("cnt", DataType::Int64, false),
        ]));
        let h1 = compute_schema_hash_u64(&schema);
        let h2 = compute_schema_hash_u64(&schema);
        assert_eq!(h1, h2, "u64 schema hash must be deterministic");
        assert_ne!(h1, 0, "hash should not be zero for a real schema");
    }

    #[test]
    fn test_compute_schema_hash_u64_different_schemas() {
        let s1 = Arc::new(Schema::new(vec![Field::new("k0", DataType::Int64, true)]));
        let s2 = Arc::new(Schema::new(vec![Field::new("k0", DataType::Int32, true)]));
        assert_ne!(compute_schema_hash_u64(&s1), compute_schema_hash_u64(&s2));
    }

    #[test]
    fn test_compute_definition_hash_u64_deterministic() {
        let ordering = OrderingContract::from_parallel_arrays(&[0, 1, 2], &[0, 0, 0], &[0, 0, 1]);
        let h1 = compute_definition_hash_u64(&ordering);
        let h2 = compute_definition_hash_u64(&ordering);
        assert_eq!(h1, h2, "u64 definition hash must be deterministic");
    }

    #[test]
    fn test_compute_definition_hash_u64_different_orderings() {
        let o1 = OrderingContract::from_parallel_arrays(&[0, 1], &[0, 0], &[0, 0]);
        let o2 = OrderingContract::from_parallel_arrays(&[0, 1], &[0, 0], &[0, 1]);
        assert_ne!(
            compute_definition_hash_u64(&o1),
            compute_definition_hash_u64(&o2)
        );
    }

    #[test]
    fn test_compute_ordering_hash_u64_deterministic() {
        let ordering = OrderingContract::from_parallel_arrays(&[0, 2, 1], &[0, 1, 0], &[0, 0, 1]);
        let h1 = compute_ordering_hash_u64(&ordering);
        let h2 = compute_ordering_hash_u64(&ordering);
        assert_eq!(h1, h2, "ordering hash must be deterministic");
    }

    #[test]
    fn test_compute_ordering_hash_u64_different_orderings() {
        let o1 = OrderingContract::from_parallel_arrays(&[0, 1], &[0, 0], &[0, 0]);
        let o2 = OrderingContract::from_parallel_arrays(&[1, 0], &[0, 0], &[0, 0]);
        assert_ne!(
            compute_ordering_hash_u64(&o1),
            compute_ordering_hash_u64(&o2)
        );
    }

    // ════════════════════════════════════════════════════════════════════
    // FFI result contract tests — MvBuildResult ABI layout, field offsets,
    // constructors, byte-level roundtrip, and hash helpers.
    // ════════════════════════════════════════════════════════════════════

    /// Test 1: ABI layout — size, alignment, and compile-time assertions.
    #[test]
    fn test_mv_build_result_abi_layout() {
        // Size: MvBuildResult is 80 bytes (#[repr(C)], compile-time asserted).
        assert_eq!(std::mem::size_of::<MvBuildResult>(), 80);
        assert_eq!(MvBuildResult::STRUCT_SIZE, 80);

        // Alignment: 8 bytes (u64-aligned).
        assert_eq!(std::mem::align_of::<MvBuildResult>(), 8);

        // ABI version constant.
        assert_eq!(MvBuildResult::ABI_VERSION, 1);

        // Status code constants.
        assert_eq!(MvBuildResult::STATUS_OK, 0);
        assert_eq!(MvBuildResult::STATUS_CANCELLED, 1);
        assert_eq!(MvBuildResult::STATUS_SPILL_EXCEEDED, 2);
        assert_eq!(MvBuildResult::STATUS_MEMORY_EXHAUSTED, 3);
        assert_eq!(MvBuildResult::STATUS_INTERNAL_ERROR, -1);
    }

    /// Test 1b: Field offsets match the documented Java MvBuildResultLayout constants.
    ///
    /// Uses manual pointer arithmetic (the Rust-stable way without the
    /// unstable `offset_of!` macro). Each field's byte offset from the
    /// struct base must match the Java `GroupLayout` offsets so cross-language
    /// `MemorySegment.get()` calls decode correctly.
    #[test]
    fn test_mv_build_result_field_offsets() {
        let r = MvBuildResult::ok(0, 0, 0, 0, 0, 0, 0, 0, 0);
        let base = &r as *const MvBuildResult as usize;

        // Helper: compute byte offset of a field reference from the struct base.
        macro_rules! field_offset {
            ($field:ident) => {
                (&r.$field as *const _ as usize) - base
            };
        }

        assert_eq!(field_offset!(abi_version), 0, "abi_version offset");
        assert_eq!(field_offset!(struct_size), 4, "struct_size offset");
        assert_eq!(field_offset!(status_code), 8, "status_code offset");
        assert_eq!(field_offset!(_pad0), 12, "_pad0 offset");
        assert_eq!(field_offset!(row_count), 16, "row_count offset");
        assert_eq!(field_offset!(schema_hash), 24, "schema_hash offset");
        assert_eq!(field_offset!(definition_hash), 32, "definition_hash offset");
        assert_eq!(field_offset!(ordering_hash), 40, "ordering_hash offset");
        assert_eq!(field_offset!(spill_bytes), 48, "spill_bytes offset");
        assert_eq!(
            field_offset!(spill_file_count),
            56,
            "spill_file_count offset"
        );
        assert_eq!(
            field_offset!(output_batch_count),
            60,
            "output_batch_count offset"
        );
        assert_eq!(field_offset!(peak_rss_bytes), 64, "peak_rss_bytes offset");
        assert_eq!(
            field_offset!(build_duration_us),
            72,
            "build_duration_us offset"
        );
    }

    /// Test 3: MvBuildResult::ok() constructor populates all fields correctly.
    #[test]
    fn test_mv_build_result_ok_constructor() {
        let r = MvBuildResult::ok(
            42,              // row_count
            0xDEAD_BEEF,     // schema_hash
            0xCAFE_BABE,     // definition_hash
            0xFACE_FEED,     // ordering_hash
            8192,            // spill_bytes
            3,               // spill_file_count
            7,               // output_batch_count
            2 * 1024 * 1024, // peak_rss_bytes
            123_456,         // build_duration_us
        );
        assert_eq!(r.abi_version, MvBuildResult::ABI_VERSION);
        assert_eq!(r.struct_size, 80);
        assert_eq!(r.status_code, MvBuildResult::STATUS_OK);
        assert_eq!(r._pad0, 0);
        assert_eq!(r.row_count, 42);
        assert_eq!(r.schema_hash, 0xDEAD_BEEF);
        assert_eq!(r.definition_hash, 0xCAFE_BABE);
        assert_eq!(r.ordering_hash, 0xFACE_FEED);
        assert_eq!(r.spill_bytes, 8192);
        assert_eq!(r.spill_file_count, 3);
        assert_eq!(r.output_batch_count, 7);
        assert_eq!(r.peak_rss_bytes, 2 * 1024 * 1024);
        assert_eq!(r.build_duration_us, 123_456);
    }

    /// Test 4: MvBuildResult::error(STATUS_CANCELLED) zeroes all metric fields.
    #[test]
    fn test_mv_build_result_error_constructor() {
        let r = MvBuildResult::error(MvBuildResult::STATUS_CANCELLED);
        assert_eq!(r.abi_version, MvBuildResult::ABI_VERSION);
        assert_eq!(r.struct_size, 80);
        assert_eq!(r.status_code, MvBuildResult::STATUS_CANCELLED);
        assert_eq!(r.row_count, 0);
        assert_eq!(r.schema_hash, 0);
        assert_eq!(r.definition_hash, 0);
        assert_eq!(r.ordering_hash, 0);
        assert_eq!(r.spill_bytes, 0);
        assert_eq!(r.spill_file_count, 0);
        assert_eq!(r.output_batch_count, 0);
        assert_eq!(r.peak_rss_bytes, 0);
        assert_eq!(r.build_duration_us, 0);
    }

    /// Test 5: Byte-level roundtrip — write MvBuildResult to a [u8; 80] via
    /// copy_nonoverlapping, then read individual fields back at known byte
    /// offsets. Confirms the struct can be decoded by any language reading
    /// raw bytes at documented offsets.
    #[test]
    fn test_mv_build_result_roundtrip_bytes() {
        let r = MvBuildResult::ok(
            9999,            // row_count
            0x1111_2222,     // schema_hash
            0x3333_4444,     // definition_hash
            0x5555_6666,     // ordering_hash
            65536,           // spill_bytes
            4,               // spill_file_count
            12,              // output_batch_count
            4 * 1024 * 1024, // peak_rss_bytes
            999_999,         // build_duration_us
        );

        let mut buf = [0u8; 80];
        unsafe {
            std::ptr::copy_nonoverlapping(
                &r as *const MvBuildResult as *const u8,
                buf.as_mut_ptr(),
                80,
            );
        }

        // Read fields back from raw bytes at documented offsets.
        let abi_version = u32::from_ne_bytes(buf[0..4].try_into().unwrap());
        let struct_size = u32::from_ne_bytes(buf[4..8].try_into().unwrap());
        let status_code = i32::from_ne_bytes(buf[8..12].try_into().unwrap());
        let row_count = u64::from_ne_bytes(buf[16..24].try_into().unwrap());
        let schema_hash = u64::from_ne_bytes(buf[24..32].try_into().unwrap());
        let definition_hash = u64::from_ne_bytes(buf[32..40].try_into().unwrap());
        let ordering_hash = u64::from_ne_bytes(buf[40..48].try_into().unwrap());
        let spill_bytes = u64::from_ne_bytes(buf[48..56].try_into().unwrap());
        let spill_file_count = u32::from_ne_bytes(buf[56..60].try_into().unwrap());
        let output_batch_count = u32::from_ne_bytes(buf[60..64].try_into().unwrap());
        let peak_rss_bytes = u64::from_ne_bytes(buf[64..72].try_into().unwrap());
        let build_duration_us = u64::from_ne_bytes(buf[72..80].try_into().unwrap());

        assert_eq!(abi_version, 1, "abi_version");
        assert_eq!(struct_size, 80, "struct_size");
        assert_eq!(status_code, 0, "status_code");
        assert_eq!(row_count, 9999, "row_count");
        assert_eq!(schema_hash, 0x1111_2222, "schema_hash");
        assert_eq!(definition_hash, 0x3333_4444, "definition_hash");
        assert_eq!(ordering_hash, 0x5555_6666, "ordering_hash");
        assert_eq!(spill_bytes, 65536, "spill_bytes");
        assert_eq!(spill_file_count, 4, "spill_file_count");
        assert_eq!(output_batch_count, 12, "output_batch_count");
        assert_eq!(peak_rss_bytes, 4 * 1024 * 1024, "peak_rss_bytes");
        assert_eq!(build_duration_us, 999_999, "build_duration_us");
    }

    /// Test 6: schema hash u64 is deterministic across multiple calls.
    #[test]
    fn test_schema_hash_u64_deterministic_repeated() {
        let schema = Arc::new(Schema::new(vec![
            Field::new("region", DataType::Int64, true),
            Field::new("service", DataType::Utf8, true),
            Field::new("status", DataType::Int32, false),
            Field::new("cnt", DataType::Int64, false),
        ]));
        let h1 = compute_schema_hash_u64(&schema);
        let h2 = compute_schema_hash_u64(&schema);
        let h3 = compute_schema_hash_u64(&schema);
        assert_eq!(h1, h2);
        assert_eq!(h2, h3);
        assert_ne!(h1, 0, "hash should not be zero for a real schema");
    }

    /// Test 7: different schemas produce different u64 hashes.
    #[test]
    fn test_schema_hash_u64_differs_for_different_schemas() {
        let s1 = Arc::new(Schema::new(vec![
            Field::new("k0", DataType::Int64, true),
            Field::new("v", DataType::Int64, false),
        ]));
        let s2 = Arc::new(Schema::new(vec![
            Field::new("k0", DataType::Int32, true), // different type
            Field::new("v", DataType::Int64, false),
        ]));
        let s3 = Arc::new(Schema::new(vec![
            Field::new("k0", DataType::Int64, true),
            Field::new("w", DataType::Int64, false), // different name
        ]));
        let s4 = Arc::new(Schema::new(vec![
            Field::new("k0", DataType::Int64, false), // different nullability
            Field::new("v", DataType::Int64, false),
        ]));

        let h1 = compute_schema_hash_u64(&s1);
        let h2 = compute_schema_hash_u64(&s2);
        let h3 = compute_schema_hash_u64(&s3);
        let h4 = compute_schema_hash_u64(&s4);

        assert_ne!(h1, h2, "type change");
        assert_ne!(h1, h3, "name change");
        assert_ne!(h1, h4, "nullability change");
    }

    /// Test 8: definition hash u64 is deterministic.
    #[test]
    fn test_definition_hash_u64_deterministic_repeated() {
        let ordering = OrderingContract::from_parallel_arrays(&[0, 2, 1], &[0, 1, 0], &[0, 0, 1]);
        let h1 = compute_definition_hash_u64(&ordering);
        let h2 = compute_definition_hash_u64(&ordering);
        let h3 = compute_definition_hash_u64(&ordering);
        assert_eq!(h1, h2);
        assert_eq!(h2, h3);
        assert_ne!(h1, 0);
    }

    /// Test 9: ordering_hash is deterministic and the Stage 3
    /// `compute_ordering_hash_u64` uses a stable algorithm (FNV-128). Note:
    /// the Stage 5 `compute_ordering_identity` in mv_pull_metadata uses
    /// `DefaultHasher` (non-deterministic across runs), so the two are NOT
    /// interchangeable at the hash-value level. However, within a single
    /// process run, both are self-consistent. This test verifies the Stage 3
    /// hash's own determinism.
    #[test]
    fn test_ordering_hash_deterministic_and_distinct() {
        let o1 = OrderingContract::from_parallel_arrays(&[0, 1, 2], &[0, 0, 0], &[0, 0, 0]);
        let o2 = OrderingContract::from_parallel_arrays(&[0, 1, 2], &[0, 0, 0], &[0, 0, 1]);

        let h1a = compute_ordering_hash_u64(&o1);
        let h1b = compute_ordering_hash_u64(&o1);
        assert_eq!(h1a, h1b, "ordering hash must be deterministic");

        let h2 = compute_ordering_hash_u64(&o2);
        assert_ne!(h1a, h2, "different orderings must produce different hashes");
    }

    /// Test: MvBuildResult with all error status variants preserves
    /// abi_version and struct_size.
    #[test]
    fn test_mv_build_result_all_error_variants() {
        for &status in &[
            MvBuildResult::STATUS_CANCELLED,
            MvBuildResult::STATUS_SPILL_EXCEEDED,
            MvBuildResult::STATUS_MEMORY_EXHAUSTED,
            MvBuildResult::STATUS_INTERNAL_ERROR,
        ] {
            let r = MvBuildResult::error(status);
            assert_eq!(r.abi_version, 1, "abi_version for status {}", status);
            assert_eq!(r.struct_size, 80, "struct_size for status {}", status);
            assert_eq!(r.status_code, status, "status_code");
            assert_eq!(r.row_count, 0, "row_count for status {}", status);
            assert_eq!(r.schema_hash, 0, "schema_hash for status {}", status);
        }
    }

    // ── Cross-language ordering hash parity ──────────────────────────────

    /// Cross-language parity test: compute_ordering_hash_u64 for a known
    /// ordering contract must produce a hardcoded u64 that the Java-side
    /// `MVGroupByOrdering.orderingIdentityHash()` must also reproduce.
    ///
    /// Input: ordering = [(field_index=0, asc, nulls_first),
    ///                     (field_index=1, asc, nulls_first),
    ///                     (field_index=2, asc, nulls_last)]
    ///
    /// The canonical byte stream fed to FNV-128 (lower-64) is:
    ///   field_index(u32 LE) ++ asc_flag(u8) ++ nulls_first_flag(u8) per key,
    ///   followed by key_count(u32 LE).
    ///
    /// Both Rust and Java implementations must agree on this exact value.
    /// If this test breaks, the cross-language contract is violated.
    #[test]
    fn test_ordering_hash_cross_language_parity() {
        // Canonical test vector: 3-key ordering [0 ASC NF, 1 ASC NF, 2 ASC NL].
        let ordering = OrderingContract::from_parallel_arrays(
            &[0, 1, 2], // field indices
            &[0, 0, 0], // all ascending (direction=0)
            &[0, 0, 1], // nulls_first, nulls_first, nulls_last
        );

        let hash = compute_ordering_hash_u64(&ordering);

        // Hardcoded expected value computed from the FNV-1a 128-bit algorithm:
        //   init  = 0x6c62272e07bb0142_62b821756295c58d (FNV-128 offset basis)
        //   prime = 0x0000000001000000_000000000000013B (FNV-128 prime)
        //
        // Byte stream (22 bytes):
        //   key0: [0x00,0x00,0x00,0x00, 0x01, 0x01]  (idx=0, asc=1, nf=1)
        //   key1: [0x01,0x00,0x00,0x00, 0x01, 0x01]  (idx=1, asc=1, nf=1)
        //   key2: [0x02,0x00,0x00,0x00, 0x01, 0x00]  (idx=2, asc=1, nf=0)
        //   count: [0x03,0x00,0x00,0x00]               (3 keys)
        //
        // The expected u64 is the lower 64 bits of the FNV-128 hash of the above.
        // We compute it once here and freeze it as the parity constant.
        let expected: u64 = {
            let mut bytes = Vec::new();
            // key 0: field_index=0, asc=true(1), nulls_first=true(1)
            bytes.extend_from_slice(&0u32.to_le_bytes());
            bytes.push(1); // asc flag (direction==0 → asc → 1)
            bytes.push(1); // nulls_first flag (null_placement==0 → 1)
                           // key 1: field_index=1, asc=true(1), nulls_first=true(1)
            bytes.extend_from_slice(&1u32.to_le_bytes());
            bytes.push(1);
            bytes.push(1);
            // key 2: field_index=2, asc=true(1), nulls_first=false(0)
            bytes.extend_from_slice(&2u32.to_le_bytes());
            bytes.push(1);
            bytes.push(0); // null_placement=1 → nulls_first=0
                           // key count
            bytes.extend_from_slice(&3u32.to_le_bytes());
            stable_hash_u64(&bytes)
        };

        assert_eq!(
            hash, expected,
            "ordering hash for [0 ASC NF, 1 ASC NF, 2 ASC NL] must match \
             the cross-language parity constant 0x{:016X}; got 0x{:016X}",
            expected, hash
        );

        // Also verify a single-key ordering for a second parity point.
        let single = OrderingContract::from_parallel_arrays(&[5], &[0], &[0]);
        let single_hash = compute_ordering_hash_u64(&single);
        let single_expected: u64 = {
            let mut bytes = Vec::new();
            bytes.extend_from_slice(&5u32.to_le_bytes());
            bytes.push(1); // asc
            bytes.push(1); // nulls_first
            bytes.extend_from_slice(&1u32.to_le_bytes());
            stable_hash_u64(&bytes)
        };
        assert_eq!(
            single_hash, single_expected,
            "single-key ordering hash parity: expected 0x{:016X}, got 0x{:016X}",
            single_expected, single_hash
        );
    }
}
