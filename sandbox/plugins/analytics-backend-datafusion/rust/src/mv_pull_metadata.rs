/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Stage 5: Rust FFI metadata for MV pull-unified operations.
//!
//! Provides:
//!   1. **`PullArtifactMetadata`** — extended metadata struct returned from
//!      merge/pull completion, carrying row count, schema/definition hashes,
//!      ordering identity, spill telemetry, memory peak, fan-in, and output
//!      batch count.
//!   2. **Spill budget enforcement** — checks `_spill_byte_budget` and
//!      `_spill_file_budget` args and returns an error when exceeded.
//!   3. **Dynamic memory reservation/release** — uses DataFusion
//!      `MemoryConsumer` for reservation on all code paths.
//!   4. **Pull round bounds** — bytes_processed, ops_count,
//!      estimated_cardinality checks.
//!   5. **Native/RSS admission gating** — checks RSS before starting a new
//!      pull round.

use std::sync::atomic::{AtomicI64, Ordering as AtomicOrdering};
use std::sync::Arc;
use std::time::Instant;

use crate::memory_guard::cached_resident_bytes;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};

// ── PullArtifactMetadata ────────────────────────────────────────────────

/// Extended metadata returned from a merge/pull completion.
///
/// All fields are scalar (FFI-friendly). The Java caller reads each field
/// through individual accessor FFI functions keyed by the opaque
/// `PullArtifactMetadata` pointer.
#[derive(Debug, Clone)]
#[repr(C)]
pub struct PullArtifactMetadata {
    /// Number of rows written to the IPC artifact.
    pub row_count: i64,
    /// Deterministic hex digest of the Arrow schema (field names, types,
    /// nullability). Used for cross-file validation.
    pub schema_hash: u64,
    /// Deterministic hash of the fold/definition SQL. Two definitions are
    /// merge-compatible only when their definition hashes match.
    pub definition_hash: u64,
    /// Canonical string identity of the ordering contract. Encodes column
    /// indices, directions, and null placements. Used to verify
    /// merge-compatibility across state files.
    pub ordering_identity: u64,
    /// Total bytes spilled to disk during this merge/pull.
    pub spill_bytes: i64,
    /// Number of spill files created during this merge/pull.
    pub spill_files: i32,
    /// Peak resident set size (RSS) in bytes observed during this merge/pull.
    pub peak_rss: i64,
    /// Number of input state files merged (fan-in).
    pub fan_in: i32,
    /// Number of Arrow IPC batches written to the output artifact.
    pub output_batch_count: i32,
    /// Bytes currently reserved by native (Rust) DataFusion memory consumers
    /// for this merge/pull operation. Exposed for circuit-breaker accounting.
    pub native_reservations_bytes: i64,
    /// Estimated bytes retained by Arrow buffers (RecordBatch heap) after
    /// the merge completes. Helps Java-side GC tuning and breaker sizing.
    pub retained_estimate_bytes: i64,
    /// Opaque tag identifying which circuit breaker bucket this operation
    /// should be attributed to. 0 = default/unattributed.
    pub breaker_attribution: i64,
}

impl PullArtifactMetadata {
    /// Build metadata from individual fields.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        row_count: i64,
        schema_hash: u64,
        definition_hash: u64,
        ordering_identity: u64,
        spill_bytes: i64,
        spill_files: i32,
        peak_rss: i64,
        fan_in: i32,
        output_batch_count: i32,
        native_reservations_bytes: i64,
        retained_estimate_bytes: i64,
        breaker_attribution: i64,
    ) -> Self {
        Self {
            row_count,
            schema_hash,
            definition_hash,
            ordering_identity,
            spill_bytes,
            spill_files,
            peak_rss,
            fan_in,
            output_batch_count,
            native_reservations_bytes,
            retained_estimate_bytes,
            breaker_attribution,
        }
    }

    /// Zero-valued metadata (sentinel for error/empty paths).
    pub fn empty() -> Self {
        Self {
            row_count: 0,
            schema_hash: 0,
            definition_hash: 0,
            ordering_identity: 0,
            spill_bytes: 0,
            spill_files: 0,
            peak_rss: 0,
            fan_in: 0,
            output_batch_count: 0,
            native_reservations_bytes: 0,
            retained_estimate_bytes: 0,
            breaker_attribution: 0,
        }
    }
}

// ── Spill budget enforcement ────────────────────────────────────────────

/// Error returned when spill limits are exceeded.
#[derive(Debug, Clone)]
pub struct SpillBudgetExceeded {
    pub kind: SpillLimitKind,
    pub limit: i64,
    pub actual: i64,
}

/// Which spill limit was exceeded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum SpillLimitKind {
    Bytes,
    Files,
}

impl std::fmt::Display for SpillBudgetExceeded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self.kind {
            SpillLimitKind::Bytes => write!(
                f,
                "spill byte budget exceeded: limit={}, actual={}",
                self.limit, self.actual
            ),
            SpillLimitKind::Files => write!(
                f,
                "spill file budget exceeded: limit={}, actual={}",
                self.limit, self.actual
            ),
        }
    }
}

/// Enforces spill budget limits. Returns `Ok(())` if within budget,
/// `Err(SpillBudgetExceeded)` if either the byte budget or file budget
/// is exceeded.
///
/// A budget value ≤ 0 means "unlimited" for that dimension.
pub fn enforce_spill_budget(
    spill_byte_budget: i64,
    spill_file_budget: i32,
    current_spill_bytes: i64,
    current_spill_files: i32,
) -> Result<(), SpillBudgetExceeded> {
    if spill_byte_budget > 0 && current_spill_bytes > spill_byte_budget {
        return Err(SpillBudgetExceeded {
            kind: SpillLimitKind::Bytes,
            limit: spill_byte_budget,
            actual: current_spill_bytes,
        });
    }
    if spill_file_budget > 0 && current_spill_files > spill_file_budget {
        return Err(SpillBudgetExceeded {
            kind: SpillLimitKind::Files,
            limit: spill_file_budget as i64,
            actual: current_spill_files as i64,
        });
    }
    Ok(())
}

// ── Dynamic memory reservation ──────────────────────────────────────────

/// A scoped memory reservation for MV pull operations. Wraps a DataFusion
/// `MemoryReservation` and ensures it is released on drop.
///
/// Usage:
/// ```ignore
/// let guard = PullMemoryGuard::try_reserve(pool, "mv_merge_round_3", bytes)?;
/// // ... do work ...
/// // guard is released on drop, or call guard.release() explicitly
/// ```
pub struct PullMemoryGuard {
    reservation: MemoryReservation,
    _label: String,
}

impl PullMemoryGuard {
    /// Try to reserve `bytes` from the given `MemoryPool`. Returns an error
    /// string if the pool cannot accommodate the reservation.
    pub fn try_reserve(
        pool: &Arc<dyn MemoryPool>,
        label: &str,
        bytes: usize,
    ) -> Result<Self, String> {
        let consumer = MemoryConsumer::new(label);
        let reservation = consumer.register(pool);
        reservation.try_grow(bytes).map_err(|e| {
            format!("PullMemoryGuard({label}): reservation of {bytes} bytes failed: {e}")
        })?;
        Ok(Self {
            reservation,
            _label: label.to_string(),
        })
    }

    /// Release the reservation explicitly. Also happens on drop.
    pub fn release(self) {
        let held = self.reservation.size();
        if held > 0 {
            self.reservation.shrink(held);
        }
    }

    /// How many bytes are currently reserved.
    pub fn size(&self) -> usize {
        self.reservation.size()
    }
}

impl Drop for PullMemoryGuard {
    fn drop(&mut self) {
        let held = self.reservation.size();
        if held > 0 {
            self.reservation.shrink(held);
        }
    }
}

// ── Pull round bounds ───────────────────────────────────────────────────

/// Configuration for pull round bounds. Each bound is optional; a value of 0
/// or negative means "no limit" for that dimension.
#[derive(Debug, Clone)]
pub struct PullRoundBounds {
    /// Maximum bytes that may be processed in a single pull round.
    pub max_bytes_processed: i64,
    /// Maximum number of operations (fold/merge steps) in a single pull round.
    pub max_ops_count: i64,
    /// Maximum estimated cardinality (distinct groups) for the output.
    pub max_estimated_cardinality: i64,
}

impl PullRoundBounds {
    /// Create bounds from raw FFI values. Non-positive values disable that bound.
    pub fn new(
        max_bytes_processed: i64,
        max_ops_count: i64,
        max_estimated_cardinality: i64,
    ) -> Self {
        Self {
            max_bytes_processed,
            max_ops_count,
            max_estimated_cardinality,
        }
    }

    /// Returns `true` if all bounds are disabled (all non-positive).
    pub fn is_unbounded(&self) -> bool {
        self.max_bytes_processed <= 0
            && self.max_ops_count <= 0
            && self.max_estimated_cardinality <= 0
    }
}

/// Error returned when a pull round bound is exceeded.
#[derive(Debug, Clone)]
pub struct PullRoundExceeded {
    pub kind: PullBoundKind,
    pub limit: i64,
    pub actual: i64,
}

/// Which pull round bound was exceeded.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PullBoundKind {
    BytesProcessed,
    OpsCount,
    EstimatedCardinality,
}

impl std::fmt::Display for PullRoundExceeded {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let label = match self.kind {
            PullBoundKind::BytesProcessed => "bytes_processed",
            PullBoundKind::OpsCount => "ops_count",
            PullBoundKind::EstimatedCardinality => "estimated_cardinality",
        };
        write!(
            f,
            "pull round bound exceeded: {label} limit={}, actual={}",
            self.limit, self.actual
        )
    }
}

/// Check whether the current pull round counters exceed any configured bounds.
/// Returns `Ok(())` if within all bounds, `Err(PullRoundExceeded)` on the
/// first bound that is exceeded.
pub fn check_pull_round_bounds(
    bounds: &PullRoundBounds,
    bytes_processed: i64,
    ops_count: i64,
    estimated_cardinality: i64,
) -> Result<(), PullRoundExceeded> {
    if bounds.max_bytes_processed > 0 && bytes_processed > bounds.max_bytes_processed {
        return Err(PullRoundExceeded {
            kind: PullBoundKind::BytesProcessed,
            limit: bounds.max_bytes_processed,
            actual: bytes_processed,
        });
    }
    if bounds.max_ops_count > 0 && ops_count > bounds.max_ops_count {
        return Err(PullRoundExceeded {
            kind: PullBoundKind::OpsCount,
            limit: bounds.max_ops_count,
            actual: ops_count,
        });
    }
    if bounds.max_estimated_cardinality > 0
        && estimated_cardinality > bounds.max_estimated_cardinality
    {
        return Err(PullRoundExceeded {
            kind: PullBoundKind::EstimatedCardinality,
            limit: bounds.max_estimated_cardinality,
            actual: estimated_cardinality,
        });
    }
    Ok(())
}

// ── Native/RSS admission gating ─────────────────────────────────────────

/// RSS admission thresholds for pull round gating. The gate checks the
/// current jemalloc RSS against a fraction of the pool limit. If RSS is
/// above the threshold, the pull round is rejected (backpressure).
#[derive(Debug, Clone)]
pub struct AdmissionGate {
    /// Pool limit in bytes (the DataFusion memory pool ceiling).
    pub pool_limit: i64,
    /// Threshold as a fraction × 1000 (e.g. 850 = 85%). If RSS exceeds
    /// `pool_limit * threshold_x1000 / 1000`, admission is denied.
    pub threshold_x1000: u64,
}

impl AdmissionGate {
    pub fn new(pool_limit: i64, threshold_x1000: u64) -> Self {
        Self {
            pool_limit,
            threshold_x1000,
        }
    }

    /// Default admission gate: 85% of pool limit.
    pub fn default_for_pool(pool_limit: i64) -> Self {
        Self {
            pool_limit,
            threshold_x1000: 850,
        }
    }

    /// Check whether the current RSS is below the admission threshold.
    /// Returns `Ok(())` if the node can accept a new pull round, or
    /// `Err(String)` with a diagnostic message if RSS is too high.
    pub fn check_admission(&self) -> Result<(), String> {
        if self.pool_limit <= 0 {
            // No pool limit configured — admission is always granted.
            return Ok(());
        }
        let rss = cached_resident_bytes();
        let threshold = (self.pool_limit as u64 * self.threshold_x1000 / 1000) as i64;
        if rss >= threshold {
            Err(format!(
                "RSS admission denied: rss={rss} >= threshold={threshold} \
                 (pool_limit={}, gate={}‰)",
                self.pool_limit, self.threshold_x1000
            ))
        } else {
            Ok(())
        }
    }
}

// ── Merge-with-metadata orchestrator ────────────────────────────────────

/// Atomic spill counters tracked during a merge/pull operation.
pub struct SpillCounters {
    pub bytes: AtomicI64,
    pub files: AtomicI64,
}

impl Default for SpillCounters {
    fn default() -> Self {
        Self::new()
    }
}

impl SpillCounters {
    pub fn new() -> Self {
        Self {
            bytes: AtomicI64::new(0),
            files: AtomicI64::new(0),
        }
    }

    pub fn add_bytes(&self, n: i64) {
        self.bytes.fetch_add(n, AtomicOrdering::Relaxed);
    }

    pub fn add_files(&self, n: i32) {
        self.files.fetch_add(n as i64, AtomicOrdering::Relaxed);
    }

    pub fn total_bytes(&self) -> i64 {
        self.bytes.load(AtomicOrdering::Relaxed)
    }

    pub fn total_files(&self) -> i32 {
        self.files.load(AtomicOrdering::Relaxed) as i32
    }
}

// ── Internal helpers ────────────────────────────────────────────────────

/// Compute a deterministic hash of the ordering contract for identity checks.
fn compute_ordering_identity(indices: &[usize], asc: &[bool], nulls_first: &[bool]) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    for ((&idx, &a), &nf) in indices.iter().zip(asc.iter()).zip(nulls_first.iter()) {
        idx.hash(&mut hasher);
        a.hash(&mut hasher);
        nf.hash(&mut hasher);
    }
    indices.len().hash(&mut hasher);
    hasher.finish()
}

/// Read schema hash, definition hash (fold_ops hash), and batch count from
/// a Parquet output file. Uses the Parquet reader to inspect the file metadata.
fn read_output_hashes(output_file: &str) -> Result<(u64, u64, i32), String> {
    // Legacy Arrow IPC guard: fail closed with rebuild-required error.
    if output_file.ends_with(".mv.arrow") {
        return Err(format!(
            "read_output_hashes: legacy Arrow IPC state file '{}' is no longer supported; \
             rebuild the materialized view to generate Parquet state files",
            output_file
        ));
    }

    let file = std::fs::File::open(output_file)
        .map_err(|e| format!("read_output_hashes: open {output_file}: {e}"))?;
    let builder =
        parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| format!("read_output_hashes: reader {output_file}: {e}"))?;

    let schema = builder.schema().clone();
    let schema_hash = compute_schema_hash(&schema);

    // Definition hash: hash the schema field count + data types as a proxy.
    // The true definition hash comes from the fold SQL on the Java side;
    // here we produce a structural hash for the FFI contract.
    let definition_hash = {
        use std::hash::{Hash, Hasher};
        let mut hasher = std::collections::hash_map::DefaultHasher::new();
        for field in schema.fields() {
            format!("{:?}", field.data_type()).hash(&mut hasher);
            field.is_nullable().hash(&mut hasher);
        }
        hasher.finish()
    };

    // Count row groups in the Parquet file (closest equivalent to IPC batch count).
    let num_row_groups = builder.metadata().num_row_groups() as i32;

    Ok((schema_hash, definition_hash, num_row_groups))
}

// ── Tests ───────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use datafusion::execution::memory_pool::GreedyMemoryPool;

    #[test]
    fn test_spill_budget_within_limits() {
        assert!(enforce_spill_budget(100, 10, 50, 5).is_ok());
    }

    #[test]
    fn test_spill_budget_byte_exceeded() {
        let err = enforce_spill_budget(100, 10, 150, 5).unwrap_err();
        assert_eq!(err.kind, SpillLimitKind::Bytes);
        assert_eq!(err.limit, 100);
        assert_eq!(err.actual, 150);
    }

    #[test]
    fn test_spill_budget_file_exceeded() {
        let err = enforce_spill_budget(100, 10, 50, 15).unwrap_err();
        assert_eq!(err.kind, SpillLimitKind::Files);
        assert_eq!(err.limit, 10);
        assert_eq!(err.actual, 15);
    }

    #[test]
    fn test_spill_budget_unlimited() {
        // Non-positive budgets mean unlimited.
        assert!(enforce_spill_budget(0, 0, 999_999, 999).is_ok());
        assert!(enforce_spill_budget(-1, -1, 999_999, 999).is_ok());
    }

    #[test]
    fn test_pull_round_bounds_within_limits() {
        let bounds = PullRoundBounds::new(1000, 50, 500);
        assert!(check_pull_round_bounds(&bounds, 500, 25, 200).is_ok());
    }

    #[test]
    fn test_pull_round_bounds_bytes_exceeded() {
        let bounds = PullRoundBounds::new(1000, 50, 500);
        let err = check_pull_round_bounds(&bounds, 1500, 25, 200).unwrap_err();
        assert_eq!(err.kind, PullBoundKind::BytesProcessed);
    }

    #[test]
    fn test_pull_round_bounds_ops_exceeded() {
        let bounds = PullRoundBounds::new(1000, 50, 500);
        let err = check_pull_round_bounds(&bounds, 500, 75, 200).unwrap_err();
        assert_eq!(err.kind, PullBoundKind::OpsCount);
    }

    #[test]
    fn test_pull_round_bounds_cardinality_exceeded() {
        let bounds = PullRoundBounds::new(1000, 50, 500);
        let err = check_pull_round_bounds(&bounds, 500, 25, 700).unwrap_err();
        assert_eq!(err.kind, PullBoundKind::EstimatedCardinality);
    }

    #[test]
    fn test_pull_round_bounds_unbounded() {
        let bounds = PullRoundBounds::new(0, 0, 0);
        assert!(bounds.is_unbounded());
        assert!(check_pull_round_bounds(&bounds, i64::MAX, i64::MAX, i64::MAX).is_ok());
    }

    #[test]
    fn test_memory_guard_reserve_and_drop() {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(10_000_000));
        let guard = PullMemoryGuard::try_reserve(&pool, "test_guard", 4096).unwrap();
        assert_eq!(guard.size(), 4096);
        drop(guard);
        // After drop, reservation should be released. New reservation of the
        // full pool should succeed.
        let guard2 = PullMemoryGuard::try_reserve(&pool, "test_full", 9_000_000).unwrap();
        assert_eq!(guard2.size(), 9_000_000);
    }

    #[test]
    fn test_memory_guard_explicit_release() {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(10_000_000));
        let guard = PullMemoryGuard::try_reserve(&pool, "test_release", 5_000_000).unwrap();
        guard.release();
        // Should be able to allocate the full pool now.
        let _g2 = PullMemoryGuard::try_reserve(&pool, "after_release", 9_000_000).unwrap();
    }

    #[test]
    fn test_memory_guard_oom() {
        let pool: Arc<dyn MemoryPool> = Arc::new(GreedyMemoryPool::new(1024));
        let result = PullMemoryGuard::try_reserve(&pool, "test_oom", 2048);
        assert!(result.is_err());
        let err = match result {
            Err(e) => e,
            Ok(_) => panic!("expected error"),
        };
        assert!(err.contains("reservation of 2048 bytes failed"));
    }

    #[test]
    fn test_admission_gate_no_pool_limit() {
        let gate = AdmissionGate::new(0, 850);
        assert!(gate.check_admission().is_ok());
    }

    #[test]
    fn test_ordering_identity_deterministic() {
        let indices = vec![0, 2, 1];
        let asc = vec![true, false, true];
        let nulls_first = vec![true, true, false];
        let h1 = compute_ordering_identity(&indices, &asc, &nulls_first);
        let h2 = compute_ordering_identity(&indices, &asc, &nulls_first);
        assert_eq!(h1, h2);
    }

    #[test]
    fn test_ordering_identity_different_orders() {
        let h1 = compute_ordering_identity(&[0, 1], &[true, true], &[true, true]);
        let h2 = compute_ordering_identity(&[1, 0], &[true, true], &[true, true]);
        assert_ne!(h1, h2);
    }

    #[test]
    fn test_artifact_metadata_empty() {
        let m = PullArtifactMetadata::empty();
        assert_eq!(m.row_count, 0);
        assert_eq!(m.fan_in, 0);
        assert_eq!(m.output_batch_count, 0);
        assert_eq!(m.native_reservations_bytes, 0);
        assert_eq!(m.retained_estimate_bytes, 0);
        assert_eq!(m.breaker_attribution, 0);
    }

    #[test]
    fn test_spill_counters_atomic() {
        let c = SpillCounters::new();
        c.add_bytes(100);
        c.add_bytes(200);
        c.add_files(1);
        c.add_files(2);
        assert_eq!(c.total_bytes(), 300);
        assert_eq!(c.total_files(), 3);
    }
}

/// Schema hash for pull-artifact telemetry (relocated from the deleted
/// row-at-a-time merge engine; schema-shape fingerprint only).
pub fn compute_schema_hash(schema: &arrow_schema::Schema) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    for field in schema.fields() {
        field.name().hash(&mut hasher);
        format!("{:?}", field.data_type()).hash(&mut hasher);
        field.is_nullable().hash(&mut hasher);
    }
    hasher.finish()
}

// ── Generic parquet-file utilities (relocated from the deleted row-at-a-time merge engine) ──

/// Reads the physical field names of a Parquet MV state file from its footer
/// schema, in physical order. This is the GROUND TRUTH the merge ordering
/// identity is derived from (Java asks via `df_mv_state_field_names` instead
/// of shipping its own Parquet footer parser — one reader stack).
///
/// Footer-only: no row groups are read.
pub fn state_field_names(file_path: &str) -> Result<Vec<String>, String> {
    // Legacy Arrow IPC guard: fail closed with rebuild-required error.
    if file_path.ends_with(".mv.arrow") {
        return Err(format!(
            "state_field_names: legacy Arrow IPC state file '{}' is no longer supported; \
             rebuild the materialized view to generate Parquet state files",
            file_path
        ));
    }
    let file = std::fs::File::open(file_path)
        .map_err(|e| format!("state_field_names: open {file_path}: {e}"))?;
    let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| format!("state_field_names: read {file_path}: {e}"))?;
    Ok(builder
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect())
}
