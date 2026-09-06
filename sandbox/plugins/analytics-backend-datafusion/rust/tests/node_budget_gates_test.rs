/*
 * Defect #28a: resident-vs-threshold protection gates in DynamicLimitPool are
 * based on the NODE-level native budget (node.native_memory.limit), never a
 * single pool's limit. jemalloc resident is a whole-process measurement, so a
 * pool-limit base permanently latched MV builds shut (sort-completion
 * starvation: spillable consumers exempted through the 85% gate, then the
 * sort's mandatory UNSPILLABLE ExternalSorterMerge reservation rejected) once
 * unrelated native baseline crossed the fraction — observed as a poller
 * livelock at total idle on the 100M bench.
 *
 * These tests live in their own integration binary (separate process) because
 * they mutate the process-global node limit: inside the lib test binary that
 * gates unrelated parallel tests (spill/e2e) mid-flight.
 */

use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool};
use opensearch_datafusion::memory::DynamicLimitPool;
use std::sync::Arc;

// Production links through the umbrella crate whose global allocator is
// jemalloc; this standalone binary must match or resident reads ~0 and every
// gate path is vacuously skipped.
#[global_allocator]
static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

/// Warms `cached_resident_bytes` (cold cache returns 0 for the first ~100ms)
/// and pins a 32MB touched allocation so resident is deterministically above
/// every threshold used here. PANICS instead of skipping — silent skips are
/// how the pre-#28a gate tests never actually ran.
fn warm_resident_above(threshold: i64) -> Vec<u8> {
    let ballast = vec![7u8; 32 * 1024 * 1024];
    std::hint::black_box(&ballast);
    for _ in 0..40 {
        let r = opensearch_datafusion::memory_guard::cached_resident_bytes();
        if r >= threshold {
            return ballast;
        }
        std::thread::sleep(std::time::Duration::from_millis(120));
    }
    panic!("resident cache failed to warm above {threshold} — jemalloc must be the global allocator");
}

/// Serializes the tests in this binary — they all mutate the process-global
/// node limit. Poisoning ignored so one failure cannot cascade.
static NODE_LIMIT_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn new_pool(limit: usize) -> Arc<dyn MemoryPool> {
    let (pool, _handle) = DynamicLimitPool::new(limit);
    Arc::new(pool)
}

/// Defect #28a falsification: node budget UNSET → resident gates disabled — an
/// UNSPILLABLE consumer's grow succeeds on pool reservation accounting alone,
/// even though process resident dwarfs 85% of the pool limit. Pre-fix
/// (pool-limit base) this was rejected: the exact livelock mechanism.
#[test]
fn unspillable_grow_allowed_when_node_budget_unset() {
    let _guard = NODE_LIMIT_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    native_bridge_common::allocator::native_set_node_memory_limit(0);
    let pool = new_pool(20 * 1024 * 1024);
    let _ballast = warm_resident_above((20.0 * 1024.0 * 1024.0 * 0.85) as i64);

    let consumer = MemoryConsumer::new("unspillable_merge_back"); // can_spill = false
    let mut reservation = consumer.register(&pool);
    let result = reservation.try_grow(10 * 1024 * 1024); // the sort's 10MB merge-back shape
    assert!(
        result.is_ok(),
        "unset node budget must disable resident gates: unspillable grow within the pool \
         limit must succeed on reservation accounting alone, got {:?}",
        result.err()
    );
}

/// Companion: node budget SET low → the same unspillable grow is rejected.
/// The gate still protects the node when properly configured — and the test
/// order inside this binary doesn't matter because each test sets the global
/// explicitly and restores it.
#[test]
fn unspillable_grow_rejected_when_node_budget_set_low() {
    let _guard = NODE_LIMIT_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    let pool = new_pool(20 * 1024 * 1024);
    let _ballast = warm_resident_above((20.0 * 1024.0 * 1024.0 * 0.85) as i64);
    native_bridge_common::allocator::native_set_node_memory_limit(20 * 1024 * 1024);

    let consumer = MemoryConsumer::new("unspillable_merge_back"); // can_spill = false
    let mut reservation = consumer.register(&pool);
    let result = reservation.try_grow(10 * 1024 * 1024);
    native_bridge_common::allocator::native_set_node_memory_limit(0);
    assert!(
        result.is_err(),
        "with resident above 85% of the configured node budget, an unspillable grow must be rejected"
    );
}

/// Critical gate (95%): any grow rejected when resident exceeds critical of
/// the configured node budget. Migrated from the lib tests where a pool-limit
/// base (and no real allocator) made it vacuous.
#[test]
fn any_grow_rejected_when_resident_exceeds_critical_of_node_budget() {
    let _guard = NODE_LIMIT_LOCK.lock().unwrap_or_else(|p| p.into_inner());
    let pool = new_pool(20 * 1024 * 1024);
    let _ballast = warm_resident_above((20.0 * 1024.0 * 1024.0 * 0.95) as i64);
    native_bridge_common::allocator::native_set_node_memory_limit(20 * 1024 * 1024);

    let consumer = MemoryConsumer::new("hard_guard_test");
    let mut reservation = consumer.register(&pool);
    let result = reservation.try_grow(1024);
    native_bridge_common::allocator::native_set_node_memory_limit(0);
    assert!(
        result.is_err(),
        "try_grow must fail when resident exceeds the critical threshold (95% of the node budget)"
    );
}
