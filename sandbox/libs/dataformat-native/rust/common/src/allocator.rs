/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! jemalloc allocator interface: memory stats and runtime tuning.
//!
//! FFI convention (same as all other native bridge functions):
//!   - `>= 0` → success (the stat value in bytes, or 0 for setters)
//!   - `< 0`  → error pointer. Negate and pass to `native_error_message` / `native_error_free`.

use crate::error::{ffm_wrap, into_error_ptr};
use crate::log_info;
use std::sync::OnceLock;
use tikv_jemalloc_ctl::{epoch, epoch_mib, stats, stats::allocated_mib, stats::resident_mib};

struct StatsMib {
    epoch: epoch_mib,
    allocated: allocated_mib,
    resident: resident_mib,
}

static MIB: OnceLock<StatsMib> = OnceLock::new();

fn mib() -> &'static StatsMib {
    MIB.get_or_init(|| StatsMib {
        epoch: epoch::mib().unwrap(),
        allocated: stats::allocated::mib().unwrap(),
        resident: stats::resident::mib().unwrap(),
    })
}

/// Advances the jemalloc epoch and reads both stats atomically.
fn refresh_stats() -> Result<(i64, i64), String> {
    let m = mib();
    m.epoch
        .advance()
        .map_err(|e| format!("jemalloc epoch advance failed: {}", e))?;
    let alloc = m
        .allocated
        .read()
        .map_err(|e| format!("jemalloc allocated read failed: {}", e))? as i64;
    let res = m
        .resident
        .read()
        .map_err(|e| format!("jemalloc resident read failed: {}", e))? as i64;
    Ok((alloc, res))
}

/// Returns current jemalloc allocated bytes (live malloc'd objects).
/// Useful for application-level memory accounting and DataFusion memory pool budgeting.
/// On error: returns negative error pointer (use `native_error_message` to read).
///
/// TODO: integrate with node/stats
pub fn allocated_bytes() -> i64 {
    match refresh_stats() {
        Ok((alloc, _)) => alloc,
        Err(msg) => into_error_ptr(msg),
    }
}

/// Returns current jemalloc resident bytes (physical RAM used by native layer only).
/// Excludes JVM heap, metaspace, and other non-jemalloc allocations.
/// On error: returns negative error pointer (use `native_error_message` to read).
///
/// TODO: integrate with node/stats
pub fn resident_bytes() -> i64 {
    match refresh_stats() {
        Ok((_, res)) => res,
        Err(msg) => into_error_ptr(msg),
    }
}

/// mallctl name addressing ALL arenas (index 4096 = `MALLCTL_ARENAS_ALL`).
const ARENA_PURGE_ALL: &[u8] = b"arena.4096.purge\0";

/// Forces jemalloc to purge dirty/muzzy pages from ALL arenas back to the OS,
/// then re-reads resident bytes.
///
/// Decay-based purging only ticks on allocator activity in the owning arena:
/// pages freed by finished work (e.g. a merge storm on a rayon pool) stay
/// resident indefinitely once those threads go idle. Any gate comparing
/// jemalloc resident against a limit then latches shut permanently — even at
/// total process idle. Callers about to fail an allocation on a
/// resident-based threshold must purge and re-measure first (rate-limited),
/// mirroring the real-memory circuit breaker's collect-then-re-measure idiom
/// (G1OverLimitStrategy: GC, re-read, only then trip).
///
/// Live allocations are untouched — only freed-but-retained pages are
/// returned, so a legitimately loaded node still reads high after a purge.
///
/// Returns fresh resident bytes after the purge.
pub fn purge_all_arenas_and_refresh() -> Result<i64, String> {
    // Void mallctl command: jemalloc requires ALL pointers null for purge
    // (READONLY() + WRITEONLY() checks), so this bypasses the typed helpers.
    let rc = unsafe {
        tikv_jemalloc_sys::mallctl(
            ARENA_PURGE_ALL.as_ptr() as *const std::os::raw::c_char,
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            std::ptr::null_mut(),
            0,
        )
    };
    if rc != 0 {
        return Err(format!("jemalloc arena purge failed: rc={rc}"));
    }
    PURGE_COUNT.fetch_add(1, Ordering::Relaxed);
    refresh_stats().map(|(_, res)| res)
}

/// FFI: Returns current jemalloc allocated bytes, or negative error pointer.
#[no_mangle]
pub extern "C" fn native_jemalloc_allocated_bytes() -> i64 {
    ffm_wrap("native_jemalloc_allocated_bytes", || {
        refresh_stats().map(|(alloc, _)| alloc)
    })
}

/// FFI: Returns current jemalloc resident bytes, or negative error pointer.
#[no_mangle]
pub extern "C" fn native_jemalloc_resident_bytes() -> i64 {
    ffm_wrap("native_jemalloc_resident_bytes", || {
        refresh_stats().map(|(_, res)| res)
    })
}

/// FFI: Sets dirty_decay_ms for all arenas at runtime. Returns 0 on success, negative error pointer on failure.
/// Called from Java when the cluster setting `native.jemalloc.dirty_decay_ms` changes.
#[no_mangle]
pub extern "C" fn native_jemalloc_set_dirty_decay_ms(ms: i64) -> i64 {
    ffm_wrap("native_jemalloc_set_dirty_decay_ms", || {
        set_all_arenas(b"dirty_decay_ms\0", ms)
    })
}

/// FFI: Sets muzzy_decay_ms for all arenas at runtime. Returns 0 on success, negative error pointer on failure.
/// Called from Java when the cluster setting `native.jemalloc.muzzy_decay_ms` changes.
#[no_mangle]
pub extern "C" fn native_jemalloc_set_muzzy_decay_ms(ms: i64) -> i64 {
    ffm_wrap("native_jemalloc_set_muzzy_decay_ms", || {
        set_all_arenas(b"muzzy_decay_ms\0", ms)
    })
}

/// Applies a setting to all existing jemalloc arenas.
/// Skips arenas that are not available (destroyed or internal).
fn set_all_arenas(suffix: &[u8], ms: i64) -> Result<i64, String> {
    let narenas: u32 = unsafe { tikv_jemalloc_ctl::raw::read(b"arenas.narenas\0") }
        .map_err(|e| format!("failed to read arenas.narenas: {}", e))?;
    let suffix_str = std::str::from_utf8(&suffix[..suffix.len() - 1]).unwrap();
    let mut any_success = false;
    for i in 0..narenas {
        let key = format!("arena.{}.{}\0", i, suffix_str);
        if unsafe { tikv_jemalloc_ctl::raw::write(key.as_bytes(), ms as isize) }.is_ok() {
            any_success = true;
        }
    }
    if any_success {
        Ok(0)
    } else {
        Err(format!("failed to set {} on any arena", suffix_str))
    }
}

// ── Background purge thread ─────────────────────────────────────────────────
//
// A dedicated OS thread that periodically checks jemalloc resident bytes against
// a configurable threshold and purges all arenas if exceeded. Runs entirely in
// Rust — no FFI round-trip per check. Java pushes threshold/interval updates via
// FFI setters.

use std::sync::atomic::{AtomicBool, AtomicI64, AtomicU64, Ordering};
use std::time::Duration;

static PURGE_THRESHOLD_BYTES: AtomicI64 = AtomicI64::new(i64::MAX);
static PURGE_INTERVAL_MS: AtomicU64 = AtomicU64::new(5000);
static PURGE_COUNT: AtomicU64 = AtomicU64::new(0);
static PURGE_STOP: AtomicBool = AtomicBool::new(false);
static PURGE_THREAD: OnceLock<std::thread::Thread> = OnceLock::new();

/// Node-level native memory budget (`node.native_memory.limit`), pushed from
/// Java alongside the purge threshold. 0 = unset.
///
/// This is the ONLY correct base for resident-vs-threshold protection gates:
/// jemalloc resident is a whole-process measurement (source-index parquet
/// machinery, read caches, every native module), so comparing it against a
/// fraction of one component's pool limit latches that component shut as soon
/// as everyone else's legitimate baseline crosses the fraction — the
/// OpenSearch parent real-memory breaker idiom is total-vs-total, never
/// total-vs-component.
static NODE_NATIVE_LIMIT_BYTES: AtomicI64 = AtomicI64::new(0);

/// Returns the node-level native memory budget in bytes, or 0 when unset.
pub fn node_native_limit_bytes() -> i64 {
    NODE_NATIVE_LIMIT_BYTES.load(Ordering::Relaxed)
}

/// FFI: Pushes the raw `node.native_memory.limit` value (bytes; 0 = unset).
/// Called from Java at startup and on dynamic setting updates, alongside the
/// derived purge-threshold push. Negative values are treated as unset.
#[no_mangle]
pub extern "C" fn native_set_node_memory_limit(limit_bytes: i64) -> i64 {
    NODE_NATIVE_LIMIT_BYTES.store(limit_bytes.max(0), Ordering::Relaxed);
    0
}

fn start_purge_thread() -> &'static std::thread::Thread {
    PURGE_THREAD.get_or_init(|| {
        let handle = std::thread::Builder::new()
            .name("jemalloc-purge".into())
            .spawn(purge_thread_loop)
            .expect("failed to spawn jemalloc-purge thread");
        handle.thread().clone()
    })
}

fn purge_thread_loop() {
    loop {
        if PURGE_STOP.load(Ordering::Relaxed) {
            return;
        }
        let interval_ms = PURGE_INTERVAL_MS.load(Ordering::Relaxed);
        if interval_ms == 0 {
            std::thread::park();
            continue;
        }
        std::thread::park_timeout(Duration::from_millis(interval_ms));

        let threshold = PURGE_THRESHOLD_BYTES.load(Ordering::Relaxed);
        let resident = match refresh_stats() {
            Ok((_, res)) => res,
            Err(_) => continue,
        };

        if resident <= threshold {
            continue;
        }

        log_info!(
            "jemalloc purge starting: resident={} MB, threshold={} MB",
            resident / (1024 * 1024),
            threshold / (1024 * 1024)
        );

        // Single purge mechanism: same primitive the demand-driven memory-guard
        // path uses (all-arena purge + counter). Failures are logged-and-skipped
        // — the thread must survive transient mallctl errors.
        if let Err(msg) = purge_all_arenas_and_refresh() {
            log_info!("jemalloc background purge failed: {}", msg);
        }
    }
}

/// FFI: Starts the background purge thread and sets the initial threshold.
/// Called once from Java at node startup (NativeBridgeModule.createComponents).
/// Safe to call multiple times — only the first call spawns the thread.
#[no_mangle]
pub extern "C" fn native_jemalloc_start_purge_thread(
    threshold_bytes: i64,
    interval_ms: i64,
) -> i64 {
    PURGE_THRESHOLD_BYTES.store(threshold_bytes, Ordering::Relaxed);
    PURGE_INTERVAL_MS.store(interval_ms as u64, Ordering::Relaxed);
    start_purge_thread().unpark();
    0
}

/// FFI: Updates the purge threshold at runtime. Called when cluster settings change.
#[no_mangle]
pub extern "C" fn native_jemalloc_set_purge_threshold(threshold_bytes: i64) -> i64 {
    PURGE_THRESHOLD_BYTES.store(threshold_bytes, Ordering::Relaxed);
    0
}

/// FFI: Updates the purge check interval at runtime. Set to 0 to pause purging.
/// Wakes the thread immediately so it picks up the new interval without waiting
/// for the old sleep to expire.
#[no_mangle]
pub extern "C" fn native_jemalloc_set_purge_interval(interval_ms: i64) -> i64 {
    PURGE_INTERVAL_MS.store(interval_ms as u64, Ordering::Relaxed);
    if let Some(t) = PURGE_THREAD.get() {
        t.unpark();
    }
    0
}

/// FFI: Stops the background purge thread. Called from Java during node shutdown.
#[no_mangle]
pub extern "C" fn native_jemalloc_stop_purge_thread() -> i64 {
    PURGE_STOP.store(true, Ordering::Relaxed);
    if let Some(t) = PURGE_THREAD.get() {
        t.unpark();
    }
    0
}

/// FFI: Returns the total number of purges executed by the background thread.
#[no_mangle]
pub extern "C" fn native_jemalloc_get_purge_count() -> i64 {
    PURGE_COUNT.load(Ordering::Relaxed) as i64
}

// ── Heap profiling ──────────────────────────────────────────────────────────
//
// Requires the process to be started with `_RJEM_MALLOC_CONF=prof:true,...`
// (or the compile-time MALLOC_CONF in lib.rs to include `prof:true`).
// Without that, activate/dump will return errors — the caller (Java) handles
// this gracefully by logging a warning.

/// FFI: Activates jemalloc heap profiling. Returns 0 on success, negative error pointer on failure.
/// Called from Java when the cluster setting `native.jemalloc.heap_prof_active` is set to true.
#[no_mangle]
pub extern "C" fn native_jemalloc_heap_prof_activate() -> i64 {
    ffm_wrap("native_jemalloc_heap_prof_activate", || {
        unsafe { tikv_jemalloc_ctl::raw::write(b"prof.active\0", true) }
            .map(|_| 0i64)
            .map_err(|e| format!("failed to activate profiling: {}", e))
    })
}

/// FFI: Deactivates jemalloc heap profiling. Returns 0 on success, negative error pointer on failure.
/// Called from Java when the cluster setting `native.jemalloc.heap_prof_active` is set to false.
#[no_mangle]
pub extern "C" fn native_jemalloc_heap_prof_deactivate() -> i64 {
    ffm_wrap("native_jemalloc_heap_prof_deactivate", || {
        unsafe { tikv_jemalloc_ctl::raw::write(b"prof.active\0", false) }
            .map(|_| 0i64)
            .map_err(|e| format!("failed to deactivate profiling: {}", e))
    })
}

/// FFI: Dumps a heap profile to the given path. Path must be a null-terminated C string.
/// Returns 0 on success, negative error pointer on failure.
/// Called from Java when the cluster setting `native.jemalloc.heap_prof_dump_path` is updated.
///
/// # Safety
///
/// `path` must be a valid, non-null pointer to a null-terminated C string that
/// remains valid for the duration of this call.
#[no_mangle]
pub unsafe extern "C" fn native_jemalloc_heap_prof_dump(path: *const std::ffi::c_char) -> i64 {
    ffm_wrap("native_jemalloc_heap_prof_dump", || {
        if path.is_null() {
            return Err("null path".to_string());
        }
        let c_str = std::ffi::CStr::from_ptr(path);
        let path_bytes = c_str.to_bytes_with_nul();
        // prof.dump expects a *const c_char pointing to the file path
        tikv_jemalloc_ctl::raw::write(
            b"prof.dump\0",
            path_bytes.as_ptr() as *const std::ffi::c_char,
        )
        .map(|_| 0i64)
        .map_err(|e| format!("failed to dump heap profile: {}", e))
    })
}

/// FFI: Resets profiling state and sets a new sample interval.
/// Discards all accumulated profiling data and applies the new lg_prof_sample value
/// for future allocations. Returns 0 on success, negative error pointer on failure.
///
/// Common values: 15 (~32KB, high accuracy), 17 (~128KB, default), 19 (~512KB, low overhead).
#[no_mangle]
pub extern "C" fn native_jemalloc_heap_prof_reset(lg_sample: usize) -> i64 {
    ffm_wrap("native_jemalloc_heap_prof_reset", || {
        unsafe { tikv_jemalloc_ctl::raw::write(b"prof.reset\0", lg_sample) }
            .map(|_| 0i64)
            .map_err(|e| {
                format!(
                    "failed to reset profiling with lg_sample={}: {}",
                    lg_sample, e
                )
            })
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[global_allocator]
    static GLOBAL: tikv_jemallocator::Jemalloc = tikv_jemallocator::Jemalloc;

    /// Serializes every test that touches the process-global purge state
    /// (background thread + threshold/interval/count atomics + arena-retained
    /// pages). Without this, cargo's parallel test threads race: one test sets
    /// threshold=0 (always purge) while another asserts zero purges under
    /// threshold=MAX, and background purges deflate resident measurements made
    /// by unrelated tests. Poisoning is ignored — a prior panic must not
    /// cascade.
    static PURGE_STATE: std::sync::Mutex<()> = std::sync::Mutex::new(());

    /// Parks the background purge thread (interval=0) and waits until any
    /// in-flight purge cycle has finished, so the caller owns purge state.
    /// A thread mid-`park_timeout` may complete one more purge after the
    /// pause request — poll until the count is stable across a full window.
    fn quiesce_purge_thread() {
        native_jemalloc_set_purge_interval(0);
        let mut settle = native_jemalloc_get_purge_count();
        for _ in 0..20 {
            std::thread::sleep(Duration::from_millis(100));
            let now = native_jemalloc_get_purge_count();
            if now == settle {
                return;
            }
            settle = now;
        }
        panic!("purge thread failed to quiesce");
    }

    #[test]
    fn allocated_bytes_is_positive() {
        assert!(allocated_bytes() > 0);
    }

    #[test]
    fn resident_bytes_is_positive() {
        assert!(resident_bytes() > 0);
    }

    #[test]
    fn allocated_increases_after_allocation() {
        let before = allocated_bytes();
        let _data: Vec<u8> = vec![42u8; 1024 * 1024];
        let after = allocated_bytes();
        assert!(after > before, "expected {after} > {before}");
    }

    #[test]
    fn purge_all_arenas_returns_retained_pages() {
        let _guard = PURGE_STATE.lock().unwrap_or_else(|p| p.into_inner());
        quiesce_purge_thread();
        // Defect #27 reproduction: freed pages stay resident when decay never
        // ticks — in production because the owning arena's threads go idle
        // (decay only advances on allocator activity), modeled here by
        // disabling decay outright for the window. Without this the drop loop
        // itself ticks decay and returns most pages, hiding the retention.
        native_jemalloc_set_dirty_decay_ms(-1);
        native_jemalloc_set_muzzy_decay_ms(-1);

        // 16 KB blocks stay inside arena size classes (huge allocations would
        // be unmapped directly on free, hiding the retention this guards).
        let mut blocks: Vec<Vec<u8>> = Vec::with_capacity(16 * 1024);
        for _ in 0..(16 * 1024) {
            blocks.push(vec![1u8; 16 * 1024]); // 256 MB touched
        }
        std::hint::black_box(&blocks);
        drop(blocks);

        let after_drop = resident_bytes();
        let result = purge_all_arenas_and_refresh();

        // Restore decay before asserting so a failure cannot leak -1 into
        // the other decay tests.
        native_jemalloc_set_dirty_decay_ms(10_000);
        native_jemalloc_set_muzzy_decay_ms(10_000);

        assert!(after_drop > 0);
        let after_purge = result.expect("arena purge must succeed");
        assert!(after_purge > 0);
        // At least half of the 256 MB freed set must have been retained and
        // then returned by the purge. If purge were a no-op (the pre-fix
        // behavior of relying on decay alone at idle), after_purge ==
        // after_drop and this fails.
        assert!(
            after_purge + 128 * 1024 * 1024 <= after_drop,
            "purge should return retained pages: after_drop={after_drop} after_purge={after_purge}"
        );
    }

    #[test]
    fn set_dirty_decay_ms_applies_at_runtime() {
        let _guard = PURGE_STATE.lock().unwrap_or_else(|p| p.into_inner());
        let rc = native_jemalloc_set_dirty_decay_ms(5000);
        assert_eq!(rc, 0, "setter should succeed, got {}", rc);

        // Read back from arena 0 to verify it took effect
        let actual: isize =
            unsafe { tikv_jemalloc_ctl::raw::read(b"arena.0.dirty_decay_ms\0") }.unwrap();
        assert_eq!(actual, 5000);

        // Restore default
        native_jemalloc_set_dirty_decay_ms(30000);
    }

    #[test]
    fn set_muzzy_decay_ms_applies_at_runtime() {
        let _guard = PURGE_STATE.lock().unwrap_or_else(|p| p.into_inner());
        let rc = native_jemalloc_set_muzzy_decay_ms(10000);
        assert_eq!(rc, 0, "setter should succeed, got {}", rc);

        let actual: isize =
            unsafe { tikv_jemalloc_ctl::raw::read(b"arena.0.muzzy_decay_ms\0") }.unwrap();
        assert_eq!(actual, 10000);

        // Restore default
        native_jemalloc_set_muzzy_decay_ms(30000);
    }

    #[test]
    fn heap_prof_activate_returns_error_when_prof_disabled() {
        // When the process is not started with prof:true, activate should return
        // a negative error pointer (not crash).
        let rc = native_jemalloc_heap_prof_activate();
        // prof:true is not set in test builds, so this should fail gracefully
        assert!(rc <= 0, "expected error or 0, got {}", rc);
    }

    #[test]
    fn heap_prof_deactivate_returns_error_when_prof_disabled() {
        let rc = native_jemalloc_heap_prof_deactivate();
        assert!(rc <= 0, "expected error or 0, got {}", rc);
    }

    #[test]
    fn heap_prof_dump_null_path_returns_error() {
        let rc = unsafe { native_jemalloc_heap_prof_dump(std::ptr::null()) };
        assert!(
            rc < 0,
            "expected negative error pointer for null path, got {}",
            rc
        );
    }

    #[test]
    fn background_purge_thread_fires() {
        let _guard = PURGE_STATE.lock().unwrap_or_else(|p| p.into_inner());
        // threshold=0 means always purge; interval=50ms for fast feedback
        native_jemalloc_start_purge_thread(0, 50);
        let before = native_jemalloc_get_purge_count();
        std::thread::sleep(Duration::from_millis(200));
        assert!(
            native_jemalloc_get_purge_count() > before,
            "purge thread should have fired"
        );
        // Leave the thread parked so no purges bleed into other tests.
        native_jemalloc_set_purge_interval(0);
    }

    #[test]
    fn background_purge_pauses_when_interval_zero() {
        let _guard = PURGE_STATE.lock().unwrap_or_else(|p| p.into_inner());
        native_jemalloc_start_purge_thread(0, 50);
        std::thread::sleep(Duration::from_millis(100));
        // Pause the thread
        native_jemalloc_set_purge_interval(0);
        std::thread::sleep(Duration::from_millis(100));
        let before = native_jemalloc_get_purge_count();
        std::thread::sleep(Duration::from_millis(200));
        assert_eq!(
            native_jemalloc_get_purge_count(),
            before,
            "no purges when paused"
        );
        // Leave parked (interval=0) — tests own purge state only under the lock.
    }

    #[test]
    fn background_purge_respects_threshold() {
        let _guard = PURGE_STATE.lock().unwrap_or_else(|p| p.into_inner());
        // Set threshold to MAX — purge should never fire
        native_jemalloc_start_purge_thread(i64::MAX, 50);
        let before = native_jemalloc_get_purge_count();
        std::thread::sleep(Duration::from_millis(200));
        assert_eq!(
            native_jemalloc_get_purge_count(),
            before,
            "no purge when below threshold"
        );
        // Leave the thread parked with a neutral threshold.
        native_jemalloc_set_purge_interval(0);
        native_jemalloc_set_purge_threshold(0);
    }
}
