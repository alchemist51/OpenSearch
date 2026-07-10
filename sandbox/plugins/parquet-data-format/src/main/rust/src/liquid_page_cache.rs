/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Codec-owned, cross-query decoded-page cache backed by liquid-cache's core API.
//!
//! The Parquet DocValues codec's `PageCache` is per-query scratch: every query re-decodes the
//! same Parquet pages. This module gives the codec a **node-level** (process-lifetime) cache of
//! decoded primitive pages so a later query reuses a page an earlier query already decoded —
//! the cross-query tier the codec otherwise lacks.
//!
//! Design (v1):
//! - A single process-global `Arc<LiquidCache>` (liquid core), built lazily. This is a
//!   **codec-owned** instance, independent of the DataFusion/PPL liquid cache — same technology,
//!   separate instance and keyspace. Sharing the DataFusion instance is a later optimization.
//! - Entries are keyed by `(file_id, column_id, page_idx)` packed into liquid's `EntryID` (a
//!   `usize`). `file_id` comes from a codec-local path→id registry, so the key carries file
//!   identity (and Parquet's immutable-file/generation model means changed data = new path =
//!   new key = automatic miss — no invalidation logic needed).
//! - Values are cached as an Arrow `Int64Array` (with a null buffer derived from the page's
//!   presence bits). On a hit we convert back to the raw `Vec<i64>` + `Vec<bool>` the codec's
//!   `write_primitive_page` already consumes, so the Java/PageCache/per-doc path is byte-identical
//!   whether the page was decoded or served from cache.
//! - liquid's `insert`/`get` are async; we drive them on a dedicated single-threaded tokio runtime
//!   via `block_on`, mirroring `merge::io_task`'s `OnceLock<Runtime>` pattern (the codec's FFM
//!   entry points are synchronous `extern "C"`).
//!
//! Primitives only (INT32/INT64/date → i64 words). BYTE_ARRAY/keyword pages are not cached here.
//! Gated by `set_enabled(true)` from Java; when disabled every entry point is a cheap no-op and the
//! codec's decode path is unchanged.

use std::collections::HashMap;
use std::future::IntoFuture;
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use arrow::array::{Array, ArrayRef, Int64Array};
use liquid_cache::cache::{EntryID, LiquidCache, LiquidCacheBuilder};
use tokio::runtime::Runtime;

/// The process-global codec-owned decoded-page cache. Built on first use (or via `init`).
static CACHE: OnceLock<Arc<LiquidCache>> = OnceLock::new();

/// Dedicated runtime for driving liquid's async `insert`/`get` from the synchronous FFM path.
static RT: OnceLock<Runtime> = OnceLock::new();

/// Master on/off switch, set by Java at init. Off by default → the codec decode path is untouched.
static ENABLED: AtomicBool = AtomicBool::new(false);

/// Configured max memory budget for the cache (bytes). Applied when the cache is first built.
static MAX_MEMORY_BYTES: std::sync::atomic::AtomicUsize = std::sync::atomic::AtomicUsize::new(0);

/// Observability counters, so a benchmark can confirm the liquid path is actually exercised
/// (a "liquid-on" run that is all misses tells you nothing about the hit path). `hits` counts
/// pages served from the cache, `misses` counts lookups that fell through to decode, `backfills`
/// counts pages inserted after a miss.
static HITS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static MISSES: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
static BACKFILLS: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);

/// Snapshot of the cache counters: `(hits, misses, backfills)`.
pub fn stats() -> (u64, u64, u64) {
    (
        HITS.load(Ordering::Relaxed),
        MISSES.load(Ordering::Relaxed),
        BACKFILLS.load(Ordering::Relaxed),
    )
}

/// Codec-local file path → small integer id registry, so entries carry file identity without
/// depending on DataFusion's file numbering.
static FILE_IDS: OnceLock<Mutex<HashMap<String, u32>>> = OnceLock::new();

/// Enable/disable the cache and set the memory budget. Called by Java at plugin init when the
/// `parquet_liquid_cache` feature flag is on. A `max_memory_bytes` of 0 leaves the liquid default.
pub fn set_enabled(enabled: bool, max_memory_bytes: usize) {
    MAX_MEMORY_BYTES.store(max_memory_bytes, Ordering::Relaxed);
    ENABLED.store(enabled, Ordering::Relaxed);
}

/// True when the cache should be consulted. Cheap relaxed load on the hot path.
#[inline]
pub fn enabled() -> bool {
    ENABLED.load(Ordering::Relaxed)
}

fn runtime() -> &'static Runtime {
    RT.get_or_init(|| {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .expect("liquid_page_cache: failed to build tokio runtime")
    })
}

fn cache() -> &'static Arc<LiquidCache> {
    CACHE.get_or_init(|| {
        let mut builder = LiquidCacheBuilder::new();
        let budget = MAX_MEMORY_BYTES.load(Ordering::Relaxed);
        if budget > 0 {
            builder = builder.with_max_memory_bytes(budget);
        }
        runtime().block_on(builder.build())
    })
}

/// Resolve (or assign) a stable small id for a Parquet file path. Codec-local; independent of any
/// DataFusion file numbering.
pub fn file_id(path: &str) -> u32 {
    let map = FILE_IDS.get_or_init(|| Mutex::new(HashMap::new()));
    let mut guard = map
        .lock()
        .expect("liquid_page_cache: file id registry poisoned");
    let next = guard.len() as u32;
    *guard.entry(path.to_string()).or_insert(next)
}

/// Pack `(file_id, column_id, page_idx)` into a liquid `EntryID`. u16 column id + u32 page fit
/// alongside the file id in a usize on 64-bit targets.
#[inline]
pub fn entry_id(file_id: u32, column_id: u32, page_idx: u32) -> EntryID {
    let v = ((file_id as usize) << 48) | ((column_id as usize) << 32) | (page_idx as usize);
    EntryID::from(v)
}

/// Look up a cached decoded page as the raw cached `ArrayRef` (an `Int64Array`), or `None` on
/// a miss. The caller writes it straight into the FFM out-buffers via
/// `write_primitive_page_from_arrow`, avoiding the per-element `Vec<i64>` + `Vec<bool>` rebuild
/// that `get_page` performs. This is the warm-run hot path (every page is a hit), so skipping
/// the rebuild matters. Returns `None` (falls back to decode) if the entry isn't an
/// `Int64Array`, so the contract stays identical to `get_page`.
pub fn get_page_array(eid: EntryID) -> Option<ArrayRef> {
    let array: ArrayRef = match runtime().block_on(cache().get(&eid).read()) {
        Some(a) => a,
        None => {
            MISSES.fetch_add(1, Ordering::Relaxed);
            return None;
        }
    };
    // Only the Int64Array layout is cached (see put_page); guard the downcast so a future
    // change to the cached type falls back to decode instead of mis-reading.
    if array.as_any().downcast_ref::<Int64Array>().is_none() {
        MISSES.fetch_add(1, Ordering::Relaxed);
        return None;
    }
    HITS.fetch_add(1, Ordering::Relaxed);
    Some(array)
}

/// Look up a cached decoded page. Returns `(longs, presence)` in the exact form the decode arms
/// produce (`longs[i]` valid iff `presence[i]`), or `None` on a miss.
pub fn get_page(eid: EntryID) -> Option<(Vec<i64>, Vec<bool>)> {
    let array: ArrayRef = runtime().block_on(cache().get(&eid).read())?;
    let int_array = array.as_any().downcast_ref::<Int64Array>()?;
    let len = int_array.len();
    let mut longs = Vec::with_capacity(len);
    let mut presence = Vec::with_capacity(len);
    for i in 0..len {
        if int_array.is_null(i) {
            longs.push(0);
            presence.push(false);
        } else {
            longs.push(int_array.value(i));
            presence.push(true);
        }
    }
    Some((longs, presence))
}

/// Cache a decoded primitive page. `longs[i]` is meaningful only where `presence[i]` is true;
/// null rows are stored as Arrow nulls so a later `get_page` reconstructs presence exactly.
pub fn put_page(eid: EntryID, longs: &[i64], presence: &[bool]) {
    debug_assert_eq!(longs.len(), presence.len());
    let array: Int64Array = longs
        .iter()
        .zip(presence.iter())
        .map(|(&v, &present)| if present { Some(v) } else { None })
        .collect();
    let array_ref: ArrayRef = Arc::new(array);
    // Best-effort: a CacheFull error just means this page is not cached this time.
    // `insert`/`get` return builder types that implement `IntoFuture`, so convert before block_on.
    if runtime()
        .block_on(cache().insert(eid, array_ref).into_future())
        .is_ok()
    {
        BACKFILLS.fetch_add(1, Ordering::Relaxed);
    }
}
