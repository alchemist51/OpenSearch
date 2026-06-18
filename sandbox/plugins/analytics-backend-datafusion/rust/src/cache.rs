/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex, OnceLock};

use datafusion::execution::cache::cache_manager::{
    CachedFileMetadataEntry, FileMetadataCache, FileMetadataCacheEntry,
};
use datafusion::execution::cache::DefaultFilesMetadataCache;
use datafusion::execution::cache::CacheAccessor;
use datafusion::datasource::physical_plan::parquet::metadata::CachedParquetMetaData;
use log::error;
use object_store::path::Path;

use crate::metadata_disk_cache::MetadataDiskCache;

// Cache type constants
pub const CACHE_TYPE_METADATA: &str = "METADATA";
pub const CACHE_TYPE_STATS: &str = "STATISTICS";

// Helper function to log cache operations
fn log_cache_error(operation: &str, error: &str) {
    error!("[CACHE ERROR] {} operation failed: {}", operation, error);
}

// Wrapper to make Mutex<DefaultFilesMetadataCache> implement FileMetadataCache
pub struct MutexFileMetadataCache {
    pub inner: Mutex<DefaultFilesMetadataCache>,
    hit_count: AtomicUsize,
    miss_count: AtomicUsize,
    /// Optional on-disk tier (POC): footers serialized in `onFilesAdded` are reloaded here on an
    /// in-memory miss, turning a remote footer read into a cheap local read. Set once (via
    /// interior mutability) at runtime construction when the spill dir is known — the cache itself
    /// is created earlier in `df_create_cache`, before the runtime/spill_dir exists. Unset / set to
    /// `None` ⇒ disk tier inactive. See [`crate::metadata_disk_cache`].
    disk: OnceLock<Option<MetadataDiskCache>>,
    /// Counts in-memory misses that were satisfied from the disk tier (POC observability).
    disk_hit_count: AtomicUsize,
}

impl MutexFileMetadataCache {
    pub fn new(cache: DefaultFilesMetadataCache) -> Self {
        Self {
            inner: Mutex::new(cache),
            hit_count: AtomicUsize::new(0),
            miss_count: AtomicUsize::new(0),
            disk: OnceLock::new(),
            disk_hit_count: AtomicUsize::new(0),
        }
    }

    /// Attach the on-disk metadata tier. Idempotent: only the first call takes effect (the cache
    /// is created once per runtime). Safe to leave unset (disk tier inactive). Takes `&self` so it
    /// can be called through the shared `Arc<MutexFileMetadataCache>`.
    pub fn set_disk_cache(&self, disk: Option<MetadataDiskCache>) {
        let _ = self.disk.set(disk);
    }

    /// The on-disk tier, if configured (used by the warm path to persist footers).
    pub fn disk_cache(&self) -> Option<&MetadataDiskCache> {
        self.disk.get().and_then(|d| d.as_ref())
    }

    /// Number of in-memory misses served from disk.
    pub fn disk_hit_count(&self) -> usize {
        self.disk_hit_count.load(Ordering::Relaxed)
    }

    pub fn hit_count(&self) -> usize {
        self.hit_count.load(Ordering::Relaxed)
    }

    pub fn miss_count(&self) -> usize {
        self.miss_count.load(Ordering::Relaxed)
    }

    pub fn reset_stats(&self) {
        self.hit_count.store(0, Ordering::Relaxed);
        self.miss_count.store(0, Ordering::Relaxed);
    }

    pub fn clear_cache(&self) {
        if let Ok(cache) = self.inner.lock() {
            cache.clear();
        }
    }

    pub fn update_cache_limit(&self, new_limit: usize) {
        if let Ok(cache) = self.inner.lock() {
            cache.update_cache_limit(new_limit);
        }
    }

    pub fn get_cache_limit(&self) -> usize {
        if let Ok(cache) = self.inner.lock() {
            cache.cache_limit()
        } else {
            0
        }
    }
}

impl CacheAccessor<Path, CachedFileMetadataEntry> for MutexFileMetadataCache {
    fn get(&self, k: &Path) -> Option<CachedFileMetadataEntry> {
        // In-memory tier first.
        match self.inner.lock() {
            Ok(cache) => {
                if let Some(hit) = cache.get(k) {
                    self.hit_count.fetch_add(1, Ordering::Relaxed);
                    return Some(hit);
                }
            }
            Err(e) => {
                log_cache_error("get", &e.to_string());
                return None;
            }
        }

        // In-memory miss. Try the on-disk tier (POC): a footer serialized in onFilesAdded is
        // reloaded from local disk instead of re-reading it from the (possibly remote) store.
        if let Some(disk) = self.disk_cache() {
            if let Some((object_meta, parquet_meta)) = disk.get(k.as_ref()) {
                let entry = CachedFileMetadataEntry::new(
                    object_meta,
                    Arc::new(CachedParquetMetaData::new(parquet_meta)),
                );
                // Promote into the in-memory tier so subsequent gets skip disk. The query path
                // still validates the returned entry via is_valid_for(current_meta).
                if let Ok(cache) = self.inner.lock() {
                    cache.put(k, entry.clone());
                }
                self.disk_hit_count.fetch_add(1, Ordering::Relaxed);
                return Some(entry);
            }
        }

        self.miss_count.fetch_add(1, Ordering::Relaxed);
        None
    }

    fn put(&self, k: &Path, v: CachedFileMetadataEntry) -> Option<CachedFileMetadataEntry> {
        match self.inner.lock() {
            Ok(cache) => cache.put(k, v),
            Err(e) => {
                log_cache_error("put", &e.to_string());
                None
            }
        }
    }

    fn remove(&self, k: &Path) -> Option<CachedFileMetadataEntry> {
        match self.inner.lock() {
            Ok(cache) => cache.remove(k),
            Err(e) => {
                log_cache_error("remove", &e.to_string());
                None
            }
        }
    }

    fn contains_key(&self, k: &Path) -> bool {
        match self.inner.lock() {
            Ok(cache) => cache.contains_key(k),
            Err(e) => {
                log_cache_error("contains_key", &e.to_string());
                false
            }
        }
    }

    fn len(&self) -> usize {
        match self.inner.lock() {
            Ok(cache) => cache.len(),
            Err(e) => {
                log_cache_error("len", &e.to_string());
                0
            }
        }
    }

    fn clear(&self) {
        match self.inner.lock() {
            Ok(cache) => cache.clear(),
            Err(e) => log_cache_error("clear", &e.to_string()),
        }
    }

    fn name(&self) -> String {
        match self.inner.lock() {
            Ok(cache) => cache.name(),
            Err(e) => {
                log_cache_error("name", &e.to_string());
                "cache_error".to_string()
            }
        }
    }
}

impl FileMetadataCache for MutexFileMetadataCache {
    fn cache_limit(&self) -> usize {
        match self.inner.lock() {
            Ok(cache) => cache.cache_limit(),
            Err(e) => {
                log_cache_error("cache_limit", &e.to_string());
                0
            }
        }
    }

    fn update_cache_limit(&self, limit: usize) {
        match self.inner.lock() {
            Ok(cache) => cache.update_cache_limit(limit),
            Err(e) => log_cache_error("update_cache_limit", &e.to_string()),
        }
    }

    fn list_entries(&self) -> std::collections::HashMap<Path, FileMetadataCacheEntry> {
        match self.inner.lock() {
            Ok(cache) => cache.list_entries(),
            Err(e) => {
                log_cache_error("list_entries", &e.to_string());
                std::collections::HashMap::new()
            }
        }
    }
}
