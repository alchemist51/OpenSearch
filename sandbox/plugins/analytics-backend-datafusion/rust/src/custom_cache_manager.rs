/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

use std::sync::Arc;
use datafusion::execution::cache::cache_manager::{FileMetadataCache, FileStatisticsCache, CacheManagerConfig};
use datafusion::execution::cache::file_statistics_cache::DefaultFileStatisticsCache;
use datafusion::execution::cache::CacheAccessor;
use crate::statistics_cache::compute_parquet_statistics;
use crate::cache::MutexFileMetadataCache;
use crate::statistics_cache::CustomStatisticsCache;
use object_store::path::Path;
use object_store::ObjectMeta;
use object_store::ObjectStore;
use datafusion::datasource::physical_plan::parquet::metadata::DFParquetMetadata;
use log::{debug, error};

/// Fetch `ObjectMeta` for a file **through the object store** (`store.head`), so size /
/// last_modified / e_tag come from the same store the query path uses. This is what the
/// metadata/statistics caches key on via `is_valid_for`; deriving it from `std::fs` (or
/// worse, `Utc::now()`) instead would make cached entries fail validation on the next query.
async fn object_meta_via_store(
    store: &Arc<dyn object_store::ObjectStore>,
    file_path: &str,
) -> Result<ObjectMeta, String> {
    // Delegate to the shared resolver used by the query/reader path so both paths derive
    // ObjectMeta identically (see api::object_meta_for_file).
    crate::api::object_meta_for_file(store.as_ref(), file_path)
        .await
        .map_err(|e| format!("store.head failed for {}: {}", file_path, e))
}

/// Custom CacheManager that holds cache references directly
pub struct CustomCacheManager {
    /// Direct reference to the file metadata cache
    file_metadata_cache: Option<Arc<MutexFileMetadataCache>>,
    /// Direct reference to the statistics cache
    statistics_cache: Option<Arc<CustomStatisticsCache>>
}

impl CustomCacheManager {
    /// Create a new CustomCacheManager
    pub fn new() -> Self {
        Self {
            file_metadata_cache: None,
            statistics_cache: None
        }
    }

    /// Set the file metadata cache
    pub fn set_file_metadata_cache(&mut self, cache: Arc<MutexFileMetadataCache>) {
        self.file_metadata_cache = Some(cache);
        debug!("[CACHE INFO] File metadata cache set in CustomCacheManager");
    }

    /// Set the statistics cache
    pub fn set_statistics_cache(&mut self, cache: Arc<CustomStatisticsCache>) {
        self.statistics_cache = Some(cache);
        debug!("[CACHE INFO] Statistics cache set in CustomCacheManager");
    }

    /// Get the statistics cache
    pub fn get_statistics_cache(&self) -> Option<Arc<CustomStatisticsCache>> {
        self.statistics_cache.clone()
    }

    /// Attach the on-disk footer tier (POC) to the metadata cache, if one exists. No-op when no
    /// metadata cache is configured. Idempotent (the metadata cache's `set_disk_cache` only takes
    /// the first value). See [`crate::metadata_disk_cache`].
    pub fn attach_metadata_disk_cache(
        &self,
        disk: Option<crate::metadata_disk_cache::MetadataDiskCache>,
    ) {
        if let Some(cache) = &self.file_metadata_cache {
            cache.set_disk_cache(disk);
        }
    }

    /// Get the file metadata cache as Arc<dyn FileMetadataCache> for DataFusion
    pub fn get_file_metadata_cache_for_datafusion(&self) -> Option<Arc<dyn FileMetadataCache>> {
        self.file_metadata_cache.as_ref().map(|cache| cache.clone() as Arc<dyn FileMetadataCache>)
    }

    /// Build a CacheManagerConfig from the caches stored in this CustomCacheManager
    pub fn build_cache_manager_config(&self) -> CacheManagerConfig {
        let mut config = CacheManagerConfig::default();

        // Add file metadata cache if available
        if let Some(cache) = self.get_file_metadata_cache_for_datafusion() {
            config = config.with_file_metadata_cache(Some(cache.clone()))
                .with_metadata_cache_limit(cache.cache_limit());
        }

        // Add statistics cache if available - use CustomStatisticsCache directly
        if let Some(stats_cache) = &self.statistics_cache {
            config = config.with_file_statistics_cache(Some(stats_cache.clone() as Arc<dyn FileStatisticsCache>));
        } else {
            // Default statistics cache if none set
            let default_stats = Arc::new(DefaultFileStatisticsCache::default());
            config = config.with_file_statistics_cache(Some(default_stats));
        }

        config
    }

    /// Add multiple files to all applicable caches, reading footers through `store` — the
    /// shard's object store (TieredObjectStore for local+remote), so cache-warm matches the
    /// query path. `store` comes from the per-index `dataformatAwareStoreHandle`; when absent
    /// the FFM layer passes a default LocalFileSystem.
    ///
    /// `persist_to_disk` enables the on-disk footer tier (POC). The caller sets it only when the
    /// store is remote (`store_ptr > 0`), where re-reading a footer means remote IO; for a local
    /// store the footer read is already cheap, so the disk tier is skipped.
    pub fn add_files(
        &self,
        file_paths: &[String],
        store: &Arc<dyn object_store::ObjectStore>,
        rt_handle: &tokio::runtime::Handle,
        persist_to_disk: bool,
    ) -> Result<Vec<(String, bool)>, String> {
        let mut results = Vec::new();

        for file_path in file_paths {
            let mut any_success = false;
            let mut errors = Vec::new();

            // Add to metadata cache
            match self.metadata_cache_put(file_path, store, rt_handle, persist_to_disk) {
                Ok(true) => {
                    any_success = true;
                }
                Ok(false) => {
                    debug!("[CACHE INFO] File not added for metadata cache: {}", file_path);
                }
                Err(e) => {
                    errors.push(format!("Metadata cache: {}", e));
                }
            }

            // Add to statistics cache
            if let Some(_) = &self.statistics_cache {
                match self.statistics_cache_compute_and_put(file_path, store, rt_handle) {
                    Ok(true) => {
                        any_success = true;
                    }
                    Ok(false) => {
                        debug!("[CACHE INFO] File not added for statistics cache: {}", file_path);
                    }
                    Err(e) => {
                        errors.push(format!("Statistics cache: {}", e));
                    }
                }
            }

            let success = if !errors.is_empty() && !any_success {
                false
            } else {
                any_success
            };

            results.push((file_path.clone(), success));
        }

        Ok(results)
    }

    /// Remove multiple files from all caches
    pub fn remove_files(&self, file_paths: &[String]) -> Result<Vec<(String, bool)>, String> {
        let mut results = Vec::new();

        for file_path in file_paths {
            let mut any_removed = false;
            let mut errors = Vec::new();

            // Remove from metadata cache
            {
                let path = Path::from(file_path.clone());
                if let Some(cache) = &self.file_metadata_cache {
                    match cache.inner.lock() {
                        Ok(cache_guard) => {
                            if cache_guard.remove(&path).is_some() {
                                any_removed = true;
                            } else {
                                debug!("[CACHE INFO] File not found in metadata cache: {}", file_path);
                            }
                        }
                        Err(e) => {
                            errors.push(format!("Metadata cache: Cache remove failed: {}", e));
                        }
                    }
                } else {
                    errors.push("No metadata cache configured".to_string());
                }
            }

            // Remove from statistics cache
            if let Some(cache) = &self.statistics_cache {
                let path = Path::from(file_path.clone());
                if cache.remove(&path).is_some() {
                    any_removed = true;
                }
            }

            let removed = if !errors.is_empty() && !any_removed {
                false
            } else {
                any_removed
            };

            results.push((file_path.clone(), removed));
        }

        Ok(results)
    }

    /// Check if a file exists in any cache
    pub fn contains_file(&self, file_path: &str) -> bool {
        let mut found = false;

        // Check metadata cache
        {
            let path = Path::from(file_path);
            if let Some(cache) = &self.file_metadata_cache {
                if cache.get(&path).is_some() {
                    found = true;
                }
            }
        }

        // Check statistics cache
        if let Some(cache) = &self.statistics_cache {
            let path = Path::from(file_path);
            if cache.contains_key(&path) {
                found = true;
            }
        }

        found
    }

    /// Check if a file exists in a specific cache type
    pub fn contains_file_by_type(&self, file_path: &str, cache_type: &str) -> bool {
        match cache_type {
            crate::cache::CACHE_TYPE_METADATA => {
                let path = Path::from(file_path);
                self.file_metadata_cache
                    .as_ref()
                    .and_then(|cache| cache.get(&path))
                    .is_some()
            }
            crate::cache::CACHE_TYPE_STATS => {
                self.statistics_cache
                    .as_ref()
                    .map_or(false, |cache| cache.contains_key(&Path::from(file_path)))
            }
            _ => false
        }
    }

    /// Update the file metadata cache size limit
    pub fn update_metadata_cache_limit(&self, new_limit: usize) {
        if let Some(cache) = &self.file_metadata_cache {
            cache.update_cache_limit(new_limit);
        }
    }

    /// Update the statistics cache size limit
    pub fn update_statistics_cache_limit(&self, new_limit: usize) -> Result<(), String> {
        if let Some(cache) = &self.statistics_cache {
            cache.update_size_limit(new_limit)
                .map_err(|e| format!("Failed to update statistics cache limit: {:?}", e))
        } else {
            Err("No statistics cache configured".to_string())
        }
    }

    /// Get total memory consumed by all caches
    pub fn get_total_memory_consumed(&self) -> usize {
        let mut total = 0;

        // Add metadata cache memory
        if let Some(cache) = &self.file_metadata_cache {
            if let Ok(cache_guard) = cache.inner.lock() {
                total += cache_guard.memory_used();
            }
        }

        // Add statistics cache memory
        if let Some(cache) = &self.statistics_cache {
            total += cache.memory_consumed();
        }

        total
    }

    /// Clear all caches
    pub fn clear_all(&self) {
        if let Some(cache) = &self.file_metadata_cache {
            cache.clear();
        }
        if let Some(cache) = &self.statistics_cache {
            cache.clear();
        }
    }

    /// Clear specific cache type
    pub fn clear_cache_type(&self, cache_type: &str) -> Result<(), String> {
        match cache_type {
            crate::cache::CACHE_TYPE_METADATA => {
                if let Some(cache) = &self.file_metadata_cache {
                    cache.clear();
                    Ok(())
                } else {
                    Err("No metadata cache configured".to_string())
                }
            }
            crate::cache::CACHE_TYPE_STATS => {
                if let Some(cache) = &self.statistics_cache {
                    cache.clear();
                    Ok(())
                } else {
                    Err("No statistics cache configured".to_string())
                }
            }
            _ => Err(format!("Unknown cache type: {}", cache_type))
        }
    }

    /// Get memory consumed by specific cache type
    pub fn get_memory_consumed_by_type(&self, cache_type: &str) -> Result<usize, String> {
        match cache_type {
            crate::cache::CACHE_TYPE_METADATA => {
                if let Some(cache) = &self.file_metadata_cache {
                    if let Ok(cache_guard) = cache.inner.lock() {
                        Ok(cache_guard.memory_used())
                    } else {
                        Err("Failed to lock metadata cache".to_string())
                    }
                } else {
                    Err("No metadata cache configured".to_string())
                }
            }
            crate::cache::CACHE_TYPE_STATS => {
                if let Some(cache) = &self.statistics_cache {
                    Ok(cache.memory_consumed())
                } else {
                    Err("No statistics cache configured".to_string())
                }
            }
            _ => Err(format!("Unknown cache type: {}", cache_type))
        }
    }

    /// Internal method to put metadata into cache. `persist_to_disk` mirrors the footer to the
    /// on-disk tier (only set for remote stores; see [`Self::add_files`]).
    fn metadata_cache_put(
        &self,
        file_path: &str,
        store: &Arc<dyn object_store::ObjectStore>,
        rt_handle: &tokio::runtime::Handle,
        persist_to_disk: bool,
    ) -> Result<bool, String> {
        if !file_path.to_lowercase().ends_with(".parquet") {
            return Ok(false); // Skip unsupported formats
        }

        // Get cache reference for DataFusion metadata loading
        let cache_ref = self.file_metadata_cache.as_ref()
            .ok_or_else(|| "No file metadata cache configured".to_string())?;

        let metadata_cache = cache_ref.clone() as Arc<dyn FileMetadataCache>;

        // Use DataFusion's metadata loading by passing reference to file_metadata_cache to get complete metadata
        // IMPORTANT: When a cache is provided to DFParquetMetadata, fetch_metadata() will:
        // 1. Enable page index loading (with_page_indexes(true))
        // 2. Load the complete metadata including column and offset indexes
        // 3. Automatically put the metadata into the cache (lines 155-160 in datafusion's metadata.rs)
        // This ensures we cache exactly what DataFusion would cache during query execution.
        // The footer is read through the shard `store` (not a fabricated LocalFileSystem) and
        // the ObjectMeta is fetched via `store.head`, so the cached entry matches the query
        // path and passes `is_valid_for`.
        let (object_meta, parquet_metadata) = rt_handle.block_on(async {
            let object_meta = object_meta_via_store(store, file_path).await?;
            let df_metadata = DFParquetMetadata::new(store.as_ref(), &object_meta)
                .with_file_metadata_cache(Some(metadata_cache));

            // fetch_metadata() performs the cache put operation internally
            let parquet_metadata = df_metadata.fetch_metadata().await
                .map_err(|e| format!("Failed to fetch metadata: {}", e))?;
            Ok::<_, String>((object_meta, parquet_metadata))
        })?;

        // POC: persist the decoded footer to the on-disk tier so a future in-memory miss reloads
        // it from local disk instead of re-reading the (possibly remote) store. Best-effort; the
        // store remains the source of truth on a disk miss. We pay the remote read once, here.
        // Only for remote stores — see `persist_to_disk` in `add_files`.
        if persist_to_disk {
            if let Some(disk) = cache_ref.disk_cache() {
                disk.put(file_path, &object_meta, parquet_metadata.as_ref());
            }
        }

        // Verify the metadata was cached properly
        match cache_ref.inner.lock() {
            Ok(cache_guard) => {
                let path = Path::from(file_path.to_string());
                if cache_guard.contains_key(&path) {
                    Ok(true)
                } else {
                    debug!("[CACHE ERROR] Failed to cache metadata for: {}", file_path);
                    Ok(false)
                }
            }
            Err(e) => Err(format!("Failed to verify cache: {}", e))
        }
    }

    /// Compute and put statistics into cache, reading the parquet footer through the shard
    /// `store` (matching the query path) and keying on the store-derived `ObjectMeta`.
    pub fn statistics_cache_compute_and_put(
        &self,
        file_path: &str,
        store: &Arc<dyn object_store::ObjectStore>,
        rt_handle: &tokio::runtime::Handle,
    ) -> Result<bool, String> {
        let cache = self.statistics_cache.as_ref()
            .ok_or_else(|| "No statistics cache configured".to_string())?;

        let path = Path::from(file_path.to_string());

        // Check if already cached
        if cache.contains_key(&path) {
            return Ok(true);
        }

        // Fetch ObjectMeta + compute statistics through the shard store (async).
        let computed = rt_handle.block_on(async {
            let meta = object_meta_via_store(store, file_path).await?;
            let stats = compute_parquet_statistics(store, &meta)
                .await
                .map_err(|e| format!("Failed to compute statistics for {}: {}", file_path, e))?;
            Ok::<_, String>((meta, stats))
        });

        match computed {
            Ok((meta, stats)) => {
                cache.put_statistics(&path, Arc::new(stats), &meta);
                Ok(true)
            }
            Err(e) => {
                Err(format!("Failed to compute statistics for {}: {}", file_path, e))
            }
        }
    }

    /// Batch compute and cache statistics for multiple files, reading footers through the
    /// shard `store`.
    pub fn statistics_cache_batch_compute_and_put(
        &self,
        file_paths: &[String],
        store: &Arc<dyn object_store::ObjectStore>,
        rt_handle: &tokio::runtime::Handle,
    ) -> Result<usize, String> {
        let cache = self.statistics_cache.as_ref()
            .ok_or_else(|| "No statistics cache configured".to_string())?;

        let mut success_count = 0;
        let mut failed_files = Vec::new();

        for file_path in file_paths {
            let path = Path::from(file_path.clone());

            if cache.contains_key(&path) {
                success_count += 1;
                continue;
            }

            let computed = rt_handle.block_on(async {
                let meta = object_meta_via_store(store, file_path).await?;
                let stats = compute_parquet_statistics(store, &meta)
                    .await
                    .map_err(|e| format!("{}", e))?;
                Ok::<_, String>((meta, stats))
            });

            match computed {
                Ok((meta, stats)) => {
                    cache.put_statistics(&path, Arc::new(stats), &meta);
                    success_count += 1;
                }
                Err(e) => {
                    debug!("[STATS CACHE ERROR] Failed to compute statistics for {}: {}", file_path, e);
                    failed_files.push(file_path.clone());
                }
            }
        }

        if !failed_files.is_empty() {
            debug!("[STATS CACHE WARNING] Failed to compute statistics for {} files: {:?}",
                      failed_files.len(), failed_files);
        }

        Ok(success_count)
    }

    /// Get or compute statistics
    pub fn statistics_cache_get_or_compute(
        &self,
        file_path: &str,
        store: &Arc<dyn object_store::ObjectStore>,
        rt_handle: &tokio::runtime::Handle,
    ) -> Result<bool, String> {
        let cache = self.statistics_cache.as_ref()
            .ok_or_else(|| "No statistics cache configured".to_string())?;

        let path = Path::from(file_path.to_string());

        if cache.get(&path).is_some() {
            return Ok(true);
        }

        self.statistics_cache_compute_and_put(file_path, store, rt_handle)
    }

    /// Get statistics cache hit count
    pub fn statistics_cache_hit_count(&self) -> usize {
        self.statistics_cache.as_ref()
            .map(|cache| cache.hit_count())
            .unwrap_or(0)
    }

    /// Get statistics cache miss count
    pub fn statistics_cache_miss_count(&self) -> usize {
        self.statistics_cache.as_ref()
            .map(|cache| cache.miss_count())
            .unwrap_or(0)
    }

    /// Get statistics cache hit rate
    pub fn statistics_cache_hit_rate(&self) -> f64 {
        self.statistics_cache.as_ref()
            .map(|cache| cache.hit_rate())
            .unwrap_or(0.0)
    }

    /// Get statistics cache entry count
    pub fn statistics_cache_entry_count(&self) -> usize {
        self.statistics_cache.as_ref()
            .map(|cache| <CustomStatisticsCache as CacheAccessor<_, _>>::len(cache))
            .unwrap_or(0)
    }

    /// Get statistics cache size limit in bytes
    pub fn statistics_cache_size_limit(&self) -> usize {
        self.statistics_cache.as_ref()
            .map(|cache| cache.current_size_limit())
            .unwrap_or(0)
    }

    /// Reset statistics cache stats
    pub fn statistics_cache_reset_stats(&self) {
        if let Some(cache) = &self.statistics_cache {
            cache.reset_stats();
        }
    }

    /// Get metadata cache hit count
    pub fn metadata_cache_hit_count(&self) -> usize {
        self.file_metadata_cache.as_ref()
            .map(|cache| cache.hit_count())
            .unwrap_or(0)
    }

    /// Get metadata cache miss count
    pub fn metadata_cache_miss_count(&self) -> usize {
        self.file_metadata_cache.as_ref()
            .map(|cache| cache.miss_count())
            .unwrap_or(0)
    }

    /// Get metadata cache entry count
    pub fn metadata_cache_entry_count(&self) -> usize {
        self.file_metadata_cache.as_ref()
            .map(|cache| <MutexFileMetadataCache as CacheAccessor<_, _>>::len(cache))
            .unwrap_or(0)
    }

    /// Get metadata cache size limit in bytes
    pub fn metadata_cache_size_limit(&self) -> usize {
        self.file_metadata_cache.as_ref()
            .map(|cache| cache.get_cache_limit())
            .unwrap_or(0)
    }

    /// Reset metadata cache stats
    pub fn metadata_cache_reset_stats(&self) {
        if let Some(cache) = &self.file_metadata_cache {
            cache.reset_stats();
        }
    }
}
