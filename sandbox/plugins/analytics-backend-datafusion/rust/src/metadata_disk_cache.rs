/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! On-disk tier for the parquet **footer metadata** cache (POC).
//!
//! Motivation: when a shard has a real per-index object store (`store_ptr > 0`), reading a
//! parquet footer means remote IO (TieredObjectStore → S3/remote). That cost is paid in
//! `onFilesAdded` (cache-warm) and again on every cold query whose in-memory entry was evicted.
//! This tier spends the remote read **once** in `onFilesAdded`, serializes the decoded
//! `ParquetMetaData` to a node-local file (under the spill directory), and reloads it from local
//! disk on an in-memory miss — so an evicted entry costs a cheap local read instead of a remote
//! round-trip. The object store remains the source of truth: on a disk miss (or a stale sidecar)
//! the query path falls back to reading the footer from the store exactly as before.
//!
//! ## Layout
//! `<spill_dir>/metadata-cache/<sha256(file_path)>.{meta,footer}`
//! - `.footer` — thrift-encoded `ParquetMetaData` (via `ParquetMetaDataWriter`), incl. page index.
//! - `.meta`   — a tiny sidecar holding the `ObjectMeta` `size` + `last_modified` the footer was
//!   captured for. On read we compare it to the current `ObjectMeta`; a mismatch means the file
//!   changed (new parquet file → segments are immutable, so this should not normally happen) and
//!   the on-disk entry is ignored. This mirrors DataFusion's `CachedFileMetadataEntry::is_valid_for`.
//!
//! Parquet segment files are immutable (a change produces a new file via the
//! `onRemoved`/`onFilesAdded` lifecycle), so a present, size+mtime-matching footer is always valid.
//!
//! ## Persistence across restarts
//! This directory is intentionally exempted from the spill-directory boot cleanup in
//! `api::create_global_runtime` (which renames + GCs every other spill child). So footers survive
//! a restart, and the first post-restart query for a segment reloads its footer from local disk
//! (via the in-memory cache's `get()` miss path) instead of re-reading it from the remote store —
//! recovery is lazy, no boot-time scan. The sidecar's size+last_modified still gate validity, so a
//! stale entry (path reused for a different file) is rejected and falls back to the store.

use std::io::Write;
use std::path::{Path as FsPath, PathBuf};
use std::sync::Arc;

use chrono::{DateTime, TimeZone, Utc};
use object_store::path::Path as ObjPath;
use object_store::ObjectMeta;
use parquet::file::metadata::{ParquetMetaData, ParquetMetaDataReader, ParquetMetaDataWriter};
use log::{debug, warn};

/// Subdirectory under the spill dir that holds serialized footers.
///
/// Public so the spill-directory boot cleanup (`api::create_global_runtime`) can exempt it from
/// the rename-to-`.stale` + GC sweep — otherwise the cache would be wiped on every restart,
/// before this tier is even attached. Exempting it lets footers persist across restarts so the
/// first post-restart query reloads from local disk instead of re-reading the remote store.
pub const SUBDIR: &str = "metadata-cache";

/// On-disk metadata tier rooted at a node-local directory (a subdir of the spill dir).
///
/// Cheaply cloneable (just an `Arc<PathBuf>`); shared by the warm path (writes) and the
/// in-memory cache's read path (reads).
#[derive(Clone)]
pub struct MetadataDiskCache {
    root: Arc<PathBuf>,
}

impl MetadataDiskCache {
    /// Create a disk tier rooted at `<spill_dir>/metadata-cache`, creating the directory if
    /// needed. Returns `None` when `spill_dir` is empty (spill disabled) or the directory can't
    /// be created — in that case the caller simply skips the disk tier (store stays the truth).
    pub fn new(spill_dir: &str) -> Option<Self> {
        if spill_dir.is_empty() {
            debug!("[META DISK CACHE] disabled: no spill directory configured");
            return None;
        }
        let root = PathBuf::from(spill_dir).join(SUBDIR);
        if let Err(e) = std::fs::create_dir_all(&root) {
            warn!(
                "[META DISK CACHE] disabled: failed to create {} ({}): {}",
                root.display(),
                e.kind(),
                e
            );
            return None;
        }
        debug!("[META DISK CACHE] enabled at {}", root.display());
        Some(Self { root: Arc::new(root) })
    }

    /// SHA-256 of the file path → stable, filesystem-safe basename (avoids path-separator and
    /// length issues from using the raw object path).
    fn key(&self, file_path: &str) -> String {
        // Lightweight FNV-1a 64-bit hash rendered as hex; we only need a stable, collision-
        // resistant-enough basename for a node-local POC cache, not a cryptographic digest.
        let mut hash: u64 = 0xcbf29ce484222325;
        for b in file_path.as_bytes() {
            hash ^= *b as u64;
            hash = hash.wrapping_mul(0x100000001b3);
        }
        format!("{:016x}", hash)
    }

    fn footer_path(&self, file_path: &str) -> PathBuf {
        self.root.join(format!("{}.footer", self.key(file_path)))
    }

    fn meta_path(&self, file_path: &str) -> PathBuf {
        self.root.join(format!("{}.meta", self.key(file_path)))
    }

    /// Serialize `metadata` + a validation sidecar to disk for `file_path`. Best-effort: on any
    /// IO/encode error we log and leave the disk tier without this entry (the store remains the
    /// source of truth). Writes are done to a temp file then renamed for atomicity.
    pub fn put(&self, file_path: &str, object_meta: &ObjectMeta, metadata: &ParquetMetaData) {
        if let Err(e) = self.put_inner(file_path, object_meta, metadata) {
            warn!("[META DISK CACHE] put failed for {}: {}", file_path, e);
        }
    }

    fn put_inner(
        &self,
        file_path: &str,
        object_meta: &ObjectMeta,
        metadata: &ParquetMetaData,
    ) -> Result<(), String> {
        // Encode the footer (incl. column/offset/page indexes) into a byte buffer.
        let mut buf: Vec<u8> = Vec::new();
        ParquetMetaDataWriter::new(&mut buf, metadata)
            .finish()
            .map_err(|e| format!("encode footer: {}", e))?;

        // Sidecar: size + last_modified (epoch millis) for is_valid_for-style validation.
        let sidecar = format!(
            "{}\n{}\n",
            object_meta.size,
            object_meta.last_modified.timestamp_millis()
        );

        write_atomic(&self.footer_path(file_path), &buf)
            .map_err(|e| format!("write footer: {}", e))?;
        write_atomic(&self.meta_path(file_path), sidecar.as_bytes())
            .map_err(|e| format!("write sidecar: {}", e))?;

        debug!(
            "[META DISK CACHE] wrote {} ({} footer bytes)",
            file_path,
            buf.len()
        );
        Ok(())
    }

    /// Load the footer for `file_path` from disk if present. Returns the decoded
    /// `ParquetMetaData` **plus the `ObjectMeta` it was captured for** (reconstructed from the
    /// sidecar), so the caller can build a `CachedFileMetadataEntry` whose existing
    /// `is_valid_for(current_meta)` check validates the entry — exactly as for an in-memory entry.
    /// Returns `None` on any miss / decode error; the caller then falls back to the object store.
    pub fn get(&self, file_path: &str) -> Option<(ObjectMeta, Arc<ParquetMetaData>)> {
        let object_meta = self.read_sidecar(file_path)?;

        let footer_path = self.footer_path(file_path);
        let bytes = match std::fs::read(&footer_path) {
            Ok(b) => b,
            Err(e) => {
                // ENOENT is the common "not warmed / evicted from disk" path — debug, not warn.
                debug!("[META DISK CACHE] miss for {} ({}): {}", file_path, e.kind(), e);
                return None;
            }
        };

        // `Bytes` implements `ChunkReader` (a bare `&[u8]` does not). The serialized footer is the
        // standalone metadata layout `ParquetMetaDataWriter` produces, which
        // `parse_and_finish` reads back (incl. page index) — the crate's own round-trip path.
        // `bytes` is reached via `prost::bytes` (a direct dep) to avoid a new Cargo dependency,
        // matching `indexed_table::parquet_bridge`.
        let reader = prost::bytes::Bytes::from(bytes);
        match ParquetMetaDataReader::new()
            .with_page_indexes(true)
            .parse_and_finish(&reader)
        {
            Ok(meta) => {
                debug!("[META DISK CACHE] hit for {}", file_path);
                Some((object_meta, Arc::new(meta)))
            }
            Err(e) => {
                warn!("[META DISK CACHE] decode failed for {}: {}", file_path, e);
                None
            }
        }
    }

    /// Reconstruct the `ObjectMeta` the footer was captured for, from the sidecar. `location` is
    /// set to the file path; `size` + `last_modified` come from the sidecar (the fields
    /// `CachedFileMetadataEntry::is_valid_for` compares). Returns `None` if the sidecar is missing
    /// or malformed.
    fn read_sidecar(&self, file_path: &str) -> Option<ObjectMeta> {
        let contents = std::fs::read_to_string(self.meta_path(file_path)).ok()?;
        let mut lines = contents.lines();
        let size = lines.next()?.parse::<u64>().ok()?;
        let mtime_ms = lines.next()?.parse::<i64>().ok()?;
        let last_modified: DateTime<Utc> = Utc.timestamp_millis_opt(mtime_ms).single()?;
        Some(ObjectMeta {
            location: ObjPath::from(file_path),
            last_modified,
            size,
            e_tag: None,
            version: None,
        })
    }

    /// Remove the on-disk entry for `file_path` (best-effort; used when files are deleted).
    pub fn remove(&self, file_path: &str) {
        let _ = std::fs::remove_file(self.footer_path(file_path));
        let _ = std::fs::remove_file(self.meta_path(file_path));
    }
}

/// Write `data` to `path` atomically: write to `<path>.tmp` then rename over `path`.
fn write_atomic(path: &FsPath, data: &[u8]) -> std::io::Result<()> {
    let tmp = path.with_extension("tmp");
    {
        let mut f = std::fs::File::create(&tmp)?;
        f.write_all(data)?;
        f.sync_all()?;
    }
    std::fs::rename(&tmp, path)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Int32Array, RecordBatch, StringArray};
    use chrono::TimeZone;
    use datafusion::parquet::arrow::ArrowWriter;
    use datafusion::parquet::file::metadata::ParquetMetaDataReader;
    use std::sync::Arc;
    use tempfile::tempdir;

    /// Write a tiny parquet file and decode its real footer `ParquetMetaData` (with page index).
    fn make_parquet_metadata(path: &std::path::Path) -> ParquetMetaData {
        let schema = Arc::new(arrow::datatypes::Schema::new(vec![
            arrow::datatypes::Field::new("id", arrow::datatypes::DataType::Int32, false),
            arrow::datatypes::Field::new("name", arrow::datatypes::DataType::Utf8, true),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int32Array::from(vec![1, 2, 3])),
                Arc::new(StringArray::from(vec![Some("a"), None, Some("c")])),
            ],
        )
        .unwrap();
        let file = std::fs::File::create(path).unwrap();
        let mut writer = ArrowWriter::try_new(file, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();

        let f = std::fs::File::open(path).unwrap();
        ParquetMetaDataReader::new()
            .with_page_indexes(true)
            .parse_and_finish(&f)
            .unwrap()
    }

    fn object_meta(file_path: &str, size: u64, mtime_ms: i64) -> ObjectMeta {
        ObjectMeta {
            location: ObjPath::from(file_path),
            last_modified: Utc.timestamp_millis_opt(mtime_ms).single().unwrap(),
            size,
            e_tag: None,
            version: None,
        }
    }

    #[test]
    fn put_then_get_roundtrips_footer_and_meta() {
        let dir = tempdir().unwrap();
        let disk = MetadataDiskCache::new(dir.path().to_str().unwrap()).unwrap();

        let pq = dir.path().join("seg.parquet");
        let metadata = make_parquet_metadata(&pq);
        let file_path = pq.to_str().unwrap();
        let om = object_meta(file_path, 4242, 1_700_000_000_000);

        disk.put(file_path, &om, &metadata);

        let (got_meta, got_pq) = disk.get(file_path).expect("disk hit expected");
        // Sidecar-reconstructed ObjectMeta matches what we stored (the is_valid_for fields).
        assert_eq!(got_meta.size, 4242);
        assert_eq!(got_meta.last_modified, om.last_modified);
        // Footer round-trips: same row count and schema column count.
        assert_eq!(
            got_pq.file_metadata().num_rows(),
            metadata.file_metadata().num_rows()
        );
        assert_eq!(
            got_pq.file_metadata().schema_descr().num_columns(),
            metadata.file_metadata().schema_descr().num_columns()
        );
    }

    #[test]
    fn get_missing_file_returns_none() {
        let dir = tempdir().unwrap();
        let disk = MetadataDiskCache::new(dir.path().to_str().unwrap()).unwrap();
        assert!(disk.get("/no/such/file.parquet").is_none());
    }

    #[test]
    fn disabled_when_spill_dir_empty() {
        assert!(MetadataDiskCache::new("").is_none());
    }

    #[test]
    fn remove_deletes_entry() {
        let dir = tempdir().unwrap();
        let disk = MetadataDiskCache::new(dir.path().to_str().unwrap()).unwrap();
        let pq = dir.path().join("seg.parquet");
        let metadata = make_parquet_metadata(&pq);
        let file_path = pq.to_str().unwrap();
        disk.put(file_path, &object_meta(file_path, 10, 1), &metadata);
        assert!(disk.get(file_path).is_some());
        disk.remove(file_path);
        assert!(disk.get(file_path).is_none());
    }

    /// Recovery: a footer written by one `MetadataDiskCache` instance is readable by a fresh
    /// instance rooted at the same spill dir — i.e. it survives a process restart (the cache dir
    /// is exempted from spill cleanup in `api::create_global_runtime`). Reload is lazy, via `get`.
    #[test]
    fn survives_reinitialization() {
        let dir = tempdir().unwrap();
        let spill = dir.path().to_str().unwrap();
        let pq = dir.path().join("seg.parquet");
        let metadata = make_parquet_metadata(&pq);
        let file_path = pq.to_str().unwrap();

        // First "process": warm the disk tier.
        {
            let disk = MetadataDiskCache::new(spill).unwrap();
            disk.put(file_path, &object_meta(file_path, 4242, 1_700_000_000_000), &metadata);
        }

        // Footer files persisted under <spill>/metadata-cache/ (the dir boot cleanup exempts).
        let cache_dir = dir.path().join(SUBDIR);
        assert!(cache_dir.is_dir(), "cache subdir must persist");

        // Second "process": a brand-new instance reads the prior footer from disk.
        let disk2 = MetadataDiskCache::new(spill).unwrap();
        let (got_meta, got_pq) = disk2.get(file_path).expect("disk hit after re-init expected");
        assert_eq!(got_meta.size, 4242);
        assert_eq!(
            got_pq.file_metadata().num_rows(),
            metadata.file_metadata().num_rows()
        );
    }
}
