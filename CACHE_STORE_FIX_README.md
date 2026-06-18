# `cache-store-fix` — DataFusion parquet metadata cache: store-correct warming + on-disk footer tier

Branch-scoped notes for the work on this branch. Covers what changed, why, the
two-tier cache design, the refresh/query flows (incl. eviction + restart), the
current state, and the open follow-ups.

> **Status: POC.** The first commit (store-correct cache warming) is production-intent
> and fully tested; the on-disk footer tier (commits 2–3) is a proof of concept for
> measuring the remote-IO win — see [Open items](#open-items) before promoting it.

---

## Commits on this branch

| Commit | Title | Nature |
|---|---|---|
| `085ef74` | Add store for the cache | **Fix** — cache-warm reads footers through the shard store |
| `ad97c79` | Add on-disk footer tier for metadata cache (POC) | **POC** — persist footers to local disk |
| `763f2a4` | Persist disk metadata cache across restarts (POC) | **POC** — survive restart (spill-cleanup exemption) |

Base: `upstream/main` (b53b8fe at branch time).

---

## Background: the two caches

The DataFusion backend avoids re-reading parquet footers on every query via two
node-level caches (`sandbox/plugins/analytics-backend-datafusion/rust/src/`):

| Cache | Type | Key | Value |
|---|---|---|---|
| **Metadata** | `MutexFileMetadataCache` wrapping DataFusion's `DefaultFilesMetadataCache` (`cache.rs`) | parquet file `Path` | `CachedFileMetadataEntry { meta: ObjectMeta, file_metadata: CachedParquetMetaData(Arc<ParquetMetaData>) }` |
| **Statistics** | `CustomStatisticsCache` (`statistics_cache.rs`) | parquet file `Path` | DataFusion `Statistics` |

Both are byte-bounded; the metadata cache uses **LRU** eviction
(`DefaultFilesMetadataCacheState.lru_queue`, `datafusion.metadata.cache.size.limit`,
default 1000 KB). The statistics cache uses the S3-FIFO policy from this repo's
broader cache work.

The caches are warmed on **refresh** (`onFilesAdded`) and read on **query**
(`DFParquetMetadata::fetch_metadata` / `try_acquire_budget`). DataFusion validates a
cached entry with `CachedFileMetadataEntry::is_valid_for(current_meta)` — a `size` +
`last_modified` comparison.

---

## Commit 1 — `085ef74`: warm through the shard store (the real fix)

**Bug:** the cache-warm path (`metadata_cache_put`, statistics warming) hardcoded
`LocalFileSystem::new()` + synthesized the `ObjectMeta` from `std::fs` / `Utc::now()`.
For a shard whose data lives on a **remote** store (TieredObjectStore → S3/remote),
that warm either failed or, worse, cached entries keyed on a *fabricated* `ObjectMeta`
that never matched the query path's `is_valid_for` → entries never validated, every
query re-read the footer, and on remote shards warming couldn't read the file at all.

**Fix:** warm through the **same per-index object store the reader uses**. The store
pointer is threaded from Java's `dataformatAwareStoreHandle` (the boxed
`Box<Arc<dyn ObjectStore>>` from `getFormatStoreHandle`) down through FFM into
`df_cache_manager_add_files`, and the `ObjectMeta` comes from `store.head()` — exactly
what the query path computes.

**Shared functions (single source of truth across query + warm):**

| Function | File | Used by |
|---|---|---|
| `resolve_object_store(store_ptr)` | `api.rs` | `create_reader` (query) + `df_cache_manager_add_files` (warm). `>0` → TieredObjectStore (boxed fat ptr); `0` → `LocalFileSystem`. |
| `object_meta_for_file(store, path)` | `api.rs` | `create_object_metas` (query) + `object_meta_via_store` (warm). The single `store.head()` primitive. |
| `storePointerOrDefault(handle)` | `NativeBridge.java` | `createDatafusionReader` (reader) + `DataFusionService.onFilesAdded` (warm). null/closed/EMPTY → `0`. |

`store_ptr == 0` falls back to `LocalFileSystem` in both paths (local footer reads are
cheap; this matches the reader's hot path).

> ⚠️ **Pointer-format gotcha** (cost a SIGSEGV during dev): the store handle has two
> FFM forms — a **raw thin** `*const TieredObjectStore` from `ts_create_tiered_object_store`,
> and a **boxed fat** `Box<Arc<dyn ObjectStore>>` from `ts_get_object_store_box_ptr`.
> `df_create_reader` and `df_cache_manager_add_files` both decode the **boxed fat** form;
> feeding them the raw thin pointer reads garbage as the vtable → `SIGSEGV at get_opts pc=0`.
> Production always boxes first (`getFormatStoreHandle`); tests must call
> `getObjectStoreBoxPtr` before passing the pointer.

---

## Commits 2–3 — on-disk footer tier (POC)

### Idea
When a shard has a remote store, reading a footer is remote IO. Pay it **once** in
`onFilesAdded`, serialize the decoded footer to a node-local file, and on an in-memory
miss reload from **local disk** instead of re-reading the remote store. The object store
stays the source of truth on a disk miss.

### Module: `metadata_disk_cache.rs` → `MetadataDiskCache`
- Rooted at `<spill_dir>/metadata-cache/`.
- **Physical key:** `FNV-1a(file_path)` as a 16-hex basename (arbitrary store path →
  safe fixed-length filename). Each entry is two files:
  - `<hash>.footer` — thrift-encoded `ParquetMetaData` (via `ParquetMetaDataWriter`, incl. page index).
  - `<hash>.meta` — sidecar: `size\n` + `last_modified_millis\n` (the `is_valid_for` fields).
- `put` writes atomically (tmp + rename). `get` reads the sidecar → reconstructs
  `ObjectMeta`, decodes `.footer` → `ParquetMetaData`, returns both so the caller rebuilds
  the **exact** `CachedParquetMetaData` the query path downcasts to.

### Wiring
- **Write** (`custom_cache_manager.rs` / `ffm.rs`): `metadata_cache_put` mirrors the
  footer to disk after `fetch_metadata()`, **only when `store_ptr > 0`** (remote). Local
  warms skip the disk tier (`persist_to_disk` flag).
- **Read** (`cache.rs`): `MutexFileMetadataCache::get()` is now two-tier — memory first,
  then disk on miss; a disk hit rebuilds the entry, **promotes it into memory**, and bumps
  `disk_hit_count`. Both query read sites benefit transparently (no caller changes).
- **Attach** (`api.rs`): `create_global_runtime` builds the `MetadataDiskCache` from
  `spill_dir` and attaches it via interior mutability (`OnceLock`) — the cache is created
  earlier in `df_create_cache`, before `spill_dir` is known. `None` when spill is disabled.

### Restart persistence (commit 3)
The spill-cleanup at boot renames every spill child to `.stale` and GCs it — which would
**wipe** `metadata-cache/` on every restart, before the tier is even attached. Commit 3
**exempts** the `metadata-cache` subdir (public `SUBDIR` const) from the phase-1 rename
sweep (phase-2 GC only touches `.stale`). Recovery is **lazy**: the first post-restart
query reloads from disk via the `get()` miss path — no boot-time scan.

---

## Flow: refresh / warm (`onFilesAdded`)

```
refresh ─► DatafusionReaderManager.onFilesAdded(files)
         ─► DataFusionService.onFilesAdded(files, storeHandle)
                storePtr = storePointerOrDefault(handle)
         ─► (FFM) df_cache_manager_add_files(.., store_ptr)
                persist_to_disk = (store_ptr > 0)         ◄── REMOTE stores only
         ─► CustomCacheManager.add_files → metadata_cache_put(file)
              │
              ① object_meta = store.head(path)            ◄── remote round-trip (paid once)
                 parquet_meta = DFParquetMetadata(store, object_meta).fetch_metadata()
              │
              ├─► ② IN-MEM put → memory_used += size → evict_entries():
              │        while memory_used > limit: pop LRU   ◄── RAM eviction only
              │        (entry > whole limit ⇒ rejected)
              │
              └─► ③ if persist_to_disk → DISK put:          ◄── POC
                       <hash>.footer = encode(parquet_meta)
                       <hash>.meta   = "size\nmtime\n"       (atomic; unbounded)
```

## Flow: query (read)

```
fetch_metadata() ─► IN-MEM cache.get(path)
   │
   ├─ HIT ──► is_valid_for(current_meta)?  yes ─► return (LRU→MRU)   ✔ no IO
   │                                       no  ─┐ (stale)
   │                                            │
   └─ MISS ──────────────────────────────────► │ DISK tier present?
                                                │   │
                                                │   ├─ DISK HIT ─► local read + decode
                                                │   │               build entry, PROMOTE→IN-MEM
                                                │   │               disk_hit_count++   ✔ local IO
                                                │   │
                                                │   └─ DISK MISS ─┐
                                                └─────────────────┴─► ④ STORE fallback
                                                                       store.get_ranges(path)  ◄ remote
                                                                       decode, IN-MEM put
                                                                       (⚠ does NOT write disk)
```

## Under pressure — eviction + restart

```
onFilesAdded(A)        MEM:[A]          DISK:[A]
onFilesAdded(B,C,..)   MEM:[B,C,..]     DISK:[A,B,C,..]   ◄ A LRU-evicted from RAM, kept on disk
query A: MEM miss → DISK hit → promote  MEM:[..,A]        ✔ NO remote round-trip

RESTART (metadata-cache/ exempt from spill cleanup)
boot                   MEM:[]           DISK:[A,B,C,..]   ◄ RAM empty, disk intact
query A: MEM miss → DISK hit → promote                    ✔ local read even cold
```

### Key asymmetries
1. **Disk is written only on `onFilesAdded`**, never on a query-path store fallback —
   a never-warmed footer stays MEM-only.
2. **Eviction is RAM-only** (LRU); the disk copy is what makes a later query cheap.
3. **Validation is uniform** — RAM or disk entry both go through `is_valid_for`
   (size+mtime); a stale/reused path falls back to the store.
4. **Disk tier active only when `store_ptr > 0` and spill enabled**; otherwise inert →
   behavior is the original single-tier LRU.

---

## Test coverage (all green on the data node, release `.so`, JDK 25)

- Rust unit: `metadata_disk_cache` — round-trip, missing-file miss, disabled-when-no-spill,
  remove, `survives_reinitialization` (restart). **5 tests.**
- Rust unit: cache-policy / statistics suites unaffected (**53 tests**).
- Java: `DatafusionCacheManagerTests` (13), `DataFusionServiceTests` (14),
  `DatafusionReaderManagerTests` (12) — **39 tests**, 0 failures.

---

## Open items (before promoting the disk tier past POC)

- **No deletion wiring:** `onFilesDeleted` does not yet call `MetadataDiskCache::remove()`,
  so disk entries for deleted segments linger.
- **Unbounded disk:** no size cap and no orphan reaper — `metadata-cache/` grows until
  manually cleared.
- **No backfill on query-path store reads:** only `onFilesAdded` writes disk; a footer
  first seen via a cold query (store fallback) is not persisted to disk.
- **Statistics cache is not disk-backed** — POC scope was the metadata footer only.
- **No dedicated setting:** the tier piggybacks on the spill dir; no
  enable/disable/size-cap cluster setting yet.

See also the consolidated KB: `search/datafusion/native-store-pointer-flow.md` (store
pointer formats + the SIGSEGV pattern) and `search/datafusion/metadata-cache-settings.md`.
