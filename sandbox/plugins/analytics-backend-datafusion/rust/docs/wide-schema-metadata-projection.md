# Wide-schema parquet metadata projection — findings & design notes

> Status: **experiment / design notes — not implemented in this repo.**
> Captures a prototype done against a local arrow-rs fork to decide whether
> projecting parquet metadata decoding is worth pursuing for the composite /
> parquet-primary indexed path. Dated 2026-06-14.

## Motivation

Selective queries over **wide** parquet segments (hundreds–thousands of
columns, e.g. the textbench corpus with 375–432 columns) are disproportionately
slow even when projection pushdown and row-group pruning work correctly.

Upstream issue [apache/datafusion#21968] quantifies it: a query returning ~1k of
~12M rows runs **~11× slower** on a wide copy (681 schema paths / 163,926 column
chunks) than a narrow one (4 paths / 1,356 chunks), with identical pruning.

Cost breakdown from that issue:

| Phase | narrow → wide | Notes |
|---|---|---|
| `metadata_load_time` | 8.4 ms → 580 ms | ~3.5 µs per column chunk; **only ~10%** of the wall gap |
| unattributed serial work | — | **~420 ms**: per-column reader-state construction, schema traversal, predicate binding against the wide schema |
| `page_index_eval_time`, `bloom_filter_eval_time`, `FilterExec` | flat | unused columns don't participate once row groups are picked |

**Key implication:** most of the 11× is *downstream* of metadata decode. A
metadata-decode optimization can only address `metadata_load_time` (~10%) plus
resident metadata-cache memory — not the dominant reader-state/binding cost.

## Where we load metadata today

The indexed path loads metadata itself, so it is the natural injection point for
any projection:

- `rust/src/indexed_table/parquet_bridge.rs :: load_parquet_metadata` →
  `DFParquetMetadata::new(..).with_file_metadata_cache(..).fetch_metadata()`.
- `DFParquetMetadata::fetch_metadata` (DataFusion) forces
  `PageIndexPolicy::Optional` (full page-index decode for **all** columns)
  whenever a metadata cache is present.
- `DFParquetMetadata` exposes **no** knob to project metadata to the query's
  columns, so today we always decode + retain metadata for every column.

## Prototype (in a local arrow-rs fork — not landed)

Two opt-in knobs added to `parquet::file::metadata::ParquetMetaDataOptions`:

1. **Page-index projection** — decode the column index only for the projected
   leaf columns; others become `ColumnIndexMetaData::NONE`.
2. **Column-metadata projection** — `prot.skip()` the `ColumnMetaData` thrift
   struct for non-projected columns and push a placeholder chunk (positional
   `columns()[i]` invariant preserved).

Both validated correct on a real file (projected columns decode byte-identically
to a full decode; skipped columns are NONE / placeholder; structure intact).

## Measured results (real file: 1000 Int64 cols × 4 RG, page stats + page index)

Bench: a real `ArrowWriter` file with `EnabledStatistics::Page`, decoded via
`ParquetMetaDataPushDecoder` + `PageIndexPolicy::Required`; query keeps 4/1000
columns.

| ~50 pages/col/RG | Decode time | Retained `ParquetMetaData::memory_size()` |
|---|---|---|
| Full (all 1000 cols) | 5.50 ms | 12,381 KiB |
| **Page-index projection (4 cols)** | **3.13 ms (−43%)** | **7,615 KiB (1.6× smaller)** |
| + column-metadata projection | 3.19 ms (no extra) | 7,615 KiB (no extra) |

At ~10 pages/col the win was smaller (−33% time, 1.1× memory): **the win scales
with page-index size** (page count × stats width), so it is larger on real wide
string columns than on Int64.

### What worked vs not

- ✅ **Page-index projection is a real win** — faster decode + materially less
  retained metadata when a query needs few of many columns. This is the
  metadata-cache memory we set out to reduce.
- ❌ **Column-metadata projection buys ~nothing** — `prot.skip(Struct)` still
  walks every thrift field (compact-thrift is sequential; you can't jump past
  column N's bytes to reach N+1), so only struct *materialization* is saved,
  which is cheap. Do not pursue this path.

### Pitfall (why a naive bench shows no win)

The synthetic wide bench in arrow-rs's `parquet/benches/metadata.rs`
(`encoded_meta(false, …)`) writes column-index *offsets* but no real page index
and no stats — exactly the two things projection skips — so it cannot show a win
by construction. A real `ArrowWriter` file with page stats is required.

## Constraint & path forward

**Hard rule: no arrow-rs / datafusion fork in production.** So the win must come
through public API:

- arrow-rs 58.3.0 already exposes `ParquetMetaDataOptions` publicly with
  `ParquetStatisticsPolicy::skip_except(&[..])` for *statistics*, but **no public
  page-index projection** — that is the small upstream gap this experiment
  shows is worth proposing.
- The OpenSearch injection point (`parquet_bridge.rs::load_parquet_metadata`)
  already controls the decode, so once a public knob exists we supply the
  query's projected leaf-column set there.
- The larger ~420 ms reader-state/binding cost is a separate DataFusion-side
  problem — see [apache/datafusion#21996] (query-aware statistics).

## References

- [apache/datafusion#21968] — wide-schema slowdown (source of the numbers)
- [apache/arrow-rs#9722] — upstream epic: parquet reader scales poorly with column count
- [apache/datafusion#21996] — query-aware statistics (the downstream/binding angle)

[apache/datafusion#21968]: https://github.com/apache/datafusion/issues/21968
[apache/arrow-rs#9722]: https://github.com/apache/arrow-rs/issues/9722
[apache/datafusion#21996]: https://github.com/apache/datafusion/pull/21996
