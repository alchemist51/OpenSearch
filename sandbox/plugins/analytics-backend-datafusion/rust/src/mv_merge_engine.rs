/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Stage 4 merge engine: streaming k-way merge of IPC state files.
//!
//! Replaces the DataFusion SQL-based `mv_merge_state` (which registers a
//! ListingTable, plans a full aggregate query, collects all batches, and
//! sorts) with a purpose-built streaming pipeline:
//!
//!   1. **IPC input validation** — schema hash, definition hash, and
//!      LexOrdering verification before any data flows.
//!   2. **SortPreservingMerge across k IPC readers** — each file is an
//!      ordered stream; the k-way merge preserves the total order without
//!      materializing all rows.
//!   3. **Adjacent-key folding accumulator** — SUM/COUNT add, MIN/MAX
//!      select for rows sharing the same full group key. Memory ∝ k cursors
//!      + 1 accumulator row, not ∝ total groups.
//!   4. **Streaming IPC output writer** — folded rows are flushed as they
//!      are produced, so output begins while input files are still being
//!      read.
//!
//! The engine is CLOSED over the state algebra: its output is a valid state
//! file that can be merged again, read by the fold reader, or finalized.

use std::cmp::Ordering;
use std::collections::BinaryHeap;
use std::fs::File;
use std::io::BufWriter;
use std::sync::Arc;

use arrow::compute::{lexsort_to_indices, SortColumn, SortOptions};
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::arrow::ArrowWriter as ParquetWriter;
use parquet::basic::Compression;
use parquet::file::properties::WriterProperties;
use arrow_array::{Array, ArrayRef, RecordBatch};
use arrow_schema::{DataType, Schema, SchemaRef};

// ── Public API ──────────────────────────────────────────────────────────

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
    let file = File::open(file_path)
        .map_err(|e| format!("state_field_names: open {file_path}: {e}"))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| format!("state_field_names: read {file_path}: {e}"))?;
    Ok(builder
        .schema()
        .fields()
        .iter()
        .map(|f| f.name().clone())
        .collect())
}

/// Validates that an IPC file's schema matches the expected schema hash and
/// that its rows are sorted according to the given ordering contract.
///
/// Returns `Ok(())` when all checks pass, or an error string describing the
/// first mismatch found.
pub fn validate_parquet_header(
    file_path: &str,
    expected_schema_hash: u64,
    ordering_indices: &[usize],
    ordering_asc: &[bool],
    ordering_nulls_first: &[bool],
) -> Result<(), String> {
    // Legacy Arrow IPC guard: fail closed with rebuild-required error.
    if file_path.ends_with(".mv.arrow") {
        return Err(format!(
            "validate_parquet_header: legacy Arrow IPC state file '{}' is no longer supported; \
             rebuild the materialized view to generate Parquet state files",
            file_path
        ));
    }

    let file = File::open(file_path)
        .map_err(|e| format!("validate_parquet_header: open {file_path}: {e}"))?;
    let builder = ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| format!("validate_parquet_header: read {file_path}: {e}"))?;
    let schema = builder.schema().clone();
    let reader = builder
        .build()
        .map_err(|e| format!("validate_parquet_header: build reader {file_path}: {e}"))?;

    // Schema hash check.
    let actual_hash = compute_schema_hash(&schema);
    if actual_hash != expected_schema_hash {
        return Err(format!(
            "validate_parquet_header: schema hash mismatch in {file_path}: \
             expected {expected_schema_hash:#x}, got {actual_hash:#x}"
        ));
    }

    // LexOrdering verification: read all batches, concatenate, check sort.
    let ordering = LexOrdering::new(ordering_indices, ordering_asc, ordering_nulls_first)?;
    let mut all_batches = Vec::new();
    for batch_result in reader {
        let batch = batch_result
            .map_err(|e| format!("validate_parquet_header: read batch in {file_path}: {e}"))?;
        all_batches.push(batch);
    }
    if all_batches.is_empty() {
        return Ok(()); // empty file is trivially sorted
    }
    let concatenated = arrow::compute::concat_batches(&all_batches[0].schema(), &all_batches)
        .map_err(|e| format!("validate_parquet_header: concat {file_path}: {e}"))?;
    if !is_sorted(&concatenated, &ordering) {
        return Err(format!(
            "validate_parquet_header: rows in {file_path} are not sorted by the declared ordering"
        ));
    }
    Ok(())
}

/// Folds k group-key-sorted IPC state files into a single sorted state file
/// using a streaming k-way merge with adjacent-key folding.
///
/// This is the replacement for `mv_merge_state` (the DataFusion SQL path).
/// The fold operations are determined by analysing the schema's data types:
///   - Integer/UInt/Float columns at group-key positions → equality check
///   - Integer/UInt columns at aggregate positions → SUM
///   - Float columns at aggregate positions → SUM
///   - Utf8/LargeUtf8 at group-key positions → equality check
///
/// `fold_ops` explicitly specifies per-column fold semantics:
///   - 0 = GROUP_KEY (equality check, carried forward)
///   - 1 = SUM (additive fold)
///   - 2 = MIN (select minimum)
///   - 3 = MAX (select maximum)
///   - 4 = COUNT (additive fold, same as SUM for merge purposes)
///
/// Returns the number of rows written to `output_file`.
///
/// The optional `agg_column_names` and `ordering_identity` parameters enable
/// cross-file validation:
///   - `agg_column_names`: when non-empty, each input file's schema must
///     contain these aggregate column names at the expected positions
///     (after the group-key columns).
///   - `ordering_identity`: when non-empty, a deterministic string encoding
///     of the ordering contract (e.g. "0:region:0:0;1:os:0:0") that is
///     compared across all input files to ensure they share the same sort
///     order.
pub fn merge_state_streams(
    state_files: &[String],
    output_file: &str,
    ordering_indices: &[usize],
    ordering_asc: &[bool],
    ordering_nulls_first: &[bool],
    fold_ops: &[u8],
) -> Result<i64, String> {
    merge_state_streams_validated(
        state_files,
        output_file,
        ordering_indices,
        ordering_asc,
        ordering_nulls_first,
        fold_ops,
        &[],  // no agg column name validation
        None, // no ordering identity validation
    )
}

/// Extended merge entry point with full cross-file validation.
///
/// This is the target for the FFI layer when `agg_names` and
/// `ordering_identity` are provided by the Java side.
pub fn merge_state_streams_validated(
    state_files: &[String],
    output_file: &str,
    ordering_indices: &[usize],
    ordering_asc: &[bool],
    ordering_nulls_first: &[bool],
    fold_ops: &[u8],
    agg_column_names: &[String],
    ordering_identity: Option<&str>,
) -> Result<i64, String> {
    if state_files.is_empty() {
        return Err("merge_state_streams: no input files".to_string());
    }

    // Open all readers and validate schemas match.
    let mut readers: Vec<parquet::arrow::arrow_reader::ParquetRecordBatchReader> =
        Vec::with_capacity(state_files.len());
    let mut reference_schema: Option<SchemaRef> = None;
    let mut reference_schema_hash: Option<u64> = None;

    for path in state_files {
        // Legacy Arrow IPC guard: fail closed with rebuild-required error.
        if path.ends_with(".mv.arrow") {
            return Err(format!(
                "merge_state_streams: legacy Arrow IPC state file '{}' is no longer supported; \
                 rebuild the materialized view to generate Parquet state files",
                path
            ));
        }

        let file =
            File::open(path).map_err(|e| format!("merge_state_streams: open {path}: {e}"))?;
        let builder = ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| format!("merge_state_streams: read {path}: {e}"))?;

        let schema = builder.schema().clone();

        // Schema hash consistency: all files must share the same schema hash.
        let file_hash = compute_schema_hash(&schema);
        match reference_schema_hash {
            None => reference_schema_hash = Some(file_hash),
            Some(ref_hash) => {
                if ref_hash != file_hash {
                    return Err(format!(
                        "merge_state_streams: schema hash mismatch in {path}: \
                         expected {ref_hash:#x}, got {file_hash:#x}"
                    ));
                }
            }
        }

        match &reference_schema {
            None => reference_schema = Some(Arc::clone(&schema)),
            Some(ref_schema) => {
                if ref_schema.fields().len() != schema.fields().len() {
                    return Err(format!(
                        "merge_state_streams: arity mismatch — {} has {} fields, expected {}",
                        path,
                        schema.fields().len(),
                        ref_schema.fields().len()
                    ));
                }
                for (i, (rf, sf)) in ref_schema
                    .fields()
                    .iter()
                    .zip(schema.fields().iter())
                    .enumerate()
                {
                    if rf.data_type() != sf.data_type() {
                        return Err(format!(
                            "merge_state_streams: type mismatch at col {i} in {path}: \
                             expected {:?}, got {:?}",
                            rf.data_type(),
                            sf.data_type()
                        ));
                    }
                }
            }
        }
        let reader = builder
            .build()
            .map_err(|e| format!("merge_state_streams: build reader {path}: {e}"))?;
        readers.push(reader);
    }

    let schema = reference_schema.unwrap();
    let ordering = LexOrdering::new(ordering_indices, ordering_asc, ordering_nulls_first)?;

    // Validate fold_ops length matches schema.
    if fold_ops.len() != schema.fields().len() {
        return Err(format!(
            "merge_state_streams: fold_ops length {} != schema field count {}",
            fold_ops.len(),
            schema.fields().len()
        ));
    }

    // Validate aggregate column names against the schema when provided.
    if !agg_column_names.is_empty() {
        let num_group_keys = ordering_indices.len();
        if agg_column_names.len() + num_group_keys > schema.fields().len() {
            return Err(format!(
                "merge_state_streams: agg_column_names ({}) + group_keys ({}) > schema fields ({})",
                agg_column_names.len(),
                num_group_keys,
                schema.fields().len()
            ));
        }
        for (i, expected_name) in agg_column_names.iter().enumerate() {
            let schema_idx = num_group_keys + i;
            if schema_idx < schema.fields().len() {
                let actual_name = schema.field(schema_idx).name();
                if actual_name != expected_name {
                    return Err(format!(
                        "merge_state_streams: aggregate column name mismatch at position {schema_idx}: \
                         expected '{expected_name}', got '{actual_name}'"
                    ));
                }
            }
        }
    }

    // Validate ordering identity when provided: recompute from the ordering
    // contract and compare to the caller-supplied identity string.
    if let Some(identity) = ordering_identity {
        if !identity.is_empty() {
            let computed = compute_ordering_identity(
                &schema,
                ordering_indices,
                ordering_asc,
                ordering_nulls_first,
            );
            if computed != identity {
                return Err(format!(
                    "merge_state_streams: ordering identity mismatch — \
                     expected '{identity}', computed '{computed}'"
                ));
            }
        }
    }

    // Parse fold operations.
    let ops: Vec<FoldOp> = fold_ops
        .iter()
        .enumerate()
        .map(|(i, &op)| FoldOp::from_wire(op, i))
        .collect::<Result<Vec<_>, _>>()?;

    // Build per-reader row cursors.
    let mut cursors: Vec<RowCursor> = Vec::with_capacity(readers.len());
    for (i, reader) in readers.into_iter().enumerate() {
        match RowCursor::new(reader, i) {
            Ok(Some(cursor)) => cursors.push(cursor),
            Ok(None) => {} // empty file, skip
            Err(e) => {
                return Err(format!(
                    "merge_state_streams: cursor init for file {i}: {e}"
                ))
            }
        }
    }

    if cursors.is_empty() {
        // All files were empty — write an empty output with the right schema.
        let out_file = File::create(output_file)
            .map_err(|e| format!("merge_state_streams: create {output_file}: {e}"))?;
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(Default::default()))
            .build();
        let writer = ParquetWriter::try_new(out_file, schema.clone(), Some(props))
            .map_err(|e| format!("merge_state_streams: writer init: {e}"))?;
        writer
            .close()
            .map_err(|e| format!("merge_state_streams: close empty: {e}"))?;
        return Ok(0);
    }

    // Initialize the min-heap for k-way merge.
    let mut heap = BinaryHeap::with_capacity(cursors.len());
    for cursor in cursors {
        heap.push(MergeEntry {
            cursor,
            ordering: ordering.clone(),
        });
    }

    // Open output writer.
    let out_file = File::create(output_file)
        .map_err(|e| format!("merge_state_streams: create {output_file}: {e}"))?;
    let buf_writer = BufWriter::with_capacity(64 * 1024, out_file);
    let props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(Default::default()))
        .build();
    let mut writer = ParquetWriter::try_new(buf_writer, schema.clone(), Some(props))
        .map_err(|e| format!("merge_state_streams: writer init: {e}"))?;

    // Streaming merge + fold loop.
    let mut accumulator: Option<Vec<ArrayRef>> = None;
    let mut rows_written: i64 = 0;
    let mut output_batch_buf: Vec<Vec<ArrayRef>> = Vec::new();
    const FLUSH_THRESHOLD: usize = 8192;

    while let Some(mut entry) = heap.pop() {
        let current_row = entry.cursor.current_row_arrays();

        match &accumulator {
            None => {
                accumulator = Some(current_row);
            }
            Some(acc) => {
                if keys_equal(acc, &current_row, &ordering) {
                    // Same key group — fold into accumulator.
                    let folded = fold_row(acc, &current_row, &ops)?;
                    accumulator = Some(folded);
                } else {
                    // Key break — emit the accumulator, start new group.
                    output_batch_buf.push(accumulator.take().unwrap());
                    if output_batch_buf.len() >= FLUSH_THRESHOLD {
                        let batch = rows_to_batch(&schema, &output_batch_buf)?;
                        writer
                            .write(&batch)
                            .map_err(|e| format!("merge_state_streams: write batch: {e}"))?;
                        rows_written += output_batch_buf.len() as i64;
                        output_batch_buf.clear();
                    }
                    accumulator = Some(current_row);
                }
            }
        }

        // Advance the cursor and re-insert into the heap if it has more rows.
        if entry.cursor.advance()? {
            heap.push(entry);
        }
    }

    // Emit the final accumulator.
    if let Some(acc) = accumulator {
        output_batch_buf.push(acc);
    }
    if !output_batch_buf.is_empty() {
        let batch = rows_to_batch(&schema, &output_batch_buf)?;
        writer
            .write(&batch)
            .map_err(|e| format!("merge_state_streams: write final batch: {e}"))?;
        rows_written += output_batch_buf.len() as i64;
    }

    writer
        .close()
        .map_err(|e| format!("merge_state_streams: close: {e}"))?;

    Ok(rows_written)
}

/// Fold a slice of single-element arrays representing adjacent equal-key rows.
/// This is the "combine" step: for each column, apply the column's fold op.
///
/// Public for FFI testing; normally called only by `merge_state_streams`.
pub fn fold_adjacent_keys(
    rows: &[Vec<ArrayRef>],
    fold_ops: &[u8],
) -> Result<Vec<ArrayRef>, String> {
    if rows.is_empty() {
        return Err("fold_adjacent_keys: empty input".to_string());
    }
    let ops: Vec<FoldOp> = fold_ops
        .iter()
        .enumerate()
        .map(|(i, &op)| FoldOp::from_wire(op, i))
        .collect::<Result<Vec<_>, _>>()?;

    let mut acc = rows[0].clone();
    for row in &rows[1..] {
        acc = fold_row(&acc, row, &ops)?;
    }
    Ok(acc)
}

// ── Schema hashing ─────────────────────────────────────────────────────

/// Computes a deterministic hash of the schema (field names, types, nullable).
/// Used for cross-file validation: all IPC state files from the same
/// definition must share the same schema hash.
pub fn compute_schema_hash(schema: &Schema) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    for field in schema.fields() {
        field.name().hash(&mut hasher);
        format!("{:?}", field.data_type()).hash(&mut hasher);
        field.is_nullable().hash(&mut hasher);
    }
    hasher.finish()
}

/// Computes a definition hash from the fold SQL string. Two definitions are
/// merge-compatible only when their definition hashes match.
pub fn compute_definition_hash(fold_sql: &str) -> u64 {
    use std::hash::{Hash, Hasher};
    let mut hasher = std::collections::hash_map::DefaultHasher::new();
    fold_sql.hash(&mut hasher);
    hasher.finish()
}

/// Computes a deterministic ordering identity string from the ordering contract
/// and schema. Format: "idx:col_name:dir:null_placement;..." where dir is 0=ASC
/// 1=DESC and null_placement is 0=NULLS_FIRST 1=NULLS_LAST.
///
/// This identity is compared across files to ensure merge-compatible ordering.
pub fn compute_ordering_identity(
    schema: &Schema,
    ordering_indices: &[usize],
    ordering_asc: &[bool],
    ordering_nulls_first: &[bool],
) -> String {
    ordering_indices
        .iter()
        .zip(ordering_asc.iter())
        .zip(ordering_nulls_first.iter())
        .map(|((&idx, &asc), &nf)| {
            let col_name = if idx < schema.fields().len() {
                schema.field(idx).name().as_str()
            } else {
                "?"
            };
            let dir = if asc { 0 } else { 1 };
            let null_place = if nf { 0 } else { 1 };
            format!("{idx}:{col_name}:{dir}:{null_place}")
        })
        .collect::<Vec<_>>()
        .join(";")
}

// ── Internal types ─────────────────────────────────────────────────────

/// Lexicographic ordering contract (column indices + sort options).
#[derive(Clone, Debug)]
struct LexOrdering {
    indices: Vec<usize>,
    options: Vec<SortOptions>,
}

impl LexOrdering {
    fn new(indices: &[usize], asc: &[bool], nulls_first: &[bool]) -> Result<Self, String> {
        if indices.len() != asc.len() || indices.len() != nulls_first.len() {
            return Err(format!(
                "LexOrdering: parallel array length mismatch ({}, {}, {})",
                indices.len(),
                asc.len(),
                nulls_first.len()
            ));
        }
        let options = asc
            .iter()
            .zip(nulls_first.iter())
            .map(|(&a, &nf)| SortOptions {
                descending: !a,
                nulls_first: nf,
            })
            .collect();
        Ok(Self {
            indices: indices.to_vec(),
            options,
        })
    }
}

/// Per-column fold operation.
#[derive(Clone, Copy, Debug)]
enum FoldOp {
    GroupKey,
    Sum,
    Min,
    Max,
    Count, // equivalent to Sum for merge
}

impl FoldOp {
    fn from_wire(wire: u8, col_idx: usize) -> Result<Self, String> {
        match wire {
            0 => Ok(FoldOp::GroupKey),
            1 => Ok(FoldOp::Sum),
            2 => Ok(FoldOp::Min),
            3 => Ok(FoldOp::Max),
            4 => Ok(FoldOp::Count),
            other => Err(format!(
                "FoldOp: unknown wire value {other} for column {col_idx}"
            )),
        }
    }
}

/// Row cursor for streaming through a Parquet file one row at a time.
/// Buffers one batch internally; advances batch-by-batch from the reader.
struct RowCursor {
    reader: parquet::arrow::arrow_reader::ParquetRecordBatchReader,
    current_batch: RecordBatch,
    row_idx: usize,
    file_idx: usize,
}

impl RowCursor {
    /// Creates a new cursor. Returns None if the file is empty (no batches or
    /// all batches are zero-row).
    fn new(
        mut reader: parquet::arrow::arrow_reader::ParquetRecordBatchReader,
        file_idx: usize,
    ) -> Result<Option<Self>, String> {
        loop {
            match reader.next() {
                Some(Ok(batch)) if batch.num_rows() > 0 => {
                    return Ok(Some(Self {
                        reader,
                        current_batch: batch,
                        row_idx: 0,
                        file_idx,
                    }));
                }
                Some(Ok(_)) => continue, // skip empty batch
                Some(Err(e)) => return Err(format!("RowCursor::new file {file_idx}: {e}")),
                None => return Ok(None), // empty file
            }
        }
    }

    /// Extracts the current row as single-element arrays (one per column).
    fn current_row_arrays(&self) -> Vec<ArrayRef> {
        (0..self.current_batch.num_columns())
            .map(|c| self.current_batch.column(c).slice(self.row_idx, 1))
            .collect()
    }

    /// Advances to the next row. Returns true if there IS a next row,
    /// false if exhausted.
    fn advance(&mut self) -> Result<bool, String> {
        self.row_idx += 1;
        if self.row_idx < self.current_batch.num_rows() {
            return Ok(true);
        }
        // Need next batch.
        loop {
            match self.reader.next() {
                Some(Ok(batch)) if batch.num_rows() > 0 => {
                    self.current_batch = batch;
                    self.row_idx = 0;
                    return Ok(true);
                }
                Some(Ok(_)) => continue,
                Some(Err(e)) => {
                    return Err(format!("RowCursor::advance file {}: {e}", self.file_idx))
                }
                None => return Ok(false),
            }
        }
    }
}

/// Wrapper for BinaryHeap: compares by ordering keys. The heap is a max-heap,
/// so we reverse the comparison to get min-first behavior.
struct MergeEntry {
    cursor: RowCursor,
    ordering: LexOrdering,
}

impl MergeEntry {
    fn key_arrays(&self) -> Vec<ArrayRef> {
        self.ordering
            .indices
            .iter()
            .map(|&i| {
                self.cursor
                    .current_batch
                    .column(i)
                    .slice(self.cursor.row_idx, 1)
            })
            .collect()
    }
}

impl PartialEq for MergeEntry {
    fn eq(&self, other: &Self) -> bool {
        compare_keys(&self.key_arrays(), &other.key_arrays(), &self.ordering) == Ordering::Equal
    }
}

impl Eq for MergeEntry {}

impl PartialOrd for MergeEntry {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for MergeEntry {
    fn cmp(&self, other: &Self) -> Ordering {
        // Reverse so the BinaryHeap pops the SMALLEST key first.
        compare_keys(&other.key_arrays(), &self.key_arrays(), &self.ordering)
    }
}

// ── Comparison / fold helpers ──────────────────────────────────────────

/// Compares two single-row key tuples lexicographically.
fn compare_keys(a: &[ArrayRef], b: &[ArrayRef], ordering: &LexOrdering) -> Ordering {
    for (i, (ka, kb)) in a.iter().zip(b.iter()).enumerate() {
        let opts = &ordering.options[i];
        let cmp = compare_single_value(ka, 0, kb, 0, opts);
        if cmp != Ordering::Equal {
            return cmp;
        }
    }
    Ordering::Equal
}

/// Compares two single values from arrays, respecting sort options.
fn compare_single_value(
    a: &ArrayRef,
    a_idx: usize,
    b: &ArrayRef,
    b_idx: usize,
    opts: &SortOptions,
) -> Ordering {
    let a_null = a.is_null(a_idx);
    let b_null = b.is_null(b_idx);

    match (a_null, b_null) {
        (true, true) => Ordering::Equal,
        (true, false) => {
            if opts.nulls_first {
                Ordering::Less
            } else {
                Ordering::Greater
            }
        }
        (false, true) => {
            if opts.nulls_first {
                Ordering::Greater
            } else {
                Ordering::Less
            }
        }
        (false, false) => {
            let cmp = compare_non_null(a, a_idx, b, b_idx);
            if opts.descending {
                cmp.reverse()
            } else {
                cmp
            }
        }
    }
}

/// Compares non-null scalar values across common Arrow data types.
fn compare_non_null(a: &ArrayRef, ai: usize, b: &ArrayRef, bi: usize) -> Ordering {
    use arrow_array::*;
    macro_rules! cmp_typed {
        ($arr_type:ty) => {{
            let va = a.as_any().downcast_ref::<$arr_type>().unwrap().value(ai);
            let vb = b.as_any().downcast_ref::<$arr_type>().unwrap().value(bi);
            va.partial_cmp(&vb).unwrap_or(Ordering::Equal)
        }};
    }
    match a.data_type() {
        DataType::Int8 => cmp_typed!(Int8Array),
        DataType::Int16 => cmp_typed!(Int16Array),
        DataType::Int32 => cmp_typed!(Int32Array),
        DataType::Int64 => cmp_typed!(Int64Array),
        DataType::UInt8 => cmp_typed!(UInt8Array),
        DataType::UInt16 => cmp_typed!(UInt16Array),
        DataType::UInt32 => cmp_typed!(UInt32Array),
        DataType::UInt64 => cmp_typed!(UInt64Array),
        DataType::Float32 => cmp_typed!(Float32Array),
        DataType::Float64 => cmp_typed!(Float64Array),
        DataType::Utf8 => {
            let va = a.as_any().downcast_ref::<StringArray>().unwrap().value(ai);
            let vb = b.as_any().downcast_ref::<StringArray>().unwrap().value(bi);
            va.cmp(vb)
        }
        DataType::LargeUtf8 => {
            let va = a
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .unwrap()
                .value(ai);
            let vb = b
                .as_any()
                .downcast_ref::<LargeStringArray>()
                .unwrap()
                .value(bi);
            va.cmp(vb)
        }
        DataType::Boolean => {
            let va = a.as_any().downcast_ref::<BooleanArray>().unwrap().value(ai);
            let vb = b.as_any().downcast_ref::<BooleanArray>().unwrap().value(bi);
            va.cmp(&vb)
        }
        _ => Ordering::Equal, // unsupported types compare equal (safe fallback)
    }
}

/// Checks if two single-row key tuples have equal keys (for grouping).
fn keys_equal(a: &[ArrayRef], b: &[ArrayRef], ordering: &LexOrdering) -> bool {
    for &idx in &ordering.indices {
        let ka = &a[idx];
        let kb = &b[idx];
        if ka.is_null(0) && kb.is_null(0) {
            continue; // both null → equal for grouping
        }
        if ka.is_null(0) || kb.is_null(0) {
            return false; // one null, one not → different
        }
        if compare_non_null(ka, 0, kb, 0) != Ordering::Equal {
            return false;
        }
    }
    true
}

/// Folds two single-row arrays per the column ops.
fn fold_row(acc: &[ArrayRef], new: &[ArrayRef], ops: &[FoldOp]) -> Result<Vec<ArrayRef>, String> {
    acc.iter()
        .zip(new.iter())
        .zip(ops.iter())
        .enumerate()
        .map(|(i, ((a, b), op))| fold_column(a, b, *op, i))
        .collect()
}

/// Folds a single column value (both are length-1 arrays).
fn fold_column(
    acc: &ArrayRef,
    new: &ArrayRef,
    op: FoldOp,
    col_idx: usize,
) -> Result<ArrayRef, String> {
    match op {
        FoldOp::GroupKey => Ok(Arc::clone(acc)), // carry the key forward
        FoldOp::Sum | FoldOp::Count => fold_sum(acc, new, col_idx),
        FoldOp::Min => fold_min(acc, new, col_idx),
        FoldOp::Max => fold_max(acc, new, col_idx),
    }
}

/// SUM fold: adds the two values (null propagation: null + x = x).
fn fold_sum(acc: &ArrayRef, new: &ArrayRef, col_idx: usize) -> Result<ArrayRef, String> {
    if acc.is_null(0) {
        return Ok(Arc::clone(new));
    }
    if new.is_null(0) {
        return Ok(Arc::clone(acc));
    }
    sum_non_null(acc, new, col_idx)
}

fn sum_non_null(acc: &ArrayRef, new: &ArrayRef, _col_idx: usize) -> Result<ArrayRef, String> {
    use arrow_array::builder::*;
    use arrow_array::*;
    macro_rules! sum_typed {
        ($arr_type:ty, $builder:ty) => {{
            let va = acc.as_any().downcast_ref::<$arr_type>().unwrap().value(0);
            let vb = new.as_any().downcast_ref::<$arr_type>().unwrap().value(0);
            let mut builder = <$builder>::with_capacity(1);
            builder.append_value(va + vb);
            Ok(Arc::new(builder.finish()) as ArrayRef)
        }};
    }
    match acc.data_type() {
        DataType::Int8 => sum_typed!(Int8Array, Int8Builder),
        DataType::Int16 => sum_typed!(Int16Array, Int16Builder),
        DataType::Int32 => sum_typed!(Int32Array, Int32Builder),
        DataType::Int64 => sum_typed!(Int64Array, Int64Builder),
        DataType::UInt8 => sum_typed!(UInt8Array, UInt8Builder),
        DataType::UInt16 => sum_typed!(UInt16Array, UInt16Builder),
        DataType::UInt32 => sum_typed!(UInt32Array, UInt32Builder),
        DataType::UInt64 => sum_typed!(UInt64Array, UInt64Builder),
        DataType::Float32 => sum_typed!(Float32Array, Float32Builder),
        DataType::Float64 => sum_typed!(Float64Array, Float64Builder),
        dt => Err(format!("fold_sum: unsupported type {:?}", dt)),
    }
}

/// MIN fold: selects the smaller of two values (null is "infinity").
fn fold_min(acc: &ArrayRef, new: &ArrayRef, _col_idx: usize) -> Result<ArrayRef, String> {
    if acc.is_null(0) {
        return Ok(Arc::clone(new));
    }
    if new.is_null(0) {
        return Ok(Arc::clone(acc));
    }
    if compare_non_null(acc, 0, new, 0) == Ordering::Greater {
        Ok(Arc::clone(new))
    } else {
        Ok(Arc::clone(acc))
    }
}

/// MAX fold: selects the larger of two values (null is "-infinity").
fn fold_max(acc: &ArrayRef, new: &ArrayRef, _col_idx: usize) -> Result<ArrayRef, String> {
    if acc.is_null(0) {
        return Ok(Arc::clone(new));
    }
    if new.is_null(0) {
        return Ok(Arc::clone(acc));
    }
    if compare_non_null(acc, 0, new, 0) == Ordering::Less {
        Ok(Arc::clone(new))
    } else {
        Ok(Arc::clone(acc))
    }
}

/// Checks if a batch is sorted according to the ordering contract.
fn is_sorted(batch: &RecordBatch, ordering: &LexOrdering) -> bool {
    if batch.num_rows() <= 1 {
        return true;
    }
    let sort_columns: Vec<SortColumn> = ordering
        .indices
        .iter()
        .zip(ordering.options.iter())
        .map(|(&idx, &opts)| SortColumn {
            values: Arc::clone(batch.column(idx)),
            options: Some(opts),
        })
        .collect();
    match lexsort_to_indices(&sort_columns, None) {
        Ok(indices) => (0..indices.len()).all(|i| indices.value(i) == i as u32),
        Err(_) => false,
    }
}

/// Converts a buffer of single-row arrays into a columnar RecordBatch.
fn rows_to_batch(schema: &SchemaRef, rows: &[Vec<ArrayRef>]) -> Result<RecordBatch, String> {
    if rows.is_empty() {
        return Err("rows_to_batch: empty input".to_string());
    }
    let num_cols = rows[0].len();
    let columns: Vec<ArrayRef> = (0..num_cols)
        .map(|col_idx| {
            let col_arrays: Vec<&dyn Array> =
                rows.iter().map(|row| row[col_idx].as_ref()).collect();
            arrow::compute::concat(&col_arrays)
                .map_err(|e| format!("rows_to_batch col {col_idx}: {e}"))
        })
        .collect::<Result<Vec<_>, _>>()?;
    RecordBatch::try_new(Arc::clone(schema), columns).map_err(|e| format!("rows_to_batch: {e}"))
}

// ── Tests ──────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use arrow_array::{Float64Array, Int64Array, UInt64Array};
    use arrow_schema::Field;
    use std::sync::Arc;
    use tempfile::TempDir;

    fn state_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("RegionID", DataType::Int64, true),
            Field::new("adv_sum", DataType::Int64, true),
            Field::new("cnt", DataType::Int64, true),
            Field::new("avg_cnt", DataType::UInt64, true),
            Field::new("avg_sum", DataType::Float64, true),
        ]))
    }

    fn write_state_file(path: &str, rows: &[(i64, i64, i64, u64, f64)]) {
        let schema = state_schema();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.0).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.1).collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    rows.iter().map(|r| r.2).collect::<Vec<_>>(),
                )),
                Arc::new(UInt64Array::from(
                    rows.iter().map(|r| r.3).collect::<Vec<_>>(),
                )),
                Arc::new(Float64Array::from(
                    rows.iter().map(|r| r.4).collect::<Vec<_>>(),
                )),
            ],
        )
        .unwrap();
        let file = File::create(path).unwrap();
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(Default::default()))
            .build();
        let mut w = ParquetWriter::try_new(file, batch.schema(), Some(props)).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
    }

    fn read_rows(path: &str) -> Vec<(i64, i64, i64, u64, f64)> {
        let file = File::open(path).unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        let mut out = vec![];
        for batch in reader {
            let b = batch.unwrap();
            let c0 = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            let c1 = b.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
            let c2 = b.column(2).as_any().downcast_ref::<Int64Array>().unwrap();
            let c3 = b.column(3).as_any().downcast_ref::<UInt64Array>().unwrap();
            let c4 = b.column(4).as_any().downcast_ref::<Float64Array>().unwrap();
            for i in 0..b.num_rows() {
                out.push((
                    c0.value(i),
                    c1.value(i),
                    c2.value(i),
                    c3.value(i),
                    c4.value(i),
                ));
            }
        }
        out.sort_by_key(|r| r.0);
        out
    }

    // fold_ops: col 0 = GROUP_KEY, cols 1-4 = SUM
    fn default_fold_ops() -> Vec<u8> {
        vec![0, 1, 1, 1, 1]
    }

    fn default_ordering() -> (Vec<usize>, Vec<bool>, Vec<bool>) {
        (vec![0], vec![true], vec![true])
    }

    #[test]
    fn test_merge_folds_and_is_closed() {
        let dir = TempDir::new().unwrap();
        let p = |n: &str| dir.path().join(n).to_str().unwrap().to_string();

        write_state_file(
            &p("g1.parquet"),
            &[(2, 3, 1, 1, 1440.0), (229, 2, 2, 2, 3288.0)],
        );
        write_state_file(
            &p("g2.parquet"),
            &[(7, 0, 1, 1, 800.0), (229, 7, 1, 1, 1366.0)],
        );
        write_state_file(&p("g3.parquet"), &[(229, 1, 4, 4, 5000.0)]);

        let (oi, oa, onf) = default_ordering();
        let ops = default_fold_ops();

        let rows = merge_state_streams(
            &[p("g1.parquet"), p("g2.parquet"), p("g3.parquet")],
            &p("merged.parquet"),
            &oi,
            &oa,
            &onf,
            &ops,
        )
        .unwrap();

        assert_eq!(rows, 3, "three distinct regions");
        let merged = read_rows(&p("merged.parquet"));
        assert_eq!(
            merged,
            vec![
                (2, 3, 1, 1, 1440.0),
                (7, 0, 1, 1, 800.0),
                (229, 10, 7, 7, 9654.0),
            ]
        );
    }

    #[test]
    fn test_merge_closure_associativity() {
        let dir = TempDir::new().unwrap();
        let p = |n: &str| dir.path().join(n).to_str().unwrap().to_string();

        write_state_file(
            &p("g1.parquet"),
            &[(2, 3, 1, 1, 1440.0), (229, 2, 2, 2, 3288.0)],
        );
        write_state_file(
            &p("g2.parquet"),
            &[(7, 0, 1, 1, 800.0), (229, 7, 1, 1, 1366.0)],
        );
        write_state_file(&p("g3.parquet"), &[(229, 1, 4, 4, 5000.0)]);

        let (oi, oa, onf) = default_ordering();
        let ops = default_fold_ops();

        // 3-way merge
        merge_state_streams(
            &[p("g1.parquet"), p("g2.parquet"), p("g3.parquet")],
            &p("merged.parquet"),
            &oi,
            &oa,
            &onf,
            &ops,
        )
        .unwrap();

        // Incremental: (g1⊕g2)⊕g3
        merge_state_streams(
            &[p("g1.parquet"), p("g2.parquet")],
            &p("m12.parquet"),
            &oi,
            &oa,
            &onf,
            &ops,
        )
        .unwrap();
        merge_state_streams(
            &[p("m12.parquet"), p("g3.parquet")],
            &p("m123.parquet"),
            &oi,
            &oa,
            &onf,
            &ops,
        )
        .unwrap();

        assert_eq!(
            read_rows(&p("m123.parquet")),
            read_rows(&p("merged.parquet")),
            "merge must be associative-in-effect"
        );
    }

    #[test]
    fn test_merge_idempotent_on_folded() {
        let dir = TempDir::new().unwrap();
        let p = |n: &str| dir.path().join(n).to_str().unwrap().to_string();

        write_state_file(
            &p("g1.parquet"),
            &[(2, 3, 1, 1, 1440.0), (229, 2, 2, 2, 3288.0)],
        );

        let (oi, oa, onf) = default_ordering();
        let ops = default_fold_ops();

        merge_state_streams(&[p("g1.parquet")], &p("m_self.parquet"), &oi, &oa, &onf, &ops).unwrap();

        assert_eq!(
            read_rows(&p("m_self.parquet")),
            read_rows(&p("g1.parquet")),
            "idempotent on already-folded input"
        );
    }

    #[test]
    fn test_validate_parquet_header_passes_valid_file() {
        let dir = TempDir::new().unwrap();
        let p = dir.path().join("valid.parquet").to_str().unwrap().to_string();

        write_state_file(&p, &[(1, 10, 1, 1, 100.0), (2, 20, 2, 2, 200.0)]);

        let schema = state_schema();
        let hash = compute_schema_hash(&schema);
        let result = validate_parquet_header(&p, hash, &[0], &[true], &[true]);
        assert!(result.is_ok(), "valid file should pass: {:?}", result);
    }

    #[test]
    fn test_validate_parquet_header_rejects_wrong_hash() {
        let dir = TempDir::new().unwrap();
        let p = dir.path().join("valid.parquet").to_str().unwrap().to_string();

        write_state_file(&p, &[(1, 10, 1, 1, 100.0)]);

        let result = validate_parquet_header(&p, 0xDEADBEEF, &[0], &[true], &[true]);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("schema hash mismatch"));
    }

    #[test]
    fn test_validate_parquet_header_rejects_unsorted() {
        let dir = TempDir::new().unwrap();
        let path = dir
            .path()
            .join("unsorted.parquet")
            .to_str()
            .unwrap()
            .to_string();

        // Write unsorted data intentionally
        let schema = state_schema();
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(Int64Array::from(vec![229, 2, 7])), // NOT sorted
                Arc::new(Int64Array::from(vec![1, 2, 3])),
                Arc::new(Int64Array::from(vec![1, 1, 1])),
                Arc::new(UInt64Array::from(vec![1u64, 1, 1])),
                Arc::new(Float64Array::from(vec![1.0, 2.0, 3.0])),
            ],
        )
        .unwrap();
        let file = File::create(&path).unwrap();
        let mut w = ParquetWriter::try_new(file, batch.schema(), Some(WriterProperties::builder().set_compression(Compression::ZSTD(Default::default())).build())).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();

        let hash = compute_schema_hash(&schema);
        let result = validate_parquet_header(&path, hash, &[0], &[true], &[true]);
        assert!(result.is_err());
        assert!(result.unwrap_err().contains("not sorted"));
    }

    #[test]
    fn test_fold_adjacent_keys_basic() {
        let row1: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![10])),
            Arc::new(Int64Array::from(vec![2])),
        ];
        let row2: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![1])),
            Arc::new(Int64Array::from(vec![20])),
            Arc::new(Int64Array::from(vec![3])),
        ];
        let result = fold_adjacent_keys(
            &[row1, row2],
            &[0, 1, 1], // key, sum, sum
        )
        .unwrap();

        assert_eq!(
            result[0]
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            1
        );
        assert_eq!(
            result[1]
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            30
        );
        assert_eq!(
            result[2]
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap()
                .value(0),
            5
        );
    }

    #[test]
    fn test_min_max_fold() {
        let dir = TempDir::new().unwrap();
        let p = |n: &str| dir.path().join(n).to_str().unwrap().to_string();

        // Schema with min/max columns
        let schema = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int64, false),
            Field::new("val_sum", DataType::Int64, false),
            Field::new("val_min", DataType::Int64, false),
            Field::new("val_max", DataType::Int64, false),
        ]));

        let write = |path: &str, data: &[(i64, i64, i64, i64)]| {
            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(
                        data.iter().map(|r| r.0).collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        data.iter().map(|r| r.1).collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        data.iter().map(|r| r.2).collect::<Vec<_>>(),
                    )),
                    Arc::new(Int64Array::from(
                        data.iter().map(|r| r.3).collect::<Vec<_>>(),
                    )),
                ],
            )
            .unwrap();
            let file = File::create(path).unwrap();
            let mut w = ParquetWriter::try_new(file, batch.schema(), Some(WriterProperties::builder().set_compression(Compression::ZSTD(Default::default())).build())).unwrap();
            w.write(&batch).unwrap();
            w.close().unwrap();
        };

        write(&p("f1.parquet"), &[(1, 10, 5, 15), (2, 20, 8, 25)]);
        write(&p("f2.parquet"), &[(1, 30, 3, 20), (2, 40, 12, 18)]);

        // fold_ops: key=0, sum=1, min=2, max=3
        let rows = merge_state_streams(
            &[p("f1.parquet"), p("f2.parquet")],
            &p("out.parquet"),
            &[0],
            &[true],
            &[true],
            &[0, 1, 2, 3],
        )
        .unwrap();

        assert_eq!(rows, 2);

        let reader = ParquetRecordBatchReaderBuilder::try_new(File::open(&p("out.parquet")).unwrap())
            .unwrap()
            .build()
            .unwrap();
        for batch in reader {
            let b = batch.unwrap();
            let keys = b.column(0).as_any().downcast_ref::<Int64Array>().unwrap();
            let sums = b.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
            let mins = b.column(2).as_any().downcast_ref::<Int64Array>().unwrap();
            let maxs = b.column(3).as_any().downcast_ref::<Int64Array>().unwrap();
            // key=1: sum=40, min=3, max=20
            assert_eq!(keys.value(0), 1);
            assert_eq!(sums.value(0), 40);
            assert_eq!(mins.value(0), 3);
            assert_eq!(maxs.value(0), 20);
            // key=2: sum=60, min=8, max=25
            assert_eq!(keys.value(1), 2);
            assert_eq!(sums.value(1), 60);
            assert_eq!(mins.value(1), 8);
            assert_eq!(maxs.value(1), 25);
        }
    }

    #[test]
    fn test_empty_file_merge() {
        let dir = TempDir::new().unwrap();
        let p = |n: &str| dir.path().join(n).to_str().unwrap().to_string();

        // Write an empty file.
        let schema = state_schema();
        let file = File::create(&p("empty.parquet")).unwrap();
        let mut w = ParquetWriter::try_new(file, schema.clone(), Some(WriterProperties::builder().set_compression(Compression::ZSTD(Default::default())).build())).unwrap();
        w.close().unwrap();

        let (oi, oa, onf) = default_ordering();
        let ops = default_fold_ops();
        let rows = merge_state_streams(&[p("empty.parquet")], &p("out.parquet"), &oi, &oa, &onf, &ops)
            .unwrap();
        assert_eq!(rows, 0);
    }

    #[test]
    fn test_schema_hash_determinism() {
        let schema = state_schema();
        let h1 = compute_schema_hash(&schema);
        let h2 = compute_schema_hash(&schema);
        assert_eq!(h1, h2, "schema hash must be deterministic");
    }

    #[test]
    fn test_definition_hash_determinism() {
        let sql = "SELECT key, SUM(val) FROM t GROUP BY key";
        let h1 = compute_definition_hash(sql);
        let h2 = compute_definition_hash(sql);
        assert_eq!(h1, h2, "definition hash must be deterministic");

        let h3 = compute_definition_hash("SELECT key, COUNT(val) FROM t GROUP BY key");
        assert_ne!(h1, h3, "different SQL should produce different hashes");
    }

    #[test]
    fn test_schema_mismatch_rejected() {
        let dir = TempDir::new().unwrap();
        let p = |n: &str| dir.path().join(n).to_str().unwrap().to_string();

        // File 1 with 5-column schema
        write_state_file(&p("f1.parquet"), &[(1, 10, 1, 1, 100.0)]);

        // File 2 with different schema (3 columns)
        let schema2 = Arc::new(Schema::new(vec![
            Field::new("key", DataType::Int64, false),
            Field::new("val", DataType::Int64, false),
            Field::new("extra", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            schema2,
            vec![
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(Int64Array::from(vec![10])),
                Arc::new(Int64Array::from(vec![5])),
            ],
        )
        .unwrap();
        let file = File::create(&p("f2.parquet")).unwrap();
        let mut w = ParquetWriter::try_new(file, batch.schema(), Some(WriterProperties::builder().set_compression(Compression::ZSTD(Default::default())).build())).unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();

        let result = merge_state_streams(
            &[p("f1.parquet"), p("f2.parquet")],
            &p("out.parquet"),
            &[0],
            &[true],
            &[true],
            &[0, 1, 1, 1, 1],
        );
        assert!(result.is_err());
        // Schema hash check fires before the arity check because different
        // column counts produce different hashes.
        let err = result.unwrap_err();
        assert!(
            err.contains("schema hash mismatch") || err.contains("arity mismatch"),
            "expected schema-hash or arity error, got: {err}"
        );
    }

    #[test]
    fn test_merge_validated_with_agg_column_names() {
        let dir = TempDir::new().unwrap();
        let p = |n: &str| dir.path().join(n).to_str().unwrap().to_string();

        write_state_file(
            &p("g1.parquet"),
            &[(2, 3, 1, 1, 1440.0), (229, 2, 2, 2, 3288.0)],
        );
        write_state_file(
            &p("g2.parquet"),
            &[(7, 0, 1, 1, 800.0), (229, 7, 1, 1, 1366.0)],
        );

        let agg_names = vec![
            "adv_sum".to_string(),
            "cnt".to_string(),
            "avg_cnt".to_string(),
            "avg_sum".to_string(),
        ];

        let rows = merge_state_streams_validated(
            &[p("g1.parquet"), p("g2.parquet")],
            &p("merged.parquet"),
            &[0],
            &[true],
            &[true],
            &[0, 1, 1, 1, 1],
            &agg_names,
            None,
        )
        .unwrap();

        assert_eq!(rows, 3);
    }

    #[test]
    fn test_merge_validated_rejects_wrong_agg_column_names() {
        let dir = TempDir::new().unwrap();
        let p = |n: &str| dir.path().join(n).to_str().unwrap().to_string();

        write_state_file(&p("g1.parquet"), &[(2, 3, 1, 1, 1440.0)]);

        let wrong_agg_names = vec![
            "wrong_name".to_string(),
            "cnt".to_string(),
            "avg_cnt".to_string(),
            "avg_sum".to_string(),
        ];

        let result = merge_state_streams_validated(
            &[p("g1.parquet")],
            &p("out.parquet"),
            &[0],
            &[true],
            &[true],
            &[0, 1, 1, 1, 1],
            &wrong_agg_names,
            None,
        );

        assert!(result.is_err());
        assert!(result
            .unwrap_err()
            .contains("aggregate column name mismatch"));
    }

    #[test]
    fn test_merge_validated_with_ordering_identity() {
        let dir = TempDir::new().unwrap();
        let p = |n: &str| dir.path().join(n).to_str().unwrap().to_string();

        write_state_file(
            &p("g1.parquet"),
            &[(2, 3, 1, 1, 1440.0), (229, 2, 2, 2, 3288.0)],
        );

        // The schema has RegionID at index 0, ASC, NULLS_FIRST
        let identity = "0:RegionID:0:0";

        let rows = merge_state_streams_validated(
            &[p("g1.parquet")],
            &p("merged.parquet"),
            &[0],
            &[true],
            &[true],
            &[0, 1, 1, 1, 1],
            &[],
            Some(identity),
        )
        .unwrap();

        assert_eq!(rows, 2);
    }

    #[test]
    fn test_merge_validated_rejects_wrong_ordering_identity() {
        let dir = TempDir::new().unwrap();
        let p = |n: &str| dir.path().join(n).to_str().unwrap().to_string();

        write_state_file(&p("g1.parquet"), &[(2, 3, 1, 1, 1440.0)]);

        let wrong_identity = "0:RegionID:1:1"; // DESC NULLS_LAST, but data is ASC NULLS_FIRST

        let result = merge_state_streams_validated(
            &[p("g1.parquet")],
            &p("out.parquet"),
            &[0],
            &[true],
            &[true],
            &[0, 1, 1, 1, 1],
            &[],
            Some(wrong_identity),
        );

        assert!(result.is_err());
        assert!(result.unwrap_err().contains("ordering identity mismatch"));
    }

    #[test]
    fn test_compute_ordering_identity() {
        let schema = state_schema();
        let identity = compute_ordering_identity(&schema, &[0], &[true], &[true]);
        assert_eq!(identity, "0:RegionID:0:0");

        // Multi-key ordering
        let identity2 = compute_ordering_identity(&schema, &[0, 1], &[true, false], &[true, false]);
        assert_eq!(identity2, "0:RegionID:0:0;1:adv_sum:1:1");
    }

    #[test]
    fn test_schema_hash_consistency_across_files() {
        let dir = TempDir::new().unwrap();
        let p = |n: &str| dir.path().join(n).to_str().unwrap().to_string();

        write_state_file(&p("g1.parquet"), &[(1, 10, 1, 1, 100.0)]);
        write_state_file(&p("g2.parquet"), &[(2, 20, 2, 2, 200.0)]);

        // Both files use the same schema, so merge should succeed
        let rows = merge_state_streams_validated(
            &[p("g1.parquet"), p("g2.parquet")],
            &p("out.parquet"),
            &[0],
            &[true],
            &[true],
            &[0, 1, 1, 1, 1],
            &[],
            None,
        )
        .unwrap();

        assert_eq!(rows, 2);
    }
}
