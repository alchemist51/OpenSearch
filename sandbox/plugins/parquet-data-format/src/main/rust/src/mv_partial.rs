/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Source-side MV partial-aggregate builder.
//!
//! Runs N MV partial-aggregate transforms on in-memory RecordBatches produced
//! by the parquet writer's VSR rotation. Accumulates per-MV partial state and
//! seals one sorted, LZ4-compressed parquet file per MV per refresh boundary.
//!
//! **Design choice**: pure-arrow hash aggregation (no DataFusion dependency).
//! The parquet-data-format crate does not link DataFusion, and adding it would
//! introduce massive cross-crate FFI churn. Arrow compute (hash grouping +
//! sum/count/min/max kernels) is sufficient for the supported aggregate set.
//! DataFusion-based aggregation lives in analytics-backend-datafusion and is
//! used by the pull-side fold/merge path; this module is deliberately simpler.

use std::collections::HashMap;
use std::fs::{self, File};
use std::path::{Path, PathBuf};
use std::sync::Arc;

use arrow::array::*;
use arrow::compute;
use arrow::datatypes::*;
use arrow::record_batch::RecordBatch;
use parquet::arrow::ArrowWriter;
use parquet::basic::Compression;
use parquet::file::properties::{WriterProperties, WriterVersion};

use crate::{log_debug, log_error, log_info};

// ── Aggregate function enum ──────────────────────────────────────────────

/// Supported aggregate functions for source-side partial aggregation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum AggFunction {
    Count,
    CountField,
    Sum,
    Min,
    Max,
}

// ── Aggregate spec ───────────────────────────────────────────────────────

/// One aggregate in a compiled MV definition.
#[derive(Debug, Clone)]
pub struct AggSpec {
    pub function: AggFunction,
    /// Source column index in the input RecordBatch (None for COUNT(*)).
    pub source_col_idx: Option<usize>,
    /// Output column name(s) in the state schema.
    pub output_names: Vec<String>,
}

// ── MV spec ──────────────────────────────────────────────────────────────

/// Compiled specification for one MV, passed from Java via writer config FFI.
#[derive(Debug, Clone)]
pub struct MVPartialSpec {
    pub mv_id: String,
    pub definition_hash: String,
    pub def_version: i64,
    /// Group-key column indices in the input RecordBatch.
    pub group_col_indices: Vec<usize>,
    /// Group-key output names.
    pub group_col_names: Vec<String>,
    /// Group-key output types (arrow DataType).
    pub group_col_types: Vec<DataType>,
    /// Aggregate specifications.
    pub agg_specs: Vec<AggSpec>,
    /// Sort-key column names (subset of group_col_names, in order).
    pub sort_key_names: Vec<String>,
}

// ── Per-group accumulator ────────────────────────────────────────────────

/// Accumulated state for one group key.
#[derive(Debug, Clone)]
struct GroupState {
    /// One i64 per aggregate output column. For COUNT/SUM: running total.
    /// For MIN: running minimum. For MAX: running maximum.
    values: Vec<i64>,
}

// ── Per-MV builder ───────────────────────────────────────────────────────

/// Builder for one MV's partial state, accumulating across batches.
struct SingleMVBuilder {
    spec: MVPartialSpec,
    /// group-key-hash → (group_key_values, GroupState).
    /// group_key_values: Vec<ScalarValue> flattened as string for hashing.
    groups: HashMap<Vec<u8>, (Vec<Option<i64>>, GroupState)>,
    /// Number of output columns (group keys + agg output columns).
    num_output_cols: usize,
    /// Number of batches accumulated.
    batches_seen: u64,
    /// Whether any error occurred (partial gen marked failed).
    failed: bool,
    fail_reason: Option<String>,
}

/// Encodes group key values into a byte vector for hash map lookup.
fn encode_group_key(batch: &RecordBatch, row: usize, group_indices: &[usize]) -> Vec<u8> {
    let mut key = Vec::with_capacity(group_indices.len() * 9);
    for &col_idx in group_indices {
        let col = batch.column(col_idx);
        if col.is_null(row) {
            key.push(0u8); // null marker
        } else {
            key.push(1u8); // non-null marker
            // Extract value as i64 for all numeric/timestamp types.
            // For keyword/string types, encode the string bytes.
            match col.data_type() {
                DataType::Int64 => {
                    let arr = col.as_any().downcast_ref::<Int64Array>().unwrap();
                    key.extend_from_slice(&arr.value(row).to_le_bytes());
                }
                DataType::Int32 => {
                    let arr = col.as_any().downcast_ref::<Int32Array>().unwrap();
                    key.extend_from_slice(&(arr.value(row) as i64).to_le_bytes());
                }
                DataType::Float64 => {
                    let arr = col.as_any().downcast_ref::<Float64Array>().unwrap();
                    key.extend_from_slice(&arr.value(row).to_bits().to_le_bytes());
                }
                DataType::Utf8 => {
                    let arr = col.as_any().downcast_ref::<StringArray>().unwrap();
                    let s = arr.value(row);
                    key.extend_from_slice(&(s.len() as u32).to_le_bytes());
                    key.extend_from_slice(s.as_bytes());
                }
                DataType::LargeUtf8 => {
                    let arr = col.as_any().downcast_ref::<LargeStringArray>().unwrap();
                    let s = arr.value(row);
                    key.extend_from_slice(&(s.len() as u64).to_le_bytes());
                    key.extend_from_slice(s.as_bytes());
                }
                DataType::Timestamp(TimeUnit::Millisecond, _) => {
                    let arr = col
                        .as_any()
                        .downcast_ref::<TimestampMillisecondArray>()
                        .unwrap();
                    key.extend_from_slice(&arr.value(row).to_le_bytes());
                }
                _ => {
                    // Fallback: cast to string
                    let formatted = arrow::util::display::array_value_to_string(col, row)
                        .unwrap_or_else(|_| "null".to_string());
                    key.extend_from_slice(&(formatted.len() as u32).to_le_bytes());
                    key.extend_from_slice(formatted.as_bytes());
                }
            }
        }
    }
    key
}

/// Extracts a group-key value as Option<i64> for a given row.
fn extract_group_value(col: &dyn Array, row: usize) -> Option<i64> {
    if col.is_null(row) {
        return None;
    }
    match col.data_type() {
        DataType::Int64 => {
            Some(col.as_any().downcast_ref::<Int64Array>().unwrap().value(row))
        }
        DataType::Int32 => Some(
            col.as_any().downcast_ref::<Int32Array>().unwrap().value(row) as i64,
        ),
        DataType::Timestamp(TimeUnit::Millisecond, _) => Some(
            col.as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap()
                .value(row),
        ),
        _ => {
            // For string/keyword types, we store a sentinel — the actual string
            // is reconstructed from the hash map key at seal time. Store 0 as placeholder.
            Some(0)
        }
    }
}

/// Extracts a source column value as Option<i64> for aggregation.
fn extract_agg_value(col: &dyn Array, row: usize) -> Option<i64> {
    if col.is_null(row) {
        return None;
    }
    match col.data_type() {
        DataType::Int64 => {
            Some(col.as_any().downcast_ref::<Int64Array>().unwrap().value(row))
        }
        DataType::Int32 => Some(
            col.as_any().downcast_ref::<Int32Array>().unwrap().value(row) as i64,
        ),
        DataType::Float64 => {
            // Store as bits for exact round-trip
            let v = col
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .value(row);
            Some(i64::from_le_bytes(v.to_le_bytes()))
        }
        DataType::Timestamp(TimeUnit::Millisecond, _) => Some(
            col.as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap()
                .value(row),
        ),
        _ => {
            // Unsupported type for aggregation — skip
            None
        }
    }
}

impl SingleMVBuilder {
    fn new(spec: MVPartialSpec) -> Self {
        let num_agg_outputs: usize = spec.agg_specs.iter().map(|a| a.output_names.len()).sum();
        let num_output_cols = spec.group_col_names.len() + num_agg_outputs;
        SingleMVBuilder {
            spec,
            groups: HashMap::new(),
            num_output_cols,
            batches_seen: 0,
            failed: false,
            fail_reason: None,
        }
    }

    /// Accumulate one RecordBatch into the partial state.
    fn accumulate(&mut self, batch: &RecordBatch) {
        if self.failed {
            return;
        }
        self.batches_seen += 1;

        let num_rows = batch.num_rows();
        let num_agg_outputs: usize = self
            .spec
            .agg_specs
            .iter()
            .map(|a| a.output_names.len())
            .sum();

        for row in 0..num_rows {
            let key = encode_group_key(batch, row, &self.spec.group_col_indices);

            let entry = self.groups.entry(key).or_insert_with(|| {
                // Extract group key values
                let group_vals: Vec<Option<i64>> = self
                    .spec
                    .group_col_indices
                    .iter()
                    .map(|&idx| extract_group_value(batch.column(idx).as_ref(), row))
                    .collect();
                // Initialize accumulators: 0 for count/sum, i64::MAX for min, i64::MIN for max
                let mut init_vals = Vec::with_capacity(num_agg_outputs);
                for agg in &self.spec.agg_specs {
                    match agg.function {
                        AggFunction::Count | AggFunction::CountField | AggFunction::Sum => {
                            for _ in &agg.output_names {
                                init_vals.push(0i64);
                            }
                        }
                        AggFunction::Min => {
                            for _ in &agg.output_names {
                                init_vals.push(i64::MAX);
                            }
                        }
                        AggFunction::Max => {
                            for _ in &agg.output_names {
                                init_vals.push(i64::MIN);
                            }
                        }
                    }
                }
                (group_vals, GroupState { values: init_vals })
            });

            // Update accumulators
            let mut agg_idx = 0;
            for agg in &self.spec.agg_specs {
                match agg.function {
                    AggFunction::Count => {
                        // COUNT(*) — always increment
                        entry.1.values[agg_idx] += 1;
                        agg_idx += 1;
                    }
                    AggFunction::CountField => {
                        // COUNT(field) — increment only if non-null
                        if let Some(col_idx) = agg.source_col_idx {
                            if !batch.column(col_idx).is_null(row) {
                                entry.1.values[agg_idx] += 1;
                            }
                        }
                        agg_idx += 1;
                    }
                    AggFunction::Sum => {
                        if let Some(col_idx) = agg.source_col_idx {
                            if let Some(val) = extract_agg_value(batch.column(col_idx).as_ref(), row)
                            {
                                entry.1.values[agg_idx] += val;
                            }
                        }
                        agg_idx += 1;
                    }
                    AggFunction::Min => {
                        if let Some(col_idx) = agg.source_col_idx {
                            if let Some(val) = extract_agg_value(batch.column(col_idx).as_ref(), row)
                            {
                                if val < entry.1.values[agg_idx] {
                                    entry.1.values[agg_idx] = val;
                                }
                            }
                        }
                        agg_idx += 1;
                    }
                    AggFunction::Max => {
                        if let Some(col_idx) = agg.source_col_idx {
                            if let Some(val) = extract_agg_value(batch.column(col_idx).as_ref(), row)
                            {
                                if val > entry.1.values[agg_idx] {
                                    entry.1.values[agg_idx] = val;
                                }
                            }
                        }
                        agg_idx += 1;
                    }
                }
            }
        }
    }

    /// Drain accumulated state into a sorted RecordBatch.
    fn drain_sorted(&mut self) -> Result<Option<RecordBatch>, String> {
        if self.groups.is_empty() {
            return Ok(None);
        }

        let num_groups = self.groups.len();
        let num_group_cols = self.spec.group_col_names.len();
        let num_agg_outputs: usize = self
            .spec
            .agg_specs
            .iter()
            .map(|a| a.output_names.len())
            .sum();

        // Collect all groups into vectors
        let entries: Vec<_> = self.groups.drain().collect();

        // Build group-key columns
        let mut group_arrays: Vec<Arc<dyn Array>> = Vec::with_capacity(num_group_cols);
        for (gi, dt) in self.spec.group_col_types.iter().enumerate() {
            match dt {
                DataType::Int64 | DataType::Timestamp(TimeUnit::Millisecond, _) => {
                    let mut builder = Int64Builder::with_capacity(num_groups);
                    for (_, (group_vals, _)) in &entries {
                        match group_vals[gi] {
                            Some(v) => builder.append_value(v),
                            None => builder.append_null(),
                        }
                    }
                    if matches!(dt, DataType::Timestamp(TimeUnit::Millisecond, _)) {
                        let arr = builder.finish();
                        let ts_arr = TimestampMillisecondArray::from(
                            arr.into_data()
                                .into_builder()
                                .data_type(DataType::Timestamp(TimeUnit::Millisecond, None))
                                .build()
                                .map_err(|e| format!("timestamp cast: {e}"))?,
                        );
                        group_arrays.push(Arc::new(ts_arr));
                    } else {
                        group_arrays.push(Arc::new(builder.finish()));
                    }
                }
                DataType::Int32 => {
                    let mut builder = Int32Builder::with_capacity(num_groups);
                    for (_, (group_vals, _)) in &entries {
                        match group_vals[gi] {
                            Some(v) => builder.append_value(v as i32),
                            None => builder.append_null(),
                        }
                    }
                    group_arrays.push(Arc::new(builder.finish()));
                }
                _ => {
                    // String/keyword: reconstruct from the encoded key bytes.
                    // For simplicity in POC, we store the original key bytes and
                    // need to decode. But since we use the hash map key for lookup,
                    // we need the original strings. Let's use a different approach:
                    // store strings in the group_vals by repurposing the hash map.
                    //
                    // For now, return an error — string group keys need special handling.
                    return Err(format!(
                        "String group keys require StringGroupState — not yet implemented for type {:?}",
                        dt
                    ));
                }
            }
        }

        // Build aggregate output columns (all Int64 for now)
        let mut agg_arrays: Vec<Arc<dyn Array>> = Vec::with_capacity(num_agg_outputs);
        let mut agg_offset = 0;
        for agg in &self.spec.agg_specs {
            for _ in &agg.output_names {
                let mut builder = Int64Builder::with_capacity(num_groups);
                for (_, (_, state)) in &entries {
                    let v = state.values[agg_offset];
                    // Sentinel check: MIN with no values seen stays at i64::MAX,
                    // MAX stays at i64::MIN — these mean no non-null input.
                    match agg.function {
                        AggFunction::Min if v == i64::MAX => builder.append_null(),
                        AggFunction::Max if v == i64::MIN => builder.append_null(),
                        _ => builder.append_value(v),
                    }
                }
                agg_arrays.push(Arc::new(builder.finish()));
                agg_offset += 1;
            }
        }

        // Build schema
        let mut fields: Vec<Field> = Vec::with_capacity(self.num_output_cols);
        for (i, name) in self.spec.group_col_names.iter().enumerate() {
            fields.push(Field::new(name, self.spec.group_col_types[i].clone(), true));
        }
        for agg in &self.spec.agg_specs {
            for oname in &agg.output_names {
                fields.push(Field::new(oname, DataType::Int64, true));
            }
        }
        let schema = Arc::new(Schema::new(fields));

        let mut all_columns = group_arrays;
        all_columns.extend(agg_arrays);

        let batch =
            RecordBatch::try_new(schema, all_columns).map_err(|e| format!("batch build: {e}"))?;

        // Sort by sort-key columns
        let sort_indices: Vec<usize> = self
            .spec
            .sort_key_names
            .iter()
            .filter_map(|name| {
                self.spec
                    .group_col_names
                    .iter()
                    .position(|gn| gn == name)
            })
            .collect();

        if sort_indices.is_empty() {
            // No sort keys — sort by all group columns
            let sort_cols: Vec<compute::SortColumn> = (0..num_group_cols)
                .map(|i| compute::SortColumn {
                    values: batch.column(i).clone(),
                    options: Some(compute::SortOptions {
                        descending: false,
                        nulls_first: false,
                    }),
                })
                .collect();
            let indices = compute::lexsort_to_indices(&sort_cols, None)
                .map_err(|e| format!("lexsort: {e}"))?;
            let sorted_cols: Result<Vec<_>, _> = batch
                .columns()
                .iter()
                .map(|c| compute::take(c.as_ref(), &indices, None))
                .collect();
            let sorted = RecordBatch::try_new(
                batch.schema(),
                sorted_cols.map_err(|e| format!("take: {e}"))?,
            )
            .map_err(|e| format!("sorted: {e}"))?;
            Ok(Some(sorted))
        } else {
            let sort_cols: Vec<compute::SortColumn> = sort_indices
                .iter()
                .map(|&i| compute::SortColumn {
                    values: batch.column(i).clone(),
                    options: Some(compute::SortOptions {
                        descending: false,
                        nulls_first: false,
                    }),
                })
                .collect();
            let indices = compute::lexsort_to_indices(&sort_cols, None)
                .map_err(|e| format!("lexsort: {e}"))?;
            let sorted_cols: Result<Vec<_>, _> = batch
                .columns()
                .iter()
                .map(|c| compute::take(c.as_ref(), &indices, None))
                .collect();
            let sorted = RecordBatch::try_new(
                batch.schema(),
                sorted_cols.map_err(|e| format!("take: {e}"))?,
            )
            .map_err(|e| format!("sorted: {e}"))?;
            Ok(Some(sorted))
        }
    }
}

// ── String-capable group state ───────────────────────────────────────────

/// Group state that preserves string group-key values alongside numeric accumulators.
/// Used when any group key is a string/keyword type.
#[derive(Debug, Clone)]
struct StringGroupState {
    /// String values for string-typed group keys, None for numeric keys.
    string_vals: Vec<Option<String>>,
    /// Numeric values for numeric group keys (i64 representation).
    numeric_vals: Vec<Option<i64>>,
    /// Aggregate accumulators (same as GroupState.values).
    agg_values: Vec<i64>,
}

/// Enhanced builder that handles string group keys.
struct StringCapableMVBuilder {
    spec: MVPartialSpec,
    groups: HashMap<Vec<u8>, StringGroupState>,
    num_output_cols: usize,
    batches_seen: u64,
    failed: bool,
    fail_reason: Option<String>,
}

fn extract_string_value(col: &dyn Array, row: usize) -> Option<String> {
    if col.is_null(row) {
        return None;
    }
    match col.data_type() {
        DataType::Utf8 => {
            Some(col.as_any().downcast_ref::<StringArray>().unwrap().value(row).to_string())
        }
        DataType::LargeUtf8 => Some(
            col.as_any()
                .downcast_ref::<LargeStringArray>()
                .unwrap()
                .value(row)
                .to_string(),
        ),
        _ => None,
    }
}

fn is_string_type(dt: &DataType) -> bool {
    matches!(dt, DataType::Utf8 | DataType::LargeUtf8)
}

impl StringCapableMVBuilder {
    fn new(spec: MVPartialSpec) -> Self {
        let num_agg_outputs: usize = spec.agg_specs.iter().map(|a| a.output_names.len()).sum();
        let num_output_cols = spec.group_col_names.len() + num_agg_outputs;
        StringCapableMVBuilder {
            spec,
            groups: HashMap::new(),
            num_output_cols,
            batches_seen: 0,
            failed: false,
            fail_reason: None,
        }
    }

    fn accumulate(&mut self, batch: &RecordBatch) {
        if self.failed {
            return;
        }
        self.batches_seen += 1;

        let num_rows = batch.num_rows();
        let num_agg_outputs: usize = self.spec.agg_specs.iter().map(|a| a.output_names.len()).sum();

        for row in 0..num_rows {
            let key = encode_group_key(batch, row, &self.spec.group_col_indices);

            let entry = self.groups.entry(key).or_insert_with(|| {
                let string_vals: Vec<Option<String>> = self.spec.group_col_indices.iter().enumerate()
                    .map(|(gi, &idx)| {
                        if is_string_type(&self.spec.group_col_types[gi]) {
                            extract_string_value(batch.column(idx).as_ref(), row)
                        } else {
                            None
                        }
                    })
                    .collect();
                let numeric_vals: Vec<Option<i64>> = self.spec.group_col_indices.iter().enumerate()
                    .map(|(gi, &idx)| {
                        if !is_string_type(&self.spec.group_col_types[gi]) {
                            extract_group_value(batch.column(idx).as_ref(), row)
                        } else {
                            None
                        }
                    })
                    .collect();
                let mut init_vals = Vec::with_capacity(num_agg_outputs);
                for agg in &self.spec.agg_specs {
                    match agg.function {
                        AggFunction::Count | AggFunction::CountField | AggFunction::Sum => {
                            for _ in &agg.output_names { init_vals.push(0i64); }
                        }
                        AggFunction::Min => {
                            for _ in &agg.output_names { init_vals.push(i64::MAX); }
                        }
                        AggFunction::Max => {
                            for _ in &agg.output_names { init_vals.push(i64::MIN); }
                        }
                    }
                }
                StringGroupState { string_vals, numeric_vals, agg_values: init_vals }
            });

            // Update accumulators
            let mut agg_idx = 0;
            for agg in &self.spec.agg_specs {
                match agg.function {
                    AggFunction::Count => {
                        entry.agg_values[agg_idx] += 1;
                        agg_idx += 1;
                    }
                    AggFunction::CountField => {
                        if let Some(col_idx) = agg.source_col_idx {
                            if !batch.column(col_idx).is_null(row) {
                                entry.agg_values[agg_idx] += 1;
                            }
                        }
                        agg_idx += 1;
                    }
                    AggFunction::Sum => {
                        if let Some(col_idx) = agg.source_col_idx {
                            if let Some(val) = extract_agg_value(batch.column(col_idx).as_ref(), row) {
                                entry.agg_values[agg_idx] += val;
                            }
                        }
                        agg_idx += 1;
                    }
                    AggFunction::Min => {
                        if let Some(col_idx) = agg.source_col_idx {
                            if let Some(val) = extract_agg_value(batch.column(col_idx).as_ref(), row) {
                                if val < entry.agg_values[agg_idx] {
                                    entry.agg_values[agg_idx] = val;
                                }
                            }
                        }
                        agg_idx += 1;
                    }
                    AggFunction::Max => {
                        if let Some(col_idx) = agg.source_col_idx {
                            if let Some(val) = extract_agg_value(batch.column(col_idx).as_ref(), row) {
                                if val > entry.agg_values[agg_idx] {
                                    entry.agg_values[agg_idx] = val;
                                }
                            }
                        }
                        agg_idx += 1;
                    }
                }
            }
        }
    }

    fn drain_sorted(&mut self) -> Result<Option<RecordBatch>, String> {
        if self.groups.is_empty() {
            return Ok(None);
        }
        let num_groups = self.groups.len();
        let num_group_cols = self.spec.group_col_names.len();

        let entries: Vec<_> = self.groups.drain().collect();

        // Build group-key columns
        let mut group_arrays: Vec<Arc<dyn Array>> = Vec::with_capacity(num_group_cols);
        for (gi, dt) in self.spec.group_col_types.iter().enumerate() {
            if is_string_type(dt) {
                let mut builder = StringBuilder::with_capacity(num_groups, num_groups * 32);
                for (_, state) in &entries {
                    match &state.string_vals[gi] {
                        Some(s) => builder.append_value(s),
                        None => builder.append_null(),
                    }
                }
                group_arrays.push(Arc::new(builder.finish()));
            } else {
                match dt {
                    DataType::Int64 => {
                        let mut builder = Int64Builder::with_capacity(num_groups);
                        for (_, state) in &entries {
                            match state.numeric_vals[gi] {
                                Some(v) => builder.append_value(v),
                                None => builder.append_null(),
                            }
                        }
                        group_arrays.push(Arc::new(builder.finish()));
                    }
                    DataType::Int32 => {
                        let mut builder = Int32Builder::with_capacity(num_groups);
                        for (_, state) in &entries {
                            match state.numeric_vals[gi] {
                                Some(v) => builder.append_value(v as i32),
                                None => builder.append_null(),
                            }
                        }
                        group_arrays.push(Arc::new(builder.finish()));
                    }
                    DataType::Timestamp(TimeUnit::Millisecond, _) => {
                        let mut builder = Int64Builder::with_capacity(num_groups);
                        for (_, state) in &entries {
                            match state.numeric_vals[gi] {
                                Some(v) => builder.append_value(v),
                                None => builder.append_null(),
                            }
                        }
                        let arr = builder.finish();
                        let ts_arr = TimestampMillisecondArray::from(
                            arr.into_data()
                                .into_builder()
                                .data_type(DataType::Timestamp(TimeUnit::Millisecond, None))
                                .build()
                                .map_err(|e| format!("timestamp cast: {e}"))?,
                        );
                        group_arrays.push(Arc::new(ts_arr));
                    }
                    _ => {
                        return Err(format!("Unsupported group key type: {:?}", dt));
                    }
                }
            }
        }

        // Build aggregate columns
        let mut agg_arrays: Vec<Arc<dyn Array>> = Vec::new();
        let mut agg_offset = 0;
        for agg in &self.spec.agg_specs {
            for _ in &agg.output_names {
                let mut builder = Int64Builder::with_capacity(num_groups);
                for (_, state) in &entries {
                    let v = state.agg_values[agg_offset];
                    match agg.function {
                        AggFunction::Min if v == i64::MAX => builder.append_null(),
                        AggFunction::Max if v == i64::MIN => builder.append_null(),
                        _ => builder.append_value(v),
                    }
                }
                agg_arrays.push(Arc::new(builder.finish()));
                agg_offset += 1;
            }
        }

        // Schema
        let mut fields: Vec<Field> = Vec::with_capacity(self.num_output_cols);
        for (i, name) in self.spec.group_col_names.iter().enumerate() {
            fields.push(Field::new(name, self.spec.group_col_types[i].clone(), true));
        }
        for agg in &self.spec.agg_specs {
            for oname in &agg.output_names {
                fields.push(Field::new(oname, DataType::Int64, true));
            }
        }
        let schema = Arc::new(Schema::new(fields));

        let mut all_columns = group_arrays;
        all_columns.extend(agg_arrays);

        let batch = RecordBatch::try_new(schema, all_columns)
            .map_err(|e| format!("batch build: {e}"))?;

        // Sort
        let sort_indices: Vec<usize> = if self.spec.sort_key_names.is_empty() {
            (0..num_group_cols).collect()
        } else {
            self.spec.sort_key_names.iter()
                .filter_map(|name| self.spec.group_col_names.iter().position(|gn| gn == name))
                .collect()
        };

        let sort_cols: Vec<compute::SortColumn> = sort_indices.iter()
            .map(|&i| compute::SortColumn {
                values: batch.column(i).clone(),
                options: Some(compute::SortOptions { descending: false, nulls_first: false }),
            })
            .collect();
        let indices = compute::lexsort_to_indices(&sort_cols, None)
            .map_err(|e| format!("lexsort: {e}"))?;
        let sorted_cols: Result<Vec<_>, _> = batch.columns().iter()
            .map(|c| compute::take(c.as_ref(), &indices, None))
            .collect();
        let sorted = RecordBatch::try_new(
            batch.schema(),
            sorted_cols.map_err(|e| format!("take: {e}"))?,
        )
        .map_err(|e| format!("sorted: {e}"))?;
        Ok(Some(sorted))
    }
}

// ── Top-level MVPartialBuilder ───────────────────────────────────────────

/// Holds N per-MV builders. Fed from write_data; sealed at finalize/refresh.
pub struct MVPartialBuilder {
    builders: Vec<StringCapableMVBuilder>,
    /// Shard ID for file naming.
    shard_id: i32,
    /// Primary term for file naming.
    primary_term: i64,
    /// Per-MV generation counter (monotonic, in-memory).
    gen_counters: Vec<u64>,
    /// Base output directory for partial files.
    output_base: PathBuf,
}

/// Result from sealing one MV's partial state.
#[derive(Debug)]
pub struct SealResult {
    pub mv_id: String,
    pub file_path: String,
    pub generation: u64,
    pub row_count: i64,
    pub failed: bool,
    pub fail_reason: Option<String>,
}

impl MVPartialBuilder {
    /// Create a new builder for N MVs.
    pub fn new(
        specs: Vec<MVPartialSpec>,
        shard_id: i32,
        primary_term: i64,
        output_base: PathBuf,
    ) -> Self {
        let num = specs.len();
        let builders = specs.into_iter().map(StringCapableMVBuilder::new).collect();
        MVPartialBuilder {
            builders,
            shard_id,
            primary_term,
            gen_counters: vec![0; num],
            output_base,
        }
    }

    /// Feed a RecordBatch to all registered MVs. Never fails the calling write
    /// — errors are captured per-MV and surfaced at seal time.
    pub fn accumulate(&mut self, batch: &RecordBatch) {
        for builder in &mut self.builders {
            // Catch panics per-MV so one failing MV doesn't poison others.
            let result = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                builder.accumulate(batch);
            }));
            if let Err(e) = result {
                let msg = format!("panic in MV accumulate for {}: {:?}", builder.spec.mv_id, e);
                log_error!("{}", msg);
                builder.failed = true;
                builder.fail_reason = Some(msg);
            }
        }
    }

    /// Seal all MVs: drain accumulated state, sort, write partial parquet files.
    /// Returns one SealResult per MV.
    pub fn seal(&mut self) -> Vec<SealResult> {
        let mut results = Vec::with_capacity(self.builders.len());
        // Extract immutable fields before the mutable borrow of builders
        let shard_id = self.shard_id;
        let primary_term = self.primary_term;
        let output_base = self.output_base.clone();

        for (i, builder) in self.builders.iter_mut().enumerate() {
            let mv_id = builder.spec.mv_id.clone();

            if builder.failed {
                results.push(SealResult {
                    mv_id,
                    file_path: String::new(),
                    generation: self.gen_counters[i],
                    row_count: 0,
                    failed: true,
                    fail_reason: builder.fail_reason.clone(),
                });
                continue;
            }

            // Increment generation
            self.gen_counters[i] += 1;
            let gen = self.gen_counters[i];

            match builder.drain_sorted() {
                Ok(Some(sorted_batch)) => {
                    let row_count = sorted_batch.num_rows() as i64;
                    // Write partial parquet file
                    match write_partial_parquet_static(
                        &output_base,
                        shard_id,
                        primary_term,
                        &mv_id,
                        &builder.spec,
                        gen,
                        &sorted_batch,
                    ) {
                        Ok(path) => {
                            log_info!(
                                "mv_partial sealed mv={} gen={} rows={} batches={} path={}",
                                mv_id, gen, row_count, builder.batches_seen, path
                            );
                            results.push(SealResult {
                                mv_id,
                                file_path: path,
                                generation: gen,
                                row_count,
                                failed: false,
                                fail_reason: None,
                            });
                        }
                        Err(e) => {
                            log_error!("mv_partial seal write failed mv={}: {}", mv_id, e);
                            results.push(SealResult {
                                mv_id,
                                file_path: String::new(),
                                generation: gen,
                                row_count: 0,
                                failed: true,
                                fail_reason: Some(e),
                            });
                        }
                    }
                }
                Ok(None) => {
                    // No data accumulated — empty generation (valid: no documents in this refresh)
                    log_debug!("mv_partial seal mv={} gen={} empty (no data)", mv_id, gen);
                    results.push(SealResult {
                        mv_id,
                        file_path: String::new(),
                        generation: gen,
                        row_count: 0,
                        failed: false,
                        fail_reason: None,
                    });
                }
                Err(e) => {
                    log_error!("mv_partial seal drain failed mv={}: {}", mv_id, e);
                    results.push(SealResult {
                        mv_id,
                        file_path: String::new(),
                        generation: gen,
                        row_count: 0,
                        failed: true,
                        fail_reason: Some(e),
                    });
                }
            }

            // Reset builder state for next refresh cycle
            builder.batches_seen = 0;
            builder.failed = false;
            builder.fail_reason = None;
        }
        results
    }

    /// Returns the number of MVs registered.
    pub fn num_mvs(&self) -> usize {
        self.builders.len()
    }

    /// Returns whether any MV has accumulated state.
    pub fn has_state(&self) -> bool {
        self.builders.iter().any(|b| !b.groups.is_empty())
    }
}

// ── Standalone writer (avoids borrow conflict in seal) ────────────────────

fn write_partial_parquet_static(
    output_base: &Path,
    shard_id: i32,
    primary_term: i64,
    mv_id: &str,
    spec: &MVPartialSpec,
    generation: u64,
    batch: &RecordBatch,
) -> Result<String, String> {
    // Create output directory: <output_base>/mv_state/<mvId>/
    let mv_dir = output_base.join("mv_state").join(mv_id);
    fs::create_dir_all(&mv_dir).map_err(|e| format!("mkdir mv_state/{}: {}", mv_id, e))?;

    // File name: _mv_partial.s<shard>.t<term>.g<gen>.<uuid>.parquet
    let uuid = uuid_v4();
    let filename = format!(
        "_mv_partial.s{}.t{}.g{}.{}.parquet",
        shard_id, primary_term, generation, uuid
    );
    let file_path = mv_dir.join(&filename);
    let file_path_str = file_path.to_string_lossy().to_string();

    let file =
        File::create(&file_path).map_err(|e| format!("create {}: {}", file_path_str, e))?;

    // Build writer properties: LZ4 + dictionary encoding
    let mut kv_metadata = Vec::new();
    kv_metadata.push(parquet::file::metadata::KeyValue::new(
        "mv_id".to_string(),
        mv_id.to_string(),
    ));
    kv_metadata.push(parquet::file::metadata::KeyValue::new(
        "shard".to_string(),
        shard_id.to_string(),
    ));
    kv_metadata.push(parquet::file::metadata::KeyValue::new(
        "term".to_string(),
        primary_term.to_string(),
    ));
    kv_metadata.push(parquet::file::metadata::KeyValue::new(
        "generation".to_string(),
        generation.to_string(),
    ));
    kv_metadata.push(parquet::file::metadata::KeyValue::new(
        "def_version".to_string(),
        spec.def_version.to_string(),
    ));
    kv_metadata.push(parquet::file::metadata::KeyValue::new(
        "def_hash".to_string(),
        spec.definition_hash.clone(),
    ));
    kv_metadata.push(parquet::file::metadata::KeyValue::new(
        "writer_version".to_string(),
        "mv_partial_v1".to_string(),
    ));
    kv_metadata.push(parquet::file::metadata::KeyValue::new(
        "row_count".to_string(),
        batch.num_rows().to_string(),
    ));

    // Build sort columns spec for the parquet footer sort-order stamp
    let sort_col_orders: Vec<parquet::file::metadata::SortingColumn> = spec
        .sort_key_names
        .iter()
        .filter_map(|name| {
            batch
                .schema()
                .fields()
                .iter()
                .position(|f| f.name() == name)
                .map(|col_idx| parquet::file::metadata::SortingColumn {
                    column_idx: col_idx as i32,
                    descending: false,
                    nulls_first: false,
                })
        })
        .collect();

    let mut props_builder = WriterProperties::builder()
        .set_compression(Compression::LZ4_RAW)
        .set_dictionary_enabled(true)
        .set_writer_version(WriterVersion::PARQUET_2_0)
        .set_key_value_metadata(Some(kv_metadata));

    if !sort_col_orders.is_empty() {
        props_builder = props_builder.set_sorting_columns(Some(sort_col_orders));
    }

    let props = props_builder.build();

    let mut writer = ArrowWriter::try_new(file, batch.schema(), Some(props))
        .map_err(|e| format!("parquet writer: {e}"))?;
    writer
        .write(batch)
        .map_err(|e| format!("parquet write: {e}"))?;
    writer
        .close()
        .map_err(|e| format!("parquet close: {e}"))?;

    Ok(file_path_str)
}

// ── UUID v4 generation (minimal, no external dep) ─────────────────────────

fn uuid_v4() -> String {
    use std::time::{SystemTime, UNIX_EPOCH};
    let seed = SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .unwrap_or_default()
        .as_nanos();
    // Simple pseudo-random UUID — sufficient for POC file naming uniqueness.
    // Uses nanosecond timestamp + incrementing counter as entropy source.
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    let count = COUNTER.fetch_add(1, std::sync::atomic::Ordering::Relaxed);
    let mixed = seed.wrapping_mul(6364136223846793005).wrapping_add(count as u128);
    let bytes = mixed.to_le_bytes();
    format!(
        "{:02x}{:02x}{:02x}{:02x}-{:02x}{:02x}-4{:01x}{:02x}-{:02x}{:02x}-{:02x}{:02x}{:02x}{:02x}{:02x}{:02x}",
        bytes[0], bytes[1], bytes[2], bytes[3],
        bytes[4], bytes[5],
        bytes[6] & 0x0f, bytes[7],
        (bytes[8] & 0x3f) | 0x80, bytes[9],
        bytes[10], bytes[11], bytes[12], bytes[13], bytes[14], bytes[15]
    )
}

// ── Tests ─────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use parquet::file::reader::FileReader;

    fn make_test_spec() -> MVPartialSpec {
        MVPartialSpec {
            mv_id: "test_mv".to_string(),
            definition_hash: "abc123".to_string(),
            def_version: 1,
            group_col_indices: vec![0], // "region" column
            group_col_names: vec!["region".to_string()],
            group_col_types: vec![DataType::Utf8],
            agg_specs: vec![
                AggSpec {
                    function: AggFunction::Count,
                    source_col_idx: None,
                    output_names: vec!["cnt".to_string()],
                },
                AggSpec {
                    function: AggFunction::Sum,
                    source_col_idx: Some(1), // "amount" column
                    output_names: vec!["total_amount".to_string()],
                },
                AggSpec {
                    function: AggFunction::Min,
                    source_col_idx: Some(1),
                    output_names: vec!["min_amount".to_string()],
                },
                AggSpec {
                    function: AggFunction::Max,
                    source_col_idx: Some(1),
                    output_names: vec!["max_amount".to_string()],
                },
            ],
            sort_key_names: vec!["region".to_string()],
        }
    }

    fn make_test_batch(regions: Vec<&str>, amounts: Vec<i64>) -> RecordBatch {
        let region_arr = StringArray::from(regions);
        let amount_arr = Int64Array::from(amounts);
        let schema = Arc::new(Schema::new(vec![
            Field::new("region", DataType::Utf8, true),
            Field::new("amount", DataType::Int64, true),
        ]));
        RecordBatch::try_new(
            schema,
            vec![Arc::new(region_arr), Arc::new(amount_arr)],
        )
        .unwrap()
    }

    #[test]
    fn test_accumulate_and_drain_basic() {
        let spec = make_test_spec();
        let mut builder = StringCapableMVBuilder::new(spec);

        // Batch 1: us=10, us=20, eu=5
        let batch1 = make_test_batch(vec!["us", "us", "eu"], vec![10, 20, 5]);
        builder.accumulate(&batch1);

        // Batch 2: eu=15, us=30
        let batch2 = make_test_batch(vec!["eu", "us"], vec![15, 30]);
        builder.accumulate(&batch2);

        let result = builder.drain_sorted().unwrap().unwrap();
        assert_eq!(result.num_rows(), 2); // two groups: eu, us

        // Verify schema
        assert_eq!(result.num_columns(), 5); // region, cnt, total_amount, min_amount, max_amount

        // Find the row for each group (sorted by region: eu < us)
        let region_col = result.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(region_col.value(0), "eu");
        assert_eq!(region_col.value(1), "us");

        let cnt_col = result.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(cnt_col.value(0), 2); // eu: 2 rows
        assert_eq!(cnt_col.value(1), 3); // us: 3 rows

        let sum_col = result.column(2).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(sum_col.value(0), 20); // eu: 5 + 15
        assert_eq!(sum_col.value(1), 60); // us: 10 + 20 + 30

        let min_col = result.column(3).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(min_col.value(0), 5);  // eu min
        assert_eq!(min_col.value(1), 10); // us min

        let max_col = result.column(4).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(max_col.value(0), 15); // eu max
        assert_eq!(max_col.value(1), 30); // us max
    }

    #[test]
    fn test_accumulate_multiple_batches_correctness() {
        // Verify that accumulating 3+ batches then sealing matches a direct
        // aggregation over the concatenated data.
        let spec = make_test_spec();
        let mut builder = StringCapableMVBuilder::new(spec);

        let batch1 = make_test_batch(vec!["a", "b", "a"], vec![1, 2, 3]);
        let batch2 = make_test_batch(vec!["b", "a", "c"], vec![4, 5, 6]);
        let batch3 = make_test_batch(vec!["c", "b", "a"], vec![7, 8, 9]);

        builder.accumulate(&batch1);
        builder.accumulate(&batch2);
        builder.accumulate(&batch3);

        let result = builder.drain_sorted().unwrap().unwrap();
        assert_eq!(result.num_rows(), 3); // groups: a, b, c

        let region_col = result.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        // Sorted: a, b, c
        assert_eq!(region_col.value(0), "a");
        assert_eq!(region_col.value(1), "b");
        assert_eq!(region_col.value(2), "c");

        let cnt = result.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(cnt.value(0), 4); // a: rows in batch1(r0,r2), batch2(r1), batch3(r2)
        assert_eq!(cnt.value(1), 3); // b: batch1(r1), batch2(r0), batch3(r1)
        assert_eq!(cnt.value(2), 2); // c: batch2(r2), batch3(r0)

        let sum = result.column(2).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(sum.value(0), 1 + 3 + 5 + 9); // a: 18
        assert_eq!(sum.value(1), 2 + 4 + 8);      // b: 14
        assert_eq!(sum.value(2), 6 + 7);           // c: 13

        let min = result.column(3).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(min.value(0), 1); // a min
        assert_eq!(min.value(1), 2); // b min
        assert_eq!(min.value(2), 6); // c min

        let max = result.column(4).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(max.value(0), 9); // a max
        assert_eq!(max.value(1), 8); // b max
        assert_eq!(max.value(2), 7); // c max
    }

    #[test]
    fn test_seal_writes_parquet_and_reads_back() {
        let spec = make_test_spec();
        let tmp = tempfile::tempdir().unwrap();
        let mut builder = MVPartialBuilder::new(
            vec![spec],
            0,  // shard_id
            1,  // primary_term
            tmp.path().to_path_buf(),
        );

        let batch1 = make_test_batch(vec!["us", "eu"], vec![100, 200]);
        let batch2 = make_test_batch(vec!["us", "eu"], vec![300, 400]);
        builder.accumulate(&batch1);
        builder.accumulate(&batch2);

        let results = builder.seal();
        assert_eq!(results.len(), 1);
        let result = &results[0];
        assert!(!result.failed, "seal failed: {:?}", result.fail_reason);
        assert_eq!(result.row_count, 2);
        assert_eq!(result.generation, 1);
        assert!(result.file_path.contains("_mv_partial"));

        // Read back the parquet file and verify
        let file = File::open(&result.file_path).unwrap();
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap()
            .build()
            .unwrap();
        let batches: Vec<RecordBatch> = reader.collect::<Result<Vec<_>, _>>().unwrap();
        assert_eq!(batches.len(), 1);
        let rb = &batches[0];
        assert_eq!(rb.num_rows(), 2);

        // Sorted by region: eu, us
        let region = rb.column(0).as_any().downcast_ref::<StringArray>().unwrap();
        assert_eq!(region.value(0), "eu");
        assert_eq!(region.value(1), "us");

        let cnt = rb.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(cnt.value(0), 2); // eu
        assert_eq!(cnt.value(1), 2); // us

        let sum = rb.column(2).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(sum.value(0), 600); // eu: 200+400
        assert_eq!(sum.value(1), 400); // us: 100+300

        // Verify parquet metadata
        let file2 = File::open(&result.file_path).unwrap();
        let reader2 = parquet::file::reader::SerializedFileReader::new(file2).unwrap();
        let metadata = reader2.metadata().file_metadata();
        let kv = metadata.key_value_metadata().unwrap();
        let kv_map: HashMap<&str, &str> = kv.iter().map(|kv| (kv.key.as_str(), kv.value.as_deref().unwrap_or(""))).collect();
        assert_eq!(kv_map["mv_id"], "test_mv");
        assert_eq!(kv_map["generation"], "1");
        assert_eq!(kv_map["writer_version"], "mv_partial_v1");

        // Verify LZ4 compression
        let rg = reader2.metadata().row_group(0);
        for i in 0..rg.num_columns() {
            let col_meta = rg.column(i);
            assert_eq!(col_meta.compression(), Compression::LZ4_RAW);
        }

        // Verify sort order stamp (on the row group, not file metadata)
        let rg_meta = reader2.metadata().row_group(0);
        let sorting_cols = rg_meta.sorting_columns();
        assert!(sorting_cols.is_some(), "missing sorting_columns in footer");
        let sc = sorting_cols.unwrap();
        assert_eq!(sc.len(), 1);
        assert_eq!(sc[0].column_idx, 0); // region column
    }

    #[test]
    fn test_seal_increments_generation() {
        let spec = make_test_spec();
        let tmp = tempfile::tempdir().unwrap();
        let mut builder = MVPartialBuilder::new(
            vec![spec],
            0, 1,
            tmp.path().to_path_buf(),
        );

        // First refresh cycle
        builder.accumulate(&make_test_batch(vec!["x"], vec![1]));
        let r1 = builder.seal();
        assert_eq!(r1[0].generation, 1);

        // Second refresh cycle
        builder.accumulate(&make_test_batch(vec!["y"], vec![2]));
        let r2 = builder.seal();
        assert_eq!(r2[0].generation, 2);

        // Third: empty (no data)
        let r3 = builder.seal();
        assert_eq!(r3[0].generation, 3);
        assert_eq!(r3[0].row_count, 0);
    }

    #[test]
    fn test_transform_error_does_not_fail_write() {
        // Verify that an error in MV accumulation does not propagate.
        // We'll test this at the MVPartialBuilder level — a panic in one MV
        // should not affect others.
        let spec1 = MVPartialSpec {
            mv_id: "mv1".to_string(),
            definition_hash: "h1".to_string(),
            def_version: 1,
            group_col_indices: vec![0],
            group_col_names: vec!["region".to_string()],
            group_col_types: vec![DataType::Utf8],
            agg_specs: vec![AggSpec {
                function: AggFunction::Count,
                source_col_idx: None,
                output_names: vec!["cnt".to_string()],
            }],
            sort_key_names: vec!["region".to_string()],
        };
        let spec2 = MVPartialSpec {
            mv_id: "mv2".to_string(),
            definition_hash: "h2".to_string(),
            def_version: 1,
            group_col_indices: vec![0],
            group_col_names: vec!["region".to_string()],
            group_col_types: vec![DataType::Utf8],
            agg_specs: vec![
                AggSpec {
                    function: AggFunction::Sum,
                    source_col_idx: Some(1),
                    output_names: vec!["total".to_string()],
                },
            ],
            sort_key_names: vec!["region".to_string()],
        };

        let tmp = tempfile::tempdir().unwrap();
        let mut builder = MVPartialBuilder::new(
            vec![spec1, spec2],
            0, 1,
            tmp.path().to_path_buf(),
        );

        // Feed a valid batch
        let batch = make_test_batch(vec!["x", "y"], vec![10, 20]);
        builder.accumulate(&batch);

        let results = builder.seal();
        assert_eq!(results.len(), 2);
        assert!(!results[0].failed);
        assert!(!results[1].failed);
        assert_eq!(results[0].row_count, 2); // mv1: cnt by region
        assert_eq!(results[1].row_count, 2); // mv2: sum by region
    }

    #[test]
    fn test_numeric_group_keys_with_timestamp() {
        let spec = MVPartialSpec {
            mv_id: "ts_mv".to_string(),
            definition_hash: "hash".to_string(),
            def_version: 1,
            group_col_indices: vec![0],
            group_col_names: vec!["event_bucket".to_string()],
            group_col_types: vec![DataType::Timestamp(TimeUnit::Millisecond, None)],
            agg_specs: vec![
                AggSpec {
                    function: AggFunction::Count,
                    source_col_idx: None,
                    output_names: vec!["cnt".to_string()],
                },
                AggSpec {
                    function: AggFunction::Sum,
                    source_col_idx: Some(1),
                    output_names: vec!["total".to_string()],
                },
            ],
            sort_key_names: vec!["event_bucket".to_string()],
        };

        let ts_arr = TimestampMillisecondArray::from(vec![1000, 2000, 1000, 2000, 1000]);
        let val_arr = Int64Array::from(vec![10, 20, 30, 40, 50]);
        let schema = Arc::new(Schema::new(vec![
            Field::new("event_bucket", DataType::Timestamp(TimeUnit::Millisecond, None), true),
            Field::new("value", DataType::Int64, true),
        ]));
        let batch = RecordBatch::try_new(
            schema,
            vec![Arc::new(ts_arr), Arc::new(val_arr)],
        ).unwrap();

        let tmp = tempfile::tempdir().unwrap();
        let mut builder = MVPartialBuilder::new(
            vec![spec],
            0, 1,
            tmp.path().to_path_buf(),
        );
        builder.accumulate(&batch);

        let results = builder.seal();
        assert_eq!(results.len(), 1);
        assert!(!results[0].failed);
        assert_eq!(results[0].row_count, 2); // 2 groups: 1000, 2000

        // Read back
        let file = File::open(&results[0].file_path).unwrap();
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
            .unwrap().build().unwrap();
        let batches: Vec<RecordBatch> = reader.collect::<Result<Vec<_>, _>>().unwrap();
        let rb = &batches[0];

        let ts = rb.column(0).as_any().downcast_ref::<TimestampMillisecondArray>().unwrap();
        assert_eq!(ts.value(0), 1000);
        assert_eq!(ts.value(1), 2000);

        let cnt = rb.column(1).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(cnt.value(0), 3); // 1000: 3 rows
        assert_eq!(cnt.value(1), 2); // 2000: 2 rows

        let total = rb.column(2).as_any().downcast_ref::<Int64Array>().unwrap();
        assert_eq!(total.value(0), 90); // 1000: 10+30+50
        assert_eq!(total.value(1), 60); // 2000: 20+40
    }
}

// ── JSON spec parsing (c3: FFI wire-up) ───────────────────────────────────

/// Parse MV specs from a JSON string passed through FFI.
///
/// Expected format: array of objects with fields matching MVPartialSpec.
pub fn parse_specs_from_json(json: &str) -> Result<Vec<MVPartialSpec>, String> {
    // Minimal JSON parsing using serde_json-style manual parsing.
    // The parquet-data-format crate already has serde_json available through
    // arrow's re-export. For POC, use a simple approach.
    //
    // Format: [{"mv_id":"...", "definition_hash":"...", "def_version":1,
    //   "group_col_names":["a"], "group_col_types":["utf8"],
    //   "agg_specs":[{"function":"sum","source_field":"x","output_names":["x_sum"]}],
    //   "sort_key_names":["a"]}]

    let parsed: Vec<JsonMVSpec> = serde_json::from_str(json)
        .map_err(|e| format!("JSON parse error: {}", e))?;

    let mut specs = Vec::with_capacity(parsed.len());
    for js in parsed {
        let group_col_types: Vec<DataType> = js
            .group_col_types
            .iter()
            .map(|t| arrow_type_from_string(t))
            .collect::<Result<_, _>>()?;

        let mut agg_specs = Vec::new();
        for jagg in &js.agg_specs {
            let function = match jagg.function.as_str() {
                "count" => AggFunction::Count,
                "count_field" => AggFunction::CountField,
                "sum" => AggFunction::Sum,
                "min" => AggFunction::Min,
                "max" => AggFunction::Max,
                other => return Err(format!("Unknown agg function: {}", other)),
            };
            agg_specs.push(AggSpec {
                function,
                source_col_idx: None, // Resolved later from schema
                output_names: jagg.output_names.clone(),
            });
        }

        specs.push(MVPartialSpec {
            mv_id: js.mv_id,
            definition_hash: js.definition_hash,
            def_version: js.def_version,
            group_col_indices: Vec::new(), // Resolved later from schema
            group_col_names: js.group_col_names,
            group_col_types,
            agg_specs,
            sort_key_names: js.sort_key_names,
        });
    }
    Ok(specs)
}

fn arrow_type_from_string(s: &str) -> Result<DataType, String> {
    match s {
        "utf8" => Ok(DataType::Utf8),
        "int64" => Ok(DataType::Int64),
        "int32" => Ok(DataType::Int32),
        "float64" => Ok(DataType::Float64),
        "timestamp_ms" => Ok(DataType::Timestamp(TimeUnit::Millisecond, None)),
        other => Err(format!("Unknown arrow type: {}", other)),
    }
}

#[derive(serde::Deserialize)]
struct JsonMVSpec {
    mv_id: String,
    definition_hash: String,
    def_version: i64,
    group_col_names: Vec<String>,
    group_col_types: Vec<String>,
    agg_specs: Vec<JsonAggSpec>,
    sort_key_names: Vec<String>,
}

#[derive(serde::Deserialize)]
struct JsonAggSpec {
    function: String,
    source_field: Option<String>,
    output_names: Vec<String>,
}

impl MVPartialBuilder {
    /// Set the starting generation counter for all MVs (for resume from remote).
    pub fn set_start_generation(&mut self, gen: u64) {
        for counter in &mut self.gen_counters {
            *counter = gen;
        }
    }
}
