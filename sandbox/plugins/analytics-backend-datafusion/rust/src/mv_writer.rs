/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! POC(mv): streaming MV writer — the VSR model.
//!
//! Lifecycle mirrors the parquet native writer (`create/feed/finalize`):
//! the Java side buffers MV-referenced columns into a VSR and exports a batch
//! at each rotation; `feed` folds the batch into a sorted **background state**;
//! `finalize` writes the background state as the sorted MV state parquet.
//!
//! Background state (POC): BTreeMap<group_key, count> — sorted by
//! construction, O(log G) placement per group. The general/scale design is
//! sorted runs + k-way fold at finalize (spillable); see mv-incremental-lld.
//!
//! Hardcoded to the POC view `SELECT service, COUNT(*) GROUP BY service`:
//! input batches have one Utf8 column (service); the fold is count-increment.
//! The DF-generalized fold (Partial plan over a memtable per feed) replaces
//! `fold_batch` without touching the lifecycle.

use std::collections::BTreeMap;
use std::fs::File;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;
use std::sync::Mutex;

use arrow_array::{Array, Int64Array, RecordBatch, StringArray};
use arrow_schema::{DataType, Field, Schema};
use parquet::arrow::ArrowWriter;

/// One live MV writer: the sorted background state.
pub struct MvWriterHandle {
    state: Mutex<BTreeMap<String, i64>>,
}

static NEXT_ID: AtomicI64 = AtomicI64::new(1);

// POC-grade handle registry (avoids raw Box pointers crossing FFI unchecked).
static WRITERS: std::sync::LazyLock<Mutex<std::collections::HashMap<i64, Arc<MvWriterHandle>>>> =
    std::sync::LazyLock::new(|| Mutex::new(std::collections::HashMap::new()));

/// Creates a writer; returns its handle id.
pub fn mv_writer_create() -> i64 {
    let id = NEXT_ID.fetch_add(1, Ordering::Relaxed);
    WRITERS.lock().unwrap().insert(
        id,
        Arc::new(MvWriterHandle { state: Mutex::new(BTreeMap::new()) }),
    );
    id
}

/// Folds one rotated forward-buffer batch into the background state.
/// Batch schema (POC): col 0 = service Utf8.
pub fn mv_writer_feed(id: i64, batch: &RecordBatch) -> Result<(), String> {
    let handle = WRITERS
        .lock()
        .unwrap()
        .get(&id)
        .cloned()
        .ok_or_else(|| format!("mv_writer_feed: unknown handle {id}"))?;

    let services = batch
        .column(0)
        .as_any()
        .downcast_ref::<StringArray>()
        .ok_or("mv_writer_feed: col 0 not Utf8")?;

    // Step 1 (batch aggregation) + Step 2 (sorted placement) fused for the
    // count case: BTreeMap entry increment IS the fold. The generalized
    // version aggregates the batch first (DF Partial), then merges the mini
    // state batch into the background state.
    let mut state = handle.state.lock().unwrap();
    for i in 0..services.len() {
        if services.is_valid(i) {
            *state.entry(services.value(i).to_string()).or_insert(0) += 1;
        }
    }
    Ok(())
}

/// Writes the background state (already sorted) as the MV state parquet and
/// drops the writer. Returns the number of state rows.
pub fn mv_writer_finalize(id: i64, output_file: &str) -> Result<i64, String> {
    let handle = WRITERS
        .lock()
        .unwrap()
        .remove(&id)
        .ok_or_else(|| format!("mv_writer_finalize: unknown handle {id}"))?;
    let state = handle.state.lock().unwrap();

    // Schema matches the POC state contract: service | count(Int64(1))[count]
    let schema = Arc::new(Schema::new(vec![
        Field::new("service", DataType::Utf8, true),
        Field::new("count(Int64(1))[count]", DataType::Int64, false),
    ]));

    let keys: Vec<&str> = state.keys().map(String::as_str).collect();
    let counts: Vec<i64> = state.values().copied().collect();
    let batch = RecordBatch::try_new(
        schema.clone(),
        vec![
            Arc::new(StringArray::from(keys)),
            Arc::new(Int64Array::from(counts)),
        ],
    )
    .map_err(|e| format!("mv_writer_finalize batch: {e}"))?;

    let file =
        File::create(output_file).map_err(|e| format!("mv_writer_finalize create {output_file}: {e}"))?;
    let mut writer =
        ArrowWriter::try_new(file, schema, None).map_err(|e| format!("mv_writer_finalize writer: {e}"))?;
    writer.write(&batch).map_err(|e| format!("mv_writer_finalize write: {e}"))?;
    writer.close().map_err(|e| format!("mv_writer_finalize close: {e}"))?;
    Ok(batch.num_rows() as i64)
}

/// Drops a writer without writing (abort path).
pub fn mv_writer_abort(id: i64) {
    WRITERS.lock().unwrap().remove(&id);
}
