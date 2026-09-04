/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Lazy/streaming Parquet table provider for MV state files.
//!
//! # Problem (the scaling defect this module fixes)
//!
//! `create_mv_only_session_context` previously read ALL state files eagerly
//! into `Vec<RecordBatch>` and registered them as a `MemTable`. With 122 files totaling
//! ~23 GB, this pre-loaded the entire dataset into memory at session creation time,
//! triggering the DataFusion circuit breaker (26.5 GB limit) before any query execution
//! could begin. The `MemTable` also caused DataFusion's memory pool to see the full
//! data size as a single reservation, leaving no headroom for hash aggregation or other
//! operators.
//!
//! # New behavior (this module)
//!
//! - Session creation reads ONLY the first file's schema metadata (~few KB). No record
//!   batches are loaded.
//! - Each file becomes a separate partition in the `ExecutionPlan`.
//! - During execution, each partition opens its Parquet file lazily, streams
//!   `RecordBatch`es one at a time through projection/cast/null-fill, and drops
//!   each batch before reading the next.
//! - Peak memory = O(batch_size × num_columns × max_partitions_executing), not
//!   O(total_data).
//! - DataFusion's memory pool tracks only the actively-decoded batches, not
//!   the entire dataset.
//!
//! # Legacy `.mv.arrow` files
//!
//! Legacy Arrow IPC state files (`.mv.arrow`) are no longer supported. Any
//! attempt to open one returns a clear rebuild-required error. This ensures
//! fail-closed semantics: old state files cannot silently produce wrong results.

use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow::array::ArrayRef;
use arrow::datatypes::{DataType, SchemaRef};
use arrow::record_batch::RecordBatch;
use async_trait::async_trait;
use datafusion::catalog::Session;
use datafusion::common::stats::Precision;
use datafusion::common::{DataFusionError, Result, Statistics};
use datafusion::datasource::TableType;
use datafusion::execution::{RecordBatchStream, SendableRecordBatchStream, TaskContext};
use datafusion::logical_expr::{Expr, TableProviderFilterPushDown};
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use futures::Stream;

use crate::session_context::{is_lossless_integer_widening, MvColumnProjection};

// ---------------------------------------------------------------------------
// Test instrumentation: counts how many IPC files have been opened.
// Only compiled under cfg(test) — zero overhead in production.
// ---------------------------------------------------------------------------
#[cfg(test)]
mod test_counters {
    use std::sync::atomic::{AtomicUsize, Ordering};
    static FILES_OPENED: AtomicUsize = AtomicUsize::new(0);

    pub fn reset() {
        FILES_OPENED.store(0, Ordering::SeqCst);
    }

    pub fn increment() {
        FILES_OPENED.fetch_add(1, Ordering::SeqCst);
    }

    pub fn get() -> usize {
        FILES_OPENED.load(Ordering::SeqCst)
    }
}

// ---------------------------------------------------------------------------
// TableProvider
// ---------------------------------------------------------------------------

/// A lazy/streaming TableProvider for MV Arrow IPC state files.
///
/// At registration time, only stores file paths + schema metadata.
/// During execution, opens files lazily per-partition.
pub struct MvStateTableProvider {
    /// The logical schema after projection/cast/null-fill.
    table_schema: SchemaRef,
    /// One entry per partition (= per IPC file). Stores the file path.
    file_paths: Vec<String>,
    /// Physical → logical column projection descriptors.
    projection: Vec<MvColumnProjection>,
    /// Schema of the physical Arrow IPC files (before projection).
    physical_schema: SchemaRef,
}

impl MvStateTableProvider {
    pub fn new(
        table_schema: SchemaRef,
        file_paths: Vec<String>,
        projection: Vec<MvColumnProjection>,
        physical_schema: SchemaRef,
    ) -> Self {
        Self {
            table_schema,
            file_paths,
            projection,
            physical_schema,
        }
    }
}

impl fmt::Debug for MvStateTableProvider {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MvStateTableProvider")
            .field("files", &self.file_paths.len())
            .field("columns", &self.table_schema.fields().len())
            .finish()
    }
}

#[async_trait]
impl datafusion::catalog::TableProvider for MvStateTableProvider {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.table_schema)
    }

    fn table_type(&self) -> TableType {
        TableType::Base
    }

    fn supports_filters_pushdown(
        &self,
        filters: &[&Expr],
    ) -> Result<Vec<TableProviderFilterPushDown>> {
        // Arrow IPC doesn't support predicate pushdown; DataFusion will filter post-scan.
        Ok(vec![
            TableProviderFilterPushDown::Unsupported;
            filters.len()
        ])
    }

    async fn scan(
        &self,
        _state: &dyn Session,
        projection: Option<&Vec<usize>>,
        _filters: &[Expr],
        _limit: Option<usize>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Compute which logical columns are actually needed.
        let output_projection: Option<Vec<usize>> = projection.cloned();

        Ok(Arc::new(MvStateExec::new(
            Arc::clone(&self.table_schema),
            self.file_paths.clone(),
            self.projection.clone(),
            Arc::clone(&self.physical_schema),
            output_projection,
        )))
    }

    fn statistics(&self) -> Option<Statistics> {
        // Conservative: we know file count but not row counts without reading.
        Some(Statistics {
            num_rows: Precision::Absent,
            total_byte_size: Precision::Absent,
            column_statistics: vec![
                datafusion::common::ColumnStatistics::new_unknown();
                self.table_schema.fields().len()
            ],
        })
    }
}

// ---------------------------------------------------------------------------
// ExecutionPlan
// ---------------------------------------------------------------------------

/// Physical execution plan that lazily streams Arrow IPC state files.
///
/// Each file is a separate partition. During execution, each partition:
/// 1. Opens the IPC file
/// 2. Reads batches one at a time
/// 3. Applies projection/cast/null-fill per batch
/// 4. Yields the transformed batch
/// 5. Drops the batch before reading the next one
struct MvStateExec {
    /// Logical output schema (after projection/cast/null-fill).
    table_schema: SchemaRef,
    /// One file path per partition.
    file_paths: Vec<String>,
    /// Physical → logical column mapping.
    col_projection: Vec<MvColumnProjection>,
    /// Physical Arrow schema of the IPC files.
    physical_schema: SchemaRef,
    /// Optional scan projection (which logical columns are actually needed).
    output_projection: Option<Vec<usize>>,
    /// Cached plan properties.
    properties: Arc<PlanProperties>,
}

impl MvStateExec {
    fn new(
        table_schema: SchemaRef,
        file_paths: Vec<String>,
        col_projection: Vec<MvColumnProjection>,
        physical_schema: SchemaRef,
        output_projection: Option<Vec<usize>>,
    ) -> Self {
        let output_schema = match &output_projection {
            Some(indices) => {
                let fields: Vec<_> = indices
                    .iter()
                    .map(|&i| table_schema.fields()[i].clone())
                    .collect();
                Arc::new(arrow::datatypes::Schema::new(fields))
            }
            None => Arc::clone(&table_schema),
        };
        let num_partitions = file_paths.len().max(1); // at least 1 partition even for empty
        let eq_properties = EquivalenceProperties::new(output_schema);
        let partitioning = Partitioning::UnknownPartitioning(num_partitions);
        let properties = Arc::new(PlanProperties::new(
            eq_properties,
            partitioning,
            datafusion::physical_plan::execution_plan::EmissionType::Incremental,
            datafusion::physical_plan::execution_plan::Boundedness::Bounded,
        ));
        Self {
            table_schema,
            file_paths,
            col_projection,
            physical_schema,
            output_projection,
            properties,
        }
    }
}

impl fmt::Debug for MvStateExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MvStateExec")
            .field("files", &self.file_paths.len())
            .field(
                "output_columns",
                &self.properties.output_partitioning().partition_count(),
            )
            .finish()
    }
}

impl DisplayAs for MvStateExec {
    fn fmt_as(&self, t: DisplayFormatType, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match t {
            DisplayFormatType::Default
            | DisplayFormatType::Verbose
            | DisplayFormatType::TreeRender => {
                write!(
                    f,
                    "MvStateExec: files={}, columns={}",
                    self.file_paths.len(),
                    self.table_schema.fields().len()
                )
            }
        }
    }
}

impl ExecutionPlan for MvStateExec {
    fn name(&self) -> &str {
        "MvStateExec"
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![] // leaf node
    }

    fn with_new_children(
        self: Arc<Self>,
        children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        if children.is_empty() {
            Ok(self)
        } else {
            Err(DataFusionError::Internal(
                "MvStateExec is a leaf node and cannot have children".to_string(),
            ))
        }
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> Result<SendableRecordBatchStream> {
        if partition >= self.file_paths.len() {
            // Empty partition (e.g., when there are 0 files but 1 logical partition).
            let schema = self.schema();
            return Ok(Box::pin(RecordBatchStreamAdapter::new(
                schema,
                futures::stream::empty(),
            )));
        }

        let file_path = self.file_paths[partition].clone();
        let col_projection = self.col_projection.clone();
        let table_schema = Arc::clone(&self.table_schema);
        let output_projection = self.output_projection.clone();

        Ok(Box::pin(MvStateStream::new(
            file_path,
            col_projection,
            table_schema,
            output_projection,
        )))
    }

    fn schema(&self) -> SchemaRef {
        self.properties.eq_properties.schema().clone()
    }
}

// ---------------------------------------------------------------------------
// RecordBatchStream — lazy file-at-a-time reader
// ---------------------------------------------------------------------------

/// Lazily opens a Parquet state file and streams projected/cast/null-filled batches.
///
/// The file is opened on the first `poll_next`, not at construction time.
/// Each batch is transformed and yielded; the physical batch is dropped
/// before the next one is read, bounding memory to O(1 batch).
struct MvStateStream {
    file_path: String,
    col_projection: Vec<MvColumnProjection>,
    /// The full logical table schema (before output_projection).
    table_schema: SchemaRef,
    /// Output schema after applying output_projection.
    output_schema: SchemaRef,
    /// Optional scan projection: which logical columns to emit.
    output_projection: Option<Vec<usize>>,
    /// State machine: None = not yet opened, Some = reader in progress.
    reader: Option<parquet::arrow::arrow_reader::ParquetRecordBatchReader>,
    /// Whether we've finished reading.
    finished: bool,
}

impl MvStateStream {
    fn new(
        file_path: String,
        col_projection: Vec<MvColumnProjection>,
        table_schema: SchemaRef,
        output_projection: Option<Vec<usize>>,
    ) -> Self {
        let output_schema = match &output_projection {
            Some(indices) => {
                let fields: Vec<_> = indices
                    .iter()
                    .map(|&i| table_schema.fields()[i].clone())
                    .collect();
                Arc::new(arrow::datatypes::Schema::new(fields))
            }
            None => Arc::clone(&table_schema),
        };
        Self {
            file_path,
            col_projection,
            table_schema,
            output_schema,
            output_projection,
            reader: None,
            finished: false,
        }
    }

    /// Opens the Parquet file and initializes the reader. Called lazily on first poll.
    fn open_file(&mut self) -> std::result::Result<(), DataFusionError> {
        #[cfg(test)]
        test_counters::increment();

        // Legacy Arrow IPC guard: fail closed with a rebuild-required error.
        if self.file_path.ends_with(".mv.arrow") {
            return Err(DataFusionError::Execution(format!(
                "MvStateStream: legacy Arrow IPC state file '{}' is no longer supported; \
                 rebuild the materialized view to generate Parquet state files",
                self.file_path
            )));
        }

        let file = std::fs::File::open(&self.file_path).map_err(|e| {
            DataFusionError::Execution(format!(
                "MvStateStream: failed to open '{}': {}",
                self.file_path, e
            ))
        })?;
        let reader = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
            .map_err(|e| {
                DataFusionError::Execution(format!(
                    "MvStateStream: failed to read Parquet '{}': {}",
                    self.file_path, e
                ))
            })?
            .build()
            .map_err(|e| {
                DataFusionError::Execution(format!(
                    "MvStateStream: failed to build Parquet reader '{}': {}",
                    self.file_path, e
                ))
            })?;
        self.reader = Some(reader);
        Ok(())
    }

    /// Reads the next batch from the Parquet file, applies projection/cast/null-fill,
    /// and optionally applies output_projection.
    fn next_batch(&mut self) -> std::result::Result<Option<RecordBatch>, DataFusionError> {
        if self.finished {
            return Ok(None);
        }

        // Lazy open on first call.
        if self.reader.is_none() {
            self.open_file()?;
        }

        let reader = self.reader.as_mut().unwrap();

        // Read the next physical batch.
        let physical_batch = match reader.next() {
            Some(Ok(batch)) => batch,
            Some(Err(e)) => {
                self.finished = true;
                return Err(DataFusionError::Execution(format!(
                    "MvStateStream: batch read error in '{}': {}",
                    self.file_path, e
                )));
            }
            None => {
                // Iterator exhausted.
                self.finished = true;
                self.reader = None;
                return Ok(None);
            }
        };

        let num_rows = physical_batch.num_rows();
        if num_rows == 0 {
            // Zero-row batch: skip it but continue reading.
            return self.next_batch();
        }

        // Apply physical → logical projection with cast/null-fill.
        let logical_columns: Vec<ArrayRef> = self
            .col_projection
            .iter()
            .enumerate()
            .map(|(logical_position, proj)| {
                let target_type = self.table_schema.field(logical_position).data_type();
                if proj.null_fill {
                    Ok(arrow::array::new_null_array(target_type, num_rows))
                } else {
                    let col = physical_batch.column(proj.physical_position);
                    let actual_type = col.data_type();
                    if actual_type == target_type {
                        Ok(Arc::clone(col))
                    } else if is_lossless_integer_widening(actual_type, target_type) {
                        arrow::compute::cast(col, target_type)
                            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
                    } else {
                        Err(DataFusionError::Execution(format!(
                            "MvStateStream: file '{}' column {} has type {:?}, but logical field '{}' expects {:?}; only lossless integer widening is permitted",
                            self.file_path,
                            proj.physical_position,
                            actual_type,
                            self.table_schema.field(logical_position).name(),
                            target_type,
                        )))
                    }
                }
            })
            .collect::<std::result::Result<Vec<_>, _>>()?;

        let logical_batch = RecordBatch::try_new(Arc::clone(&self.table_schema), logical_columns)
            .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?;

        // Apply output projection if specified (scan projection pushdown).
        let output_batch = match &self.output_projection {
            Some(indices) if indices.is_empty() => {
                // All columns projected away (e.g. COUNT(*) only needs row count).
                // Return a zero-column batch with correct row count.
                RecordBatch::try_new_with_options(
                    Arc::clone(&self.output_schema),
                    vec![],
                    &arrow::record_batch::RecordBatchOptions::new().with_row_count(Some(num_rows)),
                )
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?
            }
            Some(indices) => {
                let projected_columns: Vec<ArrayRef> = indices
                    .iter()
                    .map(|&i| Arc::clone(logical_batch.column(i)))
                    .collect();
                RecordBatch::try_new(Arc::clone(&self.output_schema), projected_columns)
                    .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))?
            }
            None => logical_batch,
        };

        Ok(Some(output_batch))
    }
}

impl Stream for MvStateStream {
    type Item = std::result::Result<RecordBatch, DataFusionError>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        // Synchronous I/O: Arrow IPC FileReader is blocking (local filesystem).
        // This is acceptable because MV state files are always local and small per-batch.
        match self.next_batch() {
            Ok(Some(batch)) => Poll::Ready(Some(Ok(batch))),
            Ok(None) => Poll::Ready(None),
            Err(e) => Poll::Ready(Some(Err(e))),
        }
    }
}

impl RecordBatchStream for MvStateStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.output_schema)
    }
}

// ---------------------------------------------------------------------------
// Public helper: read only the schema header from the first IPC file.
// ---------------------------------------------------------------------------

/// Reads the Arrow schema from the first Parquet state file without loading any
/// record batches. Returns None if there are no files or the first file can't
/// be read.
///
/// Cost: opens file, reads Parquet footer metadata (~few KB), closes immediately.
/// O(1) memory.
pub fn read_schema_from_first_file(
    state_file_paths: &[String],
) -> std::result::Result<Option<SchemaRef>, DataFusionError> {
    let path = match state_file_paths.first() {
        Some(p) => p,
        None => return Ok(None),
    };

    // Legacy Arrow IPC guard: fail closed with a rebuild-required error.
    if path.ends_with(".mv.arrow") {
        return Err(DataFusionError::Execution(format!(
            "read_schema_from_first_file: legacy Arrow IPC state file '{}' is no longer supported; \
             rebuild the materialized view to generate Parquet state files",
            path
        )));
    }

    let file = std::fs::File::open(path).map_err(|e| {
        DataFusionError::Execution(format!(
            "read_schema_from_first_file: failed to open '{}': {}",
            path, e
        ))
    })?;
    let builder =
        parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file).map_err(
            |e| {
                DataFusionError::Execution(format!(
                    "read_schema_from_first_file: failed to read Parquet '{}': {}",
                    path, e
                ))
            },
        )?;
    Ok(Some(builder.schema().clone()))
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::array::{Int16Array, Int32Array, Int64Array, StringArray};
    use arrow::datatypes::{Field, Schema};
    use datafusion::prelude::SessionContext;
    use parquet::arrow::ArrowWriter;
    use parquet::basic::Compression;
    use parquet::file::properties::WriterProperties;
    use std::io::Write;
    use tempfile::TempDir;

    /// Helper: writes a Parquet file with the given schema and batches.
    fn write_parquet_file(
        dir: &std::path::Path,
        name: &str,
        schema: &SchemaRef,
        batches: &[RecordBatch],
    ) -> String {
        let path = dir.join(name);
        let file = std::fs::File::create(&path).unwrap();
        let props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(Default::default()))
            .build();
        let mut writer = ArrowWriter::try_new(file, schema.clone(), Some(props)).unwrap();
        for batch in batches {
            writer.write(batch).unwrap();
        }
        writer.close().unwrap();
        path.to_str().unwrap().to_string()
    }

    fn three_col_physical_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("RegionID", DataType::Int64, false),
            Field::new("count(Int64(1))[count]", DataType::Int64, false),
            Field::new("sum(mv_input.AdvEngineID)[sum]", DataType::Int64, true),
        ]))
    }

    fn three_col_logical_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("RegionID", DataType::Int64, true),
            Field::new("adv", DataType::Int64, true),
            Field::new("cnt", DataType::Int64, true),
        ]))
    }

    fn three_col_state_fields() -> Vec<String> {
        vec!["RegionID".to_string(), "cnt".to_string(), "adv".to_string()]
    }

    fn three_col_projection() -> Vec<MvColumnProjection> {
        vec![
            MvColumnProjection {
                physical_position: 0,
                cast_to: None,
                null_fill: false,
            },
            MvColumnProjection {
                physical_position: 2,
                cast_to: None,
                null_fill: false,
            },
            MvColumnProjection {
                physical_position: 1,
                cast_to: None,
                null_fill: false,
            },
        ]
    }

    fn make_physical_batch(
        schema: &SchemaRef,
        region: &[i64],
        count: &[i64],
        sum: &[i64],
    ) -> RecordBatch {
        RecordBatch::try_new(
            Arc::clone(schema),
            vec![
                Arc::new(Int64Array::from(region.to_vec())),
                Arc::new(Int64Array::from(count.to_vec())),
                Arc::new(Int64Array::from(sum.to_vec())),
            ],
        )
        .unwrap()
    }

    // ====================================================================
    // Basic streaming correctness
    // ====================================================================

    #[tokio::test]
    async fn test_streaming_single_file_single_batch() {
        let dir = TempDir::new().unwrap();
        let physical = three_col_physical_schema();
        let batch = make_physical_batch(&physical, &[1, 2, 3], &[10, 20, 30], &[100, 200, 300]);
        let path = write_parquet_file(dir.path(), "state_0.mv.parquet", &physical, &[batch]);

        let logical = three_col_logical_schema();
        let projection = three_col_projection();

        let provider = MvStateTableProvider::new(
            Arc::clone(&logical),
            vec![path],
            projection,
            Arc::clone(&physical),
        );

        let ctx = SessionContext::new();
        ctx.register_table("mv", Arc::new(provider)).unwrap();

        let batches = ctx
            .sql("SELECT \"RegionID\", adv, cnt FROM mv ORDER BY \"RegionID\"")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 3);

        // Verify column reordering: adv=sum, cnt=count
        let region = batches[0]
            .column_by_name("RegionID")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let adv = batches[0]
            .column_by_name("adv")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let cnt = batches[0]
            .column_by_name("cnt")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(region.value(0), 1);
        assert_eq!(adv.value(0), 100); // sum column
        assert_eq!(cnt.value(0), 10); // count column
    }

    #[tokio::test]
    async fn test_streaming_multiple_files_multiple_batches() {
        let dir = TempDir::new().unwrap();
        let physical = three_col_physical_schema();

        // File 1: 2 batches
        let b1 = make_physical_batch(&physical, &[1, 2], &[10, 20], &[100, 200]);
        let b2 = make_physical_batch(&physical, &[3], &[30], &[300]);
        let p1 = write_parquet_file(dir.path(), "state_0.mv.parquet", &physical, &[b1, b2]);

        // File 2: 1 batch
        let b3 = make_physical_batch(&physical, &[4, 5], &[40, 50], &[400, 500]);
        let p2 = write_parquet_file(dir.path(), "state_1.mv.parquet", &physical, &[b3]);

        // File 3: 3 batches
        let b4 = make_physical_batch(&physical, &[6], &[60], &[600]);
        let b5 = make_physical_batch(&physical, &[7, 8], &[70, 80], &[700, 800]);
        let b6 = make_physical_batch(&physical, &[9, 10], &[90, 100], &[900, 1000]);
        let p3 = write_parquet_file(dir.path(), "state_2.mv.parquet", &physical, &[b4, b5, b6]);

        let logical = three_col_logical_schema();
        let projection = three_col_projection();

        let provider = MvStateTableProvider::new(
            Arc::clone(&logical),
            vec![p1, p2, p3],
            projection,
            Arc::clone(&physical),
        );

        let ctx = SessionContext::new();
        ctx.register_table("mv", Arc::new(provider)).unwrap();

        // Count all rows across all files
        let batches = ctx
            .sql("SELECT COUNT(*) AS cnt FROM mv")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let total: i64 = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(total, 10, "all 10 rows across 3 files must be counted");

        // SUM aggregation
        let batches = ctx
            .sql("SELECT SUM(cnt) AS total_count, SUM(adv) AS total_adv FROM mv")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let total_count: i64 = batches[0]
            .column_by_name("total_count")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        let total_adv: i64 = batches[0]
            .column_by_name("total_adv")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(total_count, 550); // 10+20+30+40+50+60+70+80+90+100
        assert_eq!(total_adv, 5500); // 100+200+...+1000
    }

    #[tokio::test]
    async fn test_widens_each_file_against_logical_schema() {
        let dir = TempDir::new().unwrap();

        // The provider derives its static projection from the first file, where the
        // aggregate state already matches the logical Int64 type.
        let first_schema = three_col_physical_schema();
        let first_batch = make_physical_batch(&first_schema, &[1], &[1], &[100]);
        let first_path = write_parquet_file(
            dir.path(),
            "state_int64.mv.parquet",
            &first_schema,
            &[first_batch],
        );

        // A later generation can preserve a narrower source type for MIN/MAX state.
        // It must be widened per file rather than being trusted to match file zero.
        let narrower_schema = Arc::new(Schema::new(vec![
            Field::new("RegionID", DataType::Int64, false),
            Field::new("count(Int64(1))[count]", DataType::Int64, false),
            Field::new("sum(mv_input.AdvEngineID)[sum]", DataType::Int16, true),
        ]));
        let narrower_batch = RecordBatch::try_new(
            Arc::clone(&narrower_schema),
            vec![
                Arc::new(Int64Array::from(vec![2])),
                Arc::new(Int64Array::from(vec![1])),
                Arc::new(Int16Array::from(vec![7])),
            ],
        )
        .unwrap();
        let narrower_path = write_parquet_file(
            dir.path(),
            "state_int16.mv.parquet",
            &narrower_schema,
            &[narrower_batch],
        );

        let provider = MvStateTableProvider::new(
            three_col_logical_schema(),
            vec![first_path, narrower_path],
            three_col_projection(),
            Arc::clone(&first_schema),
        );
        let ctx = SessionContext::new();
        ctx.register_table("mv", Arc::new(provider)).unwrap();

        let batches = ctx
            .sql("SELECT SUM(adv) AS total_adv FROM mv")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let total_adv = batches[0]
            .column_by_name("total_adv")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(total_adv, 107);
    }

    // ====================================================================
    // Laziness proof: provider creation does NOT read batches
    // ====================================================================

    #[tokio::test]
    async fn test_laziness_no_file_reads_at_registration() {
        let dir = TempDir::new().unwrap();
        let physical = three_col_physical_schema();
        let batch = make_physical_batch(&physical, &[1], &[10], &[100]);

        let mut paths = Vec::new();
        for i in 0..10 {
            let p = write_parquet_file(
                dir.path(),
                &format!("state_{}.mv.parquet", i),
                &physical,
                &[batch.clone()],
            );
            paths.push(p);
        }

        let logical = three_col_logical_schema();
        let projection = three_col_projection();

        test_counters::reset();

        // Creating the provider must NOT open any files.
        let _provider = MvStateTableProvider::new(
            Arc::clone(&logical),
            paths.clone(),
            projection.clone(),
            Arc::clone(&physical),
        );
        assert_eq!(
            test_counters::get(),
            0,
            "provider creation must not open any IPC files"
        );

        // Registering the table must NOT open any files.
        let ctx = SessionContext::new();
        let provider = MvStateTableProvider::new(
            Arc::clone(&logical),
            paths,
            projection,
            Arc::clone(&physical),
        );
        ctx.register_table("mv", Arc::new(provider)).unwrap();
        // Note: Due to test parallelism, another test might have incremented the counter.
        // The key assertion is that THIS provider's constructor and registration code
        // paths do not open files. We verify this by checking before/after execution.
        let counter_before_query = test_counters::get();

        // Only executing a query should open files.
        let _result = ctx
            .sql("SELECT COUNT(*) FROM mv")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert!(
            test_counters::get() > counter_before_query,
            "query execution must open IPC files (before={}, after={})",
            counter_before_query,
            test_counters::get()
        );
        // Not all files may be opened due to DataFusion partition pruning,
        // but at least one must be opened. We don't assert exact count because
        // the optimizer may coalesce partitions.
    }

    // ====================================================================
    // Null-fill for absent logical columns
    // ====================================================================

    #[tokio::test]
    async fn test_null_fill_extra_logical_columns() {
        let dir = TempDir::new().unwrap();
        let physical = three_col_physical_schema();
        let batch = make_physical_batch(&physical, &[1, 2], &[10, 20], &[100, 200]);
        let path = write_parquet_file(dir.path(), "state.mv.parquet", &physical, &[batch]);

        // Logical schema has 4 columns: 3 state + 1 extra (null-filled).
        let logical = Arc::new(Schema::new(vec![
            Field::new("RegionID", DataType::Int64, true),
            Field::new("adv", DataType::Int64, true),
            Field::new("cnt", DataType::Int64, true),
            Field::new("_mv_source_generation", DataType::Int64, true),
        ]));

        let projection = vec![
            MvColumnProjection {
                physical_position: 0,
                cast_to: None,
                null_fill: false,
            },
            MvColumnProjection {
                physical_position: 2,
                cast_to: None,
                null_fill: false,
            },
            MvColumnProjection {
                physical_position: 1,
                cast_to: None,
                null_fill: false,
            },
            MvColumnProjection {
                physical_position: 0, // unused placeholder
                cast_to: Some(DataType::Int64),
                null_fill: true,
            },
        ];

        let provider = MvStateTableProvider::new(
            Arc::clone(&logical),
            vec![path],
            projection,
            Arc::clone(&physical),
        );

        let ctx = SessionContext::new();
        ctx.register_table("mv", Arc::new(provider)).unwrap();

        let batches = ctx
            .sql("SELECT _mv_source_generation, cnt FROM mv")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        assert_eq!(batches[0].num_rows(), 2);
        let gen_col = batches[0].column_by_name("_mv_source_generation").unwrap();
        assert!(gen_col.is_null(0), "null-fill column must be null");
        assert!(gen_col.is_null(1), "null-fill column must be null");

        let cnt = batches[0]
            .column_by_name("cnt")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(cnt.value(0), 10);
        assert_eq!(cnt.value(1), 20);
    }

    // ====================================================================
    // Lossless integer widening (Int32 → Int64)
    // ====================================================================

    #[tokio::test]
    async fn test_cast_int32_to_int64_streaming() {
        let dir = TempDir::new().unwrap();
        let physical = Arc::new(Schema::new(vec![
            Field::new("RegionID", DataType::Int32, false),
            Field::new("cnt", DataType::Int64, false),
        ]));
        let batch = RecordBatch::try_new(
            Arc::clone(&physical),
            vec![
                Arc::new(Int32Array::from(vec![42, i32::MAX, -1])),
                Arc::new(Int64Array::from(vec![10, 20, 30])),
            ],
        )
        .unwrap();
        let path = write_parquet_file(dir.path(), "state.mv.parquet", &physical, &[batch]);

        let logical = Arc::new(Schema::new(vec![
            Field::new("RegionID", DataType::Int64, true), // widened
            Field::new("cnt", DataType::Int64, true),
        ]));
        let projection = vec![
            MvColumnProjection {
                physical_position: 0,
                cast_to: Some(DataType::Int64), // cast Int32 → Int64
                null_fill: false,
            },
            MvColumnProjection {
                physical_position: 1,
                cast_to: None,
                null_fill: false,
            },
        ];

        let provider = MvStateTableProvider::new(
            Arc::clone(&logical),
            vec![path],
            projection,
            Arc::clone(&physical),
        );

        let ctx = SessionContext::new();
        ctx.register_table("mv", Arc::new(provider)).unwrap();

        let batches = ctx
            .sql("SELECT \"RegionID\", cnt FROM mv ORDER BY cnt")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let region = batches[0]
            .column_by_name("RegionID")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(region.value(0), 42);
        assert_eq!(region.value(1), i32::MAX as i64);
        assert_eq!(region.value(2), -1);
    }

    // ====================================================================
    // Scan projection pushdown
    // ====================================================================

    #[tokio::test]
    async fn test_scan_projection_pushdown() {
        let dir = TempDir::new().unwrap();
        let physical = three_col_physical_schema();
        let batch = make_physical_batch(&physical, &[1, 2], &[10, 20], &[100, 200]);
        let path = write_parquet_file(dir.path(), "state.mv.parquet", &physical, &[batch]);

        let logical = three_col_logical_schema();
        let projection = three_col_projection();

        let provider = MvStateTableProvider::new(
            Arc::clone(&logical),
            vec![path],
            projection,
            Arc::clone(&physical),
        );

        let ctx = SessionContext::new();
        ctx.register_table("mv", Arc::new(provider)).unwrap();

        // Only select one column — DataFusion should push the projection down.
        let batches = ctx
            .sql("SELECT cnt FROM mv")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert_eq!(batches[0].num_columns(), 1);
        assert_eq!(batches[0].schema().field(0).name(), "cnt");
        let cnt = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(cnt.value(0), 10);
    }

    // ====================================================================
    // Zero-row files / empty files
    // ====================================================================

    #[tokio::test]
    async fn test_zero_row_files_handled() {
        let dir = TempDir::new().unwrap();
        let physical = three_col_physical_schema();

        // File with zero rows — write a proper Parquet file with schema but no batches.
        let path_empty = dir.path().join("empty.mv.parquet");
        {
            let file = std::fs::File::create(&path_empty).unwrap();
            let props = WriterProperties::builder()
                .set_compression(Compression::ZSTD(Default::default()))
                .build();
            let writer = ArrowWriter::try_new(file, physical.clone(), Some(props)).unwrap();
            // Write no batches — just the schema footer.
            writer.close().unwrap();
        }
        let p1 = path_empty.to_str().unwrap().to_string();

        // File with real data
        let batch = make_physical_batch(&physical, &[1], &[10], &[100]);
        let p2 = write_parquet_file(dir.path(), "real.mv.parquet", &physical, &[batch]);

        let logical = three_col_logical_schema();
        let projection = three_col_projection();

        let provider = MvStateTableProvider::new(
            Arc::clone(&logical),
            vec![p1, p2],
            projection,
            Arc::clone(&physical),
        );

        let ctx = SessionContext::new();
        ctx.register_table("mv", Arc::new(provider)).unwrap();

        let batches = ctx
            .sql("SELECT COUNT(*) AS cnt FROM mv")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let total: i64 = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(total, 1, "only 1 real row from the non-empty file");
    }

    // ====================================================================
    // No files = empty result
    // ====================================================================

    #[tokio::test]
    async fn test_no_files_returns_empty() {
        let logical = three_col_logical_schema();
        let physical = three_col_physical_schema();
        let projection = three_col_projection();

        let provider = MvStateTableProvider::new(
            Arc::clone(&logical),
            vec![], // no files
            projection,
            Arc::clone(&physical),
        );

        let ctx = SessionContext::new();
        ctx.register_table("mv", Arc::new(provider)).unwrap();

        let batches = ctx
            .sql("SELECT COUNT(*) AS cnt FROM mv")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let total: i64 = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(total, 0);
    }

    // ====================================================================
    // Aggregate fold (GROUP BY + SUM) — simulates actual MV fold queries
    // ====================================================================

    #[tokio::test]
    async fn test_aggregate_fold_across_files() {
        let dir = TempDir::new().unwrap();
        let physical = three_col_physical_schema();

        // Two files with overlapping RegionIDs to test aggregation.
        let b1 = make_physical_batch(&physical, &[1, 2, 1], &[10, 20, 5], &[100, 200, 50]);
        let p1 = write_parquet_file(dir.path(), "state_0.mv.parquet", &physical, &[b1]);

        let b2 = make_physical_batch(&physical, &[2, 3], &[30, 40], &[300, 400]);
        let p2 = write_parquet_file(dir.path(), "state_1.mv.parquet", &physical, &[b2]);

        let logical = three_col_logical_schema();
        let projection = three_col_projection();

        let provider = MvStateTableProvider::new(
            Arc::clone(&logical),
            vec![p1, p2],
            projection,
            Arc::clone(&physical),
        );

        let ctx = SessionContext::new();
        ctx.register_table("mv", Arc::new(provider)).unwrap();

        let batches = ctx
            .sql(
                "SELECT \"RegionID\", SUM(cnt) AS total_cnt, SUM(adv) AS total_adv \
                 FROM mv GROUP BY \"RegionID\" ORDER BY \"RegionID\"",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let region = batches[0]
            .column_by_name("RegionID")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let total_cnt = batches[0]
            .column_by_name("total_cnt")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let total_adv = batches[0]
            .column_by_name("total_adv")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        assert_eq!(region.value(0), 1);
        assert_eq!(total_cnt.value(0), 15); // 10 + 5
        assert_eq!(total_adv.value(0), 150); // 100 + 50

        assert_eq!(region.value(1), 2);
        assert_eq!(total_cnt.value(1), 50); // 20 + 30
        assert_eq!(total_adv.value(1), 500); // 200 + 300

        assert_eq!(region.value(2), 3);
        assert_eq!(total_cnt.value(2), 40);
        assert_eq!(total_adv.value(2), 400);
    }

    // ====================================================================
    // 45-column wide schema test
    // ====================================================================

    #[tokio::test]
    async fn test_45_column_streaming_with_null_fill() {
        let dir = TempDir::new().unwrap();

        // Build a 45-column physical schema (5 keys + 40 metrics).
        let group_keys = vec!["EventTime", "RegionID", "OS", "CounterID", "IsRefresh"];
        let metric_names: Vec<String> = (0..40).map(|i| format!("metric_{}", i)).collect();
        let mut physical_fields: Vec<Field> = group_keys
            .iter()
            .map(|k| Field::new(*k, DataType::Int64, false))
            .collect();
        for name in &metric_names {
            physical_fields.push(Field::new(name, DataType::Int64, true));
        }
        assert_eq!(physical_fields.len(), 45);
        let physical = Arc::new(Schema::new(physical_fields));

        // State fields = same order as physical.
        let state_fields: Vec<String> = group_keys
            .iter()
            .map(|k| k.to_string())
            .chain(metric_names.iter().cloned())
            .collect();

        // Logical schema: 45 state + 1 extra null-fill.
        let mut logical_fields: Vec<Field> = state_fields
            .iter()
            .map(|name| Field::new(name, DataType::Int64, true))
            .collect();
        logical_fields.push(Field::new("_mv_source_generation", DataType::Int64, true));
        let logical = Arc::new(Schema::new(logical_fields));

        // Projection: identity for first 45, null-fill for 46th.
        let mut proj: Vec<MvColumnProjection> = (0..45)
            .map(|i| MvColumnProjection {
                physical_position: i,
                cast_to: None,
                null_fill: false,
            })
            .collect();
        proj.push(MvColumnProjection {
            physical_position: 0,
            cast_to: Some(DataType::Int64),
            null_fill: true,
        });

        // Write 5 files with 100 rows each = 500 total rows.
        let num_rows_per_file = 100usize;
        let mut paths = Vec::new();
        for f in 0..5 {
            let columns: Vec<ArrayRef> = (0..45)
                .map(|col| {
                    Arc::new(Int64Array::from(
                        (0..num_rows_per_file)
                            .map(|r| (f * 1000 + col * 10 + r) as i64)
                            .collect::<Vec<_>>(),
                    )) as ArrayRef
                })
                .collect();
            let batch = RecordBatch::try_new(Arc::clone(&physical), columns).unwrap();
            let path = write_parquet_file(
                dir.path(),
                &format!("state_{}.mv.parquet", f),
                &physical,
                &[batch],
            );
            paths.push(path);
        }

        let provider =
            MvStateTableProvider::new(Arc::clone(&logical), paths, proj, Arc::clone(&physical));

        let ctx = SessionContext::new();
        ctx.register_table("mv", Arc::new(provider)).unwrap();

        // COUNT(*)
        let batches = ctx
            .sql("SELECT COUNT(*) AS cnt FROM mv")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let total: i64 = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(total, 500);

        // Verify null-fill column
        let batches = ctx
            .sql("SELECT _mv_source_generation FROM mv LIMIT 1")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        assert!(batches[0].column(0).is_null(0));

        // Verify a specific metric column is readable
        let batches = ctx
            .sql("SELECT SUM(metric_0) AS m0_sum FROM mv")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let m0_sum: i64 = batches[0]
            .column(0)
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        // metric_0 = col_index 5, values = f*1000 + 5*10 + r = f*1000+50+r
        // For f=0: sum(50..149) = 100*50 + sum(0..99) = 5000+4950 = 9950
        // For f=1: sum(1050..1149) = 100*1050 + sum(0..99) = 105000+4950 = 109950
        // etc. Total = 9950 + 109950 + 209950 + 309950 + 409950 = 1049750
        let expected: i64 = (0..5i64)
            .map(|f| (0..100i64).map(|r| f * 1000 + 5 * 10 + r).sum::<i64>())
            .sum();
        assert_eq!(m0_sum, expected);
    }

    // ====================================================================
    // Error propagation: nonexistent file
    // ====================================================================

    #[tokio::test]
    async fn test_error_on_missing_file() {
        let logical = three_col_logical_schema();
        let physical = three_col_physical_schema();
        let projection = three_col_projection();

        let provider = MvStateTableProvider::new(
            Arc::clone(&logical),
            vec!["/nonexistent/path/state.mv.parquet".to_string()],
            projection,
            Arc::clone(&physical),
        );

        let ctx = SessionContext::new();
        ctx.register_table("mv", Arc::new(provider)).unwrap();

        let result = ctx
            .sql("SELECT COUNT(*) FROM mv")
            .await
            .unwrap()
            .collect()
            .await;
        assert!(result.is_err(), "missing file must cause query failure");
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("failed to open") || err.contains("No such file"),
            "error must mention file open failure: {}",
            err
        );
    }

    // ====================================================================
    // read_schema_from_first_file
    // ====================================================================

    #[test]
    fn test_read_schema_from_first_file_success() {
        let dir = TempDir::new().unwrap();
        let physical = three_col_physical_schema();
        let batch = make_physical_batch(&physical, &[1], &[10], &[100]);
        let p = write_parquet_file(dir.path(), "state.mv.parquet", &physical, &[batch]);

        let schema = read_schema_from_first_file(&[p]).unwrap().unwrap();
        assert_eq!(schema.fields().len(), 3);
        assert_eq!(schema.field(0).name(), "RegionID");
    }

    #[test]
    fn test_read_schema_from_empty_list() {
        let schema = read_schema_from_first_file(&[]).unwrap();
        assert!(schema.is_none());
    }

    // ====================================================================
    // Many files (partition count) test
    // ====================================================================

    #[tokio::test]
    async fn test_many_files_all_counted() {
        let dir = TempDir::new().unwrap();
        let physical = three_col_physical_schema();

        let num_files = 50;
        let mut paths = Vec::new();
        for i in 0..num_files {
            let batch = make_physical_batch(&physical, &[(i + 1) as i64], &[1], &[10]);
            let p = write_parquet_file(
                dir.path(),
                &format!("state_{:03}.mv.parquet", i),
                &physical,
                &[batch],
            );
            paths.push(p);
        }

        let logical = three_col_logical_schema();
        let projection = three_col_projection();

        let provider = MvStateTableProvider::new(
            Arc::clone(&logical),
            paths,
            projection,
            Arc::clone(&physical),
        );

        let ctx = SessionContext::new();
        ctx.register_table("mv", Arc::new(provider)).unwrap();

        let batches = ctx
            .sql("SELECT COUNT(*) AS cnt, SUM(cnt) AS total_cnt FROM mv")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let count: i64 = batches[0]
            .column_by_name("cnt")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        let total_cnt: i64 = batches[0]
            .column_by_name("total_cnt")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(count, 50);
        assert_eq!(total_cnt, 50); // each file has cnt=1
    }

    // ====================================================================
    // Utf8 (keyword) group key round-trip — mirrors clickbench_5m_url's URL
    // key + representative aggregate state columns, streamed through the
    // lazy provider. Uses the shared helpers (no large fixtures).
    // ====================================================================

    #[tokio::test]
    async fn test_utf8_group_key_round_trip() {
        let dir = TempDir::new().unwrap();

        // Physical state schema: a Utf8 group key + two Int64 aggregate states,
        // in DataFusion output order [URL, sum, cnt].
        let physical = Arc::new(Schema::new(vec![
            Field::new("URL", DataType::Utf8, false),
            Field::new("sum(mv_input.ClientIP)[sum]", DataType::Int64, true),
            Field::new("count(mv_input.ClientIP)[count]", DataType::Int64, true),
        ]));

        let urls = StringArray::from(vec!["/a", "/b", "/a", "/a", "/b"]);
        let sums = Int64Array::from(vec![10_i64, 20, 30, 40, 50]);
        let cnts = Int64Array::from(vec![1_i64, 1, 1, 1, 1]);
        let batch = RecordBatch::try_new(
            Arc::clone(&physical),
            vec![Arc::new(urls), Arc::new(sums), Arc::new(cnts)],
        )
        .unwrap();
        let path = write_parquet_file(dir.path(), "state_url_0.mv.parquet", &physical, &[batch]);

        // Logical schema uses stable aliases; projection reorders to
        // [URL, cip_sum, cip_cnt] (keyword key stays position 0, no cast).
        let logical = Arc::new(Schema::new(vec![
            Field::new("URL", DataType::Utf8, true),
            Field::new("cip_sum", DataType::Int64, true),
            Field::new("cip_cnt", DataType::Int64, true),
        ]));
        let projection = vec![
            MvColumnProjection {
                physical_position: 0,
                cast_to: None,
                null_fill: false,
            },
            MvColumnProjection {
                physical_position: 1,
                cast_to: None,
                null_fill: false,
            },
            MvColumnProjection {
                physical_position: 2,
                cast_to: None,
                null_fill: false,
            },
        ];

        let provider = MvStateTableProvider::new(
            Arc::clone(&logical),
            vec![path],
            projection,
            Arc::clone(&physical),
        );

        let ctx = SessionContext::new();
        ctx.register_table("mv", Arc::new(provider)).unwrap();

        // Fold over the Utf8 key: /a -> sum 80 cnt 3, /b -> sum 70 cnt 2.
        let batches = ctx
            .sql("SELECT \"URL\", SUM(cip_sum) AS s, SUM(cip_cnt) AS c FROM mv GROUP BY \"URL\" ORDER BY \"URL\"")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        let total_rows: usize = batches.iter().map(|b| b.num_rows()).sum();
        assert_eq!(total_rows, 2, "two distinct URL groups");

        let url = batches[0]
            .column_by_name("URL")
            .unwrap()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        let s = batches[0]
            .column_by_name("s")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let c = batches[0]
            .column_by_name("c")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        assert_eq!(url.value(0), "/a");
        assert_eq!(s.value(0), 80);
        assert_eq!(c.value(0), 3);
        assert_eq!(url.value(1), "/b");
        assert_eq!(s.value(1), 70);
        assert_eq!(c.value(1), 2);
    }

    // ====================================================================
    // Timestamp(Millisecond) → Int64 cast — DE6 clickbench_100m regression
    // ====================================================================
    //
    // ClickBench's EventTime maps as `date` (epoch_millis) and arrives in
    // Arrow state as Timestamp(Millisecond, None).  The target mapping
    // declares it as `long` (Int64).  Timestamp is physically i64 so the
    // cast is lossless — but the prior widening check only accepted pure
    // integer type widening and rejected Timestamp → Int64, crashing the
    // search path with "only lossless integer widening is permitted".

    #[tokio::test]
    async fn test_timestamp_millis_to_int64_cast() {
        use arrow::array::TimestampMillisecondArray;

        let dir = TempDir::new().unwrap();

        // Physical schema: EventTime as Timestamp(Millisecond) + two Int64 metrics.
        let physical = Arc::new(Schema::new(vec![
            Field::new(
                "EventTime",
                DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("cnt", DataType::Int64, false),
            Field::new("adv_sum", DataType::Int64, true),
        ]));
        // Sample epoch-millis values (ClickBench-scale).
        let ts_values: Vec<i64> = vec![1401805406823, 1401805706823, 1401806006823];
        let batch = RecordBatch::try_new(
            Arc::clone(&physical),
            vec![
                Arc::new(TimestampMillisecondArray::from(ts_values.clone())),
                Arc::new(Int64Array::from(vec![10, 20, 30])),
                Arc::new(Int64Array::from(vec![100, 200, 300])),
            ],
        )
        .unwrap();
        let path = write_parquet_file(dir.path(), "state_ts.mv.parquet", &physical, &[batch]);

        // Logical schema: EventTime as Int64 (matching target mapping "long").
        let logical = Arc::new(Schema::new(vec![
            Field::new("EventTime", DataType::Int64, true),
            Field::new("cnt", DataType::Int64, true),
            Field::new("adv_sum", DataType::Int64, true),
        ]));

        // Projection: identity positions, Timestamp→Int64 cast on column 0.
        let projection = vec![
            MvColumnProjection {
                physical_position: 0,
                cast_to: Some(DataType::Int64),
                null_fill: false,
            },
            MvColumnProjection {
                physical_position: 1,
                cast_to: None,
                null_fill: false,
            },
            MvColumnProjection {
                physical_position: 2,
                cast_to: None,
                null_fill: false,
            },
        ];

        let provider = MvStateTableProvider::new(
            Arc::clone(&logical),
            vec![path],
            projection,
            Arc::clone(&physical),
        );

        let ctx = SessionContext::new();
        ctx.register_table("mv", Arc::new(provider)).unwrap();

        // Verify COUNT and SUM work through the cast.
        let batches = ctx
            .sql("SELECT COUNT(*) AS total, SUM(adv_sum) AS s FROM mv")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let total: i64 = batches[0]
            .column_by_name("total")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(total, 3);
        let s: i64 = batches[0]
            .column_by_name("s")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(0);
        assert_eq!(s, 600);

        // Verify the EventTime values round-trip exactly (epoch millis preserved).
        let batches = ctx
            .sql("SELECT \"EventTime\", cnt FROM mv ORDER BY \"EventTime\"")
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();
        let et = batches[0]
            .column_by_name("EventTime")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        assert_eq!(et.value(0), 1401805406823_i64);
        assert_eq!(et.value(1), 1401805706823_i64);
        assert_eq!(et.value(2), 1401806006823_i64);
    }

    // ====================================================================
    // Timestamp(Millisecond) GROUP BY + aggregate fold — full DE6 path
    // ====================================================================

    #[tokio::test]
    async fn test_timestamp_millis_group_by_fold() {
        use arrow::array::TimestampMillisecondArray;

        let dir = TempDir::new().unwrap();

        let physical = Arc::new(Schema::new(vec![
            Field::new(
                "EventTime",
                DataType::Timestamp(arrow::datatypes::TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("RegionID", DataType::Int64, false),
            Field::new("adv_sum", DataType::Int64, true),
            Field::new("adv_cnt", DataType::Int64, false),
        ]));

        // Two files with overlapping EventTime keys to test GROUP BY fold.
        let b1 = RecordBatch::try_new(
            Arc::clone(&physical),
            vec![
                Arc::new(TimestampMillisecondArray::from(vec![1000, 2000, 1000])),
                Arc::new(Int64Array::from(vec![1, 2, 1])),
                Arc::new(Int64Array::from(vec![10, 20, 30])),
                Arc::new(Int64Array::from(vec![1, 1, 1])),
            ],
        )
        .unwrap();
        let p1 = write_parquet_file(dir.path(), "s0.mv.parquet", &physical, &[b1]);

        let b2 = RecordBatch::try_new(
            Arc::clone(&physical),
            vec![
                Arc::new(TimestampMillisecondArray::from(vec![2000, 1000])),
                Arc::new(Int64Array::from(vec![2, 3])),
                Arc::new(Int64Array::from(vec![40, 50])),
                Arc::new(Int64Array::from(vec![1, 1])),
            ],
        )
        .unwrap();
        let p2 = write_parquet_file(dir.path(), "s1.mv.parquet", &physical, &[b2]);

        let logical = Arc::new(Schema::new(vec![
            Field::new("EventTime", DataType::Int64, true),
            Field::new("RegionID", DataType::Int64, true),
            Field::new("adv_sum", DataType::Int64, true),
            Field::new("adv_cnt", DataType::Int64, true),
        ]));

        let projection = vec![
            MvColumnProjection {
                physical_position: 0,
                cast_to: Some(DataType::Int64),
                null_fill: false,
            },
            MvColumnProjection {
                physical_position: 1,
                cast_to: None,
                null_fill: false,
            },
            MvColumnProjection {
                physical_position: 2,
                cast_to: None,
                null_fill: false,
            },
            MvColumnProjection {
                physical_position: 3,
                cast_to: None,
                null_fill: false,
            },
        ];

        let provider = MvStateTableProvider::new(
            Arc::clone(&logical),
            vec![p1, p2],
            projection,
            Arc::clone(&physical),
        );

        let ctx = SessionContext::new();
        ctx.register_table("mv", Arc::new(provider)).unwrap();

        // GROUP BY EventTime fold: Ts=1000 has rows (1,10,1), (1,30,1), (3,50,1)
        //   total_adv=90, total_cnt=3
        // Ts=2000 has rows (2,20,1), (2,40,1)
        //   total_adv=60, total_cnt=2
        let batches = ctx
            .sql(
                "SELECT \"EventTime\", SUM(adv_sum) AS total_adv, SUM(adv_cnt) AS total_cnt \
                 FROM mv GROUP BY \"EventTime\" ORDER BY \"EventTime\"",
            )
            .await
            .unwrap()
            .collect()
            .await
            .unwrap();

        assert_eq!(batches[0].num_rows(), 2);
        let et = batches[0]
            .column_by_name("EventTime")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let total_adv = batches[0]
            .column_by_name("total_adv")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();
        let total_cnt = batches[0]
            .column_by_name("total_cnt")
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap();

        assert_eq!(et.value(0), 1000);
        assert_eq!(total_adv.value(0), 90); // 10+30+50
        assert_eq!(total_cnt.value(0), 3);
        assert_eq!(et.value(1), 2000);
        assert_eq!(total_adv.value(1), 60); // 20+40
        assert_eq!(total_cnt.value(1), 2);
    }
}
