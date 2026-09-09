/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! [`PhysicalExprAdapter`] implementing the MV state-file schema-evolution
//! contract on the standard DataFusion parquet scan.
//!
//! # Why this exists
//!
//! MV state generations are Parquet files whose column NAMES are writer-plan
//! aliases (e.g. `sum(mv_input.AdvEngineID)[sum]`) while queries resolve
//! against the logical `state_fields` names (e.g. `sum_AdvEngineID`). The
//! mapping is POSITIONAL: `state_fields[i]` labels physical column `i`.
//! Additionally, generations are heterogeneous over time:
//!
//! - a later generation may store a *narrower* integer type for the same
//!   column (Int16 vs Int64) — lossless widening is required per file;
//! - `date`-typed group keys arrive as `Timestamp(ms)` but the logical
//!   mapping declares them `long` (Int64) — lossless reinterpretation;
//! - logical fields may be absent from older files — null-fill.
//!
//! This adapter moves the contract to DataFusion's designed seam: the parquet
//! opener rewrites every projection and predicate expression through
//! [`PhysicalExprAdapter::rewrite`] BEFORE building pruning predicates, so
//! relabel/cast/null-fill ride directly into row-group and page pruning —
//! filters prune correctly across schema evolution instead of forcing full
//! scans.
//!
//! # Source-side POC adaptation
//!
//! Hydrated state files here are named `_mv_partial.*.parquet` (leading
//! underscore) rather than `*.mv.parquet`. Because the read path is handed the
//! EXPLICIT absolute file list, [`register_mv_state_listing_table`] sets an
//! empty `with_file_extension` filter — a non-collection `ListingTableUrl`
//! resolves via `ObjectStore::head` and never triggers directory listing's
//! hidden-file (`_`/`.`) rule, and an empty extension admits any suffix.

use std::sync::Arc;

use arrow::datatypes::SchemaRef;
use datafusion::common::tree_node::{Transformed, TransformedResult, TreeNode};
use datafusion::common::{DataFusionError, Result, ScalarValue};
use datafusion::physical_expr::expressions::{self, CastExpr, Column};
use datafusion::physical_expr_adapter::{PhysicalExprAdapter, PhysicalExprAdapterFactory};
use datafusion::physical_plan::PhysicalExpr;

use crate::session_context::is_lossless_integer_widening;

/// Factory carrying the ordered `state_fields` contract.
///
/// `state_fields[i]` is the logical name of physical column `i`. Entries at
/// positions `>= physical_file_schema.fields().len()` (per file) have no
/// physical counterpart in that file and are null-filled.
#[derive(Debug)]
pub struct MvPhysicalExprAdapterFactory {
    state_fields: Vec<String>,
}

impl MvPhysicalExprAdapterFactory {
    pub fn new(state_fields: Vec<String>) -> Self {
        Self { state_fields }
    }
}

impl PhysicalExprAdapterFactory for MvPhysicalExprAdapterFactory {
    fn create(
        &self,
        logical_file_schema: SchemaRef,
        physical_file_schema: SchemaRef,
    ) -> Result<Arc<dyn PhysicalExprAdapter>> {
        Ok(Arc::new(MvPhysicalExprAdapter {
            state_fields: self.state_fields.clone(),
            logical_file_schema,
            physical_file_schema,
        }))
    }
}

/// Per-file rewriter: logical `Column` references become physical-position
/// `Column`s (optionally wrapped in a lossless-widening `CastExpr`), and
/// logical fields absent from this file become typed NULL literals.
#[derive(Debug)]
struct MvPhysicalExprAdapter {
    state_fields: Vec<String>,
    logical_file_schema: SchemaRef,
    physical_file_schema: SchemaRef,
}

impl PhysicalExprAdapter for MvPhysicalExprAdapter {
    fn rewrite(&self, expr: Arc<dyn PhysicalExpr>) -> Result<Arc<dyn PhysicalExpr>> {
        use datafusion::common::tree_node::TreeNodeRecursion;
        let physical_count = self.physical_file_schema.fields().len();
        expr.transform_down(|e| {
            let Some(col) = e.downcast_ref::<Column>() else {
                return Ok(Transformed::no(e));
            };
            let logical_name = col.name();
            let logical_field = self
                .logical_file_schema
                .field_with_name(logical_name)
                .map_err(|_| {
                    DataFusionError::Execution(format!(
                        "MvPhysicalExprAdapter: column '{}' is not part of the MV logical schema",
                        logical_name
                    ))
                })?;

            // All replacements are terminal: never descend into the new node's
            // children (the physical column names are writer aliases that are
            // intentionally NOT part of the logical schema).
            let replaced = |node: Arc<dyn PhysicalExpr>| {
                Ok(Transformed::new(node, true, TreeNodeRecursion::Jump))
            };

            match self
                .state_fields
                .iter()
                .position(|name| name == logical_name)
            {
                Some(pos) if pos < physical_count => {
                    let physical_field = self.physical_file_schema.field(pos);
                    let physical_col: Arc<dyn PhysicalExpr> =
                        Arc::new(Column::new(physical_field.name(), pos));
                    if physical_field.data_type() == logical_field.data_type() {
                        replaced(physical_col)
                    } else if is_lossless_integer_widening(
                        physical_field.data_type(),
                        logical_field.data_type(),
                    ) {
                        replaced(Arc::new(CastExpr::new(
                            physical_col,
                            logical_field.data_type().clone(),
                            None,
                        )))
                    } else {
                        Err(DataFusionError::Execution(format!(
                            "MvPhysicalExprAdapter: state field '{}' at physical position {} \
                             has type {:?} but query expects {:?}; only lossless integer \
                             widening is permitted",
                            logical_name,
                            pos,
                            physical_field.data_type(),
                            logical_field.data_type(),
                        )))
                    }
                }
                // state_fields entry beyond this file's physical columns, or a
                // logical-only field (e.g. `_mv_source_generation`): null-fill.
                _ => {
                    let null = ScalarValue::try_from(logical_field.data_type())?;
                    replaced(expressions::lit(null))
                }
            }
        })
        .data()
    }
}

// ---------------------------------------------------------------------------
// Registration: the ONE path for reading MV state (production + tests)
// ---------------------------------------------------------------------------

/// Reads the Arrow schema from the first Parquet state file without loading
/// any record batches (footer metadata only, O(1) memory). Returns None when
/// there are no files. Legacy `.mv.arrow` files fail closed.
pub fn read_schema_from_first_file(state_file_paths: &[String]) -> Result<Option<SchemaRef>> {
    let path = match state_file_paths.first() {
        Some(p) => p,
        None => return Ok(None),
    };
    if path.ends_with(".mv.arrow") {
        return Err(DataFusionError::Execution(format!(
            "read_schema_from_first_file: legacy Arrow IPC state file '{}' is no longer \
             supported; rebuild the materialized view to generate Parquet state files",
            path
        )));
    }
    let file = std::fs::File::open(path).map_err(|e| {
        DataFusionError::Execution(format!(
            "read_schema_from_first_file: failed to open '{}': {}",
            path, e
        ))
    })?;
    let builder = parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder::try_new(file)
        .map_err(|e| {
            DataFusionError::Execution(format!(
                "read_schema_from_first_file: failed to read Parquet '{}': {}",
                path, e
            ))
        })?;
    Ok(Some(builder.schema().clone()))
}

/// Registers the MV state files as a standard `ListingTable` with the MV
/// expr adapter attached — the single read path for MV state (used by
/// production `create_mv_only_session_context` and by tests). The parquet
/// opener rewrites projections AND predicates through the adapter BEFORE
/// building pruning predicates, so filters get row-group/page statistics
/// pruning and repartitioning provides sub-file parallelism.
pub async fn register_mv_state_listing_table(
    ctx: &datafusion::prelude::SessionContext,
    register_name: &str,
    state_file_paths: &[String],
    table_schema: SchemaRef,
    state_fields: &[String],
) -> Result<()> {
    use datafusion::datasource::file_format::parquet::ParquetFormat;
    use datafusion::datasource::listing::{
        ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
    };
    for path in state_file_paths {
        if path.ends_with(".mv.arrow") {
            return Err(DataFusionError::Execution(format!(
                "register_mv_state_listing_table: legacy Arrow IPC state file '{}' is no \
                 longer supported; rebuild the materialized view",
                path
            )));
        }
        // Fail closed: a missing state generation is corruption, not an empty
        // table (ListingTable would otherwise silently treat it as no files).
        if !std::path::Path::new(path).exists() {
            return Err(DataFusionError::Execution(format!(
                "register_mv_state_listing_table: MV state file '{}' does not exist",
                path
            )));
        }
    }
    let urls: Vec<ListingTableUrl> = state_file_paths
        .iter()
        .map(|p| ListingTableUrl::parse(p.as_str()))
        .collect::<std::result::Result<_, _>>()?;
    // `target_partitions` on the LISTING OPTIONS (not the session config) gates
    // the sort-aware path twice in DF54's ListingTable::scan: it sets the initial
    // file-group count, and the statistics split is only accepted when
    // `new_groups.len() <= options.target_partitions`. Overlapping MV generations
    // cannot chain into one sorted group — each needs its own — so the ordered
    // path requires target_partitions >= file count (default is 1, which silently
    // downgrades every fold to hash aggregation). We take max(session value,
    // file count): more groups than cores is fine, the scheduler multiplexes.
    //
    // `collect_stat` MUST stay false: DF54 computes per-file statistics by NAME
    // against the table schema, and MV state files carry writer-alias column
    // names — every aggregate column resolves to all-null statistics, which
    // null-fills the scanned values (empirically: e2e tests return null sums
    // with it enabled). It is also unnecessary: row-group/page pruning reads
    // footer metadata through the expr adapter, not listing-level statistics.
    //
    // Source-side POC: state files are `_mv_partial.*.parquet` (leading
    // underscore). The read path receives EXPLICIT absolute file paths, so
    // directory listing's hidden-file rule never applies; an EMPTY extension
    // filter admits the files regardless of their suffix (the reference used
    // `.mv.parquet`, which would exclude these files).
    let session_tp = ctx.state().config().options().execution.target_partitions;
    let listing_tp = session_tp.max(state_file_paths.len()).max(1);
    let mut listing_options = ListingOptions::new(Arc::new(ParquetFormat::default()))
        .with_file_extension("")
        .with_collect_stat(false)
        .with_target_partitions(listing_tp);

    // Advertise the per-file sort order when EVERY file's parquet footer
    // carries identical SortingColumn metadata (stamped by the build and
    // merge writers). Sorted-input advertisement lets DataFusion fold group
    // keys with streaming (sorted) aggregation instead of re-hashing all
    // state rows. Fail-safe: any file without matching metadata (e.g. a
    // generation written before stamping existed) means no advertisement —
    // a false ordering claim would silently corrupt aggregation results.
    if let Some(sort_cols) = unanimous_footer_sort_order(state_file_paths)? {
        let sort_exprs: Vec<datafusion::logical_expr::SortExpr> = sort_cols
            .iter()
            .map(|sc| {
                let pos = sc.column_idx as usize;
                let logical_name = state_fields.get(pos).ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "register_mv_state_listing_table: footer sort column ordinal {} has no \
                         state_fields entry",
                        pos
                    ))
                })?;
                Ok(datafusion::prelude::col(format!("\"{}\"", logical_name))
                    .sort(!sc.descending, sc.nulls_first))
            })
            .collect::<Result<_>>()?;
        listing_options = listing_options.with_file_sort_order(vec![sort_exprs]);
    }

    let listing_config = ListingTableConfig::new_with_multi_paths(urls)
        .with_listing_options(listing_options)
        .with_schema(Arc::clone(&table_schema))
        .with_expr_adapter_factory(Arc::new(MvPhysicalExprAdapterFactory::new(
            state_fields.to_vec(),
        )));
    let table = ListingTable::try_new(listing_config)?;
    ctx.register_table(register_name, Arc::new(table))?;
    Ok(())
}

/// Reads the parquet footer `SortingColumn` metadata of every state file.
/// Returns Some(order) only when every file (and every row group within each
/// file) declares the identical order; None otherwise.
fn unanimous_footer_sort_order(
    state_file_paths: &[String],
) -> Result<Option<Vec<parquet::file::metadata::SortingColumn>>> {
    let mut agreed: Option<Vec<parquet::file::metadata::SortingColumn>> = None;
    for path in state_file_paths {
        let file = std::fs::File::open(path).map_err(|e| {
            DataFusionError::Execution(format!(
                "unanimous_footer_sort_order: failed to open '{}': {}",
                path, e
            ))
        })?;
        let reader = parquet::file::reader::SerializedFileReader::new(file).map_err(|e| {
            DataFusionError::Execution(format!(
                "unanimous_footer_sort_order: failed to read footer of '{}': {}",
                path, e
            ))
        })?;
        use parquet::file::reader::FileReader;
        let meta = reader.metadata();
        if meta.num_row_groups() == 0 {
            continue; // empty file constrains nothing
        }
        for rg in 0..meta.num_row_groups() {
            match meta.row_group(rg).sorting_columns() {
                Some(cols) if !cols.is_empty() => match &agreed {
                    None => agreed = Some(cols.clone()),
                    Some(prev) if prev == cols => {}
                    Some(_) => return Ok(None), // disagreement -> no claim
                },
                _ => return Ok(None), // unstamped row group -> no claim
            }
        }
    }
    Ok(agreed)
}

#[cfg(test)]
mod tests {
    use super::*;
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datafusion::logical_expr::Operator;
    use datafusion::physical_expr::expressions::{lit, BinaryExpr};

    fn logical_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("event_bucket", DataType::Int64, true),
            Field::new("URL", DataType::Utf8, true),
            Field::new("sum_Adv", DataType::Int64, true),
            Field::new("_mv_source_generation", DataType::Int64, true),
        ]))
    }

    /// Physical files carry writer aliases and possibly narrower types.
    fn physical_schema_wide() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new(
                "event_bucket",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("mv_input.URL", DataType::Utf8, false),
            Field::new("sum(mv_input.Adv)[sum]", DataType::Int16, true),
        ]))
    }

    fn state_fields() -> Vec<String> {
        vec![
            "event_bucket".into(),
            "URL".into(),
            "sum_Adv".into(),
            "_mv_source_generation".into(), // beyond physical count -> null-fill
        ]
    }

    fn adapter() -> Arc<dyn PhysicalExprAdapter> {
        MvPhysicalExprAdapterFactory::new(state_fields())
            .create(logical_schema(), physical_schema_wide())
            .unwrap()
    }

    #[test]
    fn relabels_string_column_positionally() {
        // URL (logical idx 1) -> physical column 1 named 'mv_input.URL', same type.
        let expr: Arc<dyn PhysicalExpr> = Arc::new(BinaryExpr::new(
            Arc::new(Column::new("URL", 1)),
            Operator::Eq,
            lit("http://x"),
        ));
        let rewritten = adapter().rewrite(expr).unwrap();
        let s = format!("{rewritten:?}");
        assert!(s.contains("mv_input.URL"), "physical name expected: {s}");
        assert!(
            !s.contains("Column { name: \"URL\""),
            "logical name must be gone: {s}"
        );
    }

    #[test]
    fn widens_narrow_aggregate_state() {
        // sum_Adv logical Int64, physical Int16 -> CastExpr inserted.
        let expr: Arc<dyn PhysicalExpr> = Arc::new(Column::new("sum_Adv", 2));
        let rewritten = adapter().rewrite(expr).unwrap();
        let s = format!("{rewritten:?}");
        assert!(s.contains("CastExpr"), "expected cast: {s}");
        assert!(
            s.contains("sum(mv_input.Adv)[sum]"),
            "physical alias expected: {s}"
        );
    }

    #[test]
    fn timestamp_group_key_reinterpreted_as_long() {
        let expr: Arc<dyn PhysicalExpr> = Arc::new(Column::new("event_bucket", 0));
        let rewritten = adapter().rewrite(expr).unwrap();
        let s = format!("{rewritten:?}");
        assert!(s.contains("CastExpr"), "timestamp->int64 cast expected: {s}");
    }

    /// Regression: a KEYWORD group key is written to the Parquet state as `Utf8`, while the
    /// plan's logical schema reports it as `Utf8View` (DataFusion's string_view default). The
    /// value domain is identical, so this must be accepted as a lossless cast — the deployed
    /// cbspan1_mv_mv1 target failed every PPL query on exactly this shape.
    #[test]
    fn utf8_group_key_widens_to_utf8_view() {
        let logical = Arc::new(Schema::new(vec![Field::new("URL", DataType::Utf8View, true)]));
        let physical = Arc::new(Schema::new(vec![Field::new("URL", DataType::Utf8, true)]));
        let a = MvPhysicalExprAdapterFactory::new(vec!["URL".into()])
            .create(logical, physical)
            .unwrap();
        let rewritten = a.rewrite(Arc::new(Column::new("URL", 0))).unwrap();
        let s = format!("{rewritten:?}");
        assert!(s.contains("CastExpr"), "utf8->utf8view cast expected: {s}");
        assert!(s.contains("Utf8View"), "cast target must be Utf8View: {s}");
    }

    #[test]
    fn absent_logical_field_null_fills() {
        let expr: Arc<dyn PhysicalExpr> = Arc::new(Column::new("_mv_source_generation", 3));
        let rewritten = adapter().rewrite(expr).unwrap();
        let s = format!("{rewritten:?}");
        assert!(
            s.contains("NULL") || s.contains("Literal"),
            "typed null literal expected: {s}"
        );
    }

    #[test]
    fn unknown_column_is_rejected() {
        let expr: Arc<dyn PhysicalExpr> = Arc::new(Column::new("not_a_field", 0));
        let err = adapter().rewrite(expr).unwrap_err().to_string();
        assert!(err.contains("not part of the MV logical schema"), "{err}");
    }

    #[test]
    fn narrowing_is_rejected() {
        // Physical wider than logical: flip the schemas.
        let logical = Arc::new(Schema::new(vec![Field::new(
            "sum_Adv",
            DataType::Int16,
            true,
        )]));
        let physical = Arc::new(Schema::new(vec![Field::new(
            "sum(mv_input.Adv)[sum]",
            DataType::Int64,
            true,
        )]));
        let a = MvPhysicalExprAdapterFactory::new(vec!["sum_Adv".into()])
            .create(logical, physical)
            .unwrap();
        let err = a
            .rewrite(Arc::new(Column::new("sum_Adv", 0)))
            .unwrap_err()
            .to_string();
        assert!(err.contains("only lossless integer widening"), "{err}");
    }
}

// ---------------------------------------------------------------------------
// End-to-end tests through the ONE production registration path
// ---------------------------------------------------------------------------

#[cfg(test)]
mod e2e_tests {
    use super::*;
    use arrow::array::{Int16Array, Int64Array, RecordBatch, StringArray, TimestampMillisecondArray};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use datafusion::prelude::SessionContext;
    use parquet::arrow::ArrowWriter;
    use parquet::file::properties::WriterProperties;
    use tempfile::TempDir;

    /// Physical file schema uses WRITER ALIASES (the real contract).
    fn physical_schema_i64() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("event_bucket", DataType::Int64, false),
            Field::new("mv_input.URL", DataType::Utf8, false),
            Field::new("sum(mv_input.Adv)[sum]", DataType::Int64, true),
            Field::new("count(mv_input.Adv)[count]", DataType::Int64, true),
        ]))
    }

    /// Logical/table schema uses state_fields names.
    fn table_schema() -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new("event_bucket", DataType::Int64, true),
            Field::new("URL", DataType::Utf8, true),
            Field::new("sum_Adv", DataType::Int64, true),
            Field::new("count_Adv", DataType::Int64, true),
        ]))
    }

    fn state_fields() -> Vec<String> {
        vec![
            "event_bucket".into(),
            "URL".into(),
            "sum_Adv".into(),
            "count_Adv".into(),
        ]
    }

    fn write_gen(
        dir: &std::path::Path,
        name: &str,
        schema: &SchemaRef,
        batch: RecordBatch,
    ) -> String {
        let path = dir.join(name);
        let file = std::fs::File::create(&path).unwrap();
        let mut w =
            ArrowWriter::try_new(file, schema.clone(), Some(WriterProperties::builder().build()))
                .unwrap();
        w.write(&batch).unwrap();
        w.close().unwrap();
        path.to_str().unwrap().to_string()
    }

    fn gen_batch(
        schema: &SchemaRef,
        buckets: &[i64],
        urls: &[&str],
        sums: &[i64],
        cnts: &[i64],
    ) -> RecordBatch {
        RecordBatch::try_new(
            Arc::clone(schema),
            vec![
                Arc::new(Int64Array::from(buckets.to_vec())),
                Arc::new(StringArray::from(urls.to_vec())),
                Arc::new(Int64Array::from(sums.to_vec())),
                Arc::new(Int64Array::from(cnts.to_vec())),
            ],
        )
        .unwrap()
    }

    async fn ctx_with(paths: Vec<String>) -> SessionContext {
        // Mirror production config: group files by statistics so the
        // advertised per-file sort order survives into the scan.
        let mut config = datafusion::prelude::SessionConfig::new();
        config
            .options_mut()
            .execution
            .split_file_groups_by_statistics = true;
        let ctx = SessionContext::new_with_config(config);
        register_mv_state_listing_table(&ctx, "mv", &paths, table_schema(), &state_fields())
            .await
            .unwrap();
        ctx
    }

    async fn sql_rows(ctx: &SessionContext, q: &str) -> Vec<RecordBatch> {
        ctx.sql(q).await.unwrap().collect().await.unwrap()
    }

    fn i64_at(batches: &[RecordBatch], col: &str, row: usize) -> i64 {
        batches[0]
            .column_by_name(col)
            .unwrap()
            .as_any()
            .downcast_ref::<Int64Array>()
            .unwrap()
            .value(row)
    }

    /// REQUIRED C1 TEST: two LEADING-UNDERSCORE state files, one carrying Int16
    /// group/agg columns (exercises the widening adapter), folded exactly
    /// through the MV-only listing path. This is the source-side POC contract:
    /// files named `_mv_partial.*.parquet` must be consumed from the explicit
    /// absolute file list (no directory listing, no hidden-file exclusion, no
    /// `.parquet` extension reliance).
    #[tokio::test]
    async fn leading_underscore_files_int16_widen_fold_exact() {
        let dir = TempDir::new().unwrap();

        // Gen 0: wide (Int64) physical state.
        let ps64 = physical_schema_i64();
        let p0 = write_gen(
            dir.path(),
            "_mv_partial.s0.t1.g0.aaaaaaaa.parquet",
            &ps64,
            gen_batch(&ps64, &[100, 200], &["/a", "/b"], &[10, 20], &[1, 2]),
        );

        // Gen 1: NARROW (Int16) group + agg columns for the SAME logical fields.
        // event_bucket Int16, sum(...) Int16 — both must losslessly widen to Int64.
        let ps16 = Arc::new(Schema::new(vec![
            Field::new("event_bucket", DataType::Int16, false),
            Field::new("mv_input.URL", DataType::Utf8, false),
            Field::new("sum(mv_input.Adv)[sum]", DataType::Int16, true),
            Field::new("count(mv_input.Adv)[count]", DataType::Int64, true),
        ]));
        let b1 = RecordBatch::try_new(
            Arc::clone(&ps16),
            vec![
                Arc::new(arrow::array::Int16Array::from(vec![100_i16, 300])),
                Arc::new(StringArray::from(vec!["/a", "/c"])),
                Arc::new(Int16Array::from(vec![5_i16, 30])),
                Arc::new(Int64Array::from(vec![1_i64, 3])),
            ],
        )
        .unwrap();
        let p1 = write_gen(dir.path(), "_mv_partial.s0.t1.g1.bbbbbbbb.parquet", &ps16, b1);

        // Sanity: both file names begin with '_' (would be hidden under listing).
        assert!(std::path::Path::new(&p0)
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .starts_with('_'));
        assert!(std::path::Path::new(&p1)
            .file_name()
            .unwrap()
            .to_str()
            .unwrap()
            .starts_with('_'));

        let ctx = ctx_with(vec![p0, p1]).await;

        // Fold over the group key: bucket 100 appears in BOTH gens (10 + 5),
        // requiring the Int16 gen to widen and add exactly.
        let b = sql_rows(
            &ctx,
            "SELECT event_bucket, SUM(\"sum_Adv\") AS s, SUM(\"count_Adv\") AS c \
             FROM mv GROUP BY event_bucket ORDER BY event_bucket",
        )
        .await;
        assert_eq!(b[0].num_rows(), 3, "buckets 100/200/300");
        assert_eq!(i64_at(&b, "event_bucket", 0), 100);
        assert_eq!(i64_at(&b, "s", 0), 15, "bucket 100: 10 (i64) + 5 (i16 widened)");
        assert_eq!(i64_at(&b, "c", 0), 2);
        assert_eq!(i64_at(&b, "event_bucket", 1), 200);
        assert_eq!(i64_at(&b, "s", 1), 20);
        assert_eq!(i64_at(&b, "event_bucket", 2), 300);
        assert_eq!(i64_at(&b, "s", 2), 30);

        // Global fold too.
        let g = sql_rows(&ctx, "SELECT SUM(\"sum_Adv\") AS s FROM mv").await;
        assert_eq!(i64_at(&g, "s", 0), 65, "10+20+5+30");
    }

    /// Aggregate fold across files with overlapping keys — the core MV query shape.
    #[tokio::test]
    async fn fold_across_generations_with_writer_alias_relabel() {
        let dir = TempDir::new().unwrap();
        let ps = physical_schema_i64();
        let p1 = write_gen(
            dir.path(),
            "_mv_partial.s0.t1.g0.parquet",
            &ps,
            gen_batch(&ps, &[100, 200], &["/a", "/b"], &[10, 20], &[1, 2]),
        );
        let p2 = write_gen(
            dir.path(),
            "_mv_partial.s0.t1.g1.parquet",
            &ps,
            gen_batch(&ps, &[100, 300], &["/a", "/c"], &[5, 30], &[1, 3]),
        );
        let ctx = ctx_with(vec![p1, p2]).await;
        let b = sql_rows(
            &ctx,
            "SELECT event_bucket, SUM(\"sum_Adv\") AS s, SUM(\"count_Adv\") AS c FROM mv GROUP BY event_bucket ORDER BY event_bucket",
        )
        .await;
        assert_eq!(b[0].num_rows(), 3);
        assert_eq!(i64_at(&b, "s", 0), 15); // bucket 100: 10+5
        assert_eq!(i64_at(&b, "c", 0), 2);
        assert_eq!(i64_at(&b, "s", 1), 20);
        assert_eq!(i64_at(&b, "s", 2), 30);
    }

    /// date-typed group key: physical Timestamp(ms), logical Int64.
    #[tokio::test]
    async fn timestamp_millis_key_folds_as_long() {
        let dir = TempDir::new().unwrap();
        let ps = Arc::new(Schema::new(vec![
            Field::new(
                "event_bucket",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("mv_input.URL", DataType::Utf8, false),
            Field::new("sum(mv_input.Adv)[sum]", DataType::Int64, true),
            Field::new("count(mv_input.Adv)[count]", DataType::Int64, true),
        ]));
        let b = RecordBatch::try_new(
            Arc::clone(&ps),
            vec![
                Arc::new(TimestampMillisecondArray::from(vec![1000_i64, 2000, 1000])),
                Arc::new(StringArray::from(vec!["/a", "/b", "/a"])),
                Arc::new(Int64Array::from(vec![10_i64, 20, 30])),
                Arc::new(Int64Array::from(vec![1_i64, 1, 1])),
            ],
        )
        .unwrap();
        let p = write_gen(dir.path(), "_mv_partial.s0.t1.g0.parquet", &ps, b);
        let ctx = ctx_with(vec![p]).await;
        let b = sql_rows(
            &ctx,
            "SELECT event_bucket, SUM(\"sum_Adv\") AS s FROM mv GROUP BY event_bucket ORDER BY event_bucket",
        )
        .await;
        assert_eq!(b[0].num_rows(), 2);
        assert_eq!(i64_at(&b, "event_bucket", 0), 1000);
        assert_eq!(i64_at(&b, "s", 0), 40);
    }

    /// DEFECT #29 SHAPE: string equality on the Utf8 state column must return
    /// the matching rows.
    #[tokio::test]
    async fn string_equality_filter_returns_rows() {
        let dir = TempDir::new().unwrap();
        let ps = physical_schema_i64();
        let p1 = write_gen(
            dir.path(),
            "_mv_partial.s0.t1.g0.parquet",
            &ps,
            gen_batch(&ps, &[100, 100], &["/a", "/b"], &[10, 20], &[1, 2]),
        );
        let p2 = write_gen(
            dir.path(),
            "_mv_partial.s0.t1.g1.parquet",
            &ps,
            gen_batch(&ps, &[200], &["/a"], &[5], &[1]),
        );
        let ctx = ctx_with(vec![p1, p2]).await;
        let b = sql_rows(
            &ctx,
            "SELECT SUM(\"sum_Adv\") AS s, SUM(\"count_Adv\") AS c FROM mv WHERE \"URL\" = '/a'",
        )
        .await;
        assert_eq!(
            i64_at(&b, "s", 0),
            15,
            "string equality must match rows across gens"
        );
        assert_eq!(i64_at(&b, "c", 0), 2);
    }

    /// Missing file must error, not silently skip (fail-closed).
    #[tokio::test]
    async fn missing_file_fails_closed() {
        let ctx = SessionContext::new();
        let missing = vec!["/nonexistent/_mv_partial.s0.t1.g0.parquet".to_string()];
        let result =
            register_mv_state_listing_table(&ctx, "mv", &missing, table_schema(), &state_fields())
                .await;
        assert!(result.is_err(), "missing file must surface an error");
    }

    /// read_schema_from_first_file contract.
    #[test]
    fn read_schema_first_file_contract() {
        let dir = TempDir::new().unwrap();
        let ps = physical_schema_i64();
        let p = write_gen(
            dir.path(),
            "_mv_partial.s0.t1.g0.parquet",
            &ps,
            gen_batch(&ps, &[1], &["/a"], &[1], &[1]),
        );
        let s = read_schema_from_first_file(&[p]).unwrap().unwrap();
        assert_eq!(s.fields().len(), 4);
        assert!(read_schema_from_first_file(&[]).unwrap().is_none());
        let legacy = read_schema_from_first_file(&["x.mv.arrow".to_string()])
            .unwrap_err()
            .to_string();
        assert!(legacy.contains("no longer supported"), "{legacy}");
    }
}
