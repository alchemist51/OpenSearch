/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Materialized-view read path: prepare-time plan surgery.
//!
//! When a session carries an [`MVBinding`] (attached by the Java shard-scan
//! handler from the catalog snapshot's coverage), `prepare_partial_plan`
//! calls [`apply_mv_binding`] on the stripped Partial plan:
//!
//! ```text
//!   AggregateExec(Partial)              UnionExec
//!        │                    ──▶      ╱        ╲
//!   scan(all raw files)         scan(mv state   AggregateExec(Partial)
//!                               files, aliased)      │
//!                                              scan(uncovered raw files)
//! ```
//!
//! MV state files ARE Partial-mode output (the zero-translation contract),
//! so the union's two branches produce the same shape and the coordinator
//! FINAL consumes the union unchanged.
//!
//! Every step is fallback-first: any mismatch (schema shape, plan shape,
//! scan-leaf shape) returns the original plan untouched — the query is then
//! answered entirely from raw parquet. Never wrong, only slower.

use std::collections::HashSet;
use std::sync::Arc;

use datafusion::arrow::datatypes::SchemaRef;
use datafusion::catalog::TableProvider;
use datafusion::common::Result;
use datafusion::datasource::file_format::parquet::ParquetFormat;
use datafusion::datasource::listing::{
    ListingOptions, ListingTable, ListingTableConfig, ListingTableUrl,
};
use datafusion::datasource::source::DataSourceExec;
use datafusion::physical_plan::aggregates::{AggregateExec, AggregateMode};
use datafusion::physical_plan::expressions::Column;
use datafusion::physical_plan::projection::ProjectionExec;
use datafusion::physical_plan::union::UnionExec;
use datafusion::physical_plan::{ExecutionPlan, PhysicalExpr};
use datafusion::prelude::SessionContext;
use datafusion_datasource::file_groups::FileGroup;
use datafusion_datasource::file_scan_config::{FileScanConfig, FileScanConfigBuilder};
use native_bridge_common::{log_debug, log_error};

/// Shard-local MV coverage, attached via `df_session_attach_mv`.
pub struct MVBinding {
    /// Absolute paths of MV state parquet files (covered segments).
    pub mv_file_paths: Vec<String>,
    /// Raw parquet file NAMES (last path segment) of covered segments —
    /// excluded from the raw branch when the MV branch is taken.
    pub covered_raw_file_names: HashSet<String>,
}

/// Rewrites a stripped Partial plan into UNION(mv scan, Partial over uncovered).
/// Returns the input plan unchanged when the binding can't be applied safely.
pub async fn apply_mv_binding(
    ctx: &SessionContext,
    stripped: Arc<dyn ExecutionPlan>,
    binding: &MVBinding,
) -> Arc<dyn ExecutionPlan> {
    match try_apply(ctx, Arc::clone(&stripped), binding).await {
        Ok(Some(plan)) => plan,
        Ok(None) => stripped,
        Err(e) => {
            log_error!("mv_read: binding failed, falling back to raw plan: {}", e);
            stripped
        }
    }
}

async fn try_apply(
    ctx: &SessionContext,
    stripped: Arc<dyn ExecutionPlan>,
    binding: &MVBinding,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    if binding.mv_file_paths.is_empty() {
        return Ok(None);
    }
    // v0 scope: the stripped plan's root must be the Partial aggregate itself.
    // TopK fragments (Sort/Fetch above the Partial) and any other wrapper are
    // out of scope — fallback. (RelabelExec wrapping happens AFTER this
    // function; see prepare_partial_plan ordering.)
    let Some(root_agg) = stripped.downcast_ref::<AggregateExec>() else {
        log_debug!("mv_read: plan root is not AggregateExec — fallback");
        return Ok(None);
    };
    if *root_agg.mode() != AggregateMode::Partial {
        log_debug!("mv_read: root aggregate is not Partial — fallback");
        return Ok(None);
    }

    // Build the MV state scan.
    let mv_scan = build_mv_scan(ctx, &binding.mv_file_paths).await?;

    // Positional schema alignment (the state contract is positional: group-by
    // columns first, then state columns). Column NAMES differ — the writer used
    // its own table alias — so names are aliased to the Partial output's names.
    // Field count or type mismatch = state files don't match this query = fallback.
    let partial_schema = stripped.schema();
    let mv_schema = mv_scan.schema();
    if !schemas_align(&partial_schema, &mv_schema) {
        log_error!(
            "mv_read: state schema does not align with partial output (partial={}, mv={}) — fallback",
            partial_schema,
            mv_schema
        );
        return Ok(None);
    }
    let aliased_mv = alias_positionally(mv_scan, &partial_schema)?;

    // Narrow the raw branch to uncovered files. If the scan leaf can't be
    // rewritten (unexpected plan shape), fall back — running covered segments
    // through BOTH branches would double-count.
    let Some(narrowed) = narrow_scan_files(Arc::clone(&stripped), &binding.covered_raw_file_names)?
    else {
        log_debug!("mv_read: could not narrow raw scan — fallback");
        return Ok(None);
    };

    log_debug!(
        "mv_read: bound {} state files; raw branch narrowed by {} covered files",
        binding.mv_file_paths.len(),
        binding.covered_raw_file_names.len()
    );
    Ok(Some(UnionExec::try_new(vec![aliased_mv, narrowed])?))
}

/// Builds a plan scanning the MV state parquet files.
async fn build_mv_scan(
    ctx: &SessionContext,
    mv_file_paths: &[String],
) -> Result<Arc<dyn ExecutionPlan>> {
    let urls: Vec<ListingTableUrl> = mv_file_paths
        .iter()
        .map(ListingTableUrl::parse)
        .collect::<Result<_>>()?;
    let listing_options =
        ListingOptions::new(Arc::new(ParquetFormat::default())).with_file_extension(".parquet");
    let schema = listing_options.infer_schema(&ctx.state(), &urls[0]).await?;
    let config = ListingTableConfig::new_with_multi_paths(urls)
        .with_listing_options(listing_options)
        .with_schema(schema);
    let table = ListingTable::try_new(config)?;
    table.scan(&ctx.state(), None, &[], None).await
}

/// Positional alignment check: equal field count and bit-compatible types.
/// Names deliberately NOT compared (writer alias differs from query alias).
fn schemas_align(partial: &SchemaRef, mv: &SchemaRef) -> bool {
    if partial.fields().len() != mv.fields().len() {
        return false;
    }
    partial
        .fields()
        .iter()
        .zip(mv.fields().iter())
        .all(|(p, m)| p.data_type() == m.data_type())
}

/// Wraps `input` in a ProjectionExec that renames columns positionally to
/// `target` field names, so UnionExec sees identical schemas on both branches.
fn alias_positionally(
    input: Arc<dyn ExecutionPlan>,
    target: &SchemaRef,
) -> Result<Arc<dyn ExecutionPlan>> {
    let exprs: Vec<(Arc<dyn PhysicalExpr>, String)> = target
        .fields()
        .iter()
        .enumerate()
        .map(|(i, f)| {
            (
                Arc::new(Column::new(input.schema().field(i).name(), i)) as Arc<dyn PhysicalExpr>,
                f.name().clone(),
            )
        })
        .collect();
    Ok(Arc::new(ProjectionExec::try_new(exprs, input)?))
}

/// Rewrites the plan's single parquet scan leaf to exclude `covered` file names
/// (matched on the last path segment). Returns None when the plan doesn't have
/// the expected shape (single-child wrappers down to one DataSourceExec leaf).
fn narrow_scan_files(
    plan: Arc<dyn ExecutionPlan>,
    covered: &HashSet<String>,
) -> Result<Option<Arc<dyn ExecutionPlan>>> {
    if let Some(source_exec) = plan.downcast_ref::<DataSourceExec>() {
        let Some(file_config) = source_exec
            .data_source()
            .as_ref()
            .downcast_ref::<FileScanConfig>()
        else {
            return Ok(None);
        };
        let narrowed_groups: Vec<FileGroup> = file_config
            .file_groups
            .iter()
            .map(|group| {
                FileGroup::new(
                    group
                        .iter()
                        .filter(|pf| {
                            let name = pf.object_meta.location.filename().unwrap_or_default();
                            !covered.contains(name)
                        })
                        .cloned()
                        .collect(),
                )
            })
            .collect();
        let narrowed_config = FileScanConfigBuilder::from(file_config.clone())
            .with_file_groups(narrowed_groups)
            .build();
        let narrowed: Arc<dyn ExecutionPlan> = DataSourceExec::from_data_source(narrowed_config);
        return Ok(Some(narrowed));
    }
    // Recurse through single-child wrappers; bail on multi-input nodes.
    let children = plan.children();
    if children.len() != 1 {
        return Ok(None);
    }
    match narrow_scan_files(Arc::clone(children[0]), covered)? {
        Some(new_child) => Ok(Some(plan.with_new_children(vec![new_child])?)),
        None => Ok(None),
    }
}
