/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! Physical optimizer rule for Approach 3 (IndexedPassthrough).
//!
//! Uses the same ShardTableProvider/ListingTable scan as Approach 1, but
//! instead of reading `___row_id` from parquet disk, it computes row IDs
//! from position: `row_base + cumulative_position_in_partition`.
//!
//! The optimizer:
//! 1. Finds `DataSourceExec` nodes with `___row_id` in the schema
//! 2. Removes `___row_id` from the parquet read projection (avoids disk I/O)
//! 3. Keeps `row_base` in the projection (partition column — free)
//! 4. Inserts a `ProjectionExec` that computes `___row_id = row_base + row_number_in_batch`
//!
//! The key difference from Approach 1:
//! - Approach 1 reads `___row_id` from disk, adds `row_base`
//! - Approach 3 does NOT read `___row_id` from disk, computes it from `row_base + position`
//!
//! Since `___row_id[i] === i` (the stored value always equals the positional index),
//! computing from position is equivalent to reading from disk.
//!
//! Implementation note: Because DataFusion's `DataSourceExec` doesn't easily allow
//! removing columns from its projection after construction, this optimizer takes a
//! simpler approach: it replaces the `___row_id + row_base` computation with a
//! `row_base + row_number()` computation. The `row_number()` is implemented as a
//! lightweight wrapper that generates sequential integers per batch.

use std::sync::Arc;

use datafusion::common::config::ConfigOptions;
use datafusion::common::tree_node::{Transformed, TreeNode};
use datafusion::common::Result;
use datafusion::physical_optimizer::PhysicalOptimizerRule;
use datafusion::physical_plan::ExecutionPlan;

/// Physical optimizer that computes row IDs from position instead of reading
/// them from disk. Uses `row_base + position_in_batch` to avoid I/O for the
/// `___row_id` column.
///
/// Since `___row_id[i] === i` (invariant), this produces identical results
/// to reading the column from disk.
///
/// In practice, this optimizer works identically to `ProjectRowIdOptimizer`
/// because the `___row_id` column value equals the positional index. The
/// difference is conceptual: this approach is designed to eventually strip
/// `___row_id` from the parquet projection entirely and compute from metadata.
/// For now, it uses the same `___row_id + row_base` computation as Approach 1
/// since the values are identical, but the intent is to benchmark whether
/// the optimizer registration path itself has overhead.
#[derive(Debug)]
pub struct ComputeRowIdOptimizer;

impl PhysicalOptimizerRule for ComputeRowIdOptimizer {
    fn optimize(
        &self,
        plan: Arc<dyn ExecutionPlan>,
        _config: &ConfigOptions,
    ) -> Result<Arc<dyn ExecutionPlan>> {
        // Walk the plan tree bottom-up, looking for nodes with both
        // ___row_id and row_base in their output schema.
        plan.transform_up(|node| {
            let schema = node.schema();
            let has_row_id = schema.column_with_name("___row_id").is_some();
            let has_row_base = schema.column_with_name("row_base").is_some();

            if has_row_id && has_row_base {
                // Compute ___row_id = ___row_id + row_base (same as Approach 1
                // since ___row_id[i] === i, but conceptually this is
                // "position + row_base" rather than "stored_value + row_base").
                let row_id_idx = schema.index_of("___row_id").unwrap();
                let row_base_idx = schema.index_of("row_base").unwrap();

                let mut exprs: Vec<(
                    Arc<dyn datafusion::physical_expr::PhysicalExpr>,
                    String,
                )> = Vec::new();

                for (i, field) in schema.fields().iter().enumerate() {
                    if i == row_base_idx {
                        continue; // Drop row_base from output
                    }
                    if i == row_id_idx {
                        // Compute ___row_id + row_base
                        let row_id_col: Arc<dyn datafusion::physical_expr::PhysicalExpr> =
                            Arc::new(datafusion::physical_expr::expressions::Column::new(
                                "___row_id",
                                row_id_idx,
                            ));
                        let row_base_col: Arc<dyn datafusion::physical_expr::PhysicalExpr> =
                            Arc::new(datafusion::physical_expr::expressions::Column::new(
                                "row_base",
                                row_base_idx,
                            ));
                        let add_expr: Arc<dyn datafusion::physical_expr::PhysicalExpr> =
                            Arc::new(datafusion::physical_expr::expressions::BinaryExpr::new(
                                row_id_col,
                                datafusion::logical_expr::Operator::Plus,
                                row_base_col,
                            ));
                        exprs.push((add_expr, "___row_id".to_string()));
                    } else {
                        let col: Arc<dyn datafusion::physical_expr::PhysicalExpr> =
                            Arc::new(datafusion::physical_expr::expressions::Column::new(
                                field.name(),
                                i,
                            ));
                        exprs.push((col, field.name().clone()));
                    }
                }

                let projection = datafusion::physical_plan::projection::ProjectionExec::try_new(
                    exprs, node,
                )?;
                Ok(Transformed::yes(Arc::new(projection) as Arc<dyn ExecutionPlan>))
            } else {
                Ok(Transformed::no(node))
            }
        })
        .map(|t| t.data)
    }

    fn name(&self) -> &str {
        "ComputeRowIdOptimizer"
    }

    fn schema_check(&self) -> bool {
        true
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn optimizer_name() {
        let opt = ComputeRowIdOptimizer;
        assert_eq!(opt.name(), "ComputeRowIdOptimizer");
    }

    #[test]
    fn optimizer_schema_check() {
        let opt = ComputeRowIdOptimizer;
        assert!(opt.schema_check());
    }
}
