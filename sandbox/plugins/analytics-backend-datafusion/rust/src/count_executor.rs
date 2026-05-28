/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

//! COUNT_DELEGATION executor.
//!
//! Activates only when the planner sets `FilterTreeShape::COUNT_DELEGATION` (i.e. every
//! predicate is delegated, the tree is a single Collector leaf — possibly a fused
//! conjunction at the planner — the surrounding plan is a `count(*)` aggregate with no
//! projection, and there are no native predicates). In that shape there is no parquet
//! to decode and no doc-id bitset to materialize: each segment can be answered with one
//! FFM `countDocs` upcall, which on the Lucene side hits `Weight.count(LeafReaderContext)`
//! when the partition spans the whole segment.
//!
//! Pipeline:
//!
//! ```text
//!   count_executor::execute(handle, substrait_bytes)
//!     ├─ decode substrait → LogicalPlan → BoolNode → single Collector annotation_id
//!     ├─ build_segments → Vec<SegmentFileInfo>
//!     ├─ create_provider(annotation_id) → ProviderHandle      (1 FFM upcall, query-scoped)
//!     ├─ SegmentGrouped.assign(layouts, target_partitions)    (whole-segment buckets)
//!     ├─ CountExec(provider, segments, assignments)           (custom ExecutionPlan)
//!     └─ wrap with AggregateExec(sum) over CoalescePartitionsExec
//! ```
//!
//! Each `CountExec::execute(partition_idx)` runs sequentially through the segments in
//! its assignment, calling `count_docs(0, max_doc)` per segment. Across partitions,
//! DataFusion runs the executes concurrently on the CPU executor — that is the only
//! parallelism level that pays here (the per-segment work is one FFM call returning a
//! long; intra-partition prefetch buys nothing).

use std::any::Any;
use std::fmt;
use std::sync::Arc;

use arrow::array::{ArrayRef, Int64Array, UInt64Array};
use arrow::datatypes::{DataType, SchemaRef};
use arrow::record_batch::RecordBatch;
use datafusion::common::DataFusionError;
use datafusion::execution::SendableRecordBatchStream;
use datafusion::execution::TaskContext;
use datafusion::physical_expr::{EquivalenceProperties, Partitioning};
use datafusion::physical_plan::execution_plan::{Boundedness, EmissionType};
use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
use datafusion::physical_plan::{DisplayAs, DisplayFormatType, ExecutionPlan, PlanProperties};
use datafusion_substrait::logical_plan::consumer::from_substrait_plan;
use prost::Message;
use substrait::proto::Plan;

use crate::cross_rt_stream::CrossRtStream;
use crate::executor::DedicatedExecutor;
use crate::indexed_table::bool_tree::BoolNode;
use crate::indexed_table::ffm_callbacks::{create_provider, FfmSegmentCollector, ProviderHandle};
use crate::indexed_table::partitioning::{
    PartitionAssignment, PartitionStrategy, SegmentGrouped, SegmentLayout,
};
use crate::indexed_table::segment_info::build_segments;
use crate::indexed_table::substrait_to_tree::{expr_to_bool_tree, extract_filter_expr};
use crate::indexed_table::table_provider::SegmentFileInfo;
use crate::session_context::SessionContextHandle;

/// Build a single-element count array sized to whatever integer type the substrait plan
/// declared for the `count(*)` measure. Coordinator-side schema validation rejects any
/// mismatch between this output and the declared partial-aggregate schema, so the type
/// the field declares is the source of truth here.
fn build_count_array(dtype: &DataType, total: i64) -> Result<ArrayRef, DataFusionError> {
    match dtype {
        DataType::Int64 => Ok(Arc::new(Int64Array::from(vec![total]))),
        DataType::UInt64 => Ok(Arc::new(UInt64Array::from(vec![total as u64]))),
        other => Err(DataFusionError::Internal(format!(
            "CountExec: unsupported count output type {:?} (expected Int64 or UInt64)",
            other
        ))),
    }
}

/// Walk the BoolNode tree to pick the single delegated leaf's annotation_id. Accepts:
///
/// - `Collector` — correctness-delegated (e.g. `match()` only Lucene can evaluate)
/// - `DelegationPossible` — performance-delegated (planner narrowed onto driver but the
///   peer can also evaluate; for COUNT we delegate to the peer for the metadata fast path)
///
/// Both variants expose `annotation_id`; that's all we need to call `createProvider` and
/// then `countDocs` per segment. The original expression carried by `DelegationPossible`
/// is irrelevant on the count path — we never decode rows.
///
/// A bare leaf or an AND of one leaf is acceptable. Anything richer (multiple leaves, OR,
/// NOT, native Predicate) is rejected — those wouldn't pass the planner-side detector
/// either, but defend at runtime in case the contract drifts.
fn extract_single_count_collector(tree: &BoolNode) -> Result<i32, DataFusionError> {
    match tree {
        BoolNode::Collector { annotation_id } => Ok(*annotation_id),
        BoolNode::DelegationPossible { annotation_id, .. } => Ok(*annotation_id),
        BoolNode::And(children) => {
            let mut found: Option<i32> = None;
            for child in children {
                match child {
                    BoolNode::Collector { annotation_id }
                    | BoolNode::DelegationPossible { annotation_id, .. } => {
                        if found.is_some() {
                            return Err(DataFusionError::Execution(
                                "COUNT_DELEGATION expects one delegated leaf; \
                                 multiple found — planner should have fused them"
                                    .into(),
                            ));
                        }
                        found = Some(*annotation_id);
                    }
                    BoolNode::Predicate(_) => {
                        return Err(DataFusionError::Execution(
                            "COUNT_DELEGATION cannot include native predicates".into(),
                        ));
                    }
                    other => {
                        return Err(DataFusionError::Execution(format!(
                            "COUNT_DELEGATION supports flat AND of delegated leaves only; got {:?}",
                            other
                        )));
                    }
                }
            }
            found.ok_or_else(|| {
                DataFusionError::Execution(
                    "COUNT_DELEGATION tree had no delegated leaves".into(),
                )
            })
        }
        other => Err(DataFusionError::Execution(format!(
            "COUNT_DELEGATION cannot handle filter shape: {:?}",
            other
        ))),
    }
}

/// Execute a count-delegation query. Consumes the session handle (matches the bitmap
/// path's ownership model) and returns a `QueryStreamHandle` pointer.
///
/// # Safety
/// `handle` is moved in; on exit ownership is transferred to the returned
/// `QueryStreamHandle` (held for the duration of the stream).
pub async fn execute(
    handle: SessionContextHandle,
    substrait_bytes: Vec<u8>,
    cpu_executor: DedicatedExecutor,
) -> Result<i64, DataFusionError> {
    let SessionContextHandle {
        ctx,
        table_path,
        object_metas,
        writer_generations,
        query_context,
        table_name: _table_name,
        indexed_config: _indexed_config,
        query_config,
        aggregate_mode: _aggregate_mode,
        prepared_plan: _prepared_plan,
        phantom_reservation: _phantom_reservation,
    } = handle;

    let state = ctx.state();
    let store = state.runtime_env().object_store(&table_path)?;
    let metadata_cache = state.runtime_env().cache_manager.get_file_metadata_cache();

    let (segments, schema) = build_segments(
        &state,
        Arc::clone(&store),
        object_metas.as_ref(),
        writer_generations.as_ref(),
        metadata_cache,
    )
    .await
    .map_err(DataFusionError::Execution)?;
    let schema = crate::schema_coerce::coerce_inferred_schema(schema);
    log::info!(
        "[count-delegation] Rust: entering count_executor with {} segment(s), target_partitions={}",
        segments.len(),
        query_config.target_partitions
    );

    // Decode substrait → LogicalPlan → BoolNode tree. The default ListingTable registered
    // by create_session_context is left in place — `from_substrait_plan` only needs it to
    // resolve NamedTable references; we never actually scan it (CountExec is a custom
    // ExecutionPlan that bypasses the table provider entirely).
    let plan = Plan::decode(substrait_bytes.as_slice())
        .map_err(|e| DataFusionError::Execution(format!("decode substrait: {}", e)))?;
    let logical_plan = from_substrait_plan(&ctx.state(), &plan).await?;
    let filter_expr = extract_filter_expr(&logical_plan).ok_or_else(|| {
        DataFusionError::Execution(
            "COUNT_DELEGATION requires a Filter node in the plan; none found".into(),
        )
    })?;
    let extraction = expr_to_bool_tree(&filter_expr, &schema, &state)
        .map_err(|e| DataFusionError::Execution(format!("expr_to_bool_tree: {}", e)))?;
    let annotation_id = extract_single_count_collector(&extraction.tree)?;

    // Lower the substrait plan to a physical plan to read its declared output schema. The
    // resulting plan is dropped — we don't execute the parquet scan it would build. Only
    // the schema of `AggregateExec(count(*))` matters: it's the wire contract the
    // coordinator's FINAL aggregate validates incoming partial rows against. Doing this
    // here keeps CountExec's output type-correct without RelabelExec gymnastics.
    let target_schema = ctx
        .state()
        .create_physical_plan(&logical_plan)
        .await
        .map_err(|e| {
            DataFusionError::Execution(format!(
                "COUNT_DELEGATION: failed to derive target schema from substrait plan: {}",
                e
            ))
        })?
        .schema();

    // One provider for the whole query — Drop releases the Java-side Weight when the
    // executor goes out of scope.
    let provider = Arc::new(
        create_provider(annotation_id).map_err(|e| DataFusionError::External(e.into()))?,
    );

    // Whole-segment partitioning so Weight.count(leaf) can fire.
    let layouts: Vec<SegmentLayout> = segments
        .iter()
        .map(|seg| SegmentLayout {
            row_groups: seg.row_groups.clone(),
        })
        .collect();
    let assignments =
        SegmentGrouped.assign(&layouts, query_config.target_partitions.max(1));
    log::info!(
        "[count-delegation] Rust: SegmentGrouped produced {} partition(s) for {} segment(s); annotation_id={}",
        assignments.len(),
        segments.len(),
        annotation_id
    );
    for (idx, assignment) in assignments.iter().enumerate() {
        let total_rows: i64 = assignment
            .chunks
            .iter()
            .map(|c| (c.doc_max - c.doc_min) as i64)
            .sum();
        log::info!(
            "[count-delegation] Rust: partition {} owns {} segment(s), {} rows total",
            idx,
            assignment.chunks.len(),
            total_rows
        );
    }

    let exec: Arc<dyn ExecutionPlan> = Arc::new(CountExec::new(
        provider,
        Arc::new(segments),
        assignments,
        target_schema,
    ));

    let df_stream = datafusion::physical_plan::execute_stream(exec, ctx.task_ctx())
        .map_err(|e| DataFusionError::Execution(format!("execute_stream: {}", e)))?;

    let (cross_rt_stream, abort_handle) =
        CrossRtStream::new_with_df_error_stream_cancellable(df_stream, cpu_executor);
    let context_id = query_context.context_id();
    if let Some(h) = abort_handle {
        crate::query_tracker::set_abort_handle(context_id, h);
    }

    let schema = cross_rt_stream.schema();
    let wrapped = RecordBatchStreamAdapter::new(schema, cross_rt_stream);
    let stream_handle =
        crate::api::QueryStreamHandle::with_session_context(wrapped, query_context, ctx);
    Ok(Box::into_raw(Box::new(stream_handle)) as i64)
}

/// Custom `ExecutionPlan` that emits one row per partition: the partition's local sum
/// of per-segment counts. DataFusion's standard `AggregateExec(sum) +
/// CoalescePartitionsExec` chain (planted by the substrait consumer for `count(*)`)
/// merges those rows into the final scalar count.
pub struct CountExec {
    provider: Arc<ProviderHandle>,
    segments: Arc<Vec<SegmentFileInfo>>,
    assignments: Vec<PartitionAssignment>,
    schema: SchemaRef,
    properties: Arc<PlanProperties>,
}

impl CountExec {
    /// `target_schema` must declare exactly one numeric column (Int64 or UInt64) — the
    /// shape DataFusion lowered the substrait `AggregateRel(count(*))` to. CountExec
    /// emits batches matching this schema directly; no downstream retag is needed.
    pub fn new(
        provider: Arc<ProviderHandle>,
        segments: Arc<Vec<SegmentFileInfo>>,
        assignments: Vec<PartitionAssignment>,
        target_schema: SchemaRef,
    ) -> Self {
        // partitions = max(1, assignments.len()) so an empty shard still emits the
        // identity (one row with count=0) rather than a zero-partition plan that
        // DataFusion would treat as an error.
        let num_partitions = assignments.len().max(1);
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(target_schema.clone()),
            Partitioning::UnknownPartitioning(num_partitions),
            EmissionType::Incremental,
            Boundedness::Bounded,
        ));
        Self {
            provider,
            segments,
            assignments,
            schema: target_schema,
            properties,
        }
    }
}

impl fmt::Debug for CountExec {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("CountExec")
            .field("partitions", &self.assignments.len())
            .field("segments", &self.segments.len())
            .finish()
    }
}

impl DisplayAs for CountExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut fmt::Formatter) -> fmt::Result {
        write!(
            f,
            "CountExec: partitions={}, segments={}",
            self.assignments.len(),
            self.segments.len(),
        )
    }
}

impl ExecutionPlan for CountExec {
    fn name(&self) -> &str {
        "CountExec"
    }
    fn as_any(&self) -> &dyn Any {
        self
    }
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }
    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![]
    }
    fn with_new_children(
        self: Arc<Self>,
        _children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> datafusion::common::Result<Arc<dyn ExecutionPlan>> {
        Ok(self)
    }

    fn execute(
        &self,
        partition: usize,
        _context: Arc<TaskContext>,
    ) -> datafusion::common::Result<SendableRecordBatchStream> {
        let assignment = self.assignments.get(partition).cloned();
        let provider = Arc::clone(&self.provider);
        let segments = Arc::clone(&self.segments);
        let schema = self.schema.clone();

        // Build the partition's row asynchronously. `futures::stream::once` produces
        // exactly one batch; the surrounding aggregate will sum across partitions.
        let stream = futures::stream::once(async move {
            let mut total: i64 = 0;
            if let Some(assignment) = assignment {
                for chunk in &assignment.chunks {
                    let segment = segments.get(chunk.segment_idx).ok_or_else(|| {
                        DataFusionError::Internal(format!(
                            "CountExec: segment_idx {} out of range",
                            chunk.segment_idx
                        ))
                    })?;

                    // Whole-segment invariant: SegmentGrouped is the only legitimate
                    // source of CountExec assignments, and it always emits chunks that
                    // span the entire segment. If a chunk arrives with a sub-segment
                    // range the Lucene-side Weight.count(leaf) fast path won't fire
                    // (it requires minDoc=0 && maxDoc=leaf.maxDoc). Returning Internal
                    // — not a debug_assert — so a release build also fails fast rather
                    // than silently regressing performance.
                    let expected_max = segment.max_doc as i32;
                    if chunk.doc_min != 0 || chunk.doc_max != expected_max {
                        return Err(DataFusionError::Internal(format!(
                            "CountExec: chunk for segment_idx {} (writer_generation={}) must \
                             cover the whole segment but doc_range=[{},{}) (expected [0,{}))",
                            chunk.segment_idx, segment.writer_generation,
                            chunk.doc_min, chunk.doc_max, expected_max
                        )));
                    }

                    let collector = FfmSegmentCollector::create(
                        provider.key(),
                        segment.writer_generation,
                        chunk.doc_min,
                        chunk.doc_max,
                    )
                    .map_err(|e| DataFusionError::External(e.into()))?;
                    let n = collector
                        .count_docs(chunk.doc_min, chunk.doc_max)
                        .map_err(|e| DataFusionError::External(e.into()))?;
                    debug_assert!(
                        n >= 0,
                        "FfmSegmentCollector::count_docs returned negative {} despite Ok",
                        n
                    );
                    log::info!(
                        "[count-delegation] Rust: partition={} writer_generation={} → {} docs",
                        partition,
                        segment.writer_generation,
                        n
                    );
                    total += n;
                    // collector dropped here → releaseCollector FFM upcall
                }
            }
            let dtype = schema.field(0).data_type().clone();
            let array = build_count_array(&dtype, total)?;
            RecordBatch::try_new(schema, vec![array])
                .map_err(|e| DataFusionError::ArrowError(Box::new(e), None))
        });

        Ok(Box::pin(RecordBatchStreamAdapter::new(
            self.schema.clone(),
            stream,
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::indexed_table::bool_tree::BoolNode;
    use datafusion::physical_expr::expressions::Column as PhysColumn;
    use datafusion::physical_expr::PhysicalExpr;

    fn collector(id: i32) -> BoolNode {
        BoolNode::Collector { annotation_id: id }
    }

    fn delegation_possible(id: i32) -> BoolNode {
        let col: Arc<dyn PhysicalExpr> = Arc::new(PhysColumn::new("BrowserCountry", 0));
        BoolNode::DelegationPossible {
            annotation_id: id,
            original_expr: col,
        }
    }

    fn predicate() -> BoolNode {
        let col: Arc<dyn PhysicalExpr> = Arc::new(PhysColumn::new("price", 0));
        BoolNode::Predicate(col)
    }

    #[test]
    fn extract_bare_collector() {
        let t = collector(7);
        assert_eq!(extract_single_count_collector(&t).unwrap(), 7);
    }

    #[test]
    fn extract_and_with_one_collector() {
        let t = BoolNode::And(vec![collector(11)]);
        assert_eq!(extract_single_count_collector(&t).unwrap(), 11);
    }

    #[test]
    fn extract_rejects_native_predicate() {
        let t = BoolNode::And(vec![collector(1), predicate()]);
        assert!(extract_single_count_collector(&t).is_err());
    }

    #[test]
    fn extract_rejects_two_collectors() {
        // The planner is supposed to fuse these into one before COUNT_DELEGATION fires.
        // If we ever see two leaves here, fail loudly rather than silently picking one.
        let t = BoolNode::And(vec![collector(1), collector(2)]);
        assert!(extract_single_count_collector(&t).is_err());
    }

    #[test]
    fn extract_rejects_or_tree() {
        let t = BoolNode::Or(vec![collector(1), collector(2)]);
        assert!(extract_single_count_collector(&t).is_err());
    }

    #[test]
    fn extract_bare_delegation_possible() {
        // Performance-delegated leaf — the post-CBO shape of a dual-viable predicate
        // that the planner narrowed onto the driving backend with the peer recorded
        // in performanceDelegationBackends. Same annotation_id semantics as Collector,
        // count_executor only needs the id to call createProvider.
        let t = delegation_possible(42);
        assert_eq!(extract_single_count_collector(&t).unwrap(), 42);
    }

    #[test]
    fn extract_and_with_one_delegation_possible() {
        let t = BoolNode::And(vec![delegation_possible(99)]);
        assert_eq!(extract_single_count_collector(&t).unwrap(), 99);
    }

    #[test]
    fn extract_rejects_two_delegation_possible_leaves() {
        let t = BoolNode::And(vec![delegation_possible(1), delegation_possible(2)]);
        assert!(extract_single_count_collector(&t).is_err());
    }

    #[test]
    fn extract_rejects_mixed_collector_and_delegation_possible() {
        // Two delegated leaves of different kinds — still two leaves, still ambiguous.
        let t = BoolNode::And(vec![collector(1), delegation_possible(2)]);
        assert!(extract_single_count_collector(&t).is_err());
    }

    #[test]
    fn build_count_array_handles_int64() {
        let arr = build_count_array(&DataType::Int64, 42).unwrap();
        let typed: &Int64Array = arr.as_any().downcast_ref().unwrap();
        assert_eq!(typed.len(), 1);
        assert_eq!(typed.value(0), 42);
    }

    #[test]
    fn build_count_array_handles_uint64() {
        let arr = build_count_array(&DataType::UInt64, 100).unwrap();
        let typed: &UInt64Array = arr.as_any().downcast_ref().unwrap();
        assert_eq!(typed.len(), 1);
        assert_eq!(typed.value(0), 100);
    }

    #[test]
    fn build_count_array_rejects_unsupported_type() {
        // If the substrait planner ever declares a count(*) measure as Float64 or some
        // other non-integer, fail loudly here rather than silently emitting wrong bytes.
        let err = build_count_array(&DataType::Float64, 1).unwrap_err();
        assert!(format!("{}", err).contains("unsupported count output type"));
    }
}
