/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

import java.util.List;
import java.util.Optional;

/**
 * Factory for creating {@link InstructionNode}s at the coordinator and
 * {@link FragmentInstructionHandler}s at the data node. One factory per backend,
 * accessed via {@code AnalyticsSearchBackendPlugin.getInstructionHandlerFactory()}.
 *
 * <p>Coordinator-side creation methods return {@link Optional#empty()} if the backend
 * does not support the instruction type. Core logs and skips unsupported instructions.
 *
 * @opensearch.internal
 */
public interface FragmentInstructionHandlerFactory {

    // ── Coordinator-side: create instruction nodes ──

    /**
     * Creates a shard scan instruction node. {@code requestsRowIds} signals that the scan
     * must emit shard-global {@code __row_id__} values (QTF query phase). {@code countQuery}
     * signals the surrounding plan is a count(*) / count(col) aggregate eligible for the
     * data-node count fast path; {@code countExistenceFields} carries the columns whose
     * {@code IS NOT NULL} must be ANDed into the count (empty for count(*));
     * {@code partialCountColumnNames} carries the partial-aggregate output column names so
     * the fast path can build an Arrow batch matching the coordinator's expected schema.
     */
    Optional<InstructionNode> createShardScanNode(
        boolean requestsRowIds,
        boolean countQuery,
        List<String> countExistenceFields,
        List<String> partialCountColumnNames
    );

    /** Creates a filter delegation instruction node with the given delegation metadata. */
    Optional<InstructionNode> createFilterDelegationNode(
        FilterTreeShape treeShape,
        int delegatedPredicateCount,
        List<DelegatedExpression> delegatedQueries
    );

    /**
     * Creates a shard scan with delegation instruction node — combines scan setup with
     * delegation config. {@code requestsRowIds} signals that the scan must emit shard-global
     * {@code __row_id__} values (QTF query phase). Backends that don't support QTF should
     * return {@link Optional#empty()} when {@code requestsRowIds} is true. The remaining
     * arguments carry the count-fast-path hint (see {@link #createShardScanNode}).
     */
    Optional<InstructionNode> createShardScanWithDelegationNode(
        FilterTreeShape treeShape,
        int delegatedPredicateCount,
        boolean requestsRowIds,
        boolean countQuery,
        List<String> countExistenceFields,
        List<String> partialCountColumnNames
    );

    /** Creates a partial aggregate instruction node. */
    Optional<InstructionNode> createPartialAggregateNode();

    /** Creates a final aggregate instruction node for coordinator reduce. */
    Optional<InstructionNode> createFinalAggregateNode();

    // ── Data-node-side: create handler for an instruction node ──

    /**
     * Creates a handler for the given instruction node. The handler's
     * {@link FragmentInstructionHandler#apply} will be called with the node
     * and the execution context.
     */
    FragmentInstructionHandler<?> createHandler(InstructionNode node);
}
