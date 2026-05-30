/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene;

import org.opensearch.analytics.spi.DelegatedExpression;
import org.opensearch.analytics.spi.FilterTreeShape;
import org.opensearch.analytics.spi.FragmentInstructionHandler;
import org.opensearch.analytics.spi.FragmentInstructionHandlerFactory;
import org.opensearch.analytics.spi.InstructionNode;
import org.opensearch.analytics.spi.ShardScanInstructionNode;
import org.opensearch.analytics.spi.ShardScanWithDelegationInstructionNode;

import java.util.List;
import java.util.Optional;

/**
 * Factory for Lucene-driver instruction nodes / handlers. Built once per backend, used at
 * both planner-time (coordinator-side {@code create*Node}) and execution-time (data-node
 * {@code createHandler}).
 *
 * <p>Only shard-scan setup nodes are supported today — these are the only nodes a
 * Lucene-driver {@code StagePlan} produces (count fast path is shard-local). Aggregate /
 * partial / final / filter-delegation nodes return {@link Optional#empty()} or throw,
 * since Lucene doesn't drive those operators.
 *
 * @opensearch.internal
 */
final class LuceneInstructionHandlerFactory implements FragmentInstructionHandlerFactory {

    private final LucenePlugin plugin;

    LuceneInstructionHandlerFactory(LucenePlugin plugin) {
        this.plugin = plugin;
    }

    // ── Coordinator-side: produce instruction nodes ──

    @Override
    public Optional<InstructionNode> createShardScanNode(
        boolean requestsRowIds,
        boolean countQuery,
        List<String> countExistenceFields,
        List<String> partialCountColumnNames
    ) {
        // Lucene driver doesn't emit row ids — QTF is DataFusion-only. If a Lucene-driver
        // alternative were ever paired with a row-id-requesting parent stage, we'd reject
        // here so the framework picks DataFusion instead.
        if (requestsRowIds) return Optional.empty();
        return Optional.of(new ShardScanInstructionNode(requestsRowIds, countQuery, countExistenceFields, partialCountColumnNames));
    }

    @Override
    public Optional<InstructionNode> createFilterDelegationNode(
        FilterTreeShape treeShape,
        int delegatedPredicateCount,
        List<DelegatedExpression> delegatedQueries
    ) {
        // Lucene as driver doesn't have a "filter delegation" concept — the filter IS the
        // Lucene query. Filter delegation only makes sense when DataFusion drives.
        return Optional.empty();
    }

    @Override
    public Optional<InstructionNode> createShardScanWithDelegationNode(
        FilterTreeShape treeShape,
        int delegatedPredicateCount,
        boolean requestsRowIds,
        boolean countQuery,
        List<String> countExistenceFields,
        List<String> partialCountColumnNames
    ) {
        // Same rationale as createFilterDelegationNode — Lucene driver doesn't accept
        // delegated predicates, so the with-delegation variant returns the plain shard-scan
        // instead. The treeShape / delegatedPredicateCount fields are ignored.
        return createShardScanNode(requestsRowIds, countQuery, countExistenceFields, partialCountColumnNames);
    }

    @Override
    public Optional<InstructionNode> createPartialAggregateNode() {
        // Lucene driver returns the count directly as a one-row partial-shaped batch —
        // no separate partial-aggregate setup step.
        return Optional.empty();
    }

    @Override
    public Optional<InstructionNode> createFinalAggregateNode() {
        // Lucene never drives a coordinator-reduce stage; final agg always runs on DataFusion.
        return Optional.empty();
    }

    // ── Data-node-side: produce handlers ──

    @Override
    @SuppressWarnings({ "rawtypes", "unchecked" })
    public FragmentInstructionHandler<?> createHandler(InstructionNode node) {
        if (node instanceof ShardScanWithDelegationInstructionNode) {
            // Lucene-driver plans wouldn't produce this variant (createShardScanWithDelegationNode
            // returns the plain ShardScanInstructionNode), but guard against it surviving the wire
            // by treating the same as a plain shard-scan — fields read by the handler are on the
            // parent class.
            return (FragmentInstructionHandler) new LuceneScanInstructionHandler(plugin);
        }
        if (node instanceof ShardScanInstructionNode) {
            return (FragmentInstructionHandler) new LuceneScanInstructionHandler(plugin);
        }
        throw new UnsupportedOperationException("Lucene driver does not handle instruction type: " + node.type());
    }
}
