/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.planner.rel.AnnotatedPredicate;
import org.opensearch.analytics.planner.rel.OpenSearchFilter;

import java.util.ArrayList;
import java.util.List;

/**
 * Per-{@link StagePlan} pass that narrows performance-delegation peers on
 * {@link AnnotatedPredicate} leaves where the runtime peer call buys nothing.
 * Runs between {@link PlanForker#forkAll(QueryDAG, org.opensearch.analytics.planner.CapabilityRegistry)}
 * and {@link BackendPlanAdapter#adaptAll(QueryDAG, org.opensearch.analytics.planner.CapabilityRegistry)}.
 *
 * <p>For each stage alternative, walks the resolved fragment's
 * {@link OpenSearchFilter}s; for each {@link AnnotatedPredicate} whose
 * {@code performanceDelegationBackends} is non-empty, polls every registered
 * {@link PerfPeerNarrowingRule}. If any rule returns {@code true} for a given
 * peer, that peer is removed. Predicate's {@code viableBackends} stays intact —
 * the driver still evaluates natively, the peer just isn't consulted at runtime.
 *
 * <p>The pass is idempotent: once a peer is dropped its rule wouldn't see it
 * again (the iterator-style narrowing rebuilds a new {@link AnnotatedPredicate}
 * each pass). Adding more rules later is one new class plus one entry in
 * {@link #DEFAULT_RULES}.
 *
 * @opensearch.internal
 */
public final class PerfPeerNarrowing {

    private static final Logger LOGGER = LogManager.getLogger(PerfPeerNarrowing.class);

    private static final List<PerfPeerNarrowingRule> DEFAULT_RULES = List.of(new DropLucenePeerForNotEqualsEmptyStringRule());

    private PerfPeerNarrowing() {}

    /** Apply the default rule set to every {@link StagePlan} alternative in the DAG. */
    public static void narrowAll(QueryDAG dag) {
        narrowAll(dag, DEFAULT_RULES);
    }

    /** Test-only entry point: run a custom rule set against the DAG. */
    static void narrowAll(QueryDAG dag, List<PerfPeerNarrowingRule> rules) {
        narrowStage(dag.rootStage(), rules);
    }

    private static void narrowStage(Stage stage, List<PerfPeerNarrowingRule> rules) {
        for (Stage child : stage.getChildStages()) {
            narrowStage(child, rules);
        }
        if (stage.getPlanAlternatives().isEmpty()) {
            return;
        }
        List<StagePlan> updated = new ArrayList<>(stage.getPlanAlternatives().size());
        for (StagePlan plan : stage.getPlanAlternatives()) {
            RelNode narrowed = narrowFragment(plan.resolvedFragment(), plan.backendId(), rules);
            if (narrowed == plan.resolvedFragment()) {
                updated.add(plan);
            } else {
                updated.add(new StagePlan(narrowed, plan.backendId()));
            }
        }
        stage.setPlanAlternatives(updated);
    }

    private static RelNode narrowFragment(RelNode node, String driverBackend, List<PerfPeerNarrowingRule> rules) {
        List<RelNode> newInputs = new ArrayList<>(node.getInputs().size());
        boolean inputsChanged = false;
        for (RelNode child : node.getInputs()) {
            RelNode narrowed = narrowFragment(child, driverBackend, rules);
            newInputs.add(narrowed);
            if (narrowed != child) inputsChanged = true;
        }

        if (node instanceof OpenSearchFilter filter) {
            RexNode newCondition = narrowCondition(filter.getCondition(), driverBackend, rules);
            if (newCondition != filter.getCondition() || inputsChanged) {
                return new OpenSearchFilter(
                    filter.getCluster(),
                    filter.getTraitSet(),
                    inputsChanged ? newInputs.getFirst() : filter.getInput(),
                    newCondition,
                    filter.getViableBackends()
                );
            }
            return filter;
        }

        return inputsChanged ? node.copy(node.getTraitSet(), newInputs) : node;
    }

    private static RexNode narrowCondition(RexNode node, String driverBackend, List<PerfPeerNarrowingRule> rules) {
        if (node instanceof AnnotatedPredicate predicate) {
            return narrowPredicate(predicate, driverBackend, rules);
        }
        if (node instanceof RexCall call) {
            List<RexNode> newOperands = new ArrayList<>(call.getOperands().size());
            boolean changed = false;
            for (RexNode operand : call.getOperands()) {
                RexNode narrowed = narrowCondition(operand, driverBackend, rules);
                newOperands.add(narrowed);
                if (narrowed != operand) changed = true;
            }
            return changed ? call.clone(call.getType(), newOperands) : call;
        }
        return node;
    }

    private static RexNode narrowPredicate(AnnotatedPredicate predicate, String driverBackend, List<PerfPeerNarrowingRule> rules) {
        List<String> peers = predicate.getPerformanceDelegationBackends();
        if (peers.isEmpty()) {
            return predicate;
        }
        if (!(predicate.unwrap() instanceof RexCall original)) {
            return predicate;
        }
        List<String> remaining = new ArrayList<>(peers.size());
        boolean changed = false;
        for (String peer : peers) {
            boolean drop = false;
            for (PerfPeerNarrowingRule rule : rules) {
                if (rule.shouldDropPeer(driverBackend, peer, predicate, original)) {
                    LOGGER.debug(
                        "PerfPeerNarrowing rule [{}] dropped peer [{}] for predicate id={} (driver=[{}])",
                        rule.name(),
                        peer,
                        predicate.getAnnotationId(),
                        driverBackend
                    );
                    drop = true;
                    break;
                }
            }
            if (drop) {
                changed = true;
            } else {
                remaining.add(peer);
            }
        }
        if (!changed) {
            return predicate;
        }
        return rebuildPredicate(predicate, remaining);
    }

    /**
     * Rebuilds an {@link AnnotatedPredicate} with a new performance-delegation peer list.
     * {@code viableBackends} stays intact — the driver hasn't changed; we're just removing
     * peers whose runtime consultation isn't useful for this predicate shape.
     *
     * <p>Goes through {@link AnnotatedPredicate#narrowTo(String)} when the result has zero
     * peers and the predicate was already narrowed to one viable backend ({@code PlanForker}'s
     * standard output); that path is the existing code's "single-viable" shape. Otherwise
     * builds a fresh annotation directly. Either way the resulting node is functionally a
     * single-viable predicate with no peer.
     */
    private static RexNode rebuildPredicate(AnnotatedPredicate predicate, List<String> remainingPeers) {
        // narrowTo always produces non-empty peers when the source has multiple viable backends.
        // We need an explicit constructor path for the empty-peers case.
        return new AnnotatedPredicateBuilder().build(
            predicate.getType(),
            predicate.unwrap(),
            predicate.getViableBackends(),
            predicate.getAnnotationId(),
            remainingPeers
        );
    }

    /**
     * Tiny helper to invoke {@link AnnotatedPredicate}'s package-private 5-arg constructor
     * via its public-but-shaped-for-this APIs. Today the constructor is private; this stub
     * exists so the rebuild path is in one place if we change construction in the future.
     *
     * <p>For now, since only {@link AnnotatedPredicate#clone} preserves all five fields,
     * we synthesize the rebuild via {@link AnnotatedPredicate}'s public surface plus a
     * narrow proxy in the same package.
     */
    private static final class AnnotatedPredicateBuilder {
        RexNode build(RelDataType type, RexNode original, List<String> viableBackends, int annotationId, List<String> peers) {
            return AnnotatedPredicate.rebuildWithPeers(type, original, viableBackends, annotationId, peers);
        }
    }
}
