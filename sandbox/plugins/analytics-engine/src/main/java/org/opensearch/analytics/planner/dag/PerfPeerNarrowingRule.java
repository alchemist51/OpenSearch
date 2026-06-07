/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.dag;

import org.apache.calcite.rex.RexCall;
import org.opensearch.analytics.planner.rel.AnnotatedPredicate;

/**
 * Decides whether a registered performance-delegation peer should be dropped from a
 * given {@link AnnotatedPredicate} on a specific {@link StagePlan}. Used by
 * {@link PerfPeerNarrowing} to remove peers whose runtime consultation buys nothing
 * (or actively costs more than it saves) for the predicate's shape.
 *
 * <p>Each rule is scoped to a (driver backend, peer backend) pair and a predicate
 * shape; if it returns {@code true}, the peer is dropped from
 * {@link AnnotatedPredicate#getPerformanceDelegationBackends()} for that one
 * predicate on that one stage alternative.
 *
 * <p>Importantly, this does NOT touch {@link AnnotatedPredicate#getViableBackends()}.
 * Backend-driver alternatives produced by {@link PlanForker} (e.g. Lucene-as-driver
 * count fast-path) are unaffected — those alternatives have their own resolved
 * predicates with the chosen backend in {@code viableBackends}, and this pass only
 * narrows perf-peers when the chosen driver is the parent stage backend.
 *
 * @opensearch.internal
 */
public interface PerfPeerNarrowingRule {

    /**
     * Stable identifier for logs / tests, e.g.
     * {@code "drop-lucene-perf-peer-for-not-equals-empty-string"}.
     */
    String name();

    /**
     * Returns {@code true} to drop {@code peerBackend} from {@code predicate}'s
     * performance-delegation backends on a stage whose driver is {@code driverBackend}.
     */
    boolean shouldDropPeer(String driverBackend, String peerBackend, AnnotatedPredicate predicate, RexCall original);
}
