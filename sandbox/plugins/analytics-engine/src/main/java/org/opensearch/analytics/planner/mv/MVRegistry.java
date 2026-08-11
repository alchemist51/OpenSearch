/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.mv;

import java.util.List;
import java.util.Map;

/**
 * Planner-facing lookup of incremental materialized views eligible for
 * transparent rewrite.
 *
 * <p>This is the seam between the planner's {@link MVRewritePhase} and MV
 * metadata ownership (cluster-state metadata from the MV CRUD milestone).
 * Until that lands, production wires {@link #EMPTY} — the rewrite phase is a
 * no-op at zero cost — and tests wire {@link #ofStatic}.
 *
 * @opensearch.internal
 */
@FunctionalInterface
public interface MVRegistry {

    /**
     * Returns the MV definitions eligible for transparent rewrite of queries
     * on {@code sourceIndex}. Eligibility (single-source, decomposable,
     * deterministic) is proven at MV create time; callers may assume every
     * returned definition is rewrite-eligible and only need shape-matching.
     *
     * @param sourceIndex resolved concrete index name
     * @return eligible definitions, empty when none exist
     */
    List<MVDefinition> eligibleFor(String sourceIndex);

    /** No MVs anywhere. The production default until MV metadata lands. */
    MVRegistry EMPTY = sourceIndex -> List.of();

    /** Fixed in-memory registry, for tests and the POC wiring. */
    static MVRegistry ofStatic(Map<String, List<MVDefinition>> byIndex) {
        Map<String, List<MVDefinition>> copy = Map.copyOf(byIndex);
        return sourceIndex -> copy.getOrDefault(sourceIndex, List.of());
    }
}
