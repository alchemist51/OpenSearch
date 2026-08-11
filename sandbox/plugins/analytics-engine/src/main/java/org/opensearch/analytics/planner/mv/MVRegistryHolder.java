/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.mv;

/**
 * Process-wide {@link MVRegistry} injection point.
 *
 * <p><b>POC wiring</b>: until MV metadata lands (CRUD milestone M0/M1 provides a
 * cluster-state-backed registry resolved per query), the registry is a static
 * holder defaulting to {@link MVRegistry#EMPTY}. {@code DefaultPlanExecutor}
 * reads it once per query into the {@code PlannerContext}; integration tests
 * (and the POC demo) install a static registry at node startup. Replace with
 * per-query cluster-state resolution when MV metadata exists — tracked in
 * mv-search-side-integration-plan.md (W2 production wiring).
 *
 * @opensearch.internal
 */
public final class MVRegistryHolder {

    private static volatile MVRegistry registry = MVRegistry.EMPTY;

    private MVRegistryHolder() {}

    public static MVRegistry get() {
        return registry;
    }

    public static void set(MVRegistry newRegistry) {
        registry = newRegistry == null ? MVRegistry.EMPTY : newRegistry;
    }

    /** Restores the default EMPTY registry (test cleanup). */
    public static void reset() {
        registry = MVRegistry.EMPTY;
    }
}
