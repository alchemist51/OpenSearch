/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.spi;

/**
 * Backend-agnostic description of the boolean tree shape when filter delegation is active.
 * Provided by the planner so backends can choose their execution strategy without
 * re-inspecting the Substrait plan.
 *
 * @opensearch.internal
 */
public enum FilterTreeShape {
    /** No delegation — all predicates handled natively by the driving backend. */
    NO_DELEGATION,
    /**
     * All predicates (delegated + native) are under a single AND — no interleaving
     * under OR/NOT. Backend can handle delegated bitsets and native predicates independently.
     */
    CONJUNCTIVE,
    /**
     * Delegated and native predicates are interleaved under OR/NOT — the boolean tree
     * mixes predicates from different backends under non-AND operators. Backend needs a
     * tree evaluator to combine bitsets from both backends per the boolean structure.
     */
    INTERLEAVED_BOOLEAN_EXPRESSION,
    /**
     * Strictly narrower than {@link #CONJUNCTIVE}: every predicate is delegated, the tree is a
     * single Collector leaf (or pure AND of Collectors fused upstream), and the surrounding plan
     * is a {@code count(*)} aggregate with no projection and no native predicates. The backend
     * may answer per-segment with a {@code long} count instead of materializing a doc-id bitset,
     * unlocking Lucene's {@code Weight.count(LeafReaderContext)} fast path.
     */
    COUNT_DELEGATION
}
