/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.mv;

import java.util.List;
import java.util.Objects;

/**
 * A planner-facing materialized view definition in canonical (matchable) form.
 *
 * <p>Definitions are stored <b>post-decomposition</b>: the MV create pipeline
 * canonicalizes the definition query through the same
 * {@code PlannerImpl#decomposeAggregates} phase the query side runs, so an
 * {@code AVG} appears here as its {@code SUM} + {@code COUNT} parts,
 * {@code COUNT(DISTINCT x)} as {@code APPROX_COUNT_DISTINCT(x)}, and PPL's
 * {@code CHECKED_LONG_SUM} as plain {@code SUM}. Matching is therefore
 * name+args equality on identically-normalized shapes — never semantic
 * equivalence proving.
 *
 * <p>{@code stateSchemaFingerprint} identifies the Partial-mode output schema
 * the MV's state files were written with; the shard drops the rewrite binding
 * when its snapshot's fingerprint disagrees (never wrong, only slower).
 *
 * @param mvId                   unique MV identifier
 * @param sourceIndex            the source index the MV is defined over
 * @param groupByColumns         group-by column names (order = state-file sort
 *                               order; matching compares as a set)
 * @param aggregates             canonical aggregate calls
 * @param stateSchemaFingerprint fingerprint of the state-file schema
 *
 * @opensearch.internal
 */
public record MVDefinition(String mvId, String sourceIndex, List<String> groupByColumns, List<AggregateSpec> aggregates,
    String stateSchemaFingerprint) {

    public MVDefinition {
        Objects.requireNonNull(mvId, "mvId");
        Objects.requireNonNull(sourceIndex, "sourceIndex");
        groupByColumns = List.copyOf(groupByColumns);
        aggregates = List.copyOf(aggregates);
        Objects.requireNonNull(stateSchemaFingerprint, "stateSchemaFingerprint");
    }

    /**
     * One canonical aggregate call: upper-cased function name plus argument
     * column names ({@code COUNT(*)} has no arguments).
     *
     * @param function   aggregate function name, e.g. {@code SUM}, {@code COUNT},
     *                   {@code MIN}, {@code MAX}, {@code APPROX_COUNT_DISTINCT}
     * @param argColumns argument column names, empty for {@code COUNT(*)}
     */
    public record AggregateSpec(String function, List<String> argColumns) {
        public AggregateSpec {
            function = Objects.requireNonNull(function, "function").toUpperCase(java.util.Locale.ROOT);
            argColumns = List.copyOf(argColumns);
        }

        public static AggregateSpec of(String function, String... argColumns) {
            return new AggregateSpec(function, List.of(argColumns));
        }
    }
}
