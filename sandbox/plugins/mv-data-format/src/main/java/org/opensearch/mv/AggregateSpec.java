/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import java.util.List;
import java.util.Locale;
import java.util.Objects;

/**
 * One aggregate function in a compiled MV definition.
 *
 * <p>Each aggregate decomposes into one or more <em>state columns</em> — the
 * physical columns stored in the {@code .si}/{@code .parquet} state files.
 * For example, {@code AVG(x)} decomposes into a count state and a sum state
 * so the merge can combine partial results correctly.</p>
 *
 * <p>User-visible aliases are stable and NEVER contain DataFusion internal
 * names like {@code count(Int64(1))[count]} or {@code sum(mv_input.x)[sum]}.
 * The alias is what appears in the OpenSearch mapping and search results.</p>
 */
public record AggregateSpec(AggFunction function, String sourceField, String userAlias, List<StateColumn> stateColumns,
    String partialSqlFragment, String foldSqlFragment, String targetMappingType) {

    /** Supported aggregate functions. */
    public enum AggFunction {
        COUNT,
        SUM,
        MIN,
        MAX,
        AVG
    }

    /**
     * One physical state column within an aggregate.
     *
     * @param name         stable state column name (e.g. {@code "cnt"}, {@code "avg_count_x"})
     * @param physicalType physical storage type
     */
    public record StateColumn(String name, String physicalType) {
        public StateColumn {
            Objects.requireNonNull(name, "name");
            Objects.requireNonNull(physicalType, "physicalType");
        }
    }

    public AggregateSpec {
        Objects.requireNonNull(function, "function");
        Objects.requireNonNull(userAlias, "userAlias");
        Objects.requireNonNull(stateColumns, "stateColumns");
        stateColumns = List.copyOf(stateColumns);
        Objects.requireNonNull(partialSqlFragment, "partialSqlFragment");
        Objects.requireNonNull(foldSqlFragment, "foldSqlFragment");
        Objects.requireNonNull(targetMappingType, "targetMappingType");
        if (stateColumns.isEmpty()) {
            throw new IllegalArgumentException("aggregate must have at least one state column");
        }
    }

    // ── Static factories ──────────────────────────────────────────────────

    /** {@code COUNT(*)} → one state column {@code cnt} of type {@code long}. */
    public static AggregateSpec count(String alias) {
        return new AggregateSpec(
            AggFunction.COUNT,
            null,
            alias,
            List.of(new StateColumn(alias, "long")),
            "COUNT(*)",
            String.format(Locale.ROOT, "SUM(\"%s\")", alias),
            "long"
        );
    }

    /** {@code SUM(field)} → one state column of type {@code long}. */
    public static AggregateSpec sum(String sourceField, String alias) {
        Objects.requireNonNull(sourceField, "sourceField");
        return new AggregateSpec(
            AggFunction.SUM,
            sourceField,
            alias,
            List.of(new StateColumn(alias, "long")),
            String.format(Locale.ROOT, "SUM(\"%s\")", sourceField),
            String.format(Locale.ROOT, "SUM(\"%s\")", alias),
            "long"
        );
    }

    /** {@code MIN(field)} → one state column of type {@code long}. */
    public static AggregateSpec min(String sourceField, String alias) {
        Objects.requireNonNull(sourceField, "sourceField");
        return new AggregateSpec(
            AggFunction.MIN,
            sourceField,
            alias,
            List.of(new StateColumn(alias, "long")),
            String.format(Locale.ROOT, "MIN(\"%s\")", sourceField),
            String.format(Locale.ROOT, "MIN(\"%s\")", alias),
            "long"
        );
    }

    /** {@code MAX(field)} → one state column of type {@code long}. */
    public static AggregateSpec max(String sourceField, String alias) {
        Objects.requireNonNull(sourceField, "sourceField");
        return new AggregateSpec(
            AggFunction.MAX,
            sourceField,
            alias,
            List.of(new StateColumn(alias, "long")),
            String.format(Locale.ROOT, "MAX(\"%s\")", sourceField),
            String.format(Locale.ROOT, "MAX(\"%s\")", alias),
            "long"
        );
    }

    /**
     * {@code AVG(field)} → decomposes into two state columns:
     * <ul>
     *   <li>{@code avg_count_<field>} — the count component</li>
     *   <li>{@code avg_sum_<field>} — the sum component</li>
     * </ul>
     * The user alias is {@code avg_<field>}. At read time the coordinator
     * computes {@code avg_sum / avg_count} to produce the final AVG value.
     */
    public static AggregateSpec avg(String sourceField) {
        Objects.requireNonNull(sourceField, "sourceField");
        String alias = "avg_" + sourceField;
        String countState = "avg_count_" + sourceField;
        String sumState = "avg_sum_" + sourceField;
        return new AggregateSpec(
            AggFunction.AVG,
            sourceField,
            alias,
            List.of(new StateColumn(countState, "long"), new StateColumn(sumState, "long")),
            String.format(Locale.ROOT, "COUNT(\"%s\"), SUM(\"%s\")", sourceField, sourceField),
            String.format(Locale.ROOT, "SUM(\"%s\"), SUM(\"%s\")", countState, sumState),
            "double"
        );
    }
}
