/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * A fully compiled MV definition that captures the query, mapping, and
 * projection metadata for a materialized view. Each definition is built
 * from a named {@link MVDefinitionSpec} and validated against a persisted
 * definition hash to detect schema drift.
 *
 * <p>The definition is immutable once built. The {@link #hash()} is stable
 * across JVM restarts (SHA-256 of the canonical form of group keys and
 * aggregate specs) and is persisted in index metadata to detect schema
 * drift at startup, poll, search, and merge time.</p>
 *
 * <p><b>Design invariant:</b> no DataFusion internal names (e.g.
 * {@code count(Int64(1))[count]}, {@code sum(mv_input.x)[sum]}) leak into
 * user-visible aliases, mappings, or projection orders. All user-facing
 * column names are stable aliases set at definition time.</p>
 */
public final class MVCompiledDefinition {

    private final String definitionHash;
    private final List<GroupKey> groupKeys;
    private final List<AggregateSpec> aggregates;
    private final Map<String, String> targetMapping;

    private MVCompiledDefinition(List<GroupKey> groupKeys, List<AggregateSpec> aggregates) {
        this.groupKeys = List.copyOf(groupKeys);
        this.aggregates = List.copyOf(aggregates);
        this.targetMapping = buildTargetMapping(this.groupKeys, this.aggregates);
        this.definitionHash = computeHash(this.groupKeys, this.aggregates);
    }

    // ── Programmatic builders ─────────────────────────────────────────────

    /**
     * Build a definition for {@code COUNT(*), SUM(sumField), MIN(minField),
     * MAX(maxField), AVG(avgField)} grouped by {@code groupField}.
     *
     * <p>Any field parameter may be {@code null} to omit that aggregate.
     * At minimum, the group field is required.</p>
     */
    public static MVCompiledDefinition forCountSumMinMaxAvg(
        String groupField,
        String sumField,
        String minField,
        String maxField,
        String avgField
    ) {
        Objects.requireNonNull(groupField, "groupField");
        List<GroupKey> keys = List.of(GroupKey.of(groupField, GroupKey.ColumnType.LONG));
        List<AggregateSpec> aggs = new ArrayList<>();
        aggs.add(AggregateSpec.count("cnt"));
        if (sumField != null) {
            aggs.add(AggregateSpec.sum(sumField, "sum_" + sumField));
        }
        if (minField != null) {
            aggs.add(AggregateSpec.min(minField, "min_" + minField));
        }
        if (maxField != null) {
            aggs.add(AggregateSpec.max(maxField, "max_" + maxField));
        }
        if (avgField != null) {
            aggs.add(AggregateSpec.avg(avgField));
        }
        return new MVCompiledDefinition(keys, aggs);
    }

    /**
     * Generic builder: supply arbitrary group keys and aggregate specs.
     */
    public static MVCompiledDefinition of(List<GroupKey> groupKeys, List<AggregateSpec> aggregates) {
        if (groupKeys == null || groupKeys.isEmpty()) {
            throw new IllegalArgumentException("at least one group key is required");
        }
        if (aggregates == null || aggregates.isEmpty()) {
            throw new IllegalArgumentException("at least one aggregate is required");
        }
        return new MVCompiledDefinition(groupKeys, aggregates);
    }

    // ── Accessors ─────────────────────────────────────────────────────────

    /** Stable definition hash (SHA-256 hex). */
    public String hash() {
        return definitionHash;
    }

    /** Ordered group key columns. */
    public List<GroupKey> groupKeys() {
        return groupKeys;
    }

    /** Ordered aggregate specs. */
    public List<AggregateSpec> aggregates() {
        return aggregates;
    }

    /** Target mapping: fieldName → OpenSearch type. Unmodifiable. */
    public Map<String, String> targetMapping() {
        return targetMapping;
    }

    // ── Schema validation ─────────────────────────────────────────────────

    /**
     * Validate that an external schema (map of field → type) is compatible
     * with this definition. Throws {@link IllegalStateException} on mismatch.
     *
     * @param schema field-name to type-string map from the actual index mapping
     *               or state file schema
     */
    public void validateSchema(Map<String, Object> schema) {
        for (Map.Entry<String, String> entry : targetMapping.entrySet()) {
            String field = entry.getKey();
            Object actual = schema.get(field);
            if (actual == null) {
                throw new IllegalStateException(
                    String.format(
                        Locale.ROOT,
                        "MV definition hash [%s] schema validation failed: missing field [%s] (expected type [%s])",
                        definitionHash,
                        field,
                        entry.getValue()
                    )
                );
            }
        }
    }

    // ── SQL generation ────────────────────────────────────────────────────

    /**
     * Generate the partial (incremental) SQL for computing state from raw
     * source data. The SQL operates over the given {@code tableName}
     * (typically {@code mv_input}).
     *
     * <p>Example output:
     * <pre>
     * SELECT "RegionID", COUNT(*), SUM("AdvEngineID"), COUNT("ResWidth"), SUM("ResWidth")
     * FROM mv_input GROUP BY "RegionID"
     * </pre>
     */
    public String buildPartialSql(String tableName) {
        Objects.requireNonNull(tableName, "tableName");
        StringBuilder sb = new StringBuilder("SELECT ");
        // Group keys
        sb.append(groupKeys.stream().map(k -> "\"" + k.name() + "\"").collect(Collectors.joining(", ")));
        // Aggregate fragments
        for (AggregateSpec agg : aggregates) {
            sb.append(", ").append(agg.partialSqlFragment());
        }
        sb.append(" FROM ").append(tableName);
        sb.append(" GROUP BY ").append(groupKeys.stream().map(k -> "\"" + k.name() + "\"").collect(Collectors.joining(", ")));
        return sb.toString();
    }

    /**
     * Generate the fold/merge SQL that combines partial state rows.
     * The fold SQL uses stable state column names (never DataFusion internals).
     *
     * <p>Example output:
     * <pre>
     * SELECT "RegionID", SUM("cnt"), SUM("sum_AdvEngineID"), SUM("avg_count_ResWidth"), SUM("avg_sum_ResWidth")
     * FROM source_table GROUP BY "RegionID"
     * </pre>
     */
    public String buildFoldSql(String sourceTable) {
        Objects.requireNonNull(sourceTable, "sourceTable");
        StringBuilder sb = new StringBuilder("SELECT ");
        // Group keys
        sb.append(groupKeys.stream().map(k -> "\"" + k.name() + "\"").collect(Collectors.joining(", ")));
        // Fold fragments
        for (AggregateSpec agg : aggregates) {
            sb.append(", ").append(agg.foldSqlFragment());
        }
        sb.append(" FROM ").append(sourceTable);
        sb.append(" GROUP BY ").append(groupKeys.stream().map(k -> "\"" + k.name() + "\"").collect(Collectors.joining(", ")));
        return sb.toString();
    }

    /**
     * Ordered projection column names for search results. Group keys first,
     * then state columns in definition order. This is the deterministic
     * column ordering contract for all readers.
     */
    public List<String> projectionOrder() {
        List<String> columns = new ArrayList<>();
        for (GroupKey key : groupKeys) {
            columns.add(key.name());
        }
        for (AggregateSpec agg : aggregates) {
            for (AggregateSpec.StateColumn sc : agg.stateColumns()) {
                columns.add(sc.name());
            }
        }
        return Collections.unmodifiableList(columns);
    }

    /**
     * Returns the ordered list of all state column names (group keys + all
     * aggregate state columns), matching the physical layout.
     */
    public List<String> stateColumnNames() {
        return projectionOrder();
    }

    // ── Internal ──────────────────────────────────────────────────────────

    private static Map<String, String> buildTargetMapping(List<GroupKey> keys, List<AggregateSpec> aggs) {
        Map<String, String> mapping = new LinkedHashMap<>();
        for (GroupKey key : keys) {
            mapping.put(key.name(), key.columnType().osType());
        }
        for (AggregateSpec agg : aggs) {
            // The user alias maps to the target mapping type
            mapping.put(agg.userAlias(), agg.targetMappingType());
        }
        return Collections.unmodifiableMap(mapping);
    }

    /**
     * Compute a stable SHA-256 hash of the definition's canonical form.
     * The canonical form is: group keys (name|type) sorted, then aggregates
     * (function|sourceField|alias|stateColumns) in definition order.
     * This ensures the same logical definition always produces the same hash
     * regardless of JVM instance or restart.
     */
    private static String computeHash(List<GroupKey> keys, List<AggregateSpec> aggs) {
        StringBuilder canonical = new StringBuilder();
        canonical.append("groups:");
        for (GroupKey k : keys) {
            canonical.append(k.name()).append("|").append(k.columnType().name()).append("|").append(k.osFieldPath()).append(";");
        }
        canonical.append("aggs:");
        for (AggregateSpec a : aggs) {
            canonical.append(a.function().name()).append("|");
            canonical.append(a.sourceField() == null ? "" : a.sourceField()).append("|");
            canonical.append(a.userAlias()).append("|");
            for (AggregateSpec.StateColumn sc : a.stateColumns()) {
                canonical.append(sc.name()).append(":").append(sc.physicalType()).append(",");
            }
            canonical.append(";");
        }
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            byte[] hash = digest.digest(canonical.toString().getBytes(StandardCharsets.UTF_8));
            return bytesToHex(hash);
        } catch (NoSuchAlgorithmException e) {
            throw new AssertionError("SHA-256 not available", e);
        }
    }

    private static String bytesToHex(byte[] bytes) {
        StringBuilder sb = new StringBuilder(bytes.length * 2);
        for (byte b : bytes) {
            sb.append(String.format(Locale.ROOT, "%02x", b));
        }
        return sb.toString();
    }

    @Override
    public String toString() {
        return "MVCompiledDefinition{hash="
            + definitionHash
            + ", groupKeys="
            + groupKeys
            + ", aggregates="
            + aggregates.stream().map(a -> a.function() + "(" + a.sourceField() + ")→" + a.userAlias()).collect(Collectors.joining(", "))
            + "}";
    }
}
