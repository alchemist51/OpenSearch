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
 * A fully compiled MV definition capturing group keys, aggregates, mapping,
 * and ordering. Stripped of FFI/native/ship/merge machinery for the
 * definition-only layer. Substrait/native emission deferred to commit 2.
 */
public final class MVCompiledDefinition {

    private final String definitionHash;
    private final List<GroupKey> groupKeys;
    private final List<AggregateSpec> aggregates;
    private final Map<String, String> targetMapping;
    private final MVGroupByOrdering groupByOrdering;

    private MVCompiledDefinition(List<GroupKey> groupKeys, List<AggregateSpec> aggregates) {
        this.groupKeys = List.copyOf(groupKeys);
        this.aggregates = List.copyOf(aggregates);
        this.targetMapping = buildTargetMapping(this.groupKeys, this.aggregates);
        this.definitionHash = computeHash(this.groupKeys, this.aggregates);
        this.groupByOrdering = MVGroupByOrdering.fromGroupKeys(this.groupKeys);
    }

    // ── Builders ─────────────────────────────────────────────────────────

    public static MVCompiledDefinition of(List<GroupKey> groupKeys, List<AggregateSpec> aggregates) {
        if (groupKeys == null || groupKeys.isEmpty()) {
            throw new IllegalArgumentException("at least one group key is required");
        }
        if (aggregates == null || aggregates.isEmpty()) {
            throw new IllegalArgumentException("at least one aggregate is required");
        }
        return new MVCompiledDefinition(groupKeys, aggregates);
    }

    // ── Descriptor round-trip ────────────────────────────────────────────

    public static MVCompiledDefinition fromDescriptor(MVDefinitionDescriptor descriptor) {
        Objects.requireNonNull(descriptor, "descriptor");
        MVCompiledDefinition def = new MVCompiledDefinition(descriptor.toGroupKeys(), descriptor.toAggregateSpecs());
        descriptor.definitionHash().ifPresent(expected -> {
            if (expected.equals(def.definitionHash) == false) {
                throw new IllegalArgumentException(
                    String.format(
                        Locale.ROOT,
                        "MV descriptor integrity check failed: descriptor hash [%s] does not match recomputed definition hash [%s]",
                        expected,
                        def.definitionHash
                    )
                );
            }
        });
        return def;
    }

    public MVDefinitionDescriptor toDescriptor() {
        return MVDefinitionDescriptor.fromCompiled(this);
    }

    // ── Accessors ────────────────────────────────────────────────────────

    public String hash() { return definitionHash; }
    public List<GroupKey> groupKeys() { return groupKeys; }
    public List<AggregateSpec> aggregates() { return aggregates; }
    public Map<String, String> targetMapping() { return targetMapping; }
    public MVGroupByOrdering groupByOrdering() { return groupByOrdering; }

    // ── SQL generation ───────────────────────────────────────────────────

    public String buildPartialSql(String tableName) {
        Objects.requireNonNull(tableName, "tableName");
        StringBuilder sb = new StringBuilder("SELECT ");
        sb.append(groupKeys.stream().map(MVCompiledDefinition::partialSelectExpr).collect(Collectors.joining(", ")));
        for (AggregateSpec agg : aggregates) {
            sb.append(", ").append(agg.partialSqlFragment());
        }
        sb.append(" FROM ").append(tableName);
        sb.append(" GROUP BY ").append(groupKeys.stream().map(GroupKey::sqlExpression).collect(Collectors.joining(", ")));
        return sb.toString();
    }

    private static String partialSelectExpr(GroupKey key) {
        if (key.isPlainColumn()) {
            return "\"" + key.name() + "\"";
        }
        return key.sqlExpression() + " AS \"" + key.name() + "\"";
    }

    public String buildFoldSql(String sourceTable) {
        Objects.requireNonNull(sourceTable, "sourceTable");
        StringBuilder sb = new StringBuilder("SELECT ");
        sb.append(groupKeys.stream().map(k -> "\"" + k.name() + "\"").collect(Collectors.joining(", ")));
        for (AggregateSpec agg : aggregates) {
            sb.append(", ").append(agg.foldSqlFragment());
        }
        sb.append(" FROM ").append(sourceTable);
        sb.append(" GROUP BY ").append(groupKeys.stream().map(k -> "\"" + k.name() + "\"").collect(Collectors.joining(", ")));
        return sb.toString();
    }

    // ── Projection ───────────────────────────────────────────────────────

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

    public List<String> stateColumnNames() {
        return projectionOrder();
    }

    // ── Schema validation ────────────────────────────────────────────────

    public void validateSchema(Map<String, Object> schema) {
        for (Map.Entry<String, String> entry : targetMapping.entrySet()) {
            String field = entry.getKey();
            Object actual = schema.get(field);
            if (actual == null) {
                throw new IllegalStateException(
                    String.format(
                        Locale.ROOT,
                        "MV definition hash [%s] schema validation failed: missing field [%s] (expected type [%s])",
                        definitionHash, field, entry.getValue()
                    )
                );
            }
        }
    }

    // ── Internal ─────────────────────────────────────────────────────────

    private static Map<String, String> buildTargetMapping(List<GroupKey> keys, List<AggregateSpec> aggs) {
        Map<String, String> mapping = new LinkedHashMap<>();
        for (GroupKey key : keys) {
            mapping.put(key.name(), key.columnType().osType());
        }
        for (AggregateSpec agg : aggs) {
            mapping.put(agg.userAlias(), agg.targetMappingType());
        }
        return Collections.unmodifiableMap(mapping);
    }

    private static String computeHash(List<GroupKey> keys, List<AggregateSpec> aggs) {
        StringBuilder canonical = new StringBuilder();
        canonical.append("groups:");
        for (GroupKey k : keys) {
            canonical.append(k.name()).append("|").append(k.columnType().name()).append("|").append(k.osFieldPath());
            if (k.isPlainColumn() == false) {
                canonical.append("|expr=").append(k.sqlExpression());
            }
            canonical.append(";");
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
        return "MVCompiledDefinition{hash=" + definitionHash + ", groupKeys=" + groupKeys
            + ", aggregates=" + aggregates.stream()
                .map(a -> a.function() + "(" + a.sourceField() + ")→" + a.userAlias())
                .collect(Collectors.joining(", "))
            + "}";
    }
}
