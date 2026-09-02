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
import java.util.Optional;

/**
 * Outcome of running {@link MVShapeMatcher#match} over a post-CBO Calcite plan.
 *
 * <p>A result is either {@link Status#MATCHED} — the plan is a legal
 * materialized-view (MV) shape and carries the extracted {@link GroupKey group
 * keys}, {@link Aggregate aggregates}, and a ready-to-persist
 * {@link #descriptorJson() descriptor JSON} — or {@link Status#REJECTED} with a
 * machine-readable {@link Reason} and a human-readable message. The matcher
 * never throws on an unsupported shape; every rejection is reported as data.
 *
 * <p>The JSON is byte-shaped to the {@code MVDefinitionDescriptor} wire contract
 * of the {@code mv-data-format} plugin (Stage&nbsp;1): {@code descriptor_version},
 * optional {@code source_language}/{@code source_text}, an ordered
 * {@code group_keys} array, and an ordered {@code aggregates} array. It omits the
 * optional {@code definition_hash} — the compiler recomputes the authoritative
 * hash when it rebuilds the definition via {@code MVCompiledDefinition.fromDescriptor}.
 *
 * <p><b>Stage&nbsp;3/5 entry contract:</b> callers run
 * {@code MVShapeResult r = MVShapeMatcher.match(root)}; on {@code r.isMatched()}
 * they persist / transport {@code r.descriptorJson()}, which the mv-data-format
 * plugin loads with {@code MVDefinitionDescriptor.fromXContent} followed by
 * {@code MVCompiledDefinition.fromDescriptor}. On rejection they surface
 * {@code r.reason()} + {@code r.message()}.
 *
 * <p>Instances are immutable.
 *
 * @opensearch.internal
 */
public final class MVShapeResult {

    /** Whether the plan is a legal MV shape. */
    public enum Status {
        /** The plan is a legal MV shape; the descriptor fields are populated. */
        MATCHED,
        /** The plan is not a legal MV shape; {@link #reason} + {@link #message} are populated. */
        REJECTED
    }

    /**
     * Machine-readable rejection categories. {@link #UNSUPPORTED_AGG} and
     * {@link #NON_DETERMINISTIC_EXPR} carry the offending function name in the
     * result {@link #message()}.
     */
    public enum Reason {
        /** A join is present — MV v1 is single-table only. */
        JOIN,
        /** A window function ({@code OVER}) is present. */
        WINDOW,
        /** An {@code ORDER BY} / {@code LIMIT} (top-N) is present. */
        SORT_OR_LIMIT,
        /** A {@code WHERE} / {@code HAVING} filter is present — MV v1 materializes the full grouping. */
        FILTER_WHERE,
        /** A {@code DISTINCT} or {@code FILTER (WHERE …)} aggregate is present. */
        DISTINCT_AGG,
        /** An aggregate function outside the {SUM, MIN, MAX, COUNT, AVG} allow-list is present. */
        UNSUPPORTED_AGG,
        /** A subquery, set operation, or more than one source table is present. */
        SUBQUERY_OR_MULTI_TABLE,
        /** A non-deterministic expression (e.g. {@code RAND()}, {@code NOW()}) is present. */
        NON_DETERMINISTIC_EXPR,
        /** The aggregate has no group keys — an MV must {@code GROUP BY} at least one key. */
        ZERO_GROUP_KEYS,
        /** A group-key expression is outside the derived-key whitelist. */
        UNSUPPORTED_KEY_EXPR,
        /** A group-key type does not map to one of {KEYWORD, LONG, INTEGER, DOUBLE}. */
        UNSUPPORTED_TYPE
    }

    /** Physical/logical column type of a group key — mirrors {@code GroupKey.ColumnType} tokens. */
    public enum ColumnType {
        /** String / keyword column. */
        KEYWORD,
        /** 64-bit integer column. */
        LONG,
        /** 32-bit (or narrower) integer column. */
        INTEGER,
        /** Floating-point column. */
        DOUBLE
    }

    /** Aggregate function token — mirrors {@code MVDefinitionDescriptor.AggFunctionToken}. */
    public enum AggToken {
        /** {@code SUM(field)}. */
        SUM,
        /** {@code MIN(field)}. */
        MIN,
        /** {@code MAX(field)}. */
        MAX,
        /** {@code COUNT(*)} — no source field. */
        COUNT,
        /** {@code COUNT(field)} — counts non-null values of a specific field. */
        COUNT_FIELD,
        /** {@code AVG(field)}. */
        AVG
    }

    /**
     * One extracted group key.
     *
     * @param name         stable output alias / materialized column name
     * @param type         mapped column type
     * @param expression   DataFusion-compatible SQL expression for a derived key, else {@code null}
     * @param sourceColumn OpenSearch source field the key reads from, or {@code null} when it equals {@link #name}
     */
    public record GroupKey(String name, ColumnType type, String expression, String sourceColumn) {
        public GroupKey {
            Objects.requireNonNull(name, "name");
            Objects.requireNonNull(type, "type");
        }

        /** True when this key is a derived (expression) key. */
        public boolean isExpression() {
            return expression != null;
        }
    }

    /**
     * One extracted aggregate.
     *
     * @param function the aggregate function token
     * @param field    source field, or {@code null} for {@code COUNT(*)}
     * @param alias    stable output alias
     */
    public record Aggregate(AggToken function, String field, String alias) {
        public Aggregate {
            Objects.requireNonNull(function, "function");
            Objects.requireNonNull(alias, "alias");
        }
    }

    private final Status status;
    private final List<GroupKey> groupKeys;
    private final List<Aggregate> aggregates;
    private final String descriptorJson;
    private final Reason reason;
    private final String message;

    private MVShapeResult(
        Status status,
        List<GroupKey> groupKeys,
        List<Aggregate> aggregates,
        String descriptorJson,
        Reason reason,
        String message
    ) {
        this.status = status;
        this.groupKeys = groupKeys == null ? List.of() : List.copyOf(groupKeys);
        this.aggregates = aggregates == null ? List.of() : List.copyOf(aggregates);
        this.descriptorJson = descriptorJson;
        this.reason = reason;
        this.message = message;
    }

    /** Build a MATCHED result. */
    static MVShapeResult matched(List<GroupKey> groupKeys, List<Aggregate> aggregates, String descriptorJson) {
        Objects.requireNonNull(descriptorJson, "descriptorJson");
        return new MVShapeResult(Status.MATCHED, groupKeys, aggregates, descriptorJson, null, null);
    }

    /** Build a REJECTED result. */
    static MVShapeResult rejected(Reason reason, String message) {
        Objects.requireNonNull(reason, "reason");
        Objects.requireNonNull(message, "message");
        return new MVShapeResult(Status.REJECTED, List.of(), List.of(), null, reason, message);
    }

    /** The outcome status. */
    public Status status() {
        return status;
    }

    /** True iff the plan matched a legal MV shape. */
    public boolean isMatched() {
        return status == Status.MATCHED;
    }

    /** Ordered group keys (GROUP BY order). Empty on rejection. */
    public List<GroupKey> groupKeys() {
        return groupKeys;
    }

    /** Ordered aggregates. Empty on rejection. */
    public List<Aggregate> aggregates() {
        return aggregates;
    }

    /** Descriptor JSON (MVDefinitionDescriptor wire contract). Present only when matched. */
    public String descriptorJson() {
        if (isMatched() == false) {
            throw new IllegalStateException("descriptorJson() is only available on a MATCHED result");
        }
        return descriptorJson;
    }

    /** Rejection reason, if rejected. */
    public Optional<Reason> reason() {
        return Optional.ofNullable(reason);
    }

    /** Human-readable rejection message, if rejected. */
    public Optional<String> message() {
        return Optional.ofNullable(message);
    }

    @Override
    public String toString() {
        if (isMatched()) {
            return "MVShapeResult{MATCHED, groupKeys=" + groupKeys + ", aggregates=" + aggregates + "}";
        }
        return "MVShapeResult{REJECTED, reason=" + reason + ", message=" + message + "}";
    }
}
