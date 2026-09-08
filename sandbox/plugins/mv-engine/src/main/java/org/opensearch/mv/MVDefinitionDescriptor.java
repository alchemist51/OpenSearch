/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

/**
 * A versioned, serializable, language-agnostic description of a compiled MV
 * definition. The descriptor is the persistence/transport contract that the
 * Stage&nbsp;2 PPL matcher emits and that {@link MVCompiledDefinition#fromDescriptor}
 * consumes, replacing the hardcoded {@code compiledFor()} switch.
 *
 * <p><b>What it stores (authoritative content).</b> Only the minimal logical
 * shape needed to rebuild a definition:
 * <ul>
 *   <li>an ordered list of {@link GroupKeyDescriptor group keys} — alias/column
 *       name, {@link GroupKey.ColumnType}, and (for derived keys) the SQL
 *       expression plus the source column it reads from;</li>
 *   <li>an ordered list of {@link AggregateDescriptor aggregates} — the
 *       function ({@code SUM/MIN/MAX/COUNT/COUNT_FIELD/AVG}), the optional
 *       source field, and the stable user alias;</li>
 *   <li>optional provenance metadata: the source language (e.g. {@code ppl})
 *       and the original source text the definition was compiled from.</li>
 * </ul>
 *
 * <p><b>What it does NOT store (derived data).</b> The canonical partial SQL,
 * fold SQL, {@code state_fields}/projection order, target mapping, group-by
 * ordering identity, and definition hash are all <em>recomputed</em> by
 * {@link MVCompiledDefinition} when the descriptor is loaded. They are never
 * authoritative in the descriptor. The one exception is an optional
 * {@link #definitionHash()} <em>integrity check</em>: when present it is
 * validated against the recomputed hash in {@link MVCompiledDefinition#fromDescriptor}
 * and the load fails closed on mismatch. It carries no schema information — it
 * only detects drift between the persisted descriptor and the compiler.
 *
 * <p><b>Determinism.</b> {@link #toXContent} writes a stable, fixed field order
 * with no map iteration, so serialization is byte-reproducible and the
 * {@code toXContent → fromXContent → toXContent} round-trip is exact.
 *
 * <p>Instances are immutable and value-based ({@link #equals}/{@link #hashCode}
 * depend only on the stored content).
 */
public final class MVDefinitionDescriptor implements ToXContentObject {

    /** Current descriptor schema version. */
    public static final int CURRENT_VERSION = 1;

    // ── XContent field names (stable wire contract) ─────────────────────
    static final String F_DESCRIPTOR_VERSION = "descriptor_version";
    static final String F_SOURCE_LANGUAGE = "source_language";
    static final String F_SOURCE_TEXT = "source_text";
    static final String F_DEFINITION_HASH = "definition_hash";
    static final String F_GROUP_KEYS = "group_keys";
    static final String F_AGGREGATES = "aggregates";

    static final String F_GK_NAME = "name";
    static final String F_GK_COLUMN_TYPE = "column_type";
    static final String F_GK_EXPRESSION = "expression";
    static final String F_GK_SOURCE_COLUMN = "source_column";
    static final String F_GK_SPAN_INTERVAL_MS = "span_interval_ms";

    static final String F_AGG_FUNCTION = "function";
    static final String F_AGG_FIELD = "field";
    static final String F_AGG_ALIAS = "alias";

    private final int descriptorVersion;
    private final List<GroupKeyDescriptor> groupKeys;
    private final List<AggregateDescriptor> aggregates;
    private final String sourceLanguage; // nullable provenance
    private final String sourceText;     // nullable provenance
    private final String definitionHash;  // nullable integrity check

    private MVDefinitionDescriptor(
        int descriptorVersion,
        List<GroupKeyDescriptor> groupKeys,
        List<AggregateDescriptor> aggregates,
        String sourceLanguage,
        String sourceText,
        String definitionHash
    ) {
        if (descriptorVersion < 1) {
            throw new IllegalArgumentException(String.format(Locale.ROOT, "descriptorVersion must be >= 1, got [%d]", descriptorVersion));
        }
        if (descriptorVersion > CURRENT_VERSION) {
            throw new IllegalArgumentException(
                String.format(
                    Locale.ROOT,
                    "unsupported MV descriptor version [%d]: this node understands versions up to [%d]",
                    descriptorVersion,
                    CURRENT_VERSION
                )
            );
        }
        Objects.requireNonNull(groupKeys, "groupKeys");
        Objects.requireNonNull(aggregates, "aggregates");
        if (groupKeys.isEmpty()) {
            throw new IllegalArgumentException("at least one group key is required (an MV definition must GROUP BY >= 1 key)");
        }
        if (aggregates.isEmpty()) {
            throw new IllegalArgumentException("at least one aggregate is required");
        }
        this.groupKeys = List.copyOf(groupKeys);
        this.aggregates = List.copyOf(aggregates);
        this.sourceLanguage = sourceLanguage;
        this.sourceText = sourceText;
        this.definitionHash = definitionHash;
        this.descriptorVersion = descriptorVersion;
        validateAliases(this.groupKeys, this.aggregates);
    }

    /** Reject blank and duplicate output aliases across group keys and aggregates. */
    private static void validateAliases(List<GroupKeyDescriptor> keys, List<AggregateDescriptor> aggs) {
        Set<String> seen = new HashSet<>();
        for (GroupKeyDescriptor k : keys) {
            if (seen.add(k.name()) == false) {
                throw new IllegalArgumentException(String.format(Locale.ROOT, "duplicate output alias [%s]", k.name()));
            }
        }
        for (AggregateDescriptor a : aggs) {
            if (seen.add(a.alias()) == false) {
                throw new IllegalArgumentException(String.format(Locale.ROOT, "duplicate output alias [%s]", a.alias()));
            }
        }
    }

    // ── Builders ─────────────────────────────────────────────────────────

    /** Build a descriptor from explicit group-key and aggregate descriptors (no provenance). */
    public static MVDefinitionDescriptor of(List<GroupKeyDescriptor> groupKeys, List<AggregateDescriptor> aggregates) {
        return new MVDefinitionDescriptor(CURRENT_VERSION, groupKeys, aggregates, null, null, null);
    }

    /** Full builder including optional provenance and integrity hash. */
    public static MVDefinitionDescriptor create(
        List<GroupKeyDescriptor> groupKeys,
        List<AggregateDescriptor> aggregates,
        String sourceLanguage,
        String sourceText,
        String definitionHash
    ) {
        return new MVDefinitionDescriptor(CURRENT_VERSION, groupKeys, aggregates, sourceLanguage, sourceText, definitionHash);
    }

    /**
     * Derive a descriptor from a compiled definition. The recomputed
     * definition hash is embedded as an integrity check so a later
     * {@link MVCompiledDefinition#fromDescriptor} round-trip fails closed on drift.
     */
    public static MVDefinitionDescriptor fromCompiled(MVCompiledDefinition def) {
        return fromCompiled(def, null, null);
    }

    /** Derive a descriptor from a compiled definition, attaching source-language provenance. */
    public static MVDefinitionDescriptor fromCompiled(MVCompiledDefinition def, String sourceLanguage, String sourceText) {
        Objects.requireNonNull(def, "def");
        List<GroupKeyDescriptor> keys = new ArrayList<>(def.groupKeys().size());
        for (GroupKey gk : def.groupKeys()) {
            keys.add(GroupKeyDescriptor.fromGroupKey(gk));
        }
        List<AggregateDescriptor> aggs = new ArrayList<>(def.aggregates().size());
        for (AggregateSpec a : def.aggregates()) {
            aggs.add(AggregateDescriptor.fromAggregateSpec(a));
        }
        return new MVDefinitionDescriptor(CURRENT_VERSION, keys, aggs, sourceLanguage, sourceText, def.hash());
    }

    // ── Reconstruction to compiler inputs ────────────────────────────────

    /** Rebuild the ordered {@link GroupKey} list for the compiler. */
    public List<GroupKey> toGroupKeys() {
        List<GroupKey> keys = new ArrayList<>(groupKeys.size());
        for (GroupKeyDescriptor d : groupKeys) {
            keys.add(d.toGroupKey());
        }
        return keys;
    }

    /** Rebuild the ordered {@link AggregateSpec} list for the compiler. */
    public List<AggregateSpec> toAggregateSpecs() {
        List<AggregateSpec> aggs = new ArrayList<>(aggregates.size());
        for (AggregateDescriptor d : aggregates) {
            aggs.add(d.toAggregateSpec());
        }
        return aggs;
    }

    // ── Accessors ─────────────────────────────────────────────────────────

    /** Descriptor schema version. */
    public int descriptorVersion() {
        return descriptorVersion;
    }

    /** Ordered group keys. Unmodifiable. */
    public List<GroupKeyDescriptor> groupKeys() {
        return groupKeys;
    }

    /** Ordered aggregates. Unmodifiable. */
    public List<AggregateDescriptor> aggregates() {
        return aggregates;
    }

    /** Optional source language provenance (e.g. {@code ppl}). */
    public Optional<String> sourceLanguage() {
        return Optional.ofNullable(sourceLanguage);
    }

    /** Optional source text provenance. */
    public Optional<String> sourceText() {
        return Optional.ofNullable(sourceText);
    }

    /** Optional integrity hash validated on load. */
    public Optional<String> definitionHash() {
        return Optional.ofNullable(definitionHash);
    }

    // ── Serialization ─────────────────────────────────────────────────────

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(F_DESCRIPTOR_VERSION, descriptorVersion);
        if (sourceLanguage != null) {
            builder.field(F_SOURCE_LANGUAGE, sourceLanguage);
        }
        if (sourceText != null) {
            builder.field(F_SOURCE_TEXT, sourceText);
        }
        if (definitionHash != null) {
            builder.field(F_DEFINITION_HASH, definitionHash);
        }
        builder.startArray(F_GROUP_KEYS);
        for (GroupKeyDescriptor k : groupKeys) {
            k.toXContent(builder, params);
        }
        builder.endArray();
        builder.startArray(F_AGGREGATES);
        for (AggregateDescriptor a : aggregates) {
            a.toXContent(builder, params);
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    /**
     * Parse a descriptor from XContent. Fails closed on unknown future
     * {@code descriptor_version}, unknown fields, unknown column types, and
     * unknown aggregate functions.
     */
    public static MVDefinitionDescriptor fromXContent(XContentParser parser) throws IOException {
        XContentParser.Token token = parser.currentToken();
        if (token == null) {
            token = parser.nextToken();
        }
        if (token != XContentParser.Token.START_OBJECT) {
            throw new IllegalArgumentException("expected START_OBJECT but got [" + token + "]");
        }

        Integer version = null;
        String sourceLanguage = null;
        String sourceText = null;
        String definitionHash = null;
        List<GroupKeyDescriptor> keys = new ArrayList<>();
        List<AggregateDescriptor> aggs = new ArrayList<>();
        boolean sawGroupKeys = false;
        boolean sawAggregates = false;

        String fieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
                continue;
            }
            switch (fieldName) {
                case F_DESCRIPTOR_VERSION -> version = parser.intValue();
                case F_SOURCE_LANGUAGE -> sourceLanguage = parser.text();
                case F_SOURCE_TEXT -> sourceText = parser.text();
                case F_DEFINITION_HASH -> definitionHash = parser.text();
                case F_GROUP_KEYS -> {
                    sawGroupKeys = true;
                    if (token != XContentParser.Token.START_ARRAY) {
                        throw new IllegalArgumentException("[" + F_GROUP_KEYS + "] must be an array");
                    }
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        keys.add(GroupKeyDescriptor.fromXContent(parser));
                    }
                }
                case F_AGGREGATES -> {
                    sawAggregates = true;
                    if (token != XContentParser.Token.START_ARRAY) {
                        throw new IllegalArgumentException("[" + F_AGGREGATES + "] must be an array");
                    }
                    while (parser.nextToken() != XContentParser.Token.END_ARRAY) {
                        aggs.add(AggregateDescriptor.fromXContent(parser));
                    }
                }
                default -> throw new IllegalArgumentException("unknown field [" + fieldName + "] in MV definition descriptor");
            }
        }

        if (version == null) {
            throw new IllegalArgumentException("missing required field [" + F_DESCRIPTOR_VERSION + "]");
        }
        if (sawGroupKeys == false) {
            throw new IllegalArgumentException("missing required field [" + F_GROUP_KEYS + "]");
        }
        if (sawAggregates == false) {
            throw new IllegalArgumentException("missing required field [" + F_AGGREGATES + "]");
        }
        return new MVDefinitionDescriptor(version, keys, aggs, sourceLanguage, sourceText, definitionHash);
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if ((o instanceof MVDefinitionDescriptor) == false) {
            return false;
        }
        MVDefinitionDescriptor that = (MVDefinitionDescriptor) o;
        return descriptorVersion == that.descriptorVersion
            && groupKeys.equals(that.groupKeys)
            && aggregates.equals(that.aggregates)
            && Objects.equals(sourceLanguage, that.sourceLanguage)
            && Objects.equals(sourceText, that.sourceText)
            && Objects.equals(definitionHash, that.definitionHash);
    }

    @Override
    public int hashCode() {
        return Objects.hash(descriptorVersion, groupKeys, aggregates, sourceLanguage, sourceText, definitionHash);
    }

    @Override
    public String toString() {
        return "MVDefinitionDescriptor{v="
            + descriptorVersion
            + ", groupKeys="
            + groupKeys
            + ", aggregates="
            + aggregates
            + (sourceLanguage == null ? "" : ", sourceLanguage=" + sourceLanguage)
            + (definitionHash == null ? "" : ", definitionHash=" + definitionHash)
            + "}";
    }

    // ── Nested descriptors ────────────────────────────────────────────────

    /**
     * Serializable description of one group key. A plain-column key stores only
     * {@link #name} and {@link #columnType}; a derived (expression) key also
     * stores the SQL {@link #expression} and the {@link #sourceColumn} it reads
     * from. A plain key whose OpenSearch field path differs from its output
     * name also carries {@link #sourceColumn} (with no expression).
     *
     * <p>A <b>span key</b> carries {@link #spanIntervalMs} — the bucket width
     * in milliseconds — and is reconstructed via {@link GroupKey#ofSpan}. The
     * SQL expression is derived from the interval, not stored literally.</p>
     *
     * @param name             stable output alias / materialized column name
     * @param columnType       physical/logical column type
     * @param expression       SQL expression for a derived key, else {@code null}
     * @param sourceColumn     OpenSearch source field path, or {@code null} when it
     *                         equals {@link #name}
     * @param spanIntervalMs   bucket width in milliseconds for span keys, or -1
     */
    public record GroupKeyDescriptor(String name, GroupKey.ColumnType columnType, String expression, String sourceColumn,
        long spanIntervalMs) implements ToXContentObject {

        public GroupKeyDescriptor {
            if (name == null || name.isBlank()) {
                throw new IllegalArgumentException("group key name must not be blank");
            }
            Objects.requireNonNull(columnType, "columnType");
            if (expression != null && expression.isBlank()) {
                throw new IllegalArgumentException(String.format(Locale.ROOT, "group key [%s] expression must not be blank", name));
            }
            if (spanIntervalMs > 0 && columnType != GroupKey.ColumnType.TIMESTAMP) {
                throw new IllegalArgumentException(
                    String.format(Locale.ROOT, "span key [%s] must have TIMESTAMP column type, got [%s]", name, columnType)
                );
            }
        }

        /** Plain-column key convenience factory (source column == name). */
        public static GroupKeyDescriptor plain(String name, GroupKey.ColumnType columnType) {
            return new GroupKeyDescriptor(name, columnType, null, null, -1);
        }

        /** Derived (expression) key factory. */
        public static GroupKeyDescriptor expression(String name, GroupKey.ColumnType columnType, String expression, String sourceColumn) {
            Objects.requireNonNull(expression, "expression");
            Objects.requireNonNull(sourceColumn, "sourceColumn");
            return new GroupKeyDescriptor(name, columnType, expression, sourceColumn, -1);
        }

        /** Span (date_bin time-bucket) key factory. */
        public static GroupKeyDescriptor span(String name, long intervalMs, String sourceColumn) {
            Objects.requireNonNull(sourceColumn, "sourceColumn");
            if (intervalMs <= 0) {
                throw new IllegalArgumentException("span interval must be positive, got " + intervalMs);
            }
            return new GroupKeyDescriptor(name, GroupKey.ColumnType.TIMESTAMP, null, sourceColumn, intervalMs);
        }

        /** True when this is a span (date_bin) key. */
        public boolean isSpan() {
            return spanIntervalMs > 0;
        }

        /** Map an existing compiled {@link GroupKey} to its descriptor form. */
        static GroupKeyDescriptor fromGroupKey(GroupKey gk) {
            // Span keys are recognized by their isSpanKey() flag.
            if (gk.isSpanKey()) {
                long intervalMs = gk.spanIntervalMs();
                return new GroupKeyDescriptor(gk.name(), gk.columnType(), null, gk.osFieldPath(), intervalMs);
            }
            if (gk.isPlainColumn()) {
                // Only carry the source column when it differs from the output name.
                String src = gk.osFieldPath().equals(gk.name()) ? null : gk.osFieldPath();
                return new GroupKeyDescriptor(gk.name(), gk.columnType(), null, src, -1);
            }
            return new GroupKeyDescriptor(gk.name(), gk.columnType(), gk.sqlExpression(), gk.osFieldPath(), -1);
        }

        /** Rebuild the compiled {@link GroupKey} via the same factory path used by the typed builders. */
        GroupKey toGroupKey() {
            if (spanIntervalMs > 0) {
                return GroupKey.ofSpan(name, spanIntervalMs, sourceColumn != null ? sourceColumn : name);
            }
            if (expression != null) {
                return GroupKey.ofExpression(name, columnType, expression, sourceColumn != null ? sourceColumn : name);
            }
            if (sourceColumn != null) {
                // Plain column with a custom OpenSearch field path.
                return new GroupKey(name, columnType, sourceColumn);
            }
            return GroupKey.of(name, columnType);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(F_GK_NAME, name);
            builder.field(F_GK_COLUMN_TYPE, columnType.name());
            if (expression != null) {
                builder.field(F_GK_EXPRESSION, expression);
            }
            if (sourceColumn != null) {
                builder.field(F_GK_SOURCE_COLUMN, sourceColumn);
            }
            if (spanIntervalMs > 0) {
                builder.field(F_GK_SPAN_INTERVAL_MS, spanIntervalMs);
            }
            builder.endObject();
            return builder;
        }

        static GroupKeyDescriptor fromXContent(XContentParser parser) throws IOException {
            if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
                throw new IllegalArgumentException("expected START_OBJECT for a group key but got [" + parser.currentToken() + "]");
            }
            String name = null;
            GroupKey.ColumnType columnType = null;
            String expression = null;
            String sourceColumn = null;
            long spanIntervalMs = -1;
            String fieldName = null;
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    fieldName = parser.currentName();
                    continue;
                }
                switch (fieldName) {
                    case F_GK_NAME -> name = parser.text();
                    case F_GK_COLUMN_TYPE -> columnType = parseColumnType(parser.text());
                    case F_GK_EXPRESSION -> expression = parser.text();
                    case F_GK_SOURCE_COLUMN -> sourceColumn = parser.text();
                    case F_GK_SPAN_INTERVAL_MS -> spanIntervalMs = parser.longValue();
                    default -> throw new IllegalArgumentException("unknown field [" + fieldName + "] in group key descriptor");
                }
            }
            if (name == null) {
                throw new IllegalArgumentException("group key is missing required field [" + F_GK_NAME + "]");
            }
            if (columnType == null) {
                throw new IllegalArgumentException("group key [" + name + "] is missing required field [" + F_GK_COLUMN_TYPE + "]");
            }
            return new GroupKeyDescriptor(name, columnType, expression, sourceColumn, spanIntervalMs);
        }

        private static GroupKey.ColumnType parseColumnType(String raw) {
            try {
                return GroupKey.ColumnType.valueOf(raw);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("unknown group key column type [" + raw + "]");
            }
        }
    }

    /**
     * The aggregate function tokens the descriptor understands. {@code COUNT}
     * is {@code COUNT(*)} (no source field); {@code COUNT_FIELD} is
     * {@code COUNT(field)} (non-null values of a specific field). This split
     * distinguishes {@link AggregateSpec#count(String)} from
     * {@link AggregateSpec#countField(String, String)}, which otherwise share
     * the same {@link AggregateSpec.AggFunction#COUNT} enum.
     */
    public enum AggFunctionToken {
        SUM,
        MIN,
        MAX,
        COUNT,
        COUNT_FIELD,
        AVG
    }

    /**
     * Serializable description of one aggregate: the function token, the source
     * field ({@code null} only for {@code COUNT(*)}), and the stable user alias.
     *
     * @param function the aggregate function token
     * @param field    source field, or {@code null} for {@code COUNT(*)}
     * @param alias    stable user-visible output alias
     */
    public record AggregateDescriptor(AggFunctionToken function, String field, String alias) implements ToXContentObject {

        public AggregateDescriptor {
            Objects.requireNonNull(function, "function");
            if (alias == null || alias.isBlank()) {
                throw new IllegalArgumentException("aggregate alias must not be blank");
            }
            if (function == AggFunctionToken.COUNT) {
                if (field != null) {
                    throw new IllegalArgumentException("COUNT(*) aggregate [" + alias + "] must not carry a source field");
                }
            } else {
                if (field == null || field.isBlank()) {
                    throw new IllegalArgumentException(
                        String.format(Locale.ROOT, "%s aggregate [%s] requires a non-blank source field", function, alias)
                    );
                }
            }
        }

        /** {@code COUNT(*)} factory. */
        public static AggregateDescriptor count(String alias) {
            return new AggregateDescriptor(AggFunctionToken.COUNT, null, alias);
        }

        /** {@code COUNT(field)} factory. */
        public static AggregateDescriptor countField(String field, String alias) {
            return new AggregateDescriptor(AggFunctionToken.COUNT_FIELD, field, alias);
        }

        /** {@code SUM(field)} factory. */
        public static AggregateDescriptor sum(String field, String alias) {
            return new AggregateDescriptor(AggFunctionToken.SUM, field, alias);
        }

        /** {@code MIN(field)} factory. */
        public static AggregateDescriptor min(String field, String alias) {
            return new AggregateDescriptor(AggFunctionToken.MIN, field, alias);
        }

        /** {@code MAX(field)} factory. */
        public static AggregateDescriptor max(String field, String alias) {
            return new AggregateDescriptor(AggFunctionToken.MAX, field, alias);
        }

        /** {@code AVG(field)} factory (alias is derived by the compiler). */
        public static AggregateDescriptor avg(String field, String alias) {
            return new AggregateDescriptor(AggFunctionToken.AVG, field, alias);
        }

        /** Map an existing compiled {@link AggregateSpec} to its descriptor form. */
        static AggregateDescriptor fromAggregateSpec(AggregateSpec a) {
            AggFunctionToken token = switch (a.function()) {
                case SUM -> AggFunctionToken.SUM;
                case MIN -> AggFunctionToken.MIN;
                case MAX -> AggFunctionToken.MAX;
                case AVG -> AggFunctionToken.AVG;
                // COUNT(*) has no source field; COUNT(field) carries one.
                case COUNT -> a.sourceField() == null ? AggFunctionToken.COUNT : AggFunctionToken.COUNT_FIELD;
            };
            return new AggregateDescriptor(token, a.sourceField(), a.userAlias());
        }

        /** Rebuild the compiled {@link AggregateSpec} via the same factory path used by the typed builders. */
        AggregateSpec toAggregateSpec() {
            return switch (function) {
                case SUM -> AggregateSpec.sum(field, alias);
                case MIN -> AggregateSpec.min(field, alias);
                case MAX -> AggregateSpec.max(field, alias);
                case COUNT -> AggregateSpec.count(alias);
                case COUNT_FIELD -> AggregateSpec.countField(field, alias);
                case AVG -> AggregateSpec.avg(field);
            };
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject();
            builder.field(F_AGG_FUNCTION, function.name());
            if (field != null) {
                builder.field(F_AGG_FIELD, field);
            }
            builder.field(F_AGG_ALIAS, alias);
            builder.endObject();
            return builder;
        }

        static AggregateDescriptor fromXContent(XContentParser parser) throws IOException {
            if (parser.currentToken() != XContentParser.Token.START_OBJECT) {
                throw new IllegalArgumentException("expected START_OBJECT for an aggregate but got [" + parser.currentToken() + "]");
            }
            AggFunctionToken function = null;
            String field = null;
            String alias = null;
            String fieldName = null;
            XContentParser.Token token;
            while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                if (token == XContentParser.Token.FIELD_NAME) {
                    fieldName = parser.currentName();
                    continue;
                }
                switch (fieldName) {
                    case F_AGG_FUNCTION -> function = parseFunction(parser.text());
                    case F_AGG_FIELD -> field = parser.text();
                    case F_AGG_ALIAS -> alias = parser.text();
                    default -> throw new IllegalArgumentException("unknown field [" + fieldName + "] in aggregate descriptor");
                }
            }
            if (function == null) {
                throw new IllegalArgumentException("aggregate is missing required field [" + F_AGG_FUNCTION + "]");
            }
            if (alias == null) {
                throw new IllegalArgumentException("aggregate is missing required field [" + F_AGG_ALIAS + "]");
            }
            return new AggregateDescriptor(function, field, alias);
        }

        private static AggFunctionToken parseFunction(String raw) {
            try {
                return AggFunctionToken.valueOf(raw);
            } catch (IllegalArgumentException e) {
                throw new IllegalArgumentException("unknown aggregate function [" + raw + "]");
            }
        }
    }
}
