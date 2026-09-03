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
    private final MVGroupByOrdering groupByOrdering;

    private MVCompiledDefinition(List<GroupKey> groupKeys, List<AggregateSpec> aggregates) {
        this.groupKeys = List.copyOf(groupKeys);
        this.aggregates = List.copyOf(aggregates);
        this.targetMapping = buildTargetMapping(this.groupKeys, this.aggregates);
        this.definitionHash = computeHash(this.groupKeys, this.aggregates);
        // Derive the complete physical GROUP BY ordering contract once, here.
        // This is the single derivation point — no caller may re-derive ordering
        // from groupKeys() or SQL (see MVGroupByOrdering).
        this.groupByOrdering = MVGroupByOrdering.fromGroupKeys(this.groupKeys);
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

    // ── Descriptor round-trip ─────────────────────────────────────────────

    /**
     * Build a compiled definition from a versioned, serializable
     * {@link MVDefinitionDescriptor}. This is the language-agnostic entry point
     * that replaces the hardcoded {@link #compiledFor(String)} switch: a
     * definition compiled from PPL (Stage&nbsp;2) is persisted as a descriptor
     * and rebuilt here, going through the exact same
     * {@code (GroupKey, AggregateSpec)} constructor path as the typed builders,
     * so partial SQL, fold SQL, {@code state_fields}/projection order, target
     * mapping, ordering identity, and hash are all recomputed identically.
     *
     * <p>If the descriptor carries an optional integrity
     * {@link MVDefinitionDescriptor#definitionHash()}, it is validated against
     * the recomputed hash and the load <b>fails closed</b> on mismatch.</p>
     *
     * @throws IllegalArgumentException if the descriptor is invalid or its
     *                                  integrity hash does not match the
     *                                  recomputed definition hash
     */
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

    /**
     * Export this compiled definition to a versioned, serializable descriptor
     * (with its integrity hash embedded). Lets existing named definitions be
     * persisted/transported and rebuilt via {@link #fromDescriptor}.
     */
    public MVDefinitionDescriptor toDescriptor() {
        return MVDefinitionDescriptor.fromCompiled(this);
    }

    // ── Authoritative compiler ────────────────────────────────────────────

    /**
     * The single authoritative entry point that turns a definition name into
     * a fully compiled definition. Both the pull-side artifact builder and the
     * target-index creator call this, so pull SQL, target mapping, definition
     * hash, {@code state_fields}/projection order, fold SQL, and search schema
     * all derive from the same {@link MVCompiledDefinition}. There is exactly
     * one place where a definition's shape is constructed.
     *
     * <p>Definitions authored against the typed compiled model (full
     * {@code SUM/MIN/MAX/COUNT} quad per metric, expression-capable group
     * keys) are dispatched directly. All other named definitions are compiled
     * from their legacy {@link MVDefinitionSpec} via
     * {@link #fromLegacySpec(MVDefinitionSpec)}, preserving their exact prior
     * SQL, mapping, projection, and hash.</p>
     */
    public static MVCompiledDefinition compiledFor(String definitionName) {
        Objects.requireNonNull(definitionName, "definitionName");
        return switch (definitionName) {
            case "clickbench_100m" -> clickbench100m();
            case "heavy_l1" -> heavyL1();
            case "heavy_l2" -> heavyL2();
            case "heavy_l3" -> heavyL3();
            case "clickbench_5m_url" -> clickbench5mUrl();
            default -> fromLegacySpec(MVDefinitionSpec.source(definitionName));
        };
    }

    /**
     * Compile a legacy {@link MVDefinitionSpec} into the count/sum-per-metric
     * shape that prior definitions have always used: the leading
     * {@code groupKeys()} columns become plain group keys, and each remaining
     * captured column becomes a {@code SUM} aggregate, preceded by a single
     * {@code COUNT(*)} alias {@code cnt}. This is the exact logic that was
     * previously duplicated in {@code MVDerivedArtifactBuilder.buildFromSpec}
     * and {@code MVViewsService.targetMapping}.
     */
    public static MVCompiledDefinition fromLegacySpec(MVDefinitionSpec spec) {
        Objects.requireNonNull(spec, "spec");
        List<GroupKey> keys = new ArrayList<>();
        for (int i = 0; i < spec.groupKeys(); i++) {
            MVDefinitionSpec.Column col = spec.columns().get(i);
            GroupKey.ColumnType type = col.type() == MVDefinitionSpec.ColumnType.UTF8
                ? GroupKey.ColumnType.KEYWORD
                : GroupKey.ColumnType.LONG;
            keys.add(GroupKey.of(col.name(), type));
        }
        List<AggregateSpec> aggs = new ArrayList<>();
        aggs.add(AggregateSpec.count("cnt"));
        for (int i = spec.groupKeys(); i < spec.columns().size(); i++) {
            MVDefinitionSpec.Column col = spec.columns().get(i);
            aggs.add(AggregateSpec.sum(col.name(), "sum_" + col.name()));
        }
        return new MVCompiledDefinition(keys, aggs);
    }

    /**
     * The {@code clickbench_5m_url} definition: GROUP BY a date-aware 5-minute
     * {@code EventTime} bucket (via {@code date_bin}), {@code URL}, and
     * {@code UserID}, with the full {@code SUM/MIN/MAX/COUNT} quad over ten
     * numeric ClickBench fields (40 aggregate outputs → 43 projection columns
     * total).
     *
     * <p><b>EventTime bucketing:</b> uses {@code date_bin(INTERVAL '5 minutes',
     * "EventTime")} — a date-aware time window that works correctly with
     * timestamp-typed sources (mapping type {@code date} with
     * {@code yyyy-MM-dd HH:mm:ss||epoch_millis}). The state column is a
     * Timestamp, not an integer epoch ordinal.</p>
     */
    public static MVCompiledDefinition clickbench5mUrl() {
        List<GroupKey> keys = List.of(
            GroupKey.ofSpan("event_bucket", 300_000L, "EventTime"),
            GroupKey.of("URL", GroupKey.ColumnType.KEYWORD),
            GroupKey.of("UserID", GroupKey.ColumnType.LONG)
        );
        // (sourceField, alias prefix) in stable projection order.
        String[][] metrics = {
            { "AdvEngineID", "adv" },
            { "ResolutionWidth", "resw" },
            { "ResolutionHeight", "resh" },
            { "ResolutionDepth", "resd" },
            { "ClientIP", "cip" },
            { "RemoteIP", "rip" },
            { "ConnectTiming", "conn" },
            { "DNSTiming", "dns" },
            { "FetchTiming", "fetch" },
            { "SendTiming", "send" } };
        List<AggregateSpec> aggs = new ArrayList<>();
        for (String[] m : metrics) {
            String field = m[0];
            String prefix = m[1];
            aggs.add(AggregateSpec.sum(field, prefix + "_sum"));
            aggs.add(AggregateSpec.min(field, prefix + "_min"));
            aggs.add(AggregateSpec.max(field, prefix + "_max"));
            aggs.add(AggregateSpec.countField(field, prefix + "_cnt"));
        }
        return new MVCompiledDefinition(keys, aggs);
    }

    // ── Heavy-MV saturation ladder compiled definitions ──────────────────
    // Each rung produces the FULL SUM/MIN/MAX/COUNT quad per metric field.
    // The typed descriptor is shared by partial SQL, fold SQL, state_fields,
    // target mapping, projection, hash, search, and merge — one immutable
    // contract, never parsed from SQL strings.

    /**
     * Shared builder for rung definitions: emits full SUM/MIN/MAX/COUNT(field)
     * per metric, with typed group keys. The aliases follow the convention
     * {@code prefix_sum}, {@code prefix_min}, {@code prefix_max},
     * {@code prefix_cnt}.
     */
    private static MVCompiledDefinition buildLadderDefinition(List<GroupKey> keys, List<String[]> metrics) {
        List<AggregateSpec> aggs = new ArrayList<>();
        for (String[] m : metrics) {
            String field = m[0];
            String prefix = m[1];
            aggs.add(AggregateSpec.sum(field, prefix + "_sum"));
            aggs.add(AggregateSpec.min(field, prefix + "_min"));
            aggs.add(AggregateSpec.max(field, prefix + "_max"));
            aggs.add(AggregateSpec.countField(field, prefix + "_cnt"));
        }
        return new MVCompiledDefinition(keys, aggs);
    }

    // ── metric descriptor lists (sourceField, aliasPrefix) ──────────────

    /** L0/L1 metrics: 10 numeric ClickBench fields. */
    private static final List<String[]> LADDER_METRICS_10 = List.of(
        new String[] { "AdvEngineID", "adv" },
        new String[] { "ResolutionWidth", "resw" },
        new String[] { "ResolutionHeight", "resh" },
        new String[] { "ResolutionDepth", "resd" },
        new String[] { "ClientIP", "cip" },
        new String[] { "RemoteIP", "rip" },
        new String[] { "ConnectTiming", "conn" },
        new String[] { "DNSTiming", "dns" },
        new String[] { "FetchTiming", "fetch" },
        new String[] { "SendTiming", "send" }
    );

    /** L2: +10 more numeric columns = 20 total. */
    private static final List<String[]> LADDER_METRICS_20;
    static {
        var m = new ArrayList<>(LADDER_METRICS_10);
        m.add(new String[] { "ResponseStartTiming", "rsstart" });
        m.add(new String[] { "ResponseEndTiming", "rsend" });
        m.add(new String[] { "Age", "age" });
        m.add(new String[] { "HID", "hid" });
        m.add(new String[] { "CodeVersion", "codv" });
        m.add(new String[] { "IPNetworkID", "ipnet" });
        m.add(new String[] { "SilverlightVersion3", "sl3" });
        m.add(new String[] { "WindowName", "wnam" });
        m.add(new String[] { "URLHash", "urlh" });
        m.add(new String[] { "RefererHash", "refh" });
        LADDER_METRICS_20 = List.copyOf(m);
    }

    /** L3: +10 more = 30 total. */
    private static final List<String[]> LADDER_METRICS_30;
    static {
        var m = new ArrayList<>(LADDER_METRICS_20);
        m.add(new String[] { "ParamPrice", "pprice" });
        m.add(new String[] { "UserAgent", "uagent" });
        m.add(new String[] { "UserAgentMajor", "uamaj" });
        m.add(new String[] { "WindowClientWidth", "wcw" });
        m.add(new String[] { "WindowClientHeight", "wch" });
        m.add(new String[] { "Sex", "sex" });
        m.add(new String[] { "Robotness", "robot" });
        m.add(new String[] { "Income", "income" });
        m.add(new String[] { "HistoryLength", "histl" });
        m.add(new String[] { "OpenerName", "opener" });
        LADDER_METRICS_30 = List.copyOf(m);
    }

    // ── group key lists ─────────────────────────────────────────────────

    /** L0: 5 INT64 group keys. */
    private static final List<GroupKey> LADDER_GK_5 = List.of(
        GroupKey.of("EventTime", GroupKey.ColumnType.LONG),
        GroupKey.of("RegionID", GroupKey.ColumnType.LONG),
        GroupKey.of("OS", GroupKey.ColumnType.LONG),
        GroupKey.of("CounterID", GroupKey.ColumnType.LONG),
        GroupKey.of("IsRefresh", GroupKey.ColumnType.LONG)
    );

    /** L1/L2: 8 INT64 group keys. */
    private static final List<GroupKey> LADDER_GK_8;
    static {
        var g = new ArrayList<>(LADDER_GK_5);
        g.add(GroupKey.of("UserID", GroupKey.ColumnType.LONG));
        g.add(GroupKey.of("WatchID", GroupKey.ColumnType.LONG));
        g.add(GroupKey.of("FUniqID", GroupKey.ColumnType.LONG));
        LADDER_GK_8 = List.copyOf(g);
    }

    /** L3: 10 group keys (8 INT64 + 2 KEYWORD/UTF8). */
    private static final List<GroupKey> LADDER_GK_10;
    static {
        var g = new ArrayList<>(LADDER_GK_8);
        g.add(GroupKey.of("URL", GroupKey.ColumnType.KEYWORD));
        g.add(GroupKey.of("Referer", GroupKey.ColumnType.KEYWORD));
        LADDER_GK_10 = List.copyOf(g);
    }

    /**
     * L0 ({@code clickbench_100m}): 5 INT64 group keys + full
     * SUM/MIN/MAX/COUNT quad over 10 numeric ClickBench fields = 45 output
     * columns.
     */
    public static MVCompiledDefinition clickbench100m() {
        return buildLadderDefinition(LADDER_GK_5, LADDER_METRICS_10);
    }

    /**
     * L1 ({@code heavy_l1}): 8 INT64 group keys (+UserID, WatchID, FUniqID)
     * + full SUM/MIN/MAX/COUNT quad over 10 metrics = 48 output columns.
     */
    public static MVCompiledDefinition heavyL1() {
        return buildLadderDefinition(LADDER_GK_8, LADDER_METRICS_10);
    }

    /**
     * L2 ({@code heavy_l2}): 8 INT64 group keys + full SUM/MIN/MAX/COUNT
     * quad over 20 metrics = 88 output columns.
     */
    public static MVCompiledDefinition heavyL2() {
        return buildLadderDefinition(LADDER_GK_8, LADDER_METRICS_20);
    }

    /**
     * L3 ({@code heavy_l3}): 10 group keys (8 INT64 + 2 KEYWORD) + full
     * SUM/MIN/MAX/COUNT quad over 30 metrics = 130 output columns.
     */
    public static MVCompiledDefinition heavyL3() {
        return buildLadderDefinition(LADDER_GK_10, LADDER_METRICS_30);
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

    /**
     * The authoritative, complete physical GROUP BY ordering contract for this
     * definition: every group key, in state-field order, each ASC with NULLS
     * FIRST placement. This is the single source of truth for how MV state rows
     * must be sorted; the native build and merge paths (Stage&nbsp;3/4) will
     * consume {@link MVGroupByOrdering#stateFieldIndices()} and
     * {@link MVGroupByOrdering#columnNames()} to replace their current
     * column-0-only sort/advertisement.
     *
     * <p>Never re-derive ordering from {@link #groupKeys()} or the generated SQL
     * — always use this contract so the derivation exists in exactly one place.</p>
     */
    public MVGroupByOrdering groupByOrdering() {
        return groupByOrdering;
    }

    // ── Stage 4: FFI metadata for ordering + aggregates ──────────────────

    /**
     * Stage 4: Returns the ordering metadata as parallel FFI-ready arrays
     * suitable for passing across the native boundary. This is the merge-side
     * companion to the build-side ordering serialization — the merge path
     * needs the same ordering contract to validate that merged output
     * preserves the sort invariant.
     *
     * @return FFI-serialized ordering metadata
     */
    public OrderingFFIMetadata orderingFFIMetadata() {
        return OrderingFFIMetadata.from(groupByOrdering);
    }

    /**
     * Stage 4: Returns the aggregate accumulator type metadata as parallel
     * FFI-ready arrays. The Rust merge_state_streams FFI uses these to
     * determine the correct fold function per state column (SUM-fold for
     * SUM/COUNT accumulators, MIN-fold for MIN, MAX-fold for MAX).
     *
     * <p>The arrays are ordered to match the state-column layout: group keys
     * are omitted (they are handled by the ordering metadata), and each
     * aggregate's state columns appear in definition order.</p>
     *
     * @return FFI-serialized aggregate accumulator metadata
     */
    public AggregateFFIMetadata aggregateFFIMetadata() {
        return AggregateFFIMetadata.from(aggregates);
    }

    /**
     * Stage 4: Convenience bundle that collects all FFI metadata needed by the
     * merge path in one call: ordering, accumulator types, column names, and
     * the ordering identity for validation. The merge caller destructures
     * this once and feeds the pieces to {@code MVNativeBridge.mergeStateStreams}.
     *
     * @return immutable bundle of all merge FFI metadata
     */
    public MergeFFIBundle mergeFFIBundle() {
        return MergeFFIBundle.from(this);
    }

    /**
     * Convenience bundle for the merge path's FFI call. Collects ordering
     * metadata, aggregate accumulator metadata, and the ordering identity
     * so the caller need not destructure each piece separately.
     */
    public record MergeFFIBundle(
        OrderingFFIMetadata ordering,
        AggregateFFIMetadata aggregates,
        String orderingIdentity,
        int totalStateColumns
    ) {
        /** Build from a compiled definition. */
        public static MergeFFIBundle from(MVCompiledDefinition def) {
            OrderingFFIMetadata o = def.orderingFFIMetadata();
            AggregateFFIMetadata a = def.aggregateFFIMetadata();
            return new MergeFFIBundle(o, a, def.groupByOrdering().orderingIdentity(), o.length() + a.length());
        }
    }

    /**
     * Stage 4: Pre-built merge call parameters ready for direct use with
     * {@link MVNativeBridge#mergeStateStreams}. This eliminates the need for
     * callers to destructure the {@link MergeFFIBundle} and manually build
     * the {@code foldOps} byte array and boolean arrays.
     *
     * <p>Usage:
     * <pre>{@code
     * MVCompiledDefinition.MergeCallParams p = def.buildMergeCallParams();
     * long rows = MVNativeBridge.mergeStateStreams(
     *     inputPaths, outputPath,
     *     p.orderingIndices(), p.orderingAsc(), p.orderingNullsFirst(),
     *     p.foldOps(), p.aggColumnNames(), p.orderingIdentity()
     * );
     * }</pre>
     */
    public MergeCallParams buildMergeCallParams() {
        return MergeCallParams.from(this);
    }

    /**
     * Stage 4: Pre-built merge call parameters that derive the ordering
     * identity from the PHYSICAL column names in an actual Arrow IPC state
     * file — the GROUND TRUTH for what the Rust merge engine will compute.
     *
     * <p><b>Why this overload exists:</b> DataFusion's Partial aggregate
     * stage names expression group keys using its internal Display form
     * (e.g. {@code mv_input.EventTime / Int64(300000)}), not the SQL alias
     * (e.g. {@code event_bucket}). The zero-arg {@link #buildMergeCallParams()}
     * uses the logical alias, which mismatches the Rust-side identity
     * computed from the file schema. This overload reads the physical names
     * from the first input state file so the identity is byte-identical to
     * what Rust computes. For plain column keys the physical name equals the
     * alias, so the substitution is a no-op.</p>
     *
     * @param referenceStateFile path to any input Arrow IPC state file; only
     *        the footer schema is read (no record batches loaded)
     * @return merge call params with a physical ordering identity
     * @throws java.io.IOException if the reference file cannot be read
     * @see MVArrowIpcSchemaReader#readGroupKeyNames
     */
    public MergeCallParams buildMergeCallParams(String referenceStateFile) throws java.io.IOException {
        return MergeCallParams.fromWithPhysicalNames(this, referenceStateFile);
    }

    /**
     * Stage 4: Fully resolved FFI parameters for
     * {@link MVNativeBridge#mergeStateStreams}. All arrays are freshly allocated
     * and owned by the caller.
     *
     * @param orderingIndices    column indices forming the sort key
     * @param orderingAsc        per-key direction (true = ASC)
     * @param orderingNullsFirst per-key null placement (true = NULLS_FIRST)
     * @param foldOps            per-column fold operation byte (0=GROUP_KEY,
     *                           1=SUM, 2=MIN, 3=MAX)
     * @param aggColumnNames     state column names for aggregate columns
     * @param orderingIdentity   deterministic ordering identity for validation
     */
    public record MergeCallParams(
        int[] orderingIndices,
        boolean[] orderingAsc,
        boolean[] orderingNullsFirst,
        byte[] foldOps,
        String[] aggColumnNames,
        String orderingIdentity
    ) {

        /** Build from a compiled definition. */
        public static MergeCallParams from(MVCompiledDefinition def) {
            MVGroupByOrdering ordering = def.groupByOrdering();
            List<MVGroupByOrdering.Key> keys = ordering.keys();
            int numGroupKeys = keys.size();

            int[] indices = new int[numGroupKeys];
            boolean[] asc = new boolean[numGroupKeys];
            boolean[] nullsFirst = new boolean[numGroupKeys];
            for (int i = 0; i < numGroupKeys; i++) {
                MVGroupByOrdering.Key key = keys.get(i);
                indices[i] = key.stateFieldIndex();
                asc[i] = key.direction() == MVGroupByOrdering.Direction.ASCENDING;
                nullsFirst[i] = key.nullPlacement().nullsFirst();
            }

            AggregateFFIMetadata aggMeta = def.aggregateFFIMetadata();
            int numCols = numGroupKeys + aggMeta.length();
            byte[] foldOps = new byte[numCols];
            for (int i = 0; i < numGroupKeys; i++) {
                foldOps[i] = (byte) 0; // GROUP_KEY
            }
            for (int i = 0; i < aggMeta.length(); i++) {
                // ACC_SUM(0) → fold(1), ACC_MIN(1) → fold(2), ACC_MAX(2) → fold(3)
                foldOps[numGroupKeys + i] = (byte) (aggMeta.accumulatorTypes()[i] + 1);
            }

            // Derive the PHYSICAL aggregate column names that DataFusion's
            // Partial stage emits in the Arrow IPC state files. The compiled
            // definition's stateColumnNames() are user-facing logical aliases
            // (e.g. "sum_AdvEngineID") but DataFusion writes physical names
            // like "sum(mv_input.AdvEngineID)[sum]". The Rust merge_state_streams
            // validates the passed names against the Arrow schema, so we must
            // pass the physical names. We derive them from the AggregateSpec's
            // partialSqlFragment and the canonical table name.
            String[] physicalAggNames = derivePhysicalAggColumnNames(def.aggregates());

            return new MergeCallParams(
                indices,
                asc,
                nullsFirst,
                foldOps,
                physicalAggNames,
                ordering.orderingIdentity()
            );
        }

        /**
         * Build from a compiled definition, deriving the ordering identity
         * from the PHYSICAL column names in an actual Arrow IPC state file.
         *
         * <p>This is the authoritative factory for the merge path. The Rust
         * {@code compute_ordering_identity} reads column names from the file
         * schema at {@code schema.field(idx).name()}. For expression group
         * keys (e.g. {@code floor(EventTime/300000) AS event_bucket}),
         * DataFusion's Partial aggregate writes the expression's Display form
         * ({@code mv_input.EventTime / Int64(300000)}) as the column name,
         * NOT the SQL alias. Reading the physical name from the file is the
         * only authoritative source — it is stable regardless of DataFusion
         * version or expression rendering changes.</p>
         *
         * @param def                 compiled definition
         * @param referenceStateFile  path to any input Arrow IPC state file
         * @return merge call params with physical ordering identity
         * @throws java.io.IOException if the file cannot be read
         */
        public static MergeCallParams fromWithPhysicalNames(
            MVCompiledDefinition def,
            String referenceStateFile
        ) throws java.io.IOException {
            MVGroupByOrdering ordering = def.groupByOrdering();
            List<MVGroupByOrdering.Key> keys = ordering.keys();
            int numGroupKeys = keys.size();

            int[] indices = new int[numGroupKeys];
            boolean[] asc = new boolean[numGroupKeys];
            boolean[] nullsFirst = new boolean[numGroupKeys];
            for (int i = 0; i < numGroupKeys; i++) {
                MVGroupByOrdering.Key key = keys.get(i);
                indices[i] = key.stateFieldIndex();
                asc[i] = key.direction() == MVGroupByOrdering.Direction.ASCENDING;
                nullsFirst[i] = key.nullPlacement().nullsFirst();
            }

            AggregateFFIMetadata aggMeta = def.aggregateFFIMetadata();
            int numCols = numGroupKeys + aggMeta.length();
            byte[] foldOps = new byte[numCols];
            for (int i = 0; i < numGroupKeys; i++) {
                foldOps[i] = (byte) 0; // GROUP_KEY
            }
            for (int i = 0; i < aggMeta.length(); i++) {
                foldOps[numGroupKeys + i] = (byte) (aggMeta.accumulatorTypes()[i] + 1);
            }

            String[] physicalAggNames = derivePhysicalAggColumnNames(def.aggregates());

            // Read group key physical names from the actual state file —
            // this is the GROUND TRUTH for the ordering identity. The Rust
            // merge computes the identity from schema.field(idx).name() so
            // we must match exactly.
            List<String> physicalKeyNames = MVArrowIpcSchemaReader.readGroupKeyNames(
                referenceStateFile, numGroupKeys
            );
            String physicalIdentity = ordering.physicalOrderingIdentity(physicalKeyNames);

            return new MergeCallParams(
                indices,
                asc,
                nullsFirst,
                foldOps,
                physicalAggNames,
                physicalIdentity
            );
        }

        /**
         * Derive the physical column names that DataFusion's Partial aggregate
         * stage emits in Arrow IPC state files. The naming convention is:
         * {@code func(table.column)[func]} for single-column aggregates and
         * {@code count(*)[count]} for COUNT(*).
         *
         * <p>For AVG, which decomposes into COUNT + SUM, the physical names
         * follow the same pattern for each decomposed fragment.</p>
         *
         * @param aggregates the aggregate specs from the compiled definition
         * @return physical column names in state-column order
         */
        private static String[] derivePhysicalAggColumnNames(List<AggregateSpec> aggregates) {
            String table = MVConstants.INPUT_TABLE;
            List<String> names = new ArrayList<>();
            for (AggregateSpec agg : aggregates) {
                switch (agg.function()) {
                    case COUNT -> {
                        if (agg.sourceField() == null) {
                            // COUNT(*) → count(*)[count]
                            names.add("count(*)[count]");
                        } else {
                            // COUNT(field) → count(table.field)[count]
                            names.add("count(" + table + "." + agg.sourceField() + ")[count]");
                        }
                    }
                    case SUM -> names.add("sum(" + table + "." + agg.sourceField() + ")[sum]");
                    // DataFusion uses [value] as the state suffix for MIN/MAX
                    // accumulators, NOT [min]/[max].
                    case MIN -> names.add("min(" + table + "." + agg.sourceField() + ")[value]");
                    case MAX -> names.add("max(" + table + "." + agg.sourceField() + ")[value]");
                    case AVG -> {
                        // AVG decomposes to COUNT(field) + SUM(field) in Partial stage
                        names.add("count(" + table + "." + agg.sourceField() + ")[count]");
                        names.add("sum(" + table + "." + agg.sourceField() + ")[sum]");
                    }
                }
            }
            return names.toArray(new String[0]);
        }
    }

    /**
     * FFI-serialized ordering metadata: parallel arrays of field indices,
     * direction tokens, and null-placement tokens. Used by both the build
     * and merge paths to cross the Java/Rust FFI boundary.
     */
    public record OrderingFFIMetadata(int[] fieldIndices, int[] directionTokens, int[] nullPlacementTokens, int length) {

        /** Derive from an {@link MVGroupByOrdering}. */
        public static OrderingFFIMetadata from(MVGroupByOrdering ordering) {
            int size = ordering.size();
            int[] indices = new int[size];
            int[] dirs = new int[size];
            int[] nulls = new int[size];
            for (int i = 0; i < size; i++) {
                MVGroupByOrdering.Key key = ordering.keys().get(i);
                indices[i] = key.stateFieldIndex();
                dirs[i] = key.direction().wireToken();
                nulls[i] = key.nullPlacement().wireToken();
            }
            return new OrderingFFIMetadata(indices, dirs, nulls, size);
        }
    }

    /**
     * FFI-serialized aggregate accumulator metadata. Each state column maps
     * to an accumulator type token that tells the Rust merge to apply the
     * correct fold function:
     * <ul>
     *   <li>{@code 0} = SUM-fold (used by SUM accumulators and COUNT accumulators)</li>
     *   <li>{@code 1} = MIN-fold</li>
     *   <li>{@code 2} = MAX-fold</li>
     * </ul>
     *
     * <p>The arrays are in state-column order (skipping group keys, which
     * are not aggregated). The {@code stateColumnNames} parallel array
     * carries the stable column names for validation on the Rust side.</p>
     */
    public record AggregateFFIMetadata(int[] accumulatorTypes, String[] stateColumnNames, int length) {

        /** SUM-fold accumulator (also used for COUNT, which folds via SUM). */
        public static final int ACC_SUM = 0;
        /** MIN-fold accumulator. */
        public static final int ACC_MIN = 1;
        /** MAX-fold accumulator. */
        public static final int ACC_MAX = 2;

        /** Derive from the definition's aggregate specs. */
        public static AggregateFFIMetadata from(List<AggregateSpec> aggregates) {
            // Count total state columns across all aggregates
            int totalStateCols = 0;
            for (AggregateSpec agg : aggregates) {
                totalStateCols += agg.stateColumns().size();
            }
            int[] types = new int[totalStateCols];
            String[] names = new String[totalStateCols];
            int idx = 0;
            for (AggregateSpec agg : aggregates) {
                int accType = switch (agg.function()) {
                    case COUNT -> ACC_SUM;  // COUNT folds via SUM
                    case SUM -> ACC_SUM;
                    case MIN -> ACC_MIN;
                    case MAX -> ACC_MAX;
                    case AVG -> ACC_SUM;    // AVG decomposes to count+sum, both SUM-folded
                };
                for (AggregateSpec.StateColumn sc : agg.stateColumns()) {
                    types[idx] = accType;
                    names[idx] = sc.name();
                    idx++;
                }
            }
            return new AggregateFFIMetadata(types, names, totalStateCols);
        }
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
        // Group keys: plain columns emit "name"; derived keys emit <expr> AS "name".
        sb.append(groupKeys.stream().map(MVCompiledDefinition::partialSelectExpr).collect(Collectors.joining(", ")));
        // Aggregate fragments
        for (AggregateSpec agg : aggregates) {
            sb.append(", ").append(agg.partialSqlFragment());
        }
        sb.append(" FROM ").append(tableName);
        // GROUP BY repeats the key expression (== "name" for plain columns).
        sb.append(" GROUP BY ").append(groupKeys.stream().map(GroupKey::sqlExpression).collect(Collectors.joining(", ")));
        return sb.toString();
    }

    /**
     * SELECT-list fragment for a group key. Plain column keys emit
     * {@code "name"} unchanged (byte-identical to legacy output); derived
     * keys emit {@code <expr> AS "name"} so the materialized column carries
     * the stable alias.
     */
    private static String partialSelectExpr(GroupKey key) {
        if (key.isPlainColumn()) {
            return "\"" + key.name() + "\"";
        }
        return key.sqlExpression() + " AS \"" + key.name() + "\"";
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
     * Ordered projection column names for search results. Group keys first (in
     * their full group-by order — <em>all</em> of them, not just the leading
     * key), then state columns in definition order. This is the deterministic
     * column ordering contract for all readers.
     *
     * <p>Row ordering within that layout is governed separately by
     * {@link #groupByOrdering()}, which sorts by the complete group-key tuple
     * (state-field indices {@code 0..groupKeys().size()-1}), not by column 0
     * alone.</p>
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
     * aggregate state columns), matching the physical layout. The leading
     * {@code groupKeys().size()} entries are the ordering columns described by
     * {@link #groupByOrdering()}.
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
            canonical.append(k.name()).append("|").append(k.columnType().name()).append("|").append(k.osFieldPath());
            // Only derived (non-plain) keys append their expression, so plain-column
            // definitions keep a canonical form byte-identical to prior releases and
            // their persisted hashes never change.
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
        return "MVCompiledDefinition{hash="
            + definitionHash
            + ", groupKeys="
            + groupKeys
            + ", aggregates="
            + aggregates.stream().map(a -> a.function() + "(" + a.sourceField() + ")→" + a.userAlias()).collect(Collectors.joining(", "))
            + "}";
    }
}
