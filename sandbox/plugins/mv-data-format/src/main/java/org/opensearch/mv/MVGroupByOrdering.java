/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Locale;
import java.util.Objects;
import java.util.stream.Collectors;

/**
 * The authoritative, complete physical GROUP BY ordering contract for a
 * compiled MV definition.
 *
 * <p>An MV state file must be laid out so that its rows are sorted by the
 * <em>full</em> group-by key tuple, in exactly the order the keys appear in
 * the state-field layout ({@link MVCompiledDefinition#stateColumnNames()}).
 * This ordering is what lets the build, merge, and search paths rely on
 * sorted runs for streaming k-way folds and range scans.</p>
 *
 * <p><b>Why a contract, and why now.</b> The current native build/merge paths
 * sort and advertise only the <em>first</em> group column
 * ({@code sort_to_indices(concatenated.column(0), ...)} in {@code mv_poc.rs},
 * and {@code schema.field(0)} in {@code mv_poc.rs}/{@code mv_fold.rs}). That is
 * correct only for single-key definitions; for any definition with two or more
 * group keys it produces state that is <em>not</em> totally ordered — rows
 * sharing {@code key0} but differing in {@code key1}/{@code key2} may appear in
 * arbitrary relative order. This contract captures the complete lexicographic
 * ordering so later stages can replace the column-0-only sort with a true
 * multi-column {@code lexsort} and advertise every ordering column. Stage&nbsp;1
 * only <em>derives and exposes</em> the contract; it changes no runtime sort.</p>
 *
 * <p><b>Ordering semantics.</b> Every key sorts {@link Direction#ASCENDING}
 * with {@link NullPlacement#NULLS_FIRST} null placement. NULLS FIRST matches
 * the historical Arrow {@code sort_to_indices(..., None, None)} default that the
 * existing build path relies on (documented in {@code mv_poc.rs}), so adopting
 * this contract in later stages preserves the current single-key behavior while
 * extending it to all keys.</p>
 *
 * <p><b>Java/Rust crossing (Stage&nbsp;2+).</b> This contract is the single
 * derivation of the ordering — callers must not re-derive it from group keys or
 * SQL. It exposes stable, primitive-friendly metadata designed to serialize
 * across the existing native FFI boundary:</p>
 * <ul>
 *   <li>{@link #stateFieldIndices()} — the zero-based state-field positions to
 *       feed into Arrow {@code lexsort_to_indices} (replacing the hardcoded
 *       {@code column(0)}), in priority order.</li>
 *   <li>{@link #columnNames()} — the ordered state-column names the merge path
 *       ({@code mv_merge_state}) must advertise as its sort key (replacing the
 *       single {@code schema.field(0)} it advertises today).</li>
 *   <li>{@link Direction#wireToken()} / {@link NullPlacement#wireToken()} — small
 *       stable integer tokens per key so direction/null-placement can cross FFI
 *       without string parsing.</li>
 *   <li>{@link #toSqlOrderBy()} — a stable {@code ORDER BY} rendering for any
 *       SQL-driven consumer (e.g. an ordered fold plan), so the clause is
 *       produced in exactly one place.</li>
 * </ul>
 *
 * <p>Instances are immutable and value-based; {@link #equals(Object)},
 * {@link #hashCode()}, and {@link #toString()} are deterministic and depend only
 * on the ordered keys.</p>
 */
public final class MVGroupByOrdering {

    /** Sort direction for an ordering key. */
    public enum Direction {
        /** Ascending order. */
        ASCENDING("ASC", 0);

        private final String sqlToken;
        private final int wireToken;

        Direction(String sqlToken, int wireToken) {
            this.sqlToken = sqlToken;
            this.wireToken = wireToken;
        }

        /** SQL keyword form (e.g. {@code ASC}). */
        public String sqlToken() {
            return sqlToken;
        }

        /** Stable integer token for FFI/native serialization. */
        public int wireToken() {
            return wireToken;
        }
    }

    /** Null placement for an ordering key, compatible with Arrow/DataFusion. */
    public enum NullPlacement {
        /** Nulls sort before non-null values ({@code nulls_first = true}). */
        NULLS_FIRST("NULLS FIRST", 0, true),
        /** Nulls sort after non-null values ({@code nulls_first = false}). */
        NULLS_LAST("NULLS LAST", 1, false);

        private final String sqlToken;
        private final int wireToken;
        private final boolean nullsFirst;

        NullPlacement(String sqlToken, int wireToken, boolean nullsFirst) {
            this.sqlToken = sqlToken;
            this.wireToken = wireToken;
            this.nullsFirst = nullsFirst;
        }

        /** SQL keyword form (e.g. {@code NULLS FIRST}). */
        public String sqlToken() {
            return sqlToken;
        }

        /** Stable integer token for FFI/native serialization. */
        public int wireToken() {
            return wireToken;
        }

        /**
         * Arrow {@code SortOptions.nulls_first} value: {@code true} when nulls
         * sort first. This maps directly to the Arrow/DataFusion sort option so
         * the native side never re-interprets placement.
         */
        public boolean nullsFirst() {
            return nullsFirst;
        }
    }

    /**
     * One key in the ordering contract, in priority order (highest priority
     * first).
     *
     * @param stateFieldIndex zero-based position of this key within the state
     *                        field layout ({@link MVCompiledDefinition#stateColumnNames()});
     *                        this is the column index the native {@code lexsort}
     *                        must reference
     * @param column          stable state-column name / output alias of the key
     * @param sqlExpression   SQL expression that produced the key in the partial
     *                        query (for a plain column this is the quoted
     *                        {@code column}); carried so SQL-driven consumers do
     *                        not re-derive it
     * @param direction       sort direction (always {@link Direction#ASCENDING}
     *                        in the current contract)
     * @param nullPlacement   null placement (always {@link NullPlacement#NULLS_FIRST}
     *                        in the current contract)
     */
    public record Key(int stateFieldIndex, String column, String sqlExpression, Direction direction, NullPlacement nullPlacement) {
        public Key {
            if (stateFieldIndex < 0) {
                throw new IllegalArgumentException("stateFieldIndex must be >= 0, got " + stateFieldIndex);
            }
            Objects.requireNonNull(column, "column");
            Objects.requireNonNull(sqlExpression, "sqlExpression");
            Objects.requireNonNull(direction, "direction");
            Objects.requireNonNull(nullPlacement, "nullPlacement");
        }
    }

    private final List<Key> keys;

    private MVGroupByOrdering(List<Key> keys) {
        this.keys = List.copyOf(keys);
    }

    /**
     * Derive the complete ordering contract from a compiled definition's group
     * keys. This is the single place ordering is derived from a definition; all
     * consumers must go through {@link MVCompiledDefinition#groupByOrdering()}
     * rather than re-deriving from {@code groupKeys()} or SQL.
     *
     * <p>Because group keys always lead the state-field layout (see
     * {@link MVCompiledDefinition#projectionOrder()}), group key {@code i} maps
     * to state-field index {@code i}, and no aggregate-state column is ever part
     * of the ordering.</p>
     */
    static MVGroupByOrdering fromGroupKeys(List<GroupKey> groupKeys) {
        Objects.requireNonNull(groupKeys, "groupKeys");
        List<Key> derived = new ArrayList<>(groupKeys.size());
        for (int i = 0; i < groupKeys.size(); i++) {
            GroupKey gk = groupKeys.get(i);
            derived.add(new Key(i, gk.name(), gk.sqlExpression(), Direction.ASCENDING, NullPlacement.NULLS_FIRST));
        }
        return new MVGroupByOrdering(derived);
    }

    /** Ordering keys in priority order (highest priority first). Unmodifiable. */
    public List<Key> keys() {
        return keys;
    }

    /** Number of ordering keys. */
    public int size() {
        return keys.size();
    }

    /** True when there are no ordering keys. */
    public boolean isEmpty() {
        return keys.isEmpty();
    }

    /**
     * The ordered state-column names forming the sort key. The merge path must
     * advertise <em>all</em> of these (not just the first) as its output
     * ordering.
     */
    public List<String> columnNames() {
        return keys.stream().map(Key::column).collect(Collectors.toUnmodifiableList());
    }

    /**
     * The zero-based state-field positions to sort by, in priority order. This
     * is the direct replacement for the hardcoded {@code column(0)} index in the
     * native build path: feed this list into Arrow {@code lexsort_to_indices}.
     */
    public List<Integer> stateFieldIndices() {
        return keys.stream().map(Key::stateFieldIndex).collect(Collectors.toUnmodifiableList());
    }

    /**
     * Render the ordering as a stable SQL {@code ORDER BY} column list (without
     * the leading {@code ORDER BY} keyword), e.g.
     * {@code "event_bucket" ASC NULLS FIRST, "URL" ASC NULLS FIRST}. Produced in
     * exactly one place so no caller hand-builds the clause. Returns an empty
     * string when there are no keys.
     */
    public String toSqlOrderBy() {
        return keys.stream()
            .map(k -> "\"" + k.column() + "\" " + k.direction().sqlToken() + " " + k.nullPlacement().sqlToken())
            .collect(Collectors.joining(", "));
    }

    /**
     * Stage 4: Returns a stable identity string for this ordering contract
     * that can be passed across the FFI boundary for merge-time validation.
     * The Rust merge_state_streams implementation compares the ordering
     * identity of all input state files against the expected ordering to
     * reject schema-drifted inputs early.
     *
     * <p>The identity is deterministic: same ordered key list always produces
     * the same string. Format:
     * {@code "idx0:col0:dir0:null0;idx1:col1:dir1:null1;..."}</p>
     *
     * <p><b>IMPORTANT — alias vs physical names:</b> This method uses the
     * <em>logical alias</em> of each group key (e.g. {@code event_bucket}).
     * For merge-time validation against actual state files, use
     * {@link #physicalOrderingIdentity(java.util.List)} instead, which
     * substitutes the PHYSICAL column names that DataFusion's Partial
     * aggregate stage writes into the Arrow IPC schema.</p>
     */
    public String orderingIdentity() {
        return keys.stream()
            .map(
                k -> k.stateFieldIndex()
                    + ":"
                    + k.column()
                    + ":"
                    + k.direction().wireToken()
                    + ":"
                    + k.nullPlacement().wireToken()
            )
            .collect(Collectors.joining(";"));
    }

    /**
     * Stage 4: Returns a physical ordering identity string that uses the
     * ACTUAL column names from the Arrow IPC state files instead of the
     * logical aliases.
     *
     * <p>DataFusion's Partial aggregate stage names its output columns using
     * the physical expression Display form (e.g.
     * {@code mv_input.EventTime / Int64(300000)}), not the SQL alias (e.g.
     * {@code event_bucket}). The Rust {@code compute_ordering_identity}
     * reads these physical names from the file schema. This method produces
     * an identity that matches the Rust computation by substituting the
     * physical group key names read from an actual state file.</p>
     *
     * <p>For plain column keys the physical name equals the alias, so the
     * substitution is a no-op. For expression keys the physical name is
     * DataFusion's internal rendering of the expression, which can vary
     * across DataFusion versions — reading it from the file is the only
     * authoritative source.</p>
     *
     * @param physicalGroupKeyNames the physical column names for each group
     *        key position, read from an actual Parquet state file's schema
     *        (e.g. via {@link MVStateSchemaReader#readGroupKeyNames})
     * @return ordering identity matching the Rust {@code compute_ordering_identity}
     * @throws IllegalArgumentException if the list size does not match the
     *         number of ordering keys
     */
    public String physicalOrderingIdentity(java.util.List<String> physicalGroupKeyNames) {
        if (physicalGroupKeyNames.size() != keys.size()) {
            throw new IllegalArgumentException(
                "physicalGroupKeyNames size (" + physicalGroupKeyNames.size()
                    + ") does not match ordering key count (" + keys.size() + ")"
            );
        }
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < keys.size(); i++) {
            if (i > 0) {
                sb.append(';');
            }
            Key k = keys.get(i);
            sb.append(k.stateFieldIndex())
                .append(':')
                .append(physicalGroupKeyNames.get(i))
                .append(':')
                .append(k.direction().wireToken())
                .append(':')
                .append(k.nullPlacement().wireToken());
        }
        return sb.toString();
    }

    /**
     * Stage 4: Validates that another ordering's identity matches this one.
     * Used by the merge path to ensure all input state files share the same
     * ordering contract before attempting a streaming merge.
     *
     * @param other the ordering to validate against
     * @throws IllegalStateException if the orderings do not match
     */
    public void validateCompatible(MVGroupByOrdering other) {
        Objects.requireNonNull(other, "other");
        if (this.equals(other) == false) {
            throw new IllegalStateException(
                String.format(
                    Locale.ROOT,
                    "MV ordering mismatch: expected identity [%s] but got [%s]",
                    this.orderingIdentity(),
                    other.orderingIdentity()
                )
            );
        }
    }

    /**
     * Compute a deterministic u64 hash of the ordering contract identity,
     * matching the Rust {@code compute_ordering_hash_u64()} function exactly.
     *
     * <p>Serialization format (byte-for-byte matching Rust):
     * <ul>
     *   <li>For each key: {@code field_index} as u32 little-endian (4 bytes),
     *       ASC flag byte (1 if direction==0 else 0),
     *       NULLS_FIRST flag byte (1 if nullPlacement==0 else 0)</li>
     *   <li>Trailer: key count as u32 little-endian (4 bytes)</li>
     * </ul>
     * Hashed with FNV-1a 128-bit, returns lower 64 bits.
     *
     * @return the u64 ordering identity hash matching the native computation
     */
    public long orderingIdentityHash() {
        // 6 bytes per key + 4 bytes trailer
        byte[] bytes = new byte[keys.size() * 6 + 4];
        int pos = 0;
        for (Key key : keys) {
            int idx = key.stateFieldIndex();
            bytes[pos++] = (byte) (idx);
            bytes[pos++] = (byte) (idx >>> 8);
            bytes[pos++] = (byte) (idx >>> 16);
            bytes[pos++] = (byte) (idx >>> 24);
            bytes[pos++] = (byte) (key.direction().wireToken() == 0 ? 1 : 0);
            bytes[pos++] = (byte) (key.nullPlacement().wireToken() == 0 ? 1 : 0);
        }
        int count = keys.size();
        bytes[pos++] = (byte) (count);
        bytes[pos++] = (byte) (count >>> 8);
        bytes[pos++] = (byte) (count >>> 16);
        bytes[pos++] = (byte) (count >>> 24);
        return stableFnv1a128Lower64(bytes);
    }

    /**
     * Compute a deterministic u64 hash of the ordering-contract definition,
     * matching the Rust {@code compute_definition_hash_u64()} function exactly.
     *
     * <p>Serialization format (byte-for-byte matching Rust):
     * <ul>
     *   <li>For each key, in order: {@code field_index} as u32 little-endian
     *       (4 bytes), {@code direction} wire token as u32 little-endian
     *       (4 bytes), {@code nullPlacement} wire token as u32 little-endian
     *       (4 bytes)</li>
     *   <li><b>No</b> trailing key count (unlike
     *       {@link #orderingIdentityHash()})</li>
     * </ul>
     * Hashed with FNV-1a 128-bit, returns lower 64 bits.
     *
     * <p>This is the Java mirror of the {@code definition_hash} field returned
     * by the native {@code df_mv_validate_definition} cross-check, so
     * {@link MVDefinitionValidator} can fail closed on any disagreement between
     * the descriptor-derived contract and what the engine physically produces.
     *
     * @return the u64 definition identity hash matching the native computation
     */
    public long definitionIdentityHash() {
        // 12 bytes per key (three u32 LE), no trailer.
        byte[] bytes = new byte[keys.size() * 12];
        int pos = 0;
        for (Key key : keys) {
            pos = putIntLE(bytes, pos, key.stateFieldIndex());
            pos = putIntLE(bytes, pos, key.direction().wireToken());
            pos = putIntLE(bytes, pos, key.nullPlacement().wireToken());
        }
        return stableFnv1a128Lower64(bytes);
    }

    /** Write {@code v} as a little-endian u32 at {@code pos}; return the next position. */
    private static int putIntLE(byte[] b, int pos, int v) {
        b[pos] = (byte) (v);
        b[pos + 1] = (byte) (v >>> 8);
        b[pos + 2] = (byte) (v >>> 16);
        b[pos + 3] = (byte) (v >>> 24);
        return pos + 4;
    }

    /**
     * FNV-1a 128-bit hash returning the lower 64 bits. Matches the Rust
     * {@code stable_hash_u64()} function exactly (same offset basis and prime).
     */
    static long stableFnv1a128Lower64(byte[] data) {
        // FNV-128 offset basis: 0x6c62272e07bb0142_62b821756295c58d
        long hi = 0x6c62272e07bb0142L;
        long lo = 0x62b821756295c58dL;
        // FNV-128 prime: 0x0000000001000000_000000000000013B
        final long pHi = 0x0000000001000000L;
        final long pLo = 0x000000000000013BL;

        for (byte b : data) {
            // XOR low byte
            lo ^= (b & 0xFFL);
            // 128-bit multiply: (hi:lo) * (pHi:pLo) mod 2^128
            // unsignedMultiplyHigh(a, b) = Math.multiplyHigh(a,b) + ((a>>63)&b) + ((b>>63)&a)
            long umh = Math.multiplyHigh(lo, pLo) + ((lo >> 63) & pLo) + ((pLo >> 63) & lo);
            long newLo = lo * pLo;
            long newHi = hi * pLo + lo * pHi + umh;
            lo = newLo;
            hi = newHi;
        }
        return lo;
    }

    /**
     * Stage 4: Returns raw FFI-ready arrays for the ordering contract, suitable
     * for passing directly to native merge/build functions. This is a
     * convenience that avoids callers needing to loop over {@link #keys()} to
     * build the arrays themselves.
     *
     * @return a record containing the three parallel arrays and their length
     */
    public FFIArrays toFFIArrays() {
        int size = keys.size();
        int[] indices = new int[size];
        int[] dirs = new int[size];
        int[] nulls = new int[size];
        for (int i = 0; i < size; i++) {
            Key key = keys.get(i);
            indices[i] = key.stateFieldIndex();
            dirs[i] = key.direction().wireToken();
            nulls[i] = key.nullPlacement().wireToken();
        }
        return new FFIArrays(indices, dirs, nulls, size);
    }

    /**
     * Stage 4: Raw FFI-ready arrays extracted from the ordering contract.
     *
     * @param indices     state-field indices to sort by, in priority order
     * @param directions  direction wire tokens per key (0=ASC)
     * @param nulls       null-placement wire tokens per key (0=NULLS_FIRST)
     * @param length      number of ordering keys
     */
    public record FFIArrays(int[] indices, int[] directions, int[] nulls, int length) {}

    @Override
    public boolean equals(Object o) {
        if (this == o) {
            return true;
        }
        if ((o instanceof MVGroupByOrdering) == false) {
            return false;
        }
        return keys.equals(((MVGroupByOrdering) o).keys);
    }

    @Override
    public int hashCode() {
        return keys.hashCode();
    }

    @Override
    public String toString() {
        return String.format(Locale.ROOT, "MVGroupByOrdering{%s}", Collections.unmodifiableList(keys));
    }
}
