/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import org.opensearch.test.OpenSearchTestCase;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Pure-Java, compare-level unit tests for {@link MVDefinitionValidator}.
 *
 * <p>These tests exercise the fail-closed comparison and the type/hash mapping
 * logic WITHOUT loading the native library — they feed synthetic native result
 * documents to {@link MVDefinitionValidator#compare(MVCompiledDefinition, String)},
 * mirroring the {@code MvBuildResultLayoutTests}/{@code MVFFIResultContractTests}
 * pattern. End-to-end coverage of the real planning FFI
 * ({@code df_mv_validate_definition}) lives in the Rust {@code validate_definition}
 * module tests.</p>
 *
 * <p>The cross-language hash oracle constants below are computed independently
 * (FNV-1a 128-bit, lower 64 bits) from the exact byte streams the Rust
 * {@code compute_ordering_hash_u64} / {@code compute_definition_hash_u64}
 * functions hash, so they double as a Java↔Rust parity check.</p>
 */
public class MVDefinitionValidatorTests extends OpenSearchTestCase {

    // Independent FNV-1a-128 lower-64 oracle constants (see Rust helpers).
    private static final long ORD_HASH_SINGLE_KEY = 0xd0509d0a0c01f8b6L;
    private static final long DEF_HASH_SINGLE_KEY = 0x9a938dc3a9940c9dL;
    private static final long ORD_HASH_THREE_KEY = 0x35a7ee8f125748c7L;
    private static final long DEF_HASH_THREE_KEY = 0xf1a029dc1077a0beL;

    // ── Helpers ──────────────────────────────────────────────────────────

    /** Render a synthetic native validation-result document. */
    private static String nativeText(long schemaHash, long orderingHash, long defHash, List<String[]> fields) {
        StringBuilder sb = new StringBuilder();
        sb.append("schema_hash\t").append(Long.toUnsignedString(schemaHash)).append('\n');
        sb.append("ordering_identity_hash\t").append(Long.toUnsignedString(orderingHash)).append('\n');
        sb.append("definition_hash\t").append(Long.toUnsignedString(defHash)).append('\n');
        for (String[] f : fields) {
            sb.append("field\t").append(f[0]).append('\t').append(f[1]).append('\n');
        }
        return sb.toString();
    }

    /** Build a native result that AGREES with the definition (group keys keep aliases; aggregates get internal names). */
    private static String agreeingNativeText(MVCompiledDefinition def) {
        List<String> names = def.stateColumnNames();
        List<String> toks = MVDefinitionValidator.expectedArrowTokens(def);
        int ng = def.groupKeys().size();
        List<String[]> fields = new ArrayList<>();
        for (int i = 0; i < names.size(); i++) {
            String fname = i < ng ? names.get(i) : "agg(mv_input.x" + i + ")[state]";
            fields.add(new String[] { fname, toks.get(i) });
        }
        return nativeText(
            0x1234_5678L,
            def.groupByOrdering().orderingIdentityHash(),
            def.groupByOrdering().definitionIdentityHash(),
            fields
        );
    }

    /** pull_count_sum: 1 keyword key + COUNT(*) + SUM. */
    private static MVCompiledDefinition pullCountSum() {
        return MVCompiledDefinition.of(
            List.of(GroupKey.of("service", GroupKey.ColumnType.KEYWORD)),
            List.of(AggregateSpec.count("cnt"), AggregateSpec.sum("latency_ms", "lat_sum"))
        );
    }

    // ── Agreement (ok) ───────────────────────────────────────────────────

    public void testCompareAgreesPullCountSum() {
        MVCompiledDefinition def = pullCountSum();
        MVDefinitionValidator.ValidationResult r = MVDefinitionValidator.compare(def, agreeingNativeText(def));
        assertTrue("expected agreement, mismatches=" + r.mismatches(), r.ok());
        assertTrue(r.mismatches().isEmpty());
        assertEquals(def.stateColumnNames().size(), r.nativeStateFields().size());
    }

    public void testCompareAgreesClickbench5mUrl() {
        MVCompiledDefinition def = MVCompiledDefinition.clickbench5mUrl();
        // Sanity: 3 keys + 10 metrics × SUM/MIN/MAX/COUNT = 43 state columns.
        assertEquals(43, def.stateColumnNames().size());
        MVDefinitionValidator.ValidationResult r = MVDefinitionValidator.compare(def, agreeingNativeText(def));
        assertTrue("expected agreement, mismatches=" + r.mismatches(), r.ok());
        assertEquals(43, r.nativeStateFields().size());
    }

    // ── Deliberate drift: descriptor LONG vs source keyword (Utf8) ───────

    public void testDriftLongKeyBackedByKeywordSourceFailsClosed() {
        MVCompiledDefinition def = MVCompiledDefinition.of(
            List.of(GroupKey.of("UserID", GroupKey.ColumnType.LONG)),
            List.of(AggregateSpec.count("cnt"))
        );
        // Engine reports UserID as utf8 (source was actually a keyword).
        List<String[]> fields = List.of(
            new String[] { "UserID", "utf8" },
            new String[] { "count(mv_input)[count]", "int64" }
        );
        String text = nativeText(
            1L,
            def.groupByOrdering().orderingIdentityHash(),
            def.groupByOrdering().definitionIdentityHash(),
            fields
        );
        MVDefinitionValidator.ValidationResult r = MVDefinitionValidator.compare(def, text);
        assertFalse("cross-family drift must fail closed", r.ok());
        assertTrue(
            "mismatch must name the UserID field, got: " + r.mismatches(),
            r.mismatches().stream().anyMatch(m -> m.contains("UserID") && m.contains("integer") && m.contains("string"))
        );
    }

    // ── Ordering / definition hash tamper detection ──────────────────────

    public void testOrderingHashMismatchRejected() {
        MVCompiledDefinition def = pullCountSum();
        List<String> toks = MVDefinitionValidator.expectedArrowTokens(def);
        List<String[]> fields = List.of(
            new String[] { "service", toks.get(0) },
            new String[] { "count(mv_input)[count]", toks.get(1) },
            new String[] { "sum(mv_input.latency_ms)[sum]", toks.get(2) }
        );
        // Tamper the ordering identity hash.
        String text = nativeText(
            1L,
            def.groupByOrdering().orderingIdentityHash() ^ 0xFFFFL,
            def.groupByOrdering().definitionIdentityHash(),
            fields
        );
        MVDefinitionValidator.ValidationResult r = MVDefinitionValidator.compare(def, text);
        assertFalse(r.ok());
        assertTrue(
            "must report ordering identity hash mismatch: " + r.mismatches(),
            r.mismatches().stream().anyMatch(m -> m.contains("ordering identity hash mismatch"))
        );
    }

    public void testDefinitionHashMismatchRejected() {
        MVCompiledDefinition def = pullCountSum();
        List<String> toks = MVDefinitionValidator.expectedArrowTokens(def);
        List<String[]> fields = List.of(
            new String[] { "service", toks.get(0) },
            new String[] { "count(mv_input)[count]", toks.get(1) },
            new String[] { "sum(mv_input.latency_ms)[sum]", toks.get(2) }
        );
        String text = nativeText(
            1L,
            def.groupByOrdering().orderingIdentityHash(),
            def.groupByOrdering().definitionIdentityHash() + 1,
            fields
        );
        MVDefinitionValidator.ValidationResult r = MVDefinitionValidator.compare(def, text);
        assertFalse(r.ok());
        assertTrue(
            "must report definition hash mismatch: " + r.mismatches(),
            r.mismatches().stream().anyMatch(m -> m.contains("definition hash mismatch"))
        );
    }

    // ── Arity + group-key name detection ─────────────────────────────────

    public void testArityMismatchRejected() {
        MVCompiledDefinition def = pullCountSum(); // expects 3 state cols
        List<String[]> fields = List.<String[]>of(new String[] { "service", "utf8" }); // only 1
        String text = nativeText(
            1L,
            def.groupByOrdering().orderingIdentityHash(),
            def.groupByOrdering().definitionIdentityHash(),
            fields
        );
        MVDefinitionValidator.ValidationResult r = MVDefinitionValidator.compare(def, text);
        assertFalse(r.ok());
        assertTrue(
            "must report field count mismatch: " + r.mismatches(),
            r.mismatches().stream().anyMatch(m -> m.contains("state field count mismatch"))
        );
    }

    public void testGroupKeyNameMismatchRejected() {
        MVCompiledDefinition def = pullCountSum();
        List<String> toks = MVDefinitionValidator.expectedArrowTokens(def);
        List<String[]> fields = List.of(
            new String[] { "WRONG_NAME", toks.get(0) },
            new String[] { "count(mv_input)[count]", toks.get(1) },
            new String[] { "sum(mv_input.latency_ms)[sum]", toks.get(2) }
        );
        String text = nativeText(
            1L,
            def.groupByOrdering().orderingIdentityHash(),
            def.groupByOrdering().definitionIdentityHash(),
            fields
        );
        MVDefinitionValidator.ValidationResult r = MVDefinitionValidator.compare(def, text);
        assertFalse(r.ok());
        assertTrue(
            "must name the group key position: " + r.mismatches(),
            r.mismatches().stream().anyMatch(m -> m.contains("group key position 0") && m.contains("service"))
        );
    }

    // ── Width tolerance: MIN/MAX over Int16 stays Int16 (same family) ────

    public void testMinMaxInt16WidthToleratedSameFamily() {
        MVCompiledDefinition def = MVCompiledDefinition.of(
            List.of(GroupKey.of("k0", GroupKey.ColumnType.LONG)),
            List.of(AggregateSpec.min("m0", "m0_min"), AggregateSpec.max("m0", "m0_max"), AggregateSpec.count("cnt"))
        );
        // Engine returns int16 for MIN/MAX (narrower source width) — same integer family.
        List<String[]> fields = List.of(
            new String[] { "k0", "int64" },
            new String[] { "min(mv_input.m0)[value]", "int16" },
            new String[] { "max(mv_input.m0)[value]", "int16" },
            new String[] { "count(mv_input)[count]", "int64" }
        );
        String text = nativeText(
            1L,
            def.groupByOrdering().orderingIdentityHash(),
            def.groupByOrdering().definitionIdentityHash(),
            fields
        );
        MVDefinitionValidator.ValidationResult r = MVDefinitionValidator.compare(def, text);
        assertTrue("integer width difference must be tolerated, mismatches=" + r.mismatches(), r.ok());
    }

    // ── Unparseable native result ────────────────────────────────────────

    public void testUnparseableNativeResultRejected() {
        MVCompiledDefinition def = pullCountSum();
        MVDefinitionValidator.ValidationResult r = MVDefinitionValidator.compare(def, "garbage line without tabs\n");
        assertFalse(r.ok());
        assertTrue(r.mismatches().stream().anyMatch(m -> m.contains("unparseable") || m.contains("unknown record")));
    }

    public void testMissingHashRecordRejected() {
        MVCompiledDefinition def = pullCountSum();
        // Only field records, no hash records.
        String text = "field\tservice\tutf8\n";
        MVDefinitionValidator.ValidationResult r = MVDefinitionValidator.compare(def, text);
        assertFalse(r.ok());
    }

    // ── OS-type → arrow-token mapping (mirrors parquet data-format) ──────

    public void testOsTypeToArrowTokenMapping() {
        assertEquals("int64", MVDefinitionValidator.osTypeToArrowToken("long"));
        assertEquals("int32", MVDefinitionValidator.osTypeToArrowToken("integer"));
        assertEquals("int16", MVDefinitionValidator.osTypeToArrowToken("short"));
        assertEquals("int8", MVDefinitionValidator.osTypeToArrowToken("byte"));
        assertEquals("uint64", MVDefinitionValidator.osTypeToArrowToken("unsigned_long"));
        assertEquals("float64", MVDefinitionValidator.osTypeToArrowToken("double"));
        assertEquals("float32", MVDefinitionValidator.osTypeToArrowToken("float"));
        assertEquals("float16", MVDefinitionValidator.osTypeToArrowToken("half_float"));
        assertEquals("bool", MVDefinitionValidator.osTypeToArrowToken("boolean"));
        assertEquals("timestamp_ms", MVDefinitionValidator.osTypeToArrowToken("date"));
        assertEquals("utf8", MVDefinitionValidator.osTypeToArrowToken("keyword"));
        assertEquals("utf8", MVDefinitionValidator.osTypeToArrowToken("text"));
        assertEquals("utf8", MVDefinitionValidator.osTypeToArrowToken("ip"));
        assertEquals("binary", MVDefinitionValidator.osTypeToArrowToken("binary"));
        assertNull("unmappable type yields null", MVDefinitionValidator.osTypeToArrowToken("geo_point"));
        assertNull(MVDefinitionValidator.osTypeToArrowToken(null));
    }

    public void testBuildSourceSchemaWireOmitsUnmappable() {
        Map<String, String> src = new LinkedHashMap<>();
        src.put("a", "long");
        src.put("b", "keyword");
        src.put("c", "date");
        src.put("d", "geo_point"); // unmappable → omitted
        String wire = MVDefinitionValidator.buildSourceSchemaWire(src);
        assertTrue(wire.contains("a\tint64"));
        assertTrue(wire.contains("b\tutf8"));
        assertTrue(wire.contains("c\ttimestamp_ms"));
        assertFalse("unmappable field must be omitted", wire.contains("d\t"));
    }

    // ── Cross-language hash oracle (Java ↔ Rust parity) ──────────────────

    public void testDefinitionIdentityHashOracleSingleKey() {
        MVCompiledDefinition def = MVCompiledDefinition.of(
            List.of(GroupKey.of("k0", GroupKey.ColumnType.LONG)),
            List.of(AggregateSpec.count("cnt"))
        );
        assertEquals(ORD_HASH_SINGLE_KEY, def.groupByOrdering().orderingIdentityHash());
        assertEquals(DEF_HASH_SINGLE_KEY, def.groupByOrdering().definitionIdentityHash());
    }

    public void testDefinitionIdentityHashOracleThreeKeys() {
        MVCompiledDefinition def = MVCompiledDefinition.of(
            List.of(
                GroupKey.of("k0", GroupKey.ColumnType.LONG),
                GroupKey.of("k1", GroupKey.ColumnType.KEYWORD),
                GroupKey.of("k2", GroupKey.ColumnType.INTEGER)
            ),
            List.of(AggregateSpec.count("cnt"))
        );
        assertEquals(ORD_HASH_THREE_KEY, def.groupByOrdering().orderingIdentityHash());
        assertEquals(DEF_HASH_THREE_KEY, def.groupByOrdering().definitionIdentityHash());
    }

    public void testDefinitionIdentityHashDeterministicAndDistinct() {
        MVCompiledDefinition oneKey = MVCompiledDefinition.of(
            List.of(GroupKey.of("k0", GroupKey.ColumnType.LONG)),
            List.of(AggregateSpec.count("cnt"))
        );
        MVCompiledDefinition twoKey = MVCompiledDefinition.of(
            List.of(GroupKey.of("k0", GroupKey.ColumnType.LONG), GroupKey.of("k1", GroupKey.ColumnType.KEYWORD)),
            List.of(AggregateSpec.count("cnt"))
        );
        assertEquals(oneKey.groupByOrdering().definitionIdentityHash(), oneKey.groupByOrdering().definitionIdentityHash());
        assertNotEquals(oneKey.groupByOrdering().definitionIdentityHash(), twoKey.groupByOrdering().definitionIdentityHash());
    }

    public void testClickbench5mUrlOrderingMatchesThreeKeyOracle() {
        // event_bucket, URL, UserID → indices 0,1,2 all ASC NULLS FIRST.
        MVGroupByOrdering ordering = MVCompiledDefinition.clickbench5mUrl().groupByOrdering();
        assertEquals(ORD_HASH_THREE_KEY, ordering.orderingIdentityHash());
        assertEquals(DEF_HASH_THREE_KEY, ordering.definitionIdentityHash());
    }

    // ── Span key type mapping ────────────────────────────────────────────

    public void testSpanKeyExpectedArrowTokenIsTimestamp() {
        MVCompiledDefinition def = MVCompiledDefinition.of(
            List.of(GroupKey.ofSpan("bucket", 300_000L, "EventTime")),
            List.of(AggregateSpec.count("cnt"))
        );
        List<String> tokens = MVDefinitionValidator.expectedArrowTokens(def);
        assertEquals("timestamp_ms", tokens.get(0));
        assertEquals("int64", tokens.get(1)); // cnt
    }

    public void testSpanKeyValidatesAgainstTimestampNativeField() {
        MVCompiledDefinition def = MVCompiledDefinition.of(
            List.of(GroupKey.ofSpan("bucket", 300_000L, "EventTime")),
            List.of(AggregateSpec.count("cnt"))
        );
        // Engine produces timestamp for the bucket column: should agree (temporal family).
        List<String[]> fields = List.of(
            new String[] { "bucket", "timestamp_ms" },
            new String[] { "count(mv_input)[count]", "int64" }
        );
        String text = nativeText(
            1L,
            def.groupByOrdering().orderingIdentityHash(),
            def.groupByOrdering().definitionIdentityHash(),
            fields
        );
        MVDefinitionValidator.ValidationResult r = MVDefinitionValidator.compare(def, text);
        assertTrue("span key with temporal type must agree, mismatches=" + r.mismatches(), r.ok());
    }

    public void testSpanKeyRejectsIntegerNativeField() {
        MVCompiledDefinition def = MVCompiledDefinition.of(
            List.of(GroupKey.ofSpan("bucket", 300_000L, "EventTime")),
            List.of(AggregateSpec.count("cnt"))
        );
        // Engine produces int64 instead of timestamp — cross-family mismatch.
        List<String[]> fields = List.of(
            new String[] { "bucket", "int64" },
            new String[] { "count(mv_input)[count]", "int64" }
        );
        String text = nativeText(
            1L,
            def.groupByOrdering().orderingIdentityHash(),
            def.groupByOrdering().definitionIdentityHash(),
            fields
        );
        MVDefinitionValidator.ValidationResult r = MVDefinitionValidator.compare(def, text);
        assertFalse("span key against integer native field must fail", r.ok());
        assertTrue(
            "mismatch must report temporal vs integer: " + r.mismatches(),
            r.mismatches().stream().anyMatch(m -> m.contains("temporal") && m.contains("integer"))
        );
    }

    public void testOsTypeToArrowTokenDateIsTimestamp() {
        assertEquals("timestamp_ms", MVDefinitionValidator.osTypeToArrowToken("date"));
        assertEquals("timestamp_ms", MVDefinitionValidator.osTypeToArrowToken("date_nanos"));
    }

    public void testArrowFamilyTimestampIsTemporal() {
        assertEquals("temporal", MVDefinitionValidator.arrowFamily("timestamp_ms"));
        assertEquals("temporal", MVDefinitionValidator.arrowFamily("timestamp_ns"));
    }
}
