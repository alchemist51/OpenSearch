/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.mv.MVDefinitionDescriptor.AggFunctionToken;
import org.opensearch.mv.MVDefinitionDescriptor.AggregateDescriptor;
import org.opensearch.mv.MVDefinitionDescriptor.GroupKeyDescriptor;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;

/**
 * Stage 1 contract tests for {@link MVDefinitionDescriptor} and the
 * {@link MVCompiledDefinition#toDescriptor()} /
 * {@link MVCompiledDefinition#fromDescriptor(MVDefinitionDescriptor)} round-trip.
 *
 * <p>The central guarantee: for <em>every</em> named definition that
 * {@link MVCompiledDefinition#compiledFor(String)} can produce, exporting to a
 * descriptor, serializing it to XContent, parsing it back, and rebuilding a
 * definition yields a definition that is behaviorally identical — same
 * definition hash, group-by ordering identity, projection order, target
 * mapping, partial SQL, and fold SQL. This is what lets Stage 2 replace the
 * hardcoded {@code compiledFor()} switch with a persisted, PPL-compiled
 * descriptor.</p>
 */
public class MVDefinitionDescriptorTests extends OpenSearchTestCase {

    // ── Equivalence: one test per named definition in compiledFor() ──────

    public void testEquivalencePayments() throws IOException {
        assertRoundTripEquivalent("payments");
    }

    public void testEquivalencePullCountSum() throws IOException {
        assertRoundTripEquivalent("pull_count_sum");
    }

    public void testEquivalencePullCountSumUserId() throws IOException {
        assertRoundTripEquivalent("pull_count_sum_userid");
    }

    public void testEquivalenceClickbenchQ9() throws IOException {
        assertRoundTripEquivalent("clickbench_q9");
    }

    public void testEquivalenceClickbenchQ9Native() throws IOException {
        assertRoundTripEquivalent("clickbench_q9_native");
    }

    public void testEquivalenceClickbench100m() throws IOException {
        assertRoundTripEquivalent("clickbench_100m");
    }

    public void testEquivalenceClickbench5mUrl() throws IOException {
        assertRoundTripEquivalent("clickbench_5m_url");
    }

    public void testEquivalenceHeavyL1() throws IOException {
        assertRoundTripEquivalent("heavy_l1");
    }

    public void testEquivalenceHeavyL2() throws IOException {
        assertRoundTripEquivalent("heavy_l2");
    }

    public void testEquivalenceHeavyL3() throws IOException {
        assertRoundTripEquivalent("heavy_l3");
    }

    /** Belt-and-braces: every registered name at once, so a new name can't slip the net untested. */
    public void testEquivalenceAllRegisteredNames() throws IOException {
        for (String name : MVDefinitionSpec.allNames()) {
            assertRoundTripEquivalent(name);
        }
    }

    // ── XContent byte-stability (exact round-trip) ───────────────────────

    public void testByteStabilityForExpressionKeyDefinition() throws IOException {
        // clickbench_5m_url exercises a derived expression key + SUM/MIN/MAX/COUNT_FIELD.
        assertByteStable(MVCompiledDefinition.compiledFor("clickbench_5m_url").toDescriptor());
    }

    public void testByteStabilityForWideLadderDefinition() throws IOException {
        assertByteStable(MVCompiledDefinition.compiledFor("heavy_l3").toDescriptor());
    }

    public void testByteStabilityForLegacyDefinition() throws IOException {
        // Legacy path exercises COUNT(*) + SUM and KEYWORD group keys (payments).
        assertByteStable(MVCompiledDefinition.compiledFor("payments").toDescriptor());
    }

    public void testByteStabilityWithProvenance() throws IOException {
        MVDefinitionDescriptor base = MVCompiledDefinition.compiledFor("clickbench_100m").toDescriptor();
        MVDefinitionDescriptor withProvenance = MVDefinitionDescriptor.create(
            base.groupKeys(),
            base.aggregates(),
            "ppl",
            "source=hits | stats count() by RegionID",
            base.definitionHash().orElse(null)
        );
        assertByteStable(withProvenance);
    }

    public void testParsedDescriptorEqualsOriginal() throws IOException {
        MVDefinitionDescriptor original = MVCompiledDefinition.compiledFor("clickbench_5m_url").toDescriptor();
        MVDefinitionDescriptor parsed = parse(toJson(original));
        assertEquals(original, parsed);
        assertEquals(original.hashCode(), parsed.hashCode());
    }

    // ── COUNT(*) vs COUNT(field) distinction preserved ───────────────────

    public void testCountStarAndCountFieldAreDistinctAcrossRoundTrip() throws IOException {
        MVDefinitionDescriptor original = MVCompiledDefinition.compiledFor("clickbench_5m_url").toDescriptor();
        // First aggregate group in clickbench_5m_url is SUM; a per-field COUNT (COUNT_FIELD) appears too.
        boolean sawCountField = original.aggregates().stream().anyMatch(a -> a.function() == AggFunctionToken.COUNT_FIELD);
        assertTrue("clickbench_5m_url must contain a per-field COUNT_FIELD aggregate", sawCountField);

        // A legacy definition uses a bare COUNT(*).
        MVDefinitionDescriptor legacy = MVCompiledDefinition.compiledFor("pull_count_sum").toDescriptor();
        AggregateDescriptor countStar = legacy.aggregates().get(0);
        assertEquals(AggFunctionToken.COUNT, countStar.function());
        assertNull("COUNT(*) must not carry a field", countStar.field());

        // Round-trip preserves both distinctions.
        MVCompiledDefinition rebuilt5m = MVCompiledDefinition.fromDescriptor(parse(toJson(original)));
        assertEquals(MVCompiledDefinition.compiledFor("clickbench_5m_url").hash(), rebuilt5m.hash());
        MVCompiledDefinition rebuiltLegacy = MVCompiledDefinition.fromDescriptor(parse(toJson(legacy)));
        assertEquals(MVCompiledDefinition.compiledFor("pull_count_sum").hash(), rebuiltLegacy.hash());
    }

    // ── Provenance metadata round-trips ──────────────────────────────────

    public void testProvenanceMetadataRoundTrips() throws IOException {
        MVDefinitionDescriptor base = MVCompiledDefinition.compiledFor("clickbench_100m").toDescriptor();
        MVDefinitionDescriptor d = MVDefinitionDescriptor.create(
            base.groupKeys(),
            base.aggregates(),
            "ppl",
            "source=hits | stats count() by EventTime, RegionID, OS, CounterID, IsRefresh",
            base.definitionHash().orElse(null)
        );
        MVDefinitionDescriptor parsed = parse(toJson(d));
        assertEquals("ppl", parsed.sourceLanguage().orElse(null));
        assertEquals(d.sourceText().orElse(null), parsed.sourceText().orElse(null));
        // Provenance does not perturb the rebuilt definition.
        assertEquals(MVCompiledDefinition.compiledFor("clickbench_100m").hash(), MVCompiledDefinition.fromDescriptor(parsed).hash());
    }

    // ── Version handling ──────────────────────────────────────────────────

    public void testCurrentVersionIsWrittenAndAccepted() throws IOException {
        MVDefinitionDescriptor d = MVCompiledDefinition.compiledFor("pull_count_sum").toDescriptor();
        assertEquals(MVDefinitionDescriptor.CURRENT_VERSION, d.descriptorVersion());
        assertEquals(1, MVDefinitionDescriptor.CURRENT_VERSION);
        assertTrue(toJson(d).contains("\"descriptor_version\":1"));
    }

    public void testUnknownFutureVersionRejectedOnParse() {
        String json = "{\"descriptor_version\":2,"
            + "\"group_keys\":[{\"name\":\"RegionID\",\"column_type\":\"LONG\"}],"
            + "\"aggregates\":[{\"function\":\"COUNT\",\"alias\":\"cnt\"}]}";
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> parse(json));
        assertTrue(ex.getMessage(), ex.getMessage().contains("unsupported MV descriptor version [2]"));
    }

    public void testMissingVersionRejectedOnParse() {
        String json = "{\"group_keys\":[{\"name\":\"RegionID\",\"column_type\":\"LONG\"}],"
            + "\"aggregates\":[{\"function\":\"COUNT\",\"alias\":\"cnt\"}]}";
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> parse(json));
        assertTrue(ex.getMessage(), ex.getMessage().contains("descriptor_version"));
    }

    // ── Content validation: zero group keys ───────────────────────────────

    public void testZeroGroupKeysRejectedByBuilder() {
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> MVDefinitionDescriptor.of(List.of(), List.of(AggregateDescriptor.count("cnt")))
        );
        assertTrue(ex.getMessage(), ex.getMessage().contains("at least one group key"));
    }

    public void testZeroGroupKeysRejectedOnParse() {
        String json = "{\"descriptor_version\":1,\"group_keys\":[]," + "\"aggregates\":[{\"function\":\"COUNT\",\"alias\":\"cnt\"}]}";
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> parse(json));
        assertTrue(ex.getMessage(), ex.getMessage().contains("at least one group key"));
    }

    public void testZeroAggregatesRejected() {
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> MVDefinitionDescriptor.of(List.of(GroupKeyDescriptor.plain("RegionID", GroupKey.ColumnType.LONG)), List.of())
        );
        assertTrue(ex.getMessage(), ex.getMessage().contains("at least one aggregate"));
    }

    // ── Content validation: duplicate aliases ──────────────────────────────

    public void testDuplicateGroupKeyAliasRejected() {
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> MVDefinitionDescriptor.of(
                List.of(
                    GroupKeyDescriptor.plain("RegionID", GroupKey.ColumnType.LONG),
                    GroupKeyDescriptor.plain("RegionID", GroupKey.ColumnType.LONG)
                ),
                List.of(AggregateDescriptor.count("cnt"))
            )
        );
        assertTrue(ex.getMessage(), ex.getMessage().contains("duplicate output alias [RegionID]"));
    }

    public void testAliasCollisionBetweenGroupKeyAndAggregateRejected() {
        IllegalArgumentException ex = expectThrows(
            IllegalArgumentException.class,
            () -> MVDefinitionDescriptor.of(
                List.of(GroupKeyDescriptor.plain("cnt", GroupKey.ColumnType.LONG)),
                List.of(AggregateDescriptor.count("cnt"))
            )
        );
        assertTrue(ex.getMessage(), ex.getMessage().contains("duplicate output alias [cnt]"));
    }

    // ── Content validation: blank aliases ──────────────────────────────────

    public void testBlankGroupKeyNameRejected() {
        expectThrows(IllegalArgumentException.class, () -> GroupKeyDescriptor.plain("  ", GroupKey.ColumnType.LONG));
    }

    public void testBlankAggregateAliasRejected() {
        expectThrows(IllegalArgumentException.class, () -> AggregateDescriptor.count(" "));
    }

    // ── Content validation: unknown tokens on parse ─────────────────────────

    public void testUnknownAggregateFunctionRejectedOnParse() {
        String json = "{\"descriptor_version\":1,"
            + "\"group_keys\":[{\"name\":\"RegionID\",\"column_type\":\"LONG\"}],"
            + "\"aggregates\":[{\"function\":\"MEDIAN\",\"field\":\"x\",\"alias\":\"m\"}]}";
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> parse(json));
        assertTrue(ex.getMessage(), ex.getMessage().contains("unknown aggregate function [MEDIAN]"));
    }

    public void testUnknownColumnTypeRejectedOnParse() {
        String json = "{\"descriptor_version\":1,"
            + "\"group_keys\":[{\"name\":\"RegionID\",\"column_type\":\"FLOAT\"}],"
            + "\"aggregates\":[{\"function\":\"COUNT\",\"alias\":\"cnt\"}]}";
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> parse(json));
        assertTrue(ex.getMessage(), ex.getMessage().contains("unknown group key column type [FLOAT]"));
    }

    public void testUnknownTopLevelFieldRejectedOnParse() {
        String json = "{\"descriptor_version\":1,\"bogus\":true,"
            + "\"group_keys\":[{\"name\":\"RegionID\",\"column_type\":\"LONG\"}],"
            + "\"aggregates\":[{\"function\":\"COUNT\",\"alias\":\"cnt\"}]}";
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> parse(json));
        assertTrue(ex.getMessage(), ex.getMessage().contains("unknown field [bogus]"));
    }

    public void testCountStarWithFieldRejected() {
        String json = "{\"descriptor_version\":1,"
            + "\"group_keys\":[{\"name\":\"RegionID\",\"column_type\":\"LONG\"}],"
            + "\"aggregates\":[{\"function\":\"COUNT\",\"field\":\"x\",\"alias\":\"cnt\"}]}";
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> parse(json));
        assertTrue(ex.getMessage(), ex.getMessage().contains("must not carry a source field"));
    }

    public void testSumWithoutFieldRejected() {
        String json = "{\"descriptor_version\":1,"
            + "\"group_keys\":[{\"name\":\"RegionID\",\"column_type\":\"LONG\"}],"
            + "\"aggregates\":[{\"function\":\"SUM\",\"alias\":\"s\"}]}";
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> parse(json));
        assertTrue(ex.getMessage(), ex.getMessage().contains("requires a non-blank source field"));
    }

    // ── Integrity-hash validation on load ───────────────────────────────────

    public void testIntegrityHashMismatchRejectedOnLoad() {
        MVDefinitionDescriptor tampered = MVDefinitionDescriptor.create(
            List.of(GroupKeyDescriptor.plain("RegionID", GroupKey.ColumnType.LONG)),
            List.of(AggregateDescriptor.count("cnt"), AggregateDescriptor.sum("AdvEngineID", "sum_AdvEngineID")),
            null,
            null,
            "0000000000000000000000000000000000000000000000000000000000000000"
        );
        IllegalArgumentException ex = expectThrows(IllegalArgumentException.class, () -> MVCompiledDefinition.fromDescriptor(tampered));
        assertTrue(ex.getMessage(), ex.getMessage().contains("integrity check failed"));
    }

    public void testMatchingIntegrityHashAccepted() {
        MVCompiledDefinition original = MVCompiledDefinition.compiledFor("pull_count_sum");
        // toDescriptor() embeds the real hash; fromDescriptor must accept it.
        MVCompiledDefinition rebuilt = MVCompiledDefinition.fromDescriptor(original.toDescriptor());
        assertEquals(original.hash(), rebuilt.hash());
    }

    public void testDescriptorWithoutHashSkipsIntegrityCheck() {
        // A descriptor with no integrity hash still rebuilds (Stage 2 matcher may omit it).
        MVDefinitionDescriptor noHash = MVDefinitionDescriptor.of(
            List.of(GroupKeyDescriptor.plain("RegionID", GroupKey.ColumnType.LONG)),
            List.of(AggregateDescriptor.count("cnt"), AggregateDescriptor.sum("AdvEngineID", "sum_AdvEngineID"))
        );
        assertTrue(noHash.definitionHash().isEmpty());
        MVCompiledDefinition rebuilt = MVCompiledDefinition.fromDescriptor(noHash);
        assertEquals(MVCompiledDefinition.compiledFor("pull_count_sum").hash(), rebuilt.hash());
    }

    // ── Span group key is faithfully described ────────────────────────────

    public void testSpanGroupKeyCarriesIntervalAndSourceColumn() {
        MVDefinitionDescriptor d = MVCompiledDefinition.compiledFor("clickbench_5m_url").toDescriptor();
        GroupKeyDescriptor bucket = d.groupKeys().get(0);
        assertEquals("event_bucket", bucket.name());
        assertEquals(GroupKey.ColumnType.TIMESTAMP, bucket.columnType());
        assertNull("span key must carry no raw expression", bucket.expression());
        assertEquals("EventTime", bucket.sourceColumn());
        assertTrue("span key must carry span interval", bucket.isSpan());
        assertEquals(300_000L, bucket.spanIntervalMs());

        GroupKeyDescriptor plain = d.groupKeys().get(1); // URL
        assertEquals("URL", plain.name());
        assertNull("plain key must carry no expression", plain.expression());
        assertNull("plain key with matching path must carry no source column", plain.sourceColumn());
        assertFalse("plain key must not be span", plain.isSpan());
    }

    public void testSpanDescriptorRoundTrip() throws IOException {
        MVDefinitionDescriptor original = MVDefinitionDescriptor.of(
            List.of(GroupKeyDescriptor.span("bucket", 300_000L, "EventTime")),
            List.of(AggregateDescriptor.count("cnt"))
        );
        MVDefinitionDescriptor parsed = parse(toJson(original));
        assertEquals(original, parsed);
        assertTrue(parsed.groupKeys().get(0).isSpan());
        assertEquals(300_000L, parsed.groupKeys().get(0).spanIntervalMs());
    }

    public void testSpanDescriptorRebuildsToSameDefinition() throws IOException {
        MVCompiledDefinition original = MVCompiledDefinition.of(
            List.of(GroupKey.ofSpan("bucket", 60_000L, "ts")),
            List.of(AggregateSpec.count("cnt"))
        );
        MVDefinitionDescriptor descriptor = original.toDescriptor();
        MVDefinitionDescriptor parsed = parse(toJson(descriptor));
        MVCompiledDefinition rebuilt = MVCompiledDefinition.fromDescriptor(parsed);
        assertEquals(original.hash(), rebuilt.hash());
        assertEquals(original.buildPartialSql("mv_input"), rebuilt.buildPartialSql("mv_input"));
        assertEquals(original.buildFoldSql("state"), rebuilt.buildFoldSql("state"));
    }

    // ── Expression (non-span) group key round-trips ──────────────────────

    public void testExpressionGroupKeyRoundTrips() throws IOException {
        MVCompiledDefinition original = MVCompiledDefinition.of(
            List.of(GroupKey.ofExpression("bucket", GroupKey.ColumnType.LONG, "CAST(\"EventTime\" AS BIGINT) / 300000", "EventTime")),
            List.of(AggregateSpec.count("cnt"))
        );
        MVDefinitionDescriptor d = original.toDescriptor();
        GroupKeyDescriptor bucket = d.groupKeys().get(0);
        assertEquals("CAST(\"EventTime\" AS BIGINT) / 300000", bucket.expression());
        assertEquals("EventTime", bucket.sourceColumn());
        assertFalse("non-span expression key must not be span", bucket.isSpan());

        MVCompiledDefinition rebuilt = MVCompiledDefinition.fromDescriptor(parse(toJson(d)));
        assertEquals(original.hash(), rebuilt.hash());
    }

    // ── Helpers ──────────────────────────────────────────────────────────────

    /**
     * Assert the full descriptor round-trip preserves every derived contract
     * of the named definition.
     */
    private void assertRoundTripEquivalent(String name) throws IOException {
        MVCompiledDefinition original = MVCompiledDefinition.compiledFor(name);
        MVDefinitionDescriptor descriptor = original.toDescriptor();
        MVDefinitionDescriptor parsed = parse(toJson(descriptor));
        MVCompiledDefinition rebuilt = MVCompiledDefinition.fromDescriptor(parsed);

        assertEquals(name + ": definition hash", original.hash(), rebuilt.hash());
        assertEquals(
            name + ": group-by ordering identity",
            original.groupByOrdering().orderingIdentity(),
            rebuilt.groupByOrdering().orderingIdentity()
        );
        assertEquals(name + ": projection order", original.projectionOrder(), rebuilt.projectionOrder());
        assertEquals(name + ": target mapping", original.targetMapping(), rebuilt.targetMapping());
        assertEquals(
            name + ": partial SQL",
            original.buildPartialSql(MVConstants.INPUT_TABLE),
            rebuilt.buildPartialSql(MVConstants.INPUT_TABLE)
        );
        assertEquals(name + ": fold SQL", original.buildFoldSql(MVConstants.INPUT_TABLE), rebuilt.buildFoldSql(MVConstants.INPUT_TABLE));
        assertEquals(name + ": state column names", original.stateColumnNames(), rebuilt.stateColumnNames());
    }

    /** Serialize → parse → serialize and assert the JSON bytes are identical. */
    private void assertByteStable(MVDefinitionDescriptor descriptor) throws IOException {
        String json1 = toJson(descriptor);
        MVDefinitionDescriptor parsed = parse(json1);
        String json2 = toJson(parsed);
        assertEquals(json1, json2);
    }

    private static String toJson(MVDefinitionDescriptor descriptor) throws IOException {
        XContentBuilder builder = XContentFactory.jsonBuilder();
        descriptor.toXContent(builder, ToXContent.EMPTY_PARAMS);
        // toString() flushes and closes the builder via BytesReference#bytes().
        return builder.toString();
    }

    private MVDefinitionDescriptor parse(String json) throws IOException {
        return MVDefinitionDescriptor.fromXContent(createParser(JsonXContent.jsonXContent, json));
    }
}
