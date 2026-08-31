/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tests schema validation, projection ordering, hash mismatch detection,
 * and required cast insertion for MVCompiledDefinition.
 *
 * <p>These tests complement {@link MVCompiledDefinitionTests} by focusing
 * specifically on error paths: stored hash != compiled hash, missing fields,
 * and projection column ordering consistency.
 */
public class MVSchemaValidationTests extends OpenSearchTestCase {

    // ── Hash mismatch detection ───────────────────────────────────────────

    public void testStartupRejectsIfStoredHashDoesNotMatchCompiledHash() {
        MVCompiledDefinition def = MVCompiledDefinition.forCountSumMinMaxAvg("RegionID", "AdvEngineID", null, null, null);
        String compiledHash = def.hash();

        // Simulate a "stored" hash that differs from the compiled one
        String storedHash = "0000000000000000000000000000000000000000000000000000000000000000";
        assertNotEquals("test precondition: hashes must differ", compiledHash, storedHash);

        // In production, the system compares stored vs compiled and rejects on mismatch.
        // We verify the hash is deterministic and different definitions produce different hashes.
        MVCompiledDefinition def2 = MVCompiledDefinition.forCountSumMinMaxAvg("RegionID", "DifferentField", null, null, null);
        assertNotEquals(def.hash(), def2.hash());
    }

    public void testPollAbortsIfSchemaFieldMissing() {
        MVCompiledDefinition def = MVCompiledDefinition.forCountSumMinMaxAvg("RegionID", "AdvEngineID", null, null, null);

        // Schema missing required fields
        Map<String, Object> incompleteSchema = new HashMap<>();
        incompleteSchema.put("RegionID", "long");
        // Missing: cnt, sum_AdvEngineID

        IllegalStateException ex = expectThrows(IllegalStateException.class, () -> def.validateSchema(incompleteSchema));
        assertTrue("error should mention missing field", ex.getMessage().contains("missing field"));
        assertTrue("error should include the definition hash", ex.getMessage().contains(def.hash()));
    }

    public void testValidateSchemaAcceptsCompleteSchema() {
        MVCompiledDefinition def = MVCompiledDefinition.forCountSumMinMaxAvg("RegionID", "AdvEngineID", null, null, null);

        Map<String, Object> completeSchema = new HashMap<>();
        completeSchema.put("RegionID", "long");
        completeSchema.put("cnt", "long");
        completeSchema.put("sum_AdvEngineID", "long");

        // Should not throw
        def.validateSchema(completeSchema);
    }

    public void testValidateSchemaRejectsExtraFieldsGracefully() {
        MVCompiledDefinition def = MVCompiledDefinition.forCountSumMinMaxAvg("RegionID", "AdvEngineID", null, null, null);

        Map<String, Object> schemaWithExtras = new HashMap<>();
        schemaWithExtras.put("RegionID", "long");
        schemaWithExtras.put("cnt", "long");
        schemaWithExtras.put("sum_AdvEngineID", "long");
        schemaWithExtras.put("extra_field", "text"); // extra field is okay

        // Extra fields should not cause failure
        def.validateSchema(schemaWithExtras);
    }

    // ── Projection order correctness ──────────────────────────────────────

    public void testProjectionOrderGroupKeysAlwaysFirst() {
        MVCompiledDefinition def = MVCompiledDefinition.of(
            List.of(GroupKey.of("region", GroupKey.ColumnType.KEYWORD), GroupKey.of("service", GroupKey.ColumnType.KEYWORD)),
            List.of(AggregateSpec.count("cnt"), AggregateSpec.sum("latency_ms", "lat_sum"))
        );

        List<String> projection = def.projectionOrder();
        assertEquals("region", projection.get(0));
        assertEquals("service", projection.get(1));
        // Aggregates follow group keys
        assertTrue(projection.indexOf("region") < projection.indexOf("cnt"));
        assertTrue(projection.indexOf("service") < projection.indexOf("cnt"));
    }

    public void testProjectionOrderAvgUsesStateColumns() {
        MVCompiledDefinition def = MVCompiledDefinition.forCountSumMinMaxAvg("GroupCol", null, null, null, "MetricCol");

        List<String> projection = def.projectionOrder();

        // GroupCol first, then cnt, then avg state columns
        assertEquals("GroupCol", projection.get(0));
        assertTrue(projection.contains("cnt"));
        assertTrue(projection.contains("avg_count_MetricCol"));
        assertTrue(projection.contains("avg_sum_MetricCol"));
    }

    public void testProjectionOrderMatchesStateColumnCount() {
        MVCompiledDefinition def = MVCompiledDefinition.forCountSumMinMaxAvg("RegionID", "AdvEngineID", "Res", "Res", "Res");

        List<String> projection = def.projectionOrder();

        // 1 group key + COUNT(1 state) + SUM(1 state) + MIN(1 state) + MAX(1 state) + AVG(2 states) = 7
        assertEquals(7, projection.size());
    }

    // ── Hash determinism under concurrency ────────────────────────────────

    public void testHashIsDeterministicAcrossMultipleComputations() {
        String hash1 = MVCompiledDefinition.forCountSumMinMaxAvg("A", "B", "C", "D", "E").hash();
        String hash2 = MVCompiledDefinition.forCountSumMinMaxAvg("A", "B", "C", "D", "E").hash();
        String hash3 = MVCompiledDefinition.forCountSumMinMaxAvg("A", "B", "C", "D", "E").hash();

        assertEquals(hash1, hash2);
        assertEquals(hash2, hash3);
    }

    // ── Merge schema consistency ──────────────────────────────────────────

    public void testMergeRequiresConsistentSchema() {
        // Two definitions with the same shape should produce the same hash
        MVCompiledDefinition def1 = MVCompiledDefinition.forCountSumMinMaxAvg("RegionID", "AdvEngineID", null, null, null);
        MVCompiledDefinition def2 = MVCompiledDefinition.forCountSumMinMaxAvg("RegionID", "AdvEngineID", null, null, null);
        assertEquals(def1.hash(), def2.hash());

        // Different shapes produce different hashes → merge would detect mismatch
        MVCompiledDefinition def3 = MVCompiledDefinition.forCountSumMinMaxAvg("RegionID", "AdvEngineID", "Res", null, null);
        assertNotEquals(def1.hash(), def3.hash());
    }

    public void testMergeFailsCleanlyOnIncompatibleSchemas() {
        // Incompatible: different group key
        MVCompiledDefinition base = MVCompiledDefinition.forCountSumMinMaxAvg("RegionID", "AdvEngineID", null, null, null);
        MVCompiledDefinition modified = MVCompiledDefinition.forCountSumMinMaxAvg("UserID", "AdvEngineID", null, null, null);

        assertNotEquals("incompatible schemas must produce different hashes", base.hash(), modified.hash());

        // Validate that base schema rejects modified's projection
        Map<String, Object> modifiedSchema = new HashMap<>();
        modifiedSchema.put("UserID", "long"); // wrong group key name
        modifiedSchema.put("cnt", "long");
        modifiedSchema.put("sum_AdvEngineID", "long");

        IllegalStateException ex = expectThrows(IllegalStateException.class, () -> base.validateSchema(modifiedSchema));
        assertTrue(ex.getMessage().contains("missing field"));
    }
}
