/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import org.opensearch.client.Request;
import org.opensearch.client.Response;

import java.io.IOException;
import java.util.List;
import java.util.Map;

/**
 * End-to-end test for the COUNT_DELEGATION fast path.
 * <p>
 * Indexes data across multiple Lucene segments (force-flush between waves so each wave
 * becomes its own segment), then runs PPL {@code stats count()} queries with various
 * predicate shapes and asserts that the returned counts match the oracle the test
 * computed locally.
 * <p>
 * Multi-segment is critical: with one segment the
 * {@code FilterTreeCallbacks.countDocs} upcall fires once; with N segments the
 * {@code SegmentGrouped} partitioner distributes whole segments across DataFusion
 * partitions, so the test exercises the parallel summation behavior we actually care
 * about in production. The whole-segment chunk shape is what unlocks Lucene's
 * {@code Weight.count(LeafReaderContext)} metadata fast path on the data node.
 * <p>
 * Run with:
 * {@code ./gradlew :sandbox:qa:analytics-engine-rest:integTest --tests "*.CountDelegationIT" -Dsandbox.enabled=true}
 */
public class CountDelegationIT extends AnalyticsRestTestCase {

    private static final String INDEX = "count_delegation_e2e";

    /**
     * Per-userID truth table. Total docs across all segments must equal sum(values).
     * 'dave' is deliberately omitted from ingestion to assert the zero-match path;
     * 'carol_only_3rd' appears only in the third segment to validate that
     * SegmentGrouped doesn't drop or duplicate per-segment counts when most segments
     * contribute zero matches.
     */
    private static final Map<String, Integer> USER_COUNTS = Map.of("arpit", 7, "bob", 4, "carol", 5, "carol_only_3rd", 3);

    public void testCountAcrossMultipleSegments() throws Exception {
        createIndex();
        ingestThreeSegments();

        long total = USER_COUNTS.values().stream().mapToLong(Integer::longValue).sum();

        // Unfiltered count: no Lucene predicate, so the planner shouldn't pick
        // COUNT_DELEGATION here. Validates the bitmap path's count answer matches
        // the oracle.
        assertCount("stats count() as cnt", total);

        // Canonical COUNT_DELEGATION shape: a single index_filter (TermQuery on a
        // keyword field) with a count(*) aggregate, no projection. Each segment
        // hits Weight.count(leaf) → docFreq from the term dictionary.
        // 'arpit' spans all three segments: 3 + 3 + 1 = 7.
        assertCount("where userID = 'arpit' | stats count() as cnt", USER_COUNTS.get("arpit"));

        // Zero-match path: 'dave' isn't in any segment → Weight.count returns 0
        // per leaf, total is 0.
        assertCount("where userID = 'dave' | stats count() as cnt", 0);

        // Single-segment-only path: 'carol_only_3rd' appears only in segment 2
        // (3 docs). Tests that SegmentGrouped doesn't drop or duplicate per-segment
        // counts when most segments contribute zero matches.
        assertCount("where userID = 'carol_only_3rd' | stats count() as cnt", USER_COUNTS.get("carol_only_3rd"));

        // Full-coverage predicates: every doc has exactly one of these event_type
        // values, so clicks + views must equal totalDocs. Exercises the path where
        // Weight.count returns numDocs equivalent and per-segment sums add up.
        long clicks = countOf("where event_type = 'click' | stats count() as cnt");
        long views = countOf("where event_type = 'view' | stats count() as cnt");
        assertEquals(
            "clicks + views must equal total docs (every event has one type)",
            total,
            clicks + views
        );
    }

    // ── Setup ───────────────────────────────────────────────────────────────

    private void createIndex() throws Exception {
        try {
            client().performRequest(new Request("DELETE", "/" + INDEX));
        } catch (Exception ignored) {}

        String body = "{"
            + "\"settings\": {"
            + "  \"number_of_shards\": 1,"
            + "  \"number_of_replicas\": 0,"
            + "  \"index.pluggable.dataformat.enabled\": true,"
            + "  \"index.pluggable.dataformat\": \"composite\","
            + "  \"index.composite.primary_data_format\": \"parquet\","
            + "  \"index.composite.secondary_data_formats\": \"lucene\""
            + "},"
            + "\"mappings\": {"
            + "  \"properties\": {"
            + "    \"userID\": { \"type\": \"keyword\" },"
            + "    \"event_type\": { \"type\": \"keyword\" },"
            + "    \"amount\": { \"type\": \"long\" }"
            + "  }"
            + "}"
            + "}";
        Request req = new Request("PUT", "/" + INDEX);
        req.setJsonEntity(body);
        Map<String, Object> response = assertOkAndParse(client().performRequest(req), "Create index");
        assertEquals(true, response.get("acknowledged"));

        Request health = new Request("GET", "/_cluster/health/" + INDEX);
        health.addParameter("wait_for_status", "green");
        health.addParameter("timeout", "30s");
        client().performRequest(health);
    }

    /**
     * Ingest in three waves with force-flush between each wave so each wave becomes
     * its own Lucene segment. Per-userID totals match {@link #USER_COUNTS}.
     *
     * <pre>
     *   segment 0: arpit×3, bob×2, carol×2          (7 docs)
     *   segment 1: arpit×3, bob×2, carol×3          (8 docs)
     *   segment 2: arpit×1, carol_only_3rd×3        (4 docs)
     * </pre>
     *
     * Total = 19 docs spread across 3 segments. Forces SegmentGrouped to bucket
     * across multiple partitions and exercises the Weight.count fast path per
     * segment when COUNT_DELEGATION fires.
     */
    private void ingestThreeSegments() throws Exception {
        bulkIndex(
            docs(
                doc("arpit", "click", 10),
                doc("arpit", "view", 20),
                doc("arpit", "click", 30),
                doc("bob", "view", 40),
                doc("bob", "click", 50),
                doc("carol", "view", 60),
                doc("carol", "click", 70)
            )
        );
        flush();

        bulkIndex(
            docs(
                doc("arpit", "view", 11),
                doc("arpit", "click", 21),
                doc("arpit", "view", 31),
                doc("bob", "click", 41),
                doc("bob", "view", 51),
                doc("carol", "click", 61),
                doc("carol", "view", 71),
                doc("carol", "click", 81)
            )
        );
        flush();

        bulkIndex(
            docs(
                doc("arpit", "view", 12),
                doc("carol_only_3rd", "click", 22),
                doc("carol_only_3rd", "view", 32),
                doc("carol_only_3rd", "click", 42)
            )
        );
        flush();
    }

    // ── Document builders ───────────────────────────────────────────────────

    private static String doc(String userID, String eventType, long amount) {
        return "{\"userID\": \"" + userID + "\", \"event_type\": \"" + eventType + "\", \"amount\": " + amount + "}";
    }

    private static String docs(String... documents) {
        StringBuilder sb = new StringBuilder();
        for (String d : documents) {
            sb.append("{\"index\": {}}\n").append(d).append("\n");
        }
        return sb.toString();
    }

    private void bulkIndex(String ndjson) throws Exception {
        Request req = new Request("POST", "/" + INDEX + "/_bulk");
        req.setJsonEntity(ndjson);
        req.addParameter("refresh", "true");
        req.setOptions(req.getOptions().toBuilder().addHeader("Content-Type", "application/x-ndjson").build());
        Map<String, Object> response = assertOkAndParse(client().performRequest(req), "Bulk index");
        assertEquals("Bulk indexing should have no errors", false, response.get("errors"));
    }

    private void flush() throws Exception {
        client().performRequest(new Request("POST", "/" + INDEX + "/_flush?force=true"));
    }

    // ── PPL helpers ─────────────────────────────────────────────────────────

    private Map<String, Object> executePPL(String ppl) throws IOException {
        Request req = new Request("POST", "/_analytics/ppl");
        req.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\"}");
        Response response = client().performRequest(req);
        return assertOkAndParse(response, "PPL: " + ppl);
    }

    private long countOf(String pplSuffix) throws IOException {
        String ppl = "source = " + INDEX + " | " + pplSuffix;
        Map<String, Object> result = executePPL(ppl);
        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) result.get("rows");
        assertNotNull("Response missing 'rows' for: " + ppl, rows);
        assertEquals("Expected 1 row for count query: " + ppl, 1, rows.size());
        return ((Number) rows.get(0).get(0)).longValue();
    }

    private void assertCount(String pplSuffix, long expected) throws IOException {
        String ppl = "source = " + INDEX + " | " + pplSuffix;
        long actual = countOf(pplSuffix);
        assertEquals("Count mismatch for: " + ppl, expected, actual);
    }
}
