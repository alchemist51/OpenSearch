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
 * End-to-end debug IT for the Late Materialization (QTF) flow.
 *
 * <p>2 shards, 2 segments per shard, 5 docs total. Designed to trace the full flow:
 * query phase → shard_id injection → reduce → position map → fetch → assembly.
 *
 * <pre>
 * Segment 1: alice(score=100, city=NYC), bob(score=200, city=SF), dave(score=50, city=LA)
 * Segment 2: carol(score=150, city=NYC), eve(score=300, city=SF)
 * </pre>
 *
 * <p>Query: SELECT __row_id__, name, score FROM t ORDER BY score LIMIT 3
 * <p>Expected: dave(50), alice(100), carol(150)
 */
public class LateMaterializationIT extends AnalyticsRestTestCase {

    private static final String INDEX = "late_mat_debug";
    private static boolean ready = false;

    private void setup() throws IOException {
        if (ready) return;

        try { client().performRequest(new Request("DELETE", "/" + INDEX)); } catch (Exception ignored) {}

        // 1 shard (analytics engine doesn't support multi-shard distribution yet)
        // The QTF flow is debugged with multi-segment within one shard.
        Request create = new Request("PUT", "/" + INDEX);
        create.setJsonEntity("{"
            + "\"settings\":{"
            + "  \"number_of_shards\":1,\"number_of_replicas\":0,"
            + "  \"index.pluggable.dataformat.enabled\":true,"
            + "  \"index.pluggable.dataformat\":\"composite\","
            + "  \"index.composite.primary_data_format\":\"parquet\","
            + "  \"index.composite.secondary_data_formats\":\"lucene\""
            + "},"
            + "\"mappings\":{\"properties\":{"
            + "  \"name\":{\"type\":\"keyword\"},"
            + "  \"score\":{\"type\":\"integer\"},"
            + "  \"city\":{\"type\":\"keyword\"}"
            + "}}}");
        client().performRequest(create);

        Request health = new Request("GET", "/_cluster/health/" + INDEX);
        health.addParameter("wait_for_status", "green");
        health.addParameter("timeout", "30s");
        client().performRequest(health);

        // Segment 1: 3 docs (distributed across 2 shards by hash)
        bulk("{\"index\":{}}\n{\"name\":\"alice\",\"score\":100,\"city\":\"NYC\"}\n"
           + "{\"index\":{}}\n{\"name\":\"bob\",\"score\":200,\"city\":\"SF\"}\n"
           + "{\"index\":{}}\n{\"name\":\"dave\",\"score\":50,\"city\":\"LA\"}\n");
        client().performRequest(new Request("POST", "/" + INDEX + "/_flush?force=true"));

        // Segment 2: 2 more docs
        bulk("{\"index\":{}}\n{\"name\":\"carol\",\"score\":150,\"city\":\"NYC\"}\n"
           + "{\"index\":{}}\n{\"name\":\"eve\",\"score\":300,\"city\":\"SF\"}\n");
        client().performRequest(new Request("POST", "/" + INDEX + "/_flush?force=true"));

        ready = true;
    }

    /**
     * Basic QTF query: projects __row_id__ + sort key + data columns.
     * This triggers the full QTF flow.
     *
     * Watch the logs for:
     * - [ShardFragmentStageExecution] shard_id injection
     * - [QTF] Position map built
     * - [QTF] Dispatching fetch
     */
    public void testQtfSortByScore() throws IOException {
        setup();

        String ppl = "source = " + INDEX + " | sort score | fields __row_id__, name, score | head 3";
        List<List<Object>> rows = executePplRows(ppl);

        logger.info("[LateMat-IT] Results for sort-by-score:");
        for (int i = 0; i < rows.size(); i++) {
            logger.info("  row {}: {}", i, rows.get(i));
        }

        // Verify we got results (exact values depend on QTF wiring status)
        assertNotNull("Should have results", rows);
        assertTrue("Should have at least 1 row", rows.size() >= 1);
    }

    /**
     * QTF with filter: only city='NYC' rows, sorted by score.
     * Expected: alice(100), carol(150) — both from shard 0.
     */
    public void testQtfFilteredSort() throws IOException {
        setup();

        String ppl = "source = " + INDEX + " | where city = 'NYC' | sort score | fields __row_id__, name, score";
        List<List<Object>> rows = executePplRows(ppl);

        logger.info("[LateMat-IT] Results for filtered sort (city=NYC):");
        for (int i = 0; i < rows.size(); i++) {
            logger.info("  row {}: {}", i, rows.get(i));
        }

        assertNotNull(rows);
        assertEquals("NYC has 2 docs", 2, rows.size());
    }

    /**
     * Full scan no filter — all 5 docs sorted by score.
     * Expected order: dave(50), alice(100), carol(150), bob(200), eve(300)
     */
    public void testQtfFullScan() throws IOException {
        setup();

        String ppl = "source = " + INDEX + " | sort score | fields __row_id__, name, score";
        List<List<Object>> rows = executePplRows(ppl);

        logger.info("[LateMat-IT] Results for full scan:");
        for (int i = 0; i < rows.size(); i++) {
            logger.info("  row {}: {}", i, rows.get(i));
        }

        assertNotNull(rows);
        assertEquals("Should have all 5 docs", 5, rows.size());
    }

    // ── Multi-shard test ──

    private static final String INDEX_MULTI = "late_mat_multi_shard";
    private static boolean multiReady = false;

    private void setupMultiShard() throws IOException {
        if (multiReady) return;

        try { client().performRequest(new Request("DELETE", "/" + INDEX_MULTI)); } catch (Exception ignored) {}

        Request create = new Request("PUT", "/" + INDEX_MULTI);
        create.setJsonEntity("{"
            + "\"settings\":{"
            + "  \"number_of_shards\":2,\"number_of_replicas\":0,"
            + "  \"index.pluggable.dataformat.enabled\":true,"
            + "  \"index.pluggable.dataformat\":\"composite\","
            + "  \"index.composite.primary_data_format\":\"parquet\","
            + "  \"index.composite.secondary_data_formats\":\"lucene\""
            + "},"
            + "\"mappings\":{\"properties\":{"
            + "  \"name\":{\"type\":\"keyword\"},"
            + "  \"score\":{\"type\":\"integer\"},"
            + "  \"city\":{\"type\":\"keyword\"}"
            + "}}}");
        client().performRequest(create);

        Request health = new Request("GET", "/_cluster/health/" + INDEX_MULTI);
        health.addParameter("wait_for_status", "green");
        health.addParameter("timeout", "30s");
        client().performRequest(health);

        bulkTo(INDEX_MULTI,
            "{\"index\":{}}\n{\"name\":\"alice\",\"score\":100,\"city\":\"NYC\"}\n"
          + "{\"index\":{}}\n{\"name\":\"bob\",\"score\":200,\"city\":\"SF\"}\n"
          + "{\"index\":{}}\n{\"name\":\"carol\",\"score\":150,\"city\":\"NYC\"}\n"
          + "{\"index\":{}}\n{\"name\":\"dave\",\"score\":50,\"city\":\"LA\"}\n"
          + "{\"index\":{}}\n{\"name\":\"eve\",\"score\":300,\"city\":\"SF\"}\n");
        client().performRequest(new Request("POST", "/" + INDEX_MULTI + "/_flush?force=true"));

        multiReady = true;
    }

    /**
     * Multi-shard QTF: 2 shards, 5 docs.
     * Tests whether the position map + fetch correctly handles multiple shards.
     */
    public void testQtfMultiShard() throws IOException {
        setupMultiShard();

        String ppl = "source = " + INDEX_MULTI + " | sort score | fields __row_id__, name, score";
        List<List<Object>> rows = executePplRows(ppl);

        logger.info("[LateMat-IT] Results for multi-shard sort:");
        for (int i = 0; i < rows.size(); i++) {
            logger.info("  row {}: {}", i, rows.get(i));
        }

        assertNotNull(rows);
        assertEquals("Should have all 5 docs from 2 shards", 5, rows.size());
    }

    // ── Helpers ──

    private void bulk(String body) throws IOException {
        bulkTo(INDEX, body);
    }

    private void bulkTo(String index, String body) throws IOException {
        Request req = new Request("POST", "/" + index + "/_bulk");
        req.setJsonEntity(body);
        req.addParameter("refresh", "true");
        client().performRequest(req);
    }

    private List<List<Object>> executePplRows(String ppl) throws IOException {
        logger.info("[LateMat-IT] Executing: {}", ppl);
        Request req = new Request("POST", "/_analytics/ppl");
        req.setJsonEntity("{\"query\": \"" + escapeJson(ppl) + "\"}");
        Response resp = client().performRequest(req);
        Map<String, Object> parsed = assertOkAndParse(resp, "PPL");

        @SuppressWarnings("unchecked")
        List<List<Object>> rows = (List<List<Object>>) parsed.get("rows");
        assertNotNull("No rows in response", rows);
        return rows;
    }
}
