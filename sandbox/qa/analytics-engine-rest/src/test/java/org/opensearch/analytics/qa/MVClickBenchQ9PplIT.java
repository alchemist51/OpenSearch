/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa;

import org.opensearch.client.Request;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * ClickBench q9 through the REAL {@code /_plugins/_ppl} API, answered from a
 * separate-index materialized view — the full production stack end to end:
 *
 * <pre>
 * ingest into SOURCE (composite parquet+lucene+materialized_view)
 *   -> refresh-time state build (definition's Partial over the flushed parquet)
 *   -> ship-before-commit to TARGET (composite parquet+lucene+mv_state fold)
 *   -> PPL fold query over the TARGET's state docs  ==  PPL direct q9 over the SOURCE
 * </pre>
 *
 * <p>The fold works as plain PPL because MV definitions are pre-decomposed to
 * their mergeable core: {@code AVG(ResolutionWidth) = SUM(res_sum)/SUM(cnt)}
 * exactly — no approximation, any generation/merge state.
 *
 * <p>The direct query is the oracle: both queries run through the same
 * opensearch-sql -> Calcite -> analytics-engine -> DataFusion path, only the
 * scanned index differs (N state rows on the target vs all docs on the source).
 */
public class MVClickBenchQ9PplIT extends AnalyticsRestTestCase {

    private static final String SOURCE = "hits_mv";
    private static final String TARGET = "mv_hits_q9";

    /** Batches ingested with a refresh (= generation + ship) after each. */
    private static final int BATCHES = 3;

    public void testQ9FoldOverMVEqualsDirect() throws Exception {
        provision();

        // Direct q9 over the source (oracle). ORDER BY count DESC, then the
        // RegionID tiebreak keeps row order deterministic.
        Map<String, Object> direct = executePpl(
            "source=" + SOURCE + " | stats sum(AdvEngineID) as sum_adv, count() as c, avg(ResolutionWidth) as avg_res by RegionID"
                + " | sort - c, RegionID | head 10"
        );

        // q9 from the MV: fold over the target's shipped state docs.
        Map<String, Object> fold = executePpl(
            "source=" + TARGET + " | stats sum(adv_sum) as sum_adv, sum(cnt) as c, sum(res_sum) as rs by RegionID"
                + " | sort - c, RegionID | head 10"
        );

        Map<Long, double[]> directRows = rowsByRegion(direct, false);
        Map<Long, double[]> foldRows = rowsByRegion(fold, true);

        assertFalse("direct q9 returned no rows", directRows.isEmpty());
        assertEquals("group sets must match", directRows.keySet(), foldRows.keySet());
        for (Map.Entry<Long, double[]> e : directRows.entrySet()) {
            double[] d = e.getValue();
            double[] f = foldRows.get(e.getKey());
            assertEquals("sum_adv for region " + e.getKey(), d[0], f[0], 0.0001);
            assertEquals("count for region " + e.getKey(), d[1], f[1], 0.0001);
            assertEquals("avg_res for region " + e.getKey(), d[2], f[2], 0.0001);
        }
    }

    /**
     * Extracts {@code region -> [sum_adv, count, avg_res]} from a PPL response.
     * For the fold response ({@code sum_adv, c, rs}) the average is computed as
     * {@code rs / c} — the exact state algebra the read side performs.
     */
    @SuppressWarnings("unchecked")
    private static Map<Long, double[]> rowsByRegion(Map<String, Object> response, boolean foldShape) {
        List<String> cols = extractColumnNames(response);
        List<List<Object>> datarows = (List<List<Object>>) (List<?>) response.get("datarows");
        int region = cols.indexOf("RegionID");
        int sumAdv = cols.indexOf("sum_adv");
        int cnt = cols.indexOf("c");
        int third = foldShape ? cols.indexOf("rs") : cols.indexOf("avg_res");
        assertTrue("expected columns in " + cols, region >= 0 && sumAdv >= 0 && cnt >= 0 && third >= 0);
        Map<Long, double[]> out = new HashMap<>();
        for (List<Object> row : datarows) {
            double c = ((Number) row.get(cnt)).doubleValue();
            double thirdVal = ((Number) row.get(third)).doubleValue();
            out.put(
                ((Number) row.get(region)).longValue(),
                new double[] { ((Number) row.get(sumAdv)).doubleValue(), c, foldShape ? thirdVal / c : thirdVal }
            );
        }
        return out;
    }

    private void provision() throws Exception {
        for (String idx : new String[] { SOURCE, TARGET }) {
            try {
                client().performRequest(new Request("DELETE", "/" + idx));
            } catch (Exception ignored) {}
        }

        // 1. SOURCE: composite parquet+lucene+materialized_view, q9 definition,
        //    shipping state to the target before every commit.
        Request createSource = new Request("PUT", "/" + SOURCE);
        createSource.setJsonEntity(
            "{"
                + "\"settings\": {"
                + "  \"number_of_shards\": 1,"
                + "  \"number_of_replicas\": 0,"
                + "  \"index.pluggable.dataformat.enabled\": true,"
                + "  \"index.pluggable.dataformat\": \"composite\","
                + "  \"index.composite.primary_data_format\": \"parquet\","
                + "  \"index.composite.secondary_data_formats\": [\"lucene\", \"materialized_view\"],"
                + "  \"index.mv.definition\": \"clickbench_q9\","
                + "  \"index.mv.ship_targets\": [\"" + TARGET + "\"]"
                + "},"
                // integer (not long): PPL sum(long) lowers to CHECKED_LONG_SUM,
                // which has no substrait binding yet (same YAML gap family as
                // the boolean min/max case — see MinMaxBooleanAggregationIT).
                // Every qa IT sums integer fields; the MV capture is INT64
                // either way (spec columns), so nothing MV-side changes.
                + "\"mappings\": { \"dynamic\": \"false\", \"properties\": {"
                + "  \"RegionID\":        { \"type\": \"integer\" },"
                + "  \"AdvEngineID\":     { \"type\": \"integer\" },"
                + "  \"ResolutionWidth\": { \"type\": \"integer\" }"
                + "}}"
                + "}"
        );
        assertEquals(Boolean.TRUE, assertOkAndParse(client().performRequest(createSource), "create " + SOURCE).get("acknowledged"));

        // 2. TARGET: composite whose mv_state format is the FOLD of the shipped
        //    state schema; primary colocated with the source primary.
        Request createTarget = new Request("PUT", "/" + TARGET);
        createTarget.setJsonEntity(
            "{"
                + "\"settings\": {"
                + "  \"number_of_shards\": 1,"
                + "  \"number_of_replicas\": 0,"
                + "  \"index.pluggable.dataformat.enabled\": true,"
                + "  \"index.pluggable.dataformat\": \"composite\","
                + "  \"index.composite.primary_data_format\": \"parquet\","
                + "  \"index.composite.secondary_data_formats\": [\"lucene\"],"
                + "  \"index.derived.data_format\": \"materialized_view\","
                + "  \"index.mv.definition\": \"clickbench_q9\","
                + "  \"index.mv.colocate_with\": \"" + SOURCE + "\""
                + "},"
                // dynamic:false + explicit provenance fields: the ship handler
                // writes _mv_source_* on every state doc (re-ship idempotency);
                // the composite apply path can't do dynamic mapping updates
                // (MAPPING_UPDATE_REQUIRED fails the ship, and the flush).
                + "\"mappings\": { \"dynamic\": \"false\", \"properties\": {"
                + "  \"RegionID\": { \"type\": \"integer\" },"
                + "  \"cnt\":      { \"type\": \"integer\" },"
                + "  \"adv_sum\":  { \"type\": \"integer\" },"
                + "  \"res_sum\":  { \"type\": \"integer\" },"
                + "  \"res_min\":  { \"type\": \"integer\" },"
                + "  \"res_max\":  { \"type\": \"integer\" },"
                + "  \"_mv_source_generation\": { \"type\": \"long\" }"
                + "}}"
                + "}"
        );
        assertEquals(Boolean.TRUE, assertOkAndParse(client().performRequest(createTarget), "create " + TARGET).get("acknowledged"));

        Request health = new Request("GET", "/_cluster/health/" + SOURCE + "," + TARGET);
        health.addParameter("wait_for_status", "green");
        health.addParameter("timeout", "60s");
        client().performRequest(health);

        // 3. Ingest deterministic data in BATCHES generations. Regions 1..5;
        //    region r gets (r * 8) docs per batch with patterned values, so
        //    every aggregate has a hand-computable expectation and the count
        //    ordering (5 > 4 > ... > 1) is stable.
        for (int b = 0; b < BATCHES; b++) {
            StringBuilder bulk = new StringBuilder();
            for (int r = 1; r <= 5; r++) {
                for (int i = 0; i < r * 8; i++) {
                    bulk.append("{\"index\":{}}\n")
                        .append("{\"RegionID\":")
                        .append(r)
                        .append(",\"AdvEngineID\":")
                        .append((i % 3 == 0) ? r : 0)
                        .append(",\"ResolutionWidth\":")
                        .append(1000 + ((b + i) % 7) * 137)
                        .append("}\n");
                }
            }
            Request bulkReq = new Request("POST", "/" + SOURCE + "/_bulk");
            bulkReq.setJsonEntity(bulk.toString());
            Map<String, Object> bulkResp = assertOkAndParse(client().performRequest(bulkReq), "bulk batch " + b);
            assertEquals("bulk batch " + b + " must not error", Boolean.FALSE, bulkResp.get("errors"));

            // Refresh = generation flush = state build + ship-before-commit.
            client().performRequest(new Request("POST", "/" + SOURCE + "/_refresh"));
        }
        // No target refresh: the ship ack already certified searchability
        // (refresh-before-ack on the target primary) — asserting without one
        // is part of the contract under test.
    }
}
