/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.parquet;

/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.index.compositeindex.CompositeIndexSettings;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.test.OpenSearchSingleNodeTestCase;
import org.junit.After;

import static org.opensearch.common.xcontent.XContentFactory.jsonBuilder;
import static org.opensearch.index.query.QueryBuilders.termQuery;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertHitCount;

/**
 * Single node integration tests for various PIT use cases such as create pit, search etc
 */
public class ArrowTests extends OpenSearchSingleNodeTestCase {

    @Override
    protected Settings featureFlagSettings() {
        return Settings.builder().put(super.featureFlagSettings()).put(FeatureFlags.STAR_TREE_INDEX, "true").build();
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();

        createIndex("logs", Settings.EMPTY, createTestMapping());

        // Add test documents
        client().prepareIndex("logs")
            .setSource(
                jsonBuilder().startObject()
                    .field("timestamp", "2024-08-29T00:00:00+00:00")
                    .field("backend_ip", "102.238.120.248")
                    .field("backend_port", 95067583)
                    .field("backend_processing_time", 4642.624)
                    .field("backend_status_code", 200)
                    .field("client_ip", "27.52.26.21")
                    .field("client_port", 52084)
                    .field("connection_time", 81545.942)
                    .field("destination_ip", "106.138.9.178")
                    .field("destination_port", 65449)
                    .field("elb_status_code", 200)
                    .field("http_port", 41340)
                    .field("http_version", "HTTP/1.0")
                    .field("matched_rule_priority", 24582)
                    .field("received_bytes", 342256854)
                    .field("request_creation_time", "2024-08-29T00:00:00+00:00")
                    .field("request_processing_time", 43123.294)
                    .field("response_processing_time", 1636.609)
                    .field("sent_bytes", 517762982)
                    .field("target_ip", "61.32.237.24")
                    .field("target_port", 36050)
                    .field("target_processing_time", 46434.99)
                    .field("target_status_code", 200)
                    .endObject()
            )
            .get();

        client().prepareIndex("logs")
            .setSource(
                jsonBuilder().startObject()
                    .field("timestamp", "2024-08-29T00:00:00+00:00")
                    .field("backend_ip", "102.200.110.10")
                    .field("backend_port", 2011417579)
                    .field("backend_processing_time", 57178.441)
                    .field("backend_status_code", 200)
                    .field("client_ip", "22.65.27.243")
                    .field("client_port", 4980)
                    .field("connection_time", 36654.485)
                    .field("destination_ip", "101.120.199.145")
                    .field("destination_port", 63203)
                    .field("elb_status_code", 200)
                    .field("http_port", 33287)
                    .field("http_version", "HTTP/1.1")
                    .field("matched_rule_priority", 9887)
                    .field("received_bytes", 507608411)
                    .field("request_creation_time", "2024-08-29T00:00:00+00:00")
                    .field("request_processing_time", 47559.542)
                    .field("response_processing_time", 62815.975)
                    .field("sent_bytes", 236027376)
                    .field("target_ip", "136.63.127.155")
                    .field("target_port", 52649)
                    .field("target_processing_time", 11305.474)
                    .field("target_status_code", 404)
                    .endObject()
            )
            .get();

        client().admin().indices().prepareRefresh("logs").get();
    }

    public void testTermQueryWithSetting1() {

        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put(CompositeIndexSettings.STAR_TREE_INDEX_ENABLED_SETTING.getKey(), true).build())
            .execute()
            .actionGet();

        for (int i = 0; i < 1000; i++) {
            SearchResponse response = client().prepareSearch("logs")
                .setSize(0)
                .setQuery(termQuery("target_status_code", 500))
                .addAggregation(AggregationBuilders.sum("sum_status").field("target_status_code"))
                .setRequestCache(false)
                .get();
            System.out.println(response.getTook());
        }
        // assertHitCount(response, 1);
        // assertEquals(200.0, response.getAggregations().get("sum_status").getAsDouble(), 0.001);
    }

    public void testRangeQueryWithSetting() {
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(Settings.builder().put("indices.composite_index.star_tree.enabled", true))
            .get();

        SearchResponse response = client().prepareSearch("logs")
            .setSize(0)
            .setQuery(QueryBuilders.rangeQuery("target_status_code").gte(200).lte(400))
            .addAggregation(AggregationBuilders.sum("sum_status").field("target_status_code"))
            .setRequestCache(false)
            .get();

        assertHitCount(response, 1);
        // assertEquals(200.0, response.getAggregations().get("sum_status").getAsDouble(), 0.001);
    }

    public void testTermQueryWithoutSetting() {
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(Settings.builder().put("indices.composite_index.star_tree.enabled", false))
            .get();

        SearchResponse response = client().prepareSearch("logs")
            .setSize(0)
            .setQuery(termQuery("target_status_code", 200))
            .addAggregation(AggregationBuilders.sum("sum_status").field("target_status_code"))
            .setRequestCache(false)
            .get();

        assertHitCount(response, 1);
        // assertEquals(200.0, response.getAggregations().get("sum_status").getAsDouble(), 0.001);
    }

    public void testRangeQueryWithoutSetting() {
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(Settings.builder().put("indices.composite_index.star_tree.enabled", false))
            .get();

        SearchResponse response = client().prepareSearch("logs")
            .setSize(0)
            .setQuery(QueryBuilders.rangeQuery("target_status_code").gte(200).lte(400))
            .addAggregation(AggregationBuilders.sum("sum_status").field("target_status_code"))
            .setRequestCache(false)
            .get();

        assertHitCount(response, 1);

        // assertEquals(200.0, response.getAggregations().get("sum_status").getAsDouble(), 0.001);
    }

    private static String createTestMapping() {
        return "  \"properties\": {\n"
            + "    \"backend_ip\": { \"type\": \"ip\" },\n"
            + "    \"backend_port\": { \"type\": \"integer\" },\n"
            + "    \"backend_processing_time\": { \"type\": \"float\" },\n"
            + "    \"backend_status_code\": { \"type\": \"integer\" },\n"
            + "    \"client_ip\": { \"type\": \"ip\" },\n"
            + "    \"client_port\": { \"type\": \"integer\" },\n"
            + "    \"connection_time\": { \"type\": \"float\" },\n"
            + "    \"destination_ip\": { \"type\": \"ip\" },\n"
            + "    \"destination_port\": { \"type\": \"integer\" },\n"
            + "    \"elb_status_code\": { \"type\": \"integer\" },\n"
            + "    \"http_port\": { \"type\": \"integer\" },\n"
            + "    \"http_version\": { \"type\": \"text\" },\n"
            + "    \"matched_rule_priority\": { \"type\": \"integer\" },\n"
            + "    \"received_bytes\": { \"type\": \"long\" },\n"
            + "    \"request_creation_time\": { \"type\": \"date\" },\n"
            + "    \"request_processing_time\": { \"type\": \"float\" },\n"
            + "    \"response_processing_time\": { \"type\": \"float\" },\n"
            + "    \"sent_bytes\": { \"type\": \"long\" },\n"
            + "    \"target_ip\": { \"type\": \"ip\" },\n"
            + "    \"target_port\": { \"type\": \"integer\" },\n"
            + "    \"target_processing_time\": { \"type\": \"float\" },\n"
            + "    \"target_status_code\": { \"type\": \"integer\" },\n"
            + "    \"timestamp\": { \"type\": \"date\" }\n"
            + "  }";
    }

    @After
    public final void cleanupNodeSettings() {
        assertAcked(
            client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setPersistentSettings(Settings.builder().putNull("*"))
                .setTransientSettings(Settings.builder().putNull("*"))
        );
    }
}
