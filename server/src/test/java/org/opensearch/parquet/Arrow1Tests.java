/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.parquet;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.rest.OpenSearchRestTestCase;

import java.io.IOException;
import java.util.Map;

public class Arrow1Tests extends OpenSearchRestTestCase {

    @Override
    protected String getTestRestCluster() {
        // Override to use a specific cluster URL if needed
        return "localhost:9200";
    }

    @Override
    protected Settings restClientSettings() {
        return Settings.builder().put(super.restClientSettings()).build();
    }

    @Override
    public void setUp() throws Exception {
        super.setUp();
        initClient();
        // Apply cluster settings
        Request settingsRequest = new Request("PUT", "/_cluster/settings");
        settingsRequest.setJsonEntity("""
            {
                "transient": {
                "search.enable_intra_segment_search": true,
                "search.concurrent.max_slice_count": 4,
                "search.concurrent_segment_search.mode": "all",
                "cluster.engine.parquet.parallel.enabled": true,
                "cluster.engine.parquet.cache.enabled": true,
                "cluster.engine.parquet.leapfrog.enabled": false
                }
            }""");
        client().performRequest(settingsRequest);
    }

    public void testMatchQueryWithAggregation() throws IOException {
        // First verify the index exists and has data
        Response statsResponse = client().performRequest(new Request("GET", "/*/_stats"));
        Map<String, Object> stats = entityAsMap(statsResponse);
        logger.info("Index stats: {}", stats);

        // Clear caches
        Request clearCache = new Request("POST", "/*/_cache/clear");
        clearCache.addParameter("request", "true");
        clearCache.addParameter("query", "true");
        client().performRequest(clearCache);

        // Refresh
        client().performRequest(new Request("POST", "/*/_refresh"));

        // Warmup
        for (int i = 0; i < 10; i++) {
            Request warmupRequest = new Request("POST", "/*/_search");
            warmupRequest.setJsonEntity("""
                {
                    "size": 0,
                    "query": {
                        "match": {
                            "http_version": "HTTP/1.0"
                        }
                    },
                    "aggs": {
                        "sum_status": {
                            "sum": {
                                "field": "target_status_code"
                            }
                        }
                    }
                }""");
            Response warmupResponse = client().performRequest(warmupRequest);
            Map<String, Object> warmupResult = entityAsMap(warmupResponse);
            logger.info("Warmup {} result: {}", i, warmupResult);
        }

        // Actual test
        Request searchRequest = new Request("POST", "/*/_search");
        searchRequest.setJsonEntity("""
            {
                "size": 0,
                "query": {
                    "match": {
                        "http_version": "HTTP/1.0"
                    }
                },
                "aggs": {
                    "sum_status": {
                        "sum": {
                            "field": "target_status_code"
                        }
                    }
                }
            }""");

        for (int i = 0; i < 1000; i++) {
            long startTime = System.nanoTime();
            Response response = client().performRequest(searchRequest);
            long endTime = System.nanoTime();
            long durationMs = (endTime - startTime) / 1_000_000; // Convert to milliseconds

            Map<String, Object> result = entityAsMap(response);
            if (i % 100 == 0) {
                logger.info(
                    "Query {} took: {}ms, hits: {}, aggs: {}",
                    i,
                    durationMs,
                    ((Map<?, ?>) result.get("hits")).get("total"),
                    ((Map<?, ?>) result.get("aggregations")).get("sum_status")
                );
            }
        }
    }

    @Override
    protected boolean preserveClusterUponCompletion() {
        return true;
    }
}
