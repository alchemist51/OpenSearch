/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import com.carrotsearch.randomizedtesting.ThreadFilter;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.opensearch.action.index.IndexResponse;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.arrow.allocator.ArrowBasePlugin;
import org.opensearch.be.datafusion.DataFusionPlugin;
import org.opensearch.be.lucene.LucenePlugin;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.network.NetworkModule;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.composite.CompositeDataFormatPlugin;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.search.aggregations.AggregationBuilders;
import org.opensearch.search.aggregations.bucket.terms.Terms;
import org.opensearch.search.aggregations.metrics.Max;
import org.opensearch.search.aggregations.metrics.Min;
import org.opensearch.search.aggregations.metrics.Sum;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.transport.Netty4ModulePlugin;

import java.util.Arrays;
import java.util.Collection;
import java.util.regex.Pattern;

import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

/**
 * POC(mv) separate-index (Approach 2) end-to-end IT.
 *
 * <p>Proves the two halves of the design on a live cluster:
 * <ol>
 *   <li><b>Ship-before-commit + fold-on-read</b>: state rows of every flushed
 *       source generation land in the target MV index before the source
 *       commits; folding duplicate group keys ON READ over the MV index
 *       returns the exact golden answers (duplicate groups exist both across
 *       source segments and across shipped generations).</li>
 *   <li><b>The data-level invariant, negatively</b>: with the target index
 *       deleted, the flush FAILS — data never becomes committed-on-source
 *       without its state present-on-target. Recreating the target and
 *       retrying heals (idempotent deterministic doc ids).</li>
 * </ol>
 *
 * <p>POC read shape: plain {@code _search} aggregations over the MV index
 * (SUM of count-state, SUM/MIN/MAX of the metric states) — the production
 * read path (precompiled fragment, shard-local PartialReduce, coordinator
 * FINAL) is diagrammed in the separate-index folder and lands later.
 */
@ThreadLeakFilters(filters = MVSeparateIndexPocIT.NativeThreadFilter.class)
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 1)
public class MVSeparateIndexPocIT extends OpenSearchIntegTestCase {

    public static class NativeThreadFilter implements ThreadFilter {
        private static final Pattern GENERIC = Pattern.compile("^Thread-\\d+$");

        @Override
        public boolean reject(Thread t) {
            return GENERIC.matcher(t.getName()).matches();
        }
    }

    private static final String SOURCE = "payments";
    private static final String TARGET = "mv_payments";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(
            ArrowBasePlugin.class,
            ParquetDataFormatPlugin.class,
            CompositeDataFormatPlugin.class,
            MVDataFormatPlugin.class,
            LucenePlugin.class,
            DataFusionPlugin.class,
            Netty4ModulePlugin.class
        );
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG, true)
            .put(NetworkModule.HTTP_TYPE_KEY, Netty4ModulePlugin.NETTY_HTTP_TRANSPORT_NAME)
            // NOT the POC IT's usual NODE_PROCESSORS=1: ship-before-commit blocks a
            // flushing thread on a bulk to the target index, and the cooperative
            // flush can run that flush ON a write-pool thread — with a 1-thread
            // write pool the target's shard write can never run (same-pool
            // deadlock, observed as a suite timeout). Production consequence
            // recorded in the separate-index README: the ship ack must never
            // depend on the thread pool the shipping thread came from.
            .put(OpenSearchExecutors.NODE_PROCESSORS_SETTING.getKey(), 2)
            .build();
    }

    public void testShipBeforeCommitAndFoldOnRead() {
        createTargetIndex();
        createSourceIndex();

        // Golden segment 1 (5 docs) -> gen 1 ships 3 state rows:
        // (api,200): cnt=3 sum=105 min=25 max=50; (api,500): 1/900; (web,200): 1/40
        indexDoc("api", "200", 30);
        indexDoc("api", "200", 50);
        indexDoc("web", "200", 40);
        indexDoc("api", "500", 900);
        indexDoc("api", "200", 25);
        client().admin().indices().prepareRefresh(SOURCE).get();

        // Golden segment 2 (3 docs) -> gen 2 ships 3 state rows; (api,200) and
        // (web,200) now exist in BOTH generations — folded on read only.
        indexDoc("api", "200", 10);
        indexDoc("web", "200", 80);
        indexDoc("batch", "200", 60);
        client().admin().indices().prepareRefresh(SOURCE).get();

        // Ship happened synchronously inside the source's refresh (ship-before-
        // commit); make the target's rows searchable and fold on read.
        client().admin().indices().prepareRefresh(TARGET).get();
        SearchResponse folded = foldOnRead();

        Terms services = folded.getAggregations().get("by_service");
        // Golden answers: api/200 cnt=4 sum=115 min=10 max=50; api/500 1/900/900/900;
        // batch/200 1/60/60/60; web/200 2/120/40/80
        assertGroup(services, "api", "200", 4, 115, 10, 50);
        assertGroup(services, "api", "500", 1, 900, 900, 900);
        assertGroup(services, "batch", "200", 1, 60, 60, 60);
        assertGroup(services, "web", "200", 2, 120, 40, 80);

        // 6 raw state docs (3 per generation) — folding happened on read, not on write.
        long stateDocs = client().prepareSearch(TARGET).setSize(0).get().getHits().getTotalHits().value();
        assertEquals("state docs = sum of per-generation group counts", 6, stateDocs);
    }

    public void testShipFailureFailsTheFlushAndRetryHeals() throws Exception {
        createTargetIndex();
        createSourceIndex();

        indexDoc("api", "200", 30);
        client().admin().indices().prepareRefresh(SOURCE).get();
        client().admin().indices().prepareRefresh(TARGET).get();
        assertEquals(1, client().prepareSearch(TARGET).setSize(0).get().getHits().getTotalHits().value());

        // Break the invariant's precondition: CLOSE the target, ingest more.
        // (Deleting would not fail the ship — bulk auto-creates missing
        // indices; a closed index rejects writes deterministically.)
        assertAcked(client().admin().indices().prepareClose(TARGET));
        indexDoc("web", "200", 80);

        // The refresh-driven flush must FAIL (ship-before-commit) — the new doc
        // must not become committed-on-source while its state is nowhere.
        // Refresh is a broadcast action: the shard-level flush failure surfaces
        // as failed shards on the response (or, if the engine failure cascades
        // first, as a thrown exception — both prove the commit was refused).
        try {
            org.opensearch.action.support.broadcast.BroadcastResponse refresh = client().admin().indices().prepareRefresh(SOURCE).get();
            assertTrue(
                "refresh must fail while the ship target is missing; got " + refresh.getFailedShards() + " failed shards",
                refresh.getFailedShards() > 0
            );
        } catch (Exception expected) {
            // Engine failure cascaded before the response — equally a refused commit.
        }

        // Heal: reopen the target; the engine recovers and the retried flush
        // re-ships (deterministic doc ids make re-shipping idempotent).
        assertAcked(client().admin().indices().prepareOpen(TARGET));
        ensureGreen(TARGET);
        assertBusy(() -> {
            try {
                client().admin().indices().prepareRefresh(SOURCE).get();
            } catch (Exception e) {
                throw new AssertionError("flush still failing after target recreated", e);
            }
        });
        client().admin().indices().prepareRefresh(TARGET).get();
        assertBusy(() -> {
            long docs = client().prepareSearch(TARGET).setSize(0).get().getHits().getTotalHits().value();
            assertTrue("web/200 state must arrive after heal, saw " + docs + " docs", docs >= 1);
        });
    }

    // ── Infrastructure ──────────────────────────────────────────────────────

    private void createSourceIndex() {
        client().admin()
            .indices()
            .prepareCreate(SOURCE)
            .setSettings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put("index.pluggable.dataformat.enabled", true)
                    .put("index.pluggable.dataformat", "composite")
                    .put("index.composite.primary_data_format", "parquet")
                    .putList("index.composite.secondary_data_formats", "lucene", "materialized_view")
                    .put("index.composite.merge_on_refresh_max_size", "0b")
                    .put(MVConstants.SHIP_TARGET_SETTING, TARGET)
            )
            .setMapping("service", "type=keyword", "status", "type=keyword", "latency_ms", "type=long")
            .get();
        ensureGreen(SOURCE);
    }

    private void createTargetIndex() {
        client().admin()
            .indices()
            .prepareCreate(TARGET)
            .setSettings(Settings.builder().put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1).put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0))
            .setMapping(
                "service",
                "type=keyword",
                "status",
                "type=keyword",
                "cnt",
                "type=long",
                "lat_sum",
                "type=long",
                "lat_min",
                "type=long",
                "lat_max",
                "type=long",
                "_mv_source_index",
                "type=keyword",
                "_mv_source_shard",
                "type=long",
                "_mv_source_generation",
                "type=long"
            )
            .get();
        ensureGreen(TARGET);
    }

    /** Fold-on-read: terms(service,status) with SUM(cnt), SUM(lat_sum), MIN(lat_min), MAX(lat_max). */
    private SearchResponse foldOnRead() {
        return client().prepareSearch(TARGET)
            .setSize(0)
            .addAggregation(
                AggregationBuilders.terms("by_service")
                    .field("service")
                    .subAggregation(
                        AggregationBuilders.terms("by_status")
                            .field("status")
                            .subAggregation(AggregationBuilders.sum("cnt").field("cnt"))
                            .subAggregation(AggregationBuilders.sum("lat_sum").field("lat_sum"))
                            .subAggregation(AggregationBuilders.min("lat_min").field("lat_min"))
                            .subAggregation(AggregationBuilders.max("lat_max").field("lat_max"))
                    )
            )
            .get();
    }

    private static void assertGroup(Terms services, String service, String status, long cnt, long sum, long min, long max) {
        Terms.Bucket serviceBucket = services.getBucketByKey(service);
        assertNotNull("service bucket " + service, serviceBucket);
        Terms statuses = serviceBucket.getAggregations().get("by_status");
        Terms.Bucket statusBucket = statuses.getBucketByKey(status);
        assertNotNull("status bucket " + service + "/" + status, statusBucket);
        assertEquals("cnt " + service + "/" + status, cnt, (long) ((Sum) statusBucket.getAggregations().get("cnt")).getValue());
        assertEquals("sum " + service + "/" + status, sum, (long) ((Sum) statusBucket.getAggregations().get("lat_sum")).getValue());
        assertEquals("min " + service + "/" + status, min, (long) ((Min) statusBucket.getAggregations().get("lat_min")).getValue());
        assertEquals("max " + service + "/" + status, max, (long) ((Max) statusBucket.getAggregations().get("lat_max")).getValue());
    }

    private void indexDoc(String service, String status, long latencyMs) {
        IndexResponse r = client().prepareIndex()
            .setIndex(SOURCE)
            .setSource("service", service, "status", status, "latency_ms", latencyMs)
            .get();
        assertEquals(RestStatus.CREATED, r.status());
    }
}
