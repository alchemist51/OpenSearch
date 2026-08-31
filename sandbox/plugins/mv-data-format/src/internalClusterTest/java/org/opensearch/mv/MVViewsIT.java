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

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.opensearch.arrow.allocator.ArrowBasePlugin;
import org.opensearch.be.datafusion.DataFusionPlugin;
import org.opensearch.be.lucene.LucenePlugin;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.network.NetworkModule;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.composite.CompositeDataFormatPlugin;
import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.MockLogAppender;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.transport.Netty4ModulePlugin;

import java.util.Arrays;
import java.util.Collection;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * The `index.mv.views` UX end to end (decisions 20/21/23/24): the user
 * creates ONLY the source index with a views declaration — no formats, no
 * state schema, no target index — and the system derives the source's MV
 * settings ({@link MVViewsService.Provider}), auto-creates the colocated
 * target with the state mapping + single hidden provenance field
 * ({@link MVViewsService.TargetCreator}), and the ship/fold pipeline runs
 * exactly as if both indices had been created by hand.
 */
@ThreadLeakFilters(filters = MVViewsIT.NativeThreadFilter.class)
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 1)
public class MVViewsIT extends OpenSearchIntegTestCase {

    public static class NativeThreadFilter implements ThreadFilter {
        private static final Pattern GENERIC = Pattern.compile("^Thread-\\d+$");

        @Override
        public boolean reject(Thread t) {
            return GENERIC.matcher(t.getName()).matches();
        }
    }

    private static final String SOURCE = "payments";
    /** Generated name: {@code <source>_mv_<definition>} (decision 23, unnamed view). */
    private static final String GENERATED_TARGET = "payments_mv_payments";

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG, true)
            .put(NetworkModule.HTTP_TYPE_KEY, Netty4ModulePlugin.NETTY_HTTP_TRANSPORT_NAME)
            // 2 processors: ship-before-commit must never share a 1-thread
            // write pool with the target's apply (same-pool deadlock — see
            // MVSeparateIndexPocIT's note).
            .put(OpenSearchExecutors.NODE_PROCESSORS_SETTING.getKey(), 2)
            .build();
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(
            ArrowBasePlugin.class,
            Netty4ModulePlugin.class,
            CompositeDataFormatPlugin.class,
            ParquetDataFormatPlugin.class,
            LucenePlugin.class,
            DataFusionPlugin.class,
            MVDataFormatPlugin.class
        );
    }

    public void testViewsDeclarationDrivesTheWholePipeline() throws Exception {
        // The ONLY thing the user does: create the source with a views entry.
        client().admin()
            .indices()
            .prepareCreate(SOURCE)
            .setSettings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put("index.composite.merge_on_refresh_max_size", "0b")
                    .putList(MVConstants.VIEWS_SETTING, "payments")
            )
            // Mapping INLINE in the create — works since the provider-collision
            // fix (the composite plugin's provider defers to MV-derived formats).
            .setMapping("service", "type=keyword", "status", "type=keyword", "latency_ms", "type=long")
            .get();
        ensureGreen(SOURCE);

        // Provider must have derived the composite+MV source settings.
        Settings sourceSettings = client().admin().indices().prepareGetSettings(SOURCE).get().getIndexToSettings().get(SOURCE);
        assertEquals("composite", sourceSettings.get("index.pluggable.dataformat"));
        assertTrue(
            "materialized_view must be derived into the formats",
            sourceSettings.getAsList("index.composite.secondary_data_formats").contains("materialized_view")
        );
        assertEquals(java.util.List.of(GENERATED_TARGET), sourceSettings.getAsList(MVConstants.SHIP_TARGETS_SETTING));

        // Target must be auto-created (cluster-manager listener) — with the
        // derived state mapping and colocation.
        assertBusy(() -> {
            assertTrue(
                "target must be auto-created",
                client().admin().cluster().prepareState().get().getState().metadata().hasIndex(GENERATED_TARGET)
            );
        });
        ensureGreen(GENERATED_TARGET);
        Settings targetSettings = client().admin()
            .indices()
            .prepareGetSettings(GENERATED_TARGET)
            .get()
            .getIndexToSettings()
            .get(GENERATED_TARGET);
        assertEquals(SOURCE, targetSettings.get(MVConstants.COLOCATE_WITH_SETTING));
        assertTrue(targetSettings.getAsBoolean(MVConstants.DERIVED_INDEX_SETTING, false));
        assertEquals(java.util.List.of("lucene"), targetSettings.getAsList("index.composite.secondary_data_formats"));
        assertEquals("materialized_view", targetSettings.get(org.opensearch.cluster.metadata.DerivedIndexBinding.KEY_DATA_FORMAT));
        Map<String, Object> mapping = client().admin()
            .indices()
            .prepareGetMappings(GENERATED_TARGET)
            .get()
            .getMappings()
            .get(GENERATED_TARGET)
            .sourceAsMap();
        @SuppressWarnings("unchecked")
        Map<String, Object> properties = (Map<String, Object>) mapping.get("properties");
        assertTrue("state schema derived", properties.containsKey("cnt") && properties.containsKey("lat_sum"));
        assertTrue("single hidden provenance field (decision 21)", properties.containsKey("_mv_source_generation"));
        assertFalse("dropped provenance fields must be gone", properties.containsKey("_mv_source_index"));

        // And the pipeline actually runs: ingest -> refresh -> state shipped
        // to the AUTO-CREATED target with a searchable apply ack. (The
        // composite target has no classic _search path; the ship-ack log is
        // the same proof the main POC IT uses.)
        try (MockLogAppender appender = MockLogAppender.createForLoggers(LogManager.getLogger("org.opensearch.mv.MVStateShipper"))) {
            appender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "state shipped to the auto-created target",
                    "org.opensearch.mv.MVStateShipper",
                    Level.INFO,
                    "*-> [" + GENERATED_TARGET + "][0] (acked searchable)*"
                )
            );
            client().prepareIndex(SOURCE).setSource("service", "api", "status", "200", "latency_ms", 30).get();
            client().prepareIndex(SOURCE).setSource("service", "api", "status", "200", "latency_ms", 70).get();
            client().admin().indices().prepareRefresh(SOURCE).get();
            appender.assertAllExpectationsMatched();
        }
    }
}
