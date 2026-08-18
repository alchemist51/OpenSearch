/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import org.opensearch.Version;
import org.opensearch.analytics.AnalyticsPlugin;
import org.opensearch.analytics.sql.SqlPlanRunner;
import org.opensearch.arrow.allocator.ArrowBasePlugin;
import org.opensearch.arrow.flight.transport.FlightStreamPlugin;
import org.opensearch.be.datafusion.DataFusionPlugin;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.composite.CompositeDataFormatPlugin;
import org.opensearch.index.engine.dataformat.stub.MockCommitterEnginePlugin;
import org.opensearch.parquet.ParquetOnlyDataFormatPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.test.OpenSearchIntegTestCase;


import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;

/**
 * The native MV read (validation scope): the EXACT q9 aggregate — including
 * {@code AVG}, no eval, no state-name knowledge — served from the MV target's
 * {@code mv_state} Arrow files.
 *
 * <p>Zero-translation contract end to end: the definition IS the query
 * ({@link MVDefinitionSpec#CLICKBENCH_Q9_NATIVE}), so the shipped state
 * carries DataFusion's own partial layout (avg as its {@code [count, sum]}
 * state pair). With {@code index.mv.serve_state=true} on the target, the
 * shard fragment REPLACES its Partial with the state-file scan (STRICT —
 * any schema/type/plan misalignment throws, never a silent fallback) and the
 * coordinator's Final merges counts+sums and divides once — exactly how
 * DataFusion finishes any distributed aggregate.
 */
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.SUITE, numDataNodes = 1, numClientNodes = 0)
public class MVNativeReadIT extends OpenSearchIntegTestCase {

    private static final String SOURCE = "hits_native";
    private static final String TARGET = "mv_hits_native";

    /** Literal q9 (no ORDER BY; rows sorted client-side). The CAST matches PPL's
     * double avg semantics — Calcite SQL types AVG(integer) as integer, which
     * truncates; the MV path is exact either way (avg_sum is a double column),
     * and this validation run CAUGHT the baseline truncating (1551.0 vs
     * 1551.333...). */
    private static String q9Direct(String index) {
        return "SELECT \"RegionID\", SUM(\"AdvEngineID\"), COUNT(*), AVG(CAST(\"ResolutionWidth\" AS DOUBLE)) FROM " + index + " GROUP BY \"RegionID\"";
    }

    /**
     * q9 spoken in MV state-column names (the validation-phase hardcoded
     * mapping): the target's parquet state docs ARE partial state, so
     * SUM-of-sums / SUM-of-counts reproduces every aggregate exactly —
     * avg included, because avg_sum ships as a DOUBLE column (float
     * division by construction, no cast, no eval, no UDF).
     */
    private static String q9Mapped(String index) {
        return "SELECT \"RegionID\", SUM(adv_sum), SUM(cnt), SUM(avg_sum) / SUM(avg_cnt) FROM " + index + " GROUP BY \"RegionID\"";
    }

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(
            ArrowBasePlugin.class,
            CompositeDataFormatPlugin.class,
            MockCommitterEnginePlugin.class,
            MVDataFormatPlugin.class,
            MVStateDataFormatPlugin.class
        );
    }

    @Override
    protected Collection<PluginInfo> additionalNodePlugins() {
        return List.of(
            classpathPlugin(FlightStreamPlugin.class, List.of(ArrowBasePlugin.class.getName())),
            classpathPlugin(AnalyticsPlugin.class, Collections.emptyList()),
            classpathPlugin(ParquetOnlyDataFormatPlugin.class, Collections.emptyList()),
            classpathPlugin(DataFusionPlugin.class, List.of(AnalyticsPlugin.class.getName()))
        );
    }

    private static PluginInfo classpathPlugin(Class<? extends Plugin> pluginClass, List<String> extendedPlugins) {
        return new PluginInfo(
            pluginClass.getName(),
            "classpath plugin",
            "NA",
            Version.CURRENT,
            "1.8",
            pluginClass.getName(),
            null,
            extendedPlugins,
            false
        );
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG, true)
            .put(FeatureFlags.STREAM_TRANSPORT, true)
            .build();
    }

    public void testQ9ServedFromStateFilesMatchesDirect() throws Exception {
        provision();

        // Two generations with overlapping groups (the fold has real work).
        indexHit(229, 2, 1368);
        indexHit(229, 0, 1920);
        indexHit(2, 3, 1440);
        client().admin().indices().prepareRefresh(SOURCE).get();
        indexHit(229, 7, 1366);   // -> avg 1551.333... (non-whole: truncation would show)
        indexHit(2, 0, 1600);
        client().admin().indices().prepareRefresh(SOURCE).get();

        // 1. Baseline: q9 DIRECT over the source (raw parquet scan).
        List<Object[]> direct = sorted(runner().executeSql(q9Direct(SOURCE)));

        // 2. The moment under test: q9 in state-column terms against the MV
        //    index — served by the completely standard engine path over the
        //    target's parquet state docs (partial state in, finals out).
        List<Object[]> fromMV = sorted(runner().executeSql(q9Mapped(TARGET)));

        assertEquals("row count", direct.size(), fromMV.size());
        for (int i = 0; i < direct.size(); i++) {
            Object[] d = direct.get(i);
            Object[] m = fromMV.get(i);
            assertEquals("RegionID row " + i, ((Number) d[0]).longValue(), ((Number) m[0]).longValue());
            assertEquals("sum(AdvEngineID) row " + i, ((Number) d[1]).longValue(), ((Number) m[1]).longValue());
            assertEquals("count(*) row " + i, ((Number) d[2]).longValue(), ((Number) m[2]).longValue());
            assertEquals("avg(ResolutionWidth) row " + i, ((Number) d[3]).doubleValue(), ((Number) m[3]).doubleValue(), 1e-9);
        }
        // The non-whole average is the truncation canary.
        Object[] region229 = fromMV.stream().filter(r -> ((Number) r[0]).longValue() == 229L).findFirst().orElseThrow();
        assertEquals(1551.333333, ((Number) region229[3]).doubleValue(), 1e-4);
    }

    private void provision() {
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
                    .putList("index.composite.secondary_data_formats", "materialized_view")
                    .put("index.composite.merge_on_refresh_max_size", "0b")
                    .put(MVConstants.DEFINITION_SETTING, "clickbench_q9_native")
                    .putList(MVConstants.SHIP_TARGETS_SETTING, TARGET)
            )
            .setMapping("RegionID", "type=integer", "AdvEngineID", "type=integer", "ResolutionWidth", "type=integer")
            .get();
        ensureGreen(SOURCE);

        // Target: state fields store the shipped docs (avg_sum is DOUBLE —
        // the floating half of avg's state); original-name fields are the
        // VALIDATION SURFACE so the literal q9 text plans against this index
        // (never populated in docs; the strict read scans only state files).
        client().admin()
            .indices()
            .prepareCreate(TARGET)
            .setSettings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put("index.pluggable.dataformat.enabled", true)
                    .put("index.pluggable.dataformat", "composite")
                    .put("index.composite.primary_data_format", "parquet")
                    .putList("index.composite.secondary_data_formats", "mv_state")
                    .put("index.composite.merge_on_refresh_max_size", "0b")
                    .put(MVConstants.DEFINITION_SETTING, "clickbench_q9_native")
                    .put(MVConstants.COLOCATE_WITH_SETTING, SOURCE)
            )
            .setMapping(
                "RegionID",
                "type=integer",
                "adv_sum",
                "type=long",
                "cnt",
                "type=long",
                "avg_cnt",
                "type=long",
                "avg_sum",
                "type=double",
                "AdvEngineID",
                "type=integer",
                "ResolutionWidth",
                "type=integer",
                "_mv_source_generation",
                "type=long"
            )
            .get();
        ensureGreen(TARGET);
    }

    private void indexHit(int region, int adv, int width) {
        client().prepareIndex(SOURCE).setSource("RegionID", region, "AdvEngineID", adv, "ResolutionWidth", width).get();
    }

    private static List<Object[]> sorted(List<Object[]> rows) {
        return rows.stream().sorted(Comparator.comparingLong(r -> ((Number) r[0]).longValue())).toList();
    }

    private SqlPlanRunner runner() {
        String node = internalCluster().getNodeNames()[0];
        ClusterService clusterService = internalCluster().getInstance(ClusterService.class, node);
        org.opensearch.analytics.exec.DefaultPlanExecutor executor = internalCluster().getInstance(
            org.opensearch.analytics.exec.DefaultPlanExecutor.class,
            node
        );
        return new SqlPlanRunner(clusterService, executor);
    }
}
