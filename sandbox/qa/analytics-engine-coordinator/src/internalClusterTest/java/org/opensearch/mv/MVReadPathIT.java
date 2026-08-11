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

import org.opensearch.Version;
import org.opensearch.analytics.AnalyticsPlugin;
import org.opensearch.analytics.exec.DefaultPlanExecutor;
import org.opensearch.analytics.planner.mv.MVDefinition;
import org.opensearch.analytics.planner.mv.MVDefinition.AggregateSpec;
import org.opensearch.analytics.planner.mv.MVRegistry;
import org.opensearch.analytics.planner.mv.MVRegistryHolder;
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
import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginInfo;
import org.opensearch.test.MockLogAppender;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.junit.After;

import java.util.Collection;
import java.util.Collections;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

/**
 * End-to-end MV read path IT: a SQL aggregate on the SOURCE index is
 * transparently served from per-segment MV state files.
 *
 * <p>Full path under test: {@code MVRewritePhase} annotates the plan (registry
 * installed via the POC {@code MVRegistryHolder}) → {@code
 * FragmentConversionDriver} puts the binding on the shard-scan instruction →
 * {@code ShardScanInstructionHandler} attaches the snapshot's coverage split →
 * Rust {@code mv_read} rewrites the Partial plan to UNION(state scan, Partial
 * over uncovered) → coordinator FINAL. The index uses the POC MV data format,
 * which builds a state file for every flushed segment, so all segments are
 * covered and the raw branch scans zero files.
 *
 * <p>Correctness assertion is differential: the same query with the registry
 * EMPTY (pure raw path) must return identical rows, and both must match the
 * golden answers. The MV definition matches the POC writer's hardcoded
 * definition ({@code MVConstants.MV_SQL}).
 */
@ThreadLeakFilters(filters = MVReadPathIT.NativeThreadFilter.class)
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 1, numClientNodes = 0)
public class MVReadPathIT extends OpenSearchIntegTestCase {

    public static class NativeThreadFilter implements ThreadFilter {
        private static final Pattern GENERIC = Pattern.compile("^Thread-\\d+$");

        @Override
        public boolean reject(Thread t) {
            return GENERIC.matcher(t.getName()).matches();
        }
    }

    private static final String INDEX = "payments";
    private static final String QUERY = "SELECT service, status, COUNT(*) AS cnt, SUM(latency_ms) AS lat_sum, "
        + "MIN(latency_ms) AS lat_min, MAX(latency_ms) AS lat_max FROM "
        + INDEX
        + " GROUP BY service, status";

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(
            ArrowBasePlugin.class,
            CompositeDataFormatPlugin.class,
            ParquetOnlyDataFormatPlugin.class,
            MVDataFormatPlugin.class,
            MockCommitterEnginePlugin.class
        );
    }

    @Override
    protected Collection<PluginInfo> additionalNodePlugins() {
        return List.of(
            classpathPlugin(FlightStreamPlugin.class, List.of(ArrowBasePlugin.class.getName())),
            classpathPlugin(AnalyticsPlugin.class, Collections.emptyList()),
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

    @After
    public void resetRegistry() {
        MVRegistryHolder.reset();
    }

    /** The POC MV definition in the planner's canonical form (matches MVConstants.MV_SQL). */
    private static MVDefinition pocDefinition() {
        return new MVDefinition(
            "mv-poc-payments",
            INDEX,
            List.of("service", "status"),
            List.of(
                AggregateSpec.of("COUNT"),
                AggregateSpec.of("SUM", "latency_ms"),
                AggregateSpec.of("MIN", "latency_ms"),
                AggregateSpec.of("MAX", "latency_ms")
            ),
            "poc"
        );
    }

    public void testAggregateServedFromMVStateFilesMatchesRawPath() throws Exception {
        createCompositeIndexWithMV();
        seedGoldenSegments();

        // 1. Raw path (registry EMPTY): baseline answer.
        MVRegistryHolder.reset();
        List<Object[]> rawRows = runner().executeSql(QUERY);

        // 2. MV path: install the registry; the same query must now bind the MV.
        // The MockLogAppender expectation makes a silent fallback (phase not
        // matching, binding not travelling, attach failing) FAIL the test —
        // result equality alone cannot distinguish the MV path from raw.
        MVRegistryHolder.set(MVRegistry.ofStatic(Map.of(INDEX, List.of(pocDefinition()))));
        List<Object[]> mvRows;
        try (
            MockLogAppender appender = MockLogAppender.createForLoggers(
                LogManager.getLogger("org.opensearch.be.datafusion.ShardScanInstructionHandler")
            )
        ) {
            appender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "mv binding attached",
                    "org.opensearch.be.datafusion.ShardScanInstructionHandler",
                    Level.INFO,
                    "*mv-binding*attached*state files*"
                )
            );
            mvRows = runner().executeSql(QUERY);
            appender.assertAllExpectationsMatched();
        }

        // Differential: identical answers with and without the binding.
        assertRowsEqual(rawRows, mvRows);

        // Golden values (from the POC dataset; segments built at each refresh):
        // api,200: cnt=4 sum=115 min=10 max=50 | api,500: 1/900/900/900
        // batch,200: 1/60/60/60 | web,200: 2/120/40/80
        mvRows.sort(Comparator.comparing((Object[] r) -> String.valueOf(r[0])).thenComparing(r -> String.valueOf(r[1])));
        assertEquals(4, mvRows.size());
        assertRow(mvRows.get(0), "api", "200", 4L, 115L, 10L, 50L);
        assertRow(mvRows.get(1), "api", "500", 1L, 900L, 900L, 900L);
        assertRow(mvRows.get(2), "batch", "200", 1L, 60L, 60L, 60L);
        assertRow(mvRows.get(3), "web", "200", 2L, 120L, 40L, 80L);
    }

    public void testNonMatchingQueryFallsBackCleanly() throws Exception {
        createCompositeIndexWithMV();
        seedGoldenSegments();
        MVRegistryHolder.set(MVRegistry.ofStatic(Map.of(INDEX, List.of(pocDefinition()))));

        // Group-by differs from the MV definition — MVRewritePhase must not
        // annotate, and the query must still answer correctly from raw.
        List<Object[]> rows = runner().executeSql(
            "SELECT service, COUNT(*) AS cnt FROM " + INDEX + " GROUP BY service"
        );
        rows.sort(Comparator.comparing(r -> String.valueOf(r[0])));
        assertEquals(3, rows.size());
        assertEquals("api", String.valueOf(rows.get(0)[0]));
        assertEquals(5L, ((Number) rows.get(0)[1]).longValue());
        assertEquals("batch", String.valueOf(rows.get(1)[0]));
        assertEquals(1L, ((Number) rows.get(1)[1]).longValue());
        assertEquals("web", String.valueOf(rows.get(2)[0]));
        assertEquals(2L, ((Number) rows.get(2)[1]).longValue());
    }

    public void testStrictModeProvesQueryServedFromMVOnly() throws Exception {
        createCompositeIndexWithMV();
        seedGoldenSegments();
        MVRegistryHolder.set(MVRegistry.ofStatic(Map.of(INDEX, List.of(pocDefinition()))));

        // Strict MV-only mode: every fallback in the read chain becomes a hard
        // error and the native plan is the state-file scan ALONE — no raw scan
        // node exists, so raw parquet physically cannot be read. The query
        // SUCCEEDING with golden answers is therefore proof it was served
        // exclusively from MV state files.
        System.setProperty("mv.poc.strict_read", "true");
        try (
            MockLogAppender appender = MockLogAppender.createForLoggers(
                LogManager.getLogger("org.opensearch.be.datafusion.ShardScanInstructionHandler")
            )
        ) {
            appender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "strict mv binding attached",
                    "org.opensearch.be.datafusion.ShardScanInstructionHandler",
                    Level.INFO,
                    "*mv-binding*attached*[STRICT MV-only]*"
                )
            );
            List<Object[]> rows = runner().executeSql(QUERY);
            appender.assertAllExpectationsMatched();
            rows.sort(Comparator.comparing((Object[] r) -> String.valueOf(r[0])).thenComparing(r -> String.valueOf(r[1])));
            assertEquals(4, rows.size());
            assertRow(rows.get(0), "api", "200", 4L, 115L, 10L, 50L);
            assertRow(rows.get(1), "api", "500", 1L, 900L, 900L, 900L);
            assertRow(rows.get(2), "batch", "200", 1L, 60L, 60L, 60L);
            assertRow(rows.get(3), "web", "200", 2L, 120L, 40L, 80L);
        } finally {
            System.clearProperty("mv.poc.strict_read");
        }
    }

    // ── Infrastructure ──────────────────────────────────────────────────────

    private SqlPlanRunner runner() {
        String node = internalCluster().getNodeNames()[0];
        ClusterService clusterService = internalCluster().getInstance(ClusterService.class, node);
        DefaultPlanExecutor executor = internalCluster().getInstance(DefaultPlanExecutor.class, node);
        return new SqlPlanRunner(clusterService, executor);
    }

    private void createCompositeIndexWithMV() {
        client().admin()
            .indices()
            .prepareCreate(INDEX)
            .setSettings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 2)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put("index.pluggable.dataformat.enabled", true)
                    .put("index.pluggable.dataformat", "composite")
                    .put("index.composite.primary_data_format", "parquet")
                    .putList("index.composite.secondary_data_formats", "materialized_view")
                    .put("index.composite.merge_on_refresh_max_size", "0b")
            )
            .setMapping("service", "type=keyword,index=false", "status", "type=keyword,index=false", "latency_ms", "type=long")
            .get();
        ensureGreen(INDEX);
    }

    /** Same golden dataset as MVDataFormatPocIT: 2 segments, 8 docs, 6 state rows. */
    private void seedGoldenSegments() {
        indexDoc("api", "200", 30);
        indexDoc("api", "200", 50);
        indexDoc("web", "200", 40);
        indexDoc("api", "500", 900);
        indexDoc("api", "200", 25);
        client().admin().indices().prepareRefresh(INDEX).get();
        indexDoc("api", "200", 10);
        indexDoc("web", "200", 80);
        indexDoc("batch", "200", 60);
        client().admin().indices().prepareRefresh(INDEX).get();
    }

    private void indexDoc(String service, String status, long latencyMs) {
        // Fixed routing: all docs land on ONE shard. The POC MV writer breaks on
        // near-empty per-shard batches (pre-existing writer limitation, tracked
        // separately); the QUERY still fans out over both shards — the empty
        // shard exercises the no-coverage path (no attach, raw plan over zero
        // files), the full shard exercises the MV union path.
        client().prepareIndex()
            .setIndex(INDEX)
            .setRouting("all-on-one-shard")
            .setSource("service", service, "status", status, "latency_ms", latencyMs)
            .get();
    }

    private static void assertRow(Object[] row, String service, String status, long cnt, long sum, long min, long max) {
        assertEquals(service, String.valueOf(row[0]));
        assertEquals(status, String.valueOf(row[1]));
        assertEquals(cnt, ((Number) row[2]).longValue());
        assertEquals(sum, ((Number) row[3]).longValue());
        assertEquals(min, ((Number) row[4]).longValue());
        assertEquals(max, ((Number) row[5]).longValue());
    }

    private static void assertRowsEqual(List<Object[]> a, List<Object[]> b) {
        assertEquals("row count", a.size(), b.size());
        List<String> aKeys = a.stream().map(MVReadPathIT::rowKey).sorted().toList();
        List<String> bKeys = b.stream().map(MVReadPathIT::rowKey).sorted().toList();
        assertEquals(aKeys, bKeys);
    }

    private static String rowKey(Object[] row) {
        StringBuilder sb = new StringBuilder();
        for (Object o : row) {
            sb.append(o instanceof Number n ? String.valueOf(n.longValue()) : String.valueOf(o)).append('|');
        }
        return sb.toString();
    }
}
