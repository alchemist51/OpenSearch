/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv.pull;

import com.carrotsearch.randomizedtesting.ThreadFilter;
import com.carrotsearch.randomizedtesting.annotations.ThreadLeakFilters;

import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.analytics.AnalyticsPlugin;
import org.opensearch.arrow.allocator.ArrowBasePlugin;
import org.opensearch.arrow.flight.transport.FlightStreamPlugin;
import org.opensearch.be.datafusion.DataFusionPlugin;
import org.opensearch.be.lucene.LucenePlugin;
import org.opensearch.cluster.metadata.DerivedIndexBinding;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.network.NetworkModule;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.common.util.concurrent.OpenSearchExecutors;
import org.opensearch.composite.CompositeDataFormatPlugin;
import org.opensearch.mv.AggregateSpec;
import org.opensearch.mv.GroupKey;
import org.opensearch.mv.MVCompiledDefinition;
import org.opensearch.mv.MVCreateViewAction;
import org.opensearch.mv.MVCreateViewRequest;
import org.opensearch.mv.MVCreateViewResponse;
import org.opensearch.mv.MVDataFormat;
import org.opensearch.mv.MVDataFormatPlugin;
import org.opensearch.mv.MVDefinitionDescriptor;
import org.opensearch.mv.MVDefinitionDescriptor.AggregateDescriptor;
import org.opensearch.mv.MVDefinitionDescriptor.GroupKeyDescriptor;
import org.opensearch.mv.MVDefinitionResolver;
import org.opensearch.mv.MVDefinitionValidator;
import org.opensearch.mv.MVNativeBridge;
import org.opensearch.mv.MVStateDataFormat;
import org.opensearch.mv.MVValidateAction;
import org.opensearch.mv.MVValidateRequest;
import org.opensearch.mv.MVValidateResponse;
import org.opensearch.mv.MVValidationReasons;
import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.plugins.Plugin;
import org.opensearch.plugins.PluginsService;
import org.opensearch.test.OpenSearchIntegTestCase;
import org.opensearch.transport.Netty4ModulePlugin;

import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.regex.Pattern;

import static org.opensearch.common.util.FeatureFlags.STREAM_TRANSPORT;
import static org.opensearch.test.hamcrest.OpenSearchAssertions.assertAcked;

/**
 * End-to-end integration tests for the MV DYNAMIC DEFINITION compiler. Each
 * test exercises the FULL dynamic control-plane path for a distinct definition
 * shape:
 *
 * <pre>
 *   build descriptor
 *     -> transport {@link MVValidateAction} (native cross-check, physical schema)
 *     -> transport {@link MVCreateViewAction} (derived materialized_view target)
 *     -> derived-pull poller starts automatically
 *     -> deterministic source ingest + refresh
 *     -> watermark catch-up
 *     -> exact fold verification through {@link MVNativeBridge#searchV2}
 *     -> delete view (poller stops)
 * </pre>
 *
 * <p>The setup mirrors {@link MVPullDataFusionIT} exactly (node settings,
 * feature flags, remote-store repo, composite parquet+lucene source). Fold
 * verification uses the ENGINE-REPORTED physical state-field names returned by
 * {@link MVValidateResponse#nativeStateFields()} — so it is authoritative
 * regardless of DataFusion's internal aggregate-state naming — and asserts
 * exact per-group equality against in-test computed expectations.
 */
@ThreadLeakFilters(filters = MVDynamicDefinitionIT.NativeThreadFilter.class)
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 1)
public class MVDynamicDefinitionIT extends OpenSearchIntegTestCase {

    public static class NativeThreadFilter implements ThreadFilter {
        private static final Pattern GENERIC = Pattern.compile("^Thread-\\d+$");

        @Override
        public boolean reject(Thread t) {
            return GENERIC.matcher(t.getName()).matches();
        }
    }

    private static final String REPO = "mv-dyn-def-repo";
    private Path repoPath;

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(
            ArrowBasePlugin.class,
            FlightStreamPlugin.class,
            AnalyticsPlugin.class,
            ParquetDataFormatPlugin.class,
            CompositeDataFormatPlugin.class,
            MVDataFormatPlugin.class,
            LucenePlugin.class,
            DataFusionPlugin.class,
            Netty4ModulePlugin.class
        );
    }

    @Override
    protected boolean addMockInternalEngine() {
        return false;
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        if (repoPath == null) {
            repoPath = randomRepoPath().toAbsolutePath();
        }
        return Settings.builder()
            .put(super.nodeSettings(nodeOrdinal))
            .put(FeatureFlags.PLUGGABLE_DATAFORMAT_EXPERIMENTAL_FLAG, true)
            .put(NetworkModule.HTTP_TYPE_KEY, Netty4ModulePlugin.NETTY_HTTP_TRANSPORT_NAME)
            .put(OpenSearchExecutors.NODE_PROCESSORS_SETTING.getKey(), 2)
            .put(remoteStoreClusterSettings(REPO, repoPath))
            .build();
    }

    // ══════════════════════════════════════════════════════════════════════
    // 1. Baseline dynamic path: single LONG key + COUNT(*)/SUM.
    // ══════════════════════════════════════════════════════════════════════

    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testBaselineSingleKeyCountSum() throws Exception {
        String source = "src_baseline";
        String view = "mv_baseline";
        createCompositeSource(source, "RegionID", "type=long", "AdvEngineID", "type=long");

        MVDefinitionDescriptor descriptor = MVDefinitionDescriptor.of(
            List.of(GroupKeyDescriptor.plain("RegionID", GroupKey.ColumnType.LONG)),
            List.of(AggregateDescriptor.count("cnt"), AggregateDescriptor.sum("AdvEngineID", "adv"))
        );

        MVValidateResponse validation = validateOk(source, descriptor);
        MVCompiledDefinition def = MVCompiledDefinition.fromDescriptor(descriptor);
        List<MVDefinitionValidator.StateField> nsf = validation.nativeStateFields();

        createView(view, source, descriptor);

        // Deterministic ingest: region=i%8, adv=i%5, across three generations.
        Map<List<String>, long[]> expected = new HashMap<>();
        int totalDocs = 0;
        for (int gen = 0; gen < 3; gen++) {
            int batch = 100;
            for (int i = 0; i < batch; i++) {
                long region = totalDocs % 8;
                long adv = totalDocs % 5;
                client().prepareIndex(source).setSource("RegionID", region, "AdvEngineID", adv).get();
                List<String> key = List.of(Long.toString(region));
                long[] agg = expected.computeIfAbsent(key, k -> new long[2]);
                agg[0] += 1;    // cnt
                agg[1] += adv;  // adv sum
                totalDocs++;
            }
            client().admin().indices().prepareRefresh(source).get();
        }

        assertFoldEquals(view, def, nsf, expected, totalDocs);
        deleteViewAndAssertPollerStops(view);
    }

    // ══════════════════════════════════════════════════════════════════════
    // 2. DE7-in-miniature: 5-minute EventTime bucket expression key +
    //    KEYWORD URL + LONG UserID, SUM/MIN/MAX/COUNT_FIELD over 2 metrics.
    //    Verifies bucket-boundary correctness (docs straddling two 5-min
    //    windows land in distinct groups).
    // ══════════════════════════════════════════════════════════════════════

    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testFiveMinuteBucketMultiKey() throws Exception {
        String source = "src_de7mini";
        String view = "mv_de7mini";
        createCompositeSource(
            source,
            "EventTime",
            "type=long",
            "URL",
            "type=keyword",
            "UserID",
            "type=long",
            "AdvEngineID",
            "type=long",
            "ResolutionWidth",
            "type=long"
        );

        // event_bucket = CAST("EventTime" AS BIGINT) / 300000 (epoch-millis, 5 min = 300000 ms).
        MVDefinitionDescriptor descriptor = MVDefinitionDescriptor.of(
            List.of(
                GroupKeyDescriptor.expression("event_bucket", GroupKey.ColumnType.LONG, "CAST(\"EventTime\" AS BIGINT) / 300000", "EventTime"),
                GroupKeyDescriptor.plain("URL", GroupKey.ColumnType.KEYWORD),
                GroupKeyDescriptor.plain("UserID", GroupKey.ColumnType.LONG)
            ),
            List.of(
                AggregateDescriptor.sum("AdvEngineID", "adv_sum"),
                AggregateDescriptor.min("AdvEngineID", "adv_min"),
                AggregateDescriptor.max("AdvEngineID", "adv_max"),
                AggregateDescriptor.countField("AdvEngineID", "adv_cnt"),
                AggregateDescriptor.sum("ResolutionWidth", "rw_sum"),
                AggregateDescriptor.min("ResolutionWidth", "rw_min"),
                AggregateDescriptor.max("ResolutionWidth", "rw_max"),
                AggregateDescriptor.countField("ResolutionWidth", "rw_cnt")
            )
        );

        MVValidateResponse validation = validateOk(source, descriptor);
        MVCompiledDefinition def = MVCompiledDefinition.fromDescriptor(descriptor);
        List<MVDefinitionValidator.StateField> nsf = validation.nativeStateFields();

        createView(view, source, descriptor);

        // Deterministic docs, several deliberately straddling a 5-min boundary.
        // EventTime values chosen so buckets are 0, 1, 2, 3 (÷300000).
        long[] eventTimes = { 0L, 150_000L, 299_999L, 300_000L, 300_001L, 599_999L, 600_000L, 900_001L, 1_200_000L, 1_499_999L };
        String[] urls = { "u0", "u1" };
        long[] users = { 0L, 1L };

        Map<List<String>, long[]> expected = new HashMap<>();  // [adv_sum, adv_min, adv_max, adv_cnt, rw_sum, rw_min, rw_max, rw_cnt]
        int totalDocs = 0;
        for (int gen = 0; gen < 2; gen++) {
            for (int e = 0; e < eventTimes.length; e++) {
                long et = eventTimes[e] + gen; // keep same bucket, distinct docs
                String url = urls[e % urls.length];
                long user = users[(e / 2) % users.length];
                long adv = (e % 4) + 1;      // 1..4
                long rw = (e % 3) * 10 + 5;  // 5,15,25
                client().prepareIndex(source)
                    .setSource("EventTime", et, "URL", url, "UserID", user, "AdvEngineID", adv, "ResolutionWidth", rw)
                    .get();
                long bucket = et / 300_000L;
                List<String> key = List.of(Long.toString(bucket), url, Long.toString(user));
                long[] a = expected.get(key);
                if (a == null) {
                    a = new long[] { 0, Long.MAX_VALUE, Long.MIN_VALUE, 0, 0, Long.MAX_VALUE, Long.MIN_VALUE, 0 };
                    expected.put(key, a);
                }
                a[0] += adv;
                a[1] = Math.min(a[1], adv);
                a[2] = Math.max(a[2], adv);
                a[3] += 1;
                a[4] += rw;
                a[5] = Math.min(a[5], rw);
                a[6] = Math.max(a[6], rw);
                a[7] += 1;
                totalDocs++;
            }
            client().admin().indices().prepareRefresh(source).get();
        }

        // Sanity: boundary docs (299999 -> bucket 0, 300000 -> bucket 1) are distinct groups.
        assertTrue("bucket 0 must exist", expected.keySet().stream().anyMatch(k -> k.get(0).equals("0")));
        assertTrue("bucket 1 must exist", expected.keySet().stream().anyMatch(k -> k.get(0).equals("1")));

        assertFoldEquals(view, def, nsf, expected, totalDocs);
        deleteViewAndAssertPollerStops(view);
    }

    // ══════════════════════════════════════════════════════════════════════
    // 3. COUNT(*) vs COUNT(field): nullable field. COUNT counts all rows,
    //    COUNT_FIELD counts non-null values only.
    // ══════════════════════════════════════════════════════════════════════

    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testCountStarVsCountField() throws Exception {
        String source = "src_count";
        String view = "mv_count";
        createCompositeSource(source, "RegionID", "type=long", "val", "type=long");

        MVDefinitionDescriptor descriptor = MVDefinitionDescriptor.of(
            List.of(GroupKeyDescriptor.plain("RegionID", GroupKey.ColumnType.LONG)),
            List.of(AggregateDescriptor.count("cnt_all"), AggregateDescriptor.countField("val", "cnt_val"))
        );

        MVValidateResponse validation = validateOk(source, descriptor);
        MVCompiledDefinition def = MVCompiledDefinition.fromDescriptor(descriptor);
        List<MVDefinitionValidator.StateField> nsf = validation.nativeStateFields();

        createView(view, source, descriptor);

        Map<List<String>, long[]> expected = new HashMap<>(); // [cnt_all, cnt_val]
        int totalDocs = 0;
        for (int gen = 0; gen < 2; gen++) {
            for (int i = 0; i < 120; i++) {
                long region = totalDocs % 4;
                boolean hasVal = (totalDocs % 3) != 0; // 1/3 of docs omit "val"
                if (hasVal) {
                    client().prepareIndex(source).setSource("RegionID", region, "val", (long) totalDocs).get();
                } else {
                    client().prepareIndex(source).setSource("RegionID", region).get();
                }
                List<String> key = List.of(Long.toString(region));
                long[] a = expected.computeIfAbsent(key, k -> new long[2]);
                a[0] += 1;                 // cnt_all (COUNT(*))
                if (hasVal) {
                    a[1] += 1;             // cnt_val (COUNT(val), non-null only)
                }
                totalDocs++;
            }
            client().admin().indices().prepareRefresh(source).get();
        }

        // The field is genuinely nullable in expectations: some groups have cnt_val < cnt_all.
        assertTrue(
            "test must exercise the null distinction",
            expected.values().stream().anyMatch(a -> a[1] < a[0])
        );

        assertFoldEquals(view, def, nsf, expected, totalDocs);
        deleteViewAndAssertPollerStops(view);
    }

    // ══════════════════════════════════════════════════════════════════════
    // 4. AVG decomposition: descriptor AVG -> state carries count+sum; fold
    //    reconstructs the exact average. Exact equality on both components.
    // ══════════════════════════════════════════════════════════════════════

    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testAvgDecomposition() throws Exception {
        String source = "src_avg";
        String view = "mv_avg";
        createCompositeSource(source, "RegionID", "type=long", "val", "type=long");

        MVDefinitionDescriptor descriptor = MVDefinitionDescriptor.of(
            List.of(GroupKeyDescriptor.plain("RegionID", GroupKey.ColumnType.LONG)),
            List.of(AggregateDescriptor.avg("val", "avg_val"))
        );

        MVValidateResponse validation = validateOk(source, descriptor);
        MVCompiledDefinition def = MVCompiledDefinition.fromDescriptor(descriptor);
        List<MVDefinitionValidator.StateField> nsf = validation.nativeStateFields();
        // AVG produces exactly two physical state columns (count then sum).
        assertEquals("AVG must decompose into 2 state columns after the key", 3, nsf.size());

        createView(view, source, descriptor);

        Map<List<String>, long[]> expected = new HashMap<>(); // [avg_count, avg_sum]
        int totalDocs = 0;
        for (int gen = 0; gen < 2; gen++) {
            for (int i = 0; i < 100; i++) {
                long region = totalDocs % 5;
                long val = (totalDocs % 7) + 1;
                client().prepareIndex(source).setSource("RegionID", region, "val", val).get();
                List<String> key = List.of(Long.toString(region));
                long[] a = expected.computeIfAbsent(key, k -> new long[2]);
                a[0] += 1;   // count
                a[1] += val; // sum
                totalDocs++;
            }
            client().admin().indices().prepareRefresh(source).get();
        }

        assertFoldEquals(view, def, nsf, expected, totalDocs);

        // Additionally assert the reconstructed exact average matches the source.
        PublishedState ps = published(view);
        Map<List<String>, long[]> actual = fold(def, nsf, ps.files());
        for (Map.Entry<List<String>, long[]> e : expected.entrySet()) {
            long[] got = actual.get(e.getKey());
            double expectedAvg = (double) e.getValue()[1] / e.getValue()[0];
            double actualAvg = (double) got[1] / got[0];
            assertEquals("avg for group " + e.getKey(), expectedAvg, actualAvg, 0.0);
        }

        deleteViewAndAssertPollerStops(view);
    }

    // ══════════════════════════════════════════════════════════════════════
    // 5. MIN/MAX width preservation over an INTEGER (int32) source field.
    // ══════════════════════════════════════════════════════════════════════

    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testMinMaxWidthPreservation() throws Exception {
        String source = "src_minmax";
        String view = "mv_minmax";
        createCompositeSource(source, "RegionID", "type=long", "w", "type=integer");

        MVDefinitionDescriptor descriptor = MVDefinitionDescriptor.of(
            List.of(GroupKeyDescriptor.plain("RegionID", GroupKey.ColumnType.LONG)),
            List.of(
                AggregateDescriptor.min("w", "w_min"),
                AggregateDescriptor.max("w", "w_max"),
                AggregateDescriptor.count("cnt")
            )
        );

        MVValidateResponse validation = validateOk(source, descriptor);
        MVCompiledDefinition def = MVCompiledDefinition.fromDescriptor(descriptor);
        List<MVDefinitionValidator.StateField> nsf = validation.nativeStateFields();

        createView(view, source, descriptor);

        Map<List<String>, long[]> expected = new HashMap<>(); // [w_min, w_max, cnt]
        int totalDocs = 0;
        for (int gen = 0; gen < 2; gen++) {
            for (int i = 0; i < 80; i++) {
                long region = totalDocs % 4;
                int w = (totalDocs * 7 + 3) % 251; // deterministic spread 0..250
                client().prepareIndex(source).setSource("RegionID", region, "w", w).get();
                List<String> key = List.of(Long.toString(region));
                long[] a = expected.get(key);
                if (a == null) {
                    a = new long[] { Long.MAX_VALUE, Long.MIN_VALUE, 0 };
                    expected.put(key, a);
                }
                a[0] = Math.min(a[0], w);
                a[1] = Math.max(a[1], w);
                a[2] += 1;
                totalDocs++;
            }
            client().admin().indices().prepareRefresh(source).get();
        }

        assertFoldEquals(view, def, nsf, expected, totalDocs);
        deleteViewAndAssertPollerStops(view);
    }

    // ══════════════════════════════════════════════════════════════════════
    // 6. Rejections through MVValidateAction (machine-readable reason codes).
    // ══════════════════════════════════════════════════════════════════════

    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testValidateRejections() throws Exception {
        String source = "src_reject";
        createCompositeSource(source, "RegionID", "type=long", "URL", "type=keyword", "AdvEngineID", "type=long");

        // (a) Unknown column -> NATIVE_VALIDATION_REJECTED.
        MVDefinitionDescriptor unknownCol = MVDefinitionDescriptor.of(
            List.of(GroupKeyDescriptor.plain("DoesNotExist", GroupKey.ColumnType.LONG)),
            List.of(AggregateDescriptor.count("cnt"))
        );
        MVValidateResponse rUnknown = validate(source, unknownCol);
        assertFalse("unknown column must be rejected", rUnknown.isValid());
        assertEquals(MVValidationReasons.NATIVE_VALIDATION_REJECTED, rUnknown.reasonCode());

        // (b) LONG key declared over a keyword source field -> SCHEMA_MISMATCH.
        MVDefinitionDescriptor typeMismatch = MVDefinitionDescriptor.of(
            List.of(GroupKeyDescriptor.plain("URL", GroupKey.ColumnType.LONG)),
            List.of(AggregateDescriptor.count("cnt"))
        );
        MVValidateResponse rMismatch = validate(source, typeMismatch);
        assertFalse("type mismatch must be rejected", rMismatch.isValid());
        assertEquals(MVValidationReasons.SCHEMA_MISMATCH, rMismatch.reasonCode());

        // (c) Tampered integrity hash -> DESCRIPTOR_COMPILE_FAILED.
        MVDefinitionDescriptor tampered = MVDefinitionDescriptor.create(
            List.of(GroupKeyDescriptor.plain("RegionID", GroupKey.ColumnType.LONG)),
            List.of(AggregateDescriptor.count("cnt"), AggregateDescriptor.sum("AdvEngineID", "adv")),
            null,
            null,
            "deadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeefdeadbeef"
        );
        MVValidateResponse rTamper = validate(source, tampered);
        assertFalse("tampered hash must be rejected", rTamper.isValid());
        assertEquals(MVValidationReasons.DESCRIPTOR_COMPILE_FAILED, rTamper.reasonCode());

        // (d) Zero group keys (raw JSON, cannot be built through the Java ctor)
        //     -> DESCRIPTOR_PARSE_FAILED.
        String zeroKeyJson = "{\"descriptor_version\":1,\"group_keys\":[],\"aggregates\":[{\"function\":\"COUNT\",\"alias\":\"cnt\"}]}";
        MVValidateResponse rZero = client().execute(
            MVValidateAction.INSTANCE,
            new MVValidateRequest(source, zeroKeyJson, null, null)
        ).actionGet();
        assertFalse("zero group keys must be rejected", rZero.isValid());
        assertEquals(MVValidationReasons.DESCRIPTOR_PARSE_FAILED, rZero.reasonCode());

        // (e) Request-level validation: no descriptor / ppl / sql at all ->
        //     ActionRequestValidationException surfaced by the transport layer.
        ActionRequestValidationException are = expectThrows(
            ActionRequestValidationException.class,
            () -> client().execute(MVValidateAction.INSTANCE, new MVValidateRequest(source, null, null, null)).actionGet()
        );
        assertTrue(are.getMessage().contains("descriptor") || are.getMessage().contains("ppl") || are.getMessage().contains("sql"));

        // Missing source index -> SOURCE_INDEX_NOT_FOUND (reasoned, not thrown).
        MVDefinitionDescriptor okDesc = MVDefinitionDescriptor.of(
            List.of(GroupKeyDescriptor.plain("RegionID", GroupKey.ColumnType.LONG)),
            List.of(AggregateDescriptor.count("cnt"))
        );
        MVValidateResponse rNoSource = validate("does_not_exist_index", okDesc);
        assertFalse(rNoSource.isValid());
        assertEquals(MVValidationReasons.SOURCE_INDEX_NOT_FOUND, rNoSource.reasonCode());
    }

    // ══════════════════════════════════════════════════════════════════════
    // 7. Lifecycle: create -> publish -> full cluster restart -> descriptor
    //    still resolves (poller restarts, watermark continues after more
    //    ingest); delete -> poller stops; recreate same name works.
    // ══════════════════════════════════════════════════════════════════════

    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testLifecycleRestartDeleteRecreate() throws Exception {
        String source = "src_lifecycle";
        String view = "mv_lifecycle";
        createCompositeSource(source, "RegionID", "type=long", "AdvEngineID", "type=long");

        MVDefinitionDescriptor descriptor = MVDefinitionDescriptor.of(
            List.of(GroupKeyDescriptor.plain("RegionID", GroupKey.ColumnType.LONG)),
            List.of(AggregateDescriptor.count("cnt"), AggregateDescriptor.sum("AdvEngineID", "adv"))
        );
        MVValidateResponse validation = validateOk(source, descriptor);
        MVCompiledDefinition def = MVCompiledDefinition.fromDescriptor(descriptor);
        List<MVDefinitionValidator.StateField> nsf = validation.nativeStateFields();

        createView(view, source, descriptor);

        Map<List<String>, long[]> expected = new HashMap<>();
        int totalDocs = ingestBaselineBatch(source, expected, 0, 120);
        client().admin().indices().prepareRefresh(source).get();
        assertFoldEquals(view, def, nsf, expected, totalDocs);

        long watermarkBeforeRestart = published(view).watermark().seqNo();

        // Full cluster restart — the descriptor persisted in target IndexMetadata
        // must resolve on recovery and the poller must reconstruct itself.
        int nodesBeforeRestart = internalCluster().size();
        internalCluster().fullRestart();
        ensureStableCluster(nodesBeforeRestart);
        ensureGreen(source, view);
        assertPollerRunning(view);
        assertEquals(
            "durable MV watermark must survive full restart",
            watermarkBeforeRestart,
            published(view).watermark().seqNo()
        );

        // Continue ingest after restart; watermark must advance from where it left off.
        totalDocs = ingestBaselineBatch(source, expected, totalDocs, 60);
        client().admin().indices().prepareRefresh(source).get();
        assertFoldEquals(view, def, nsf, expected, totalDocs);

        // Delete the view -> poller stops.
        deleteViewAndAssertPollerStops(view);

        // Recreate the same view name -> works, and a fresh poller catches up
        // over the (now larger) source prefix.
        createView(view, source, descriptor);
        final int expectedDocsForRecreate = totalDocs;
        // The recreated target rebuilds the full prefix from watermark EMPTY.
        assertPollerRunning(view);
        assertFoldEquals(view, def, nsf, expected, expectedDocsForRecreate);

        deleteViewAndAssertPollerStops(view);
    }

    // ══════════════════════════════════════════════════════════════════════
    // 8. Legacy + descriptor coexistence on the SAME source: a legacy named
    //    definition target (definition_id=pull_count_sum) and a
    //    descriptor-created target both publish correctly.
    // ══════════════════════════════════════════════════════════════════════

    @LockFeatureFlag(STREAM_TRANSPORT)
    public void testLegacyAndDescriptorCoexistence() throws Exception {
        String source = "src_coexist";
        String legacyView = "mv_legacy";
        String dynView = "mv_dynamic";
        createCompositeSource(source, "RegionID", "type=long", "AdvEngineID", "type=long");

        // Descriptor equivalent to pull_count_sum: GROUP BY RegionID, COUNT(*), SUM(AdvEngineID).
        MVDefinitionDescriptor descriptor = MVDefinitionDescriptor.of(
            List.of(GroupKeyDescriptor.plain("RegionID", GroupKey.ColumnType.LONG)),
            List.of(AggregateDescriptor.count("cnt"), AggregateDescriptor.sum("AdvEngineID", "adv"))
        );
        MVValidateResponse validation = validateOk(source, descriptor);
        MVCompiledDefinition def = MVCompiledDefinition.fromDescriptor(descriptor);
        List<MVDefinitionValidator.StateField> nsf = validation.nativeStateFields();

        // Legacy target: created the "old way" (named definition_id, no descriptor).
        createLegacyTarget(legacyView, source);

        // Descriptor target via the dynamic control-plane action.
        createView(dynView, source, descriptor);

        Map<List<String>, long[]> expected = new HashMap<>();
        int totalDocs = 0;
        for (int gen = 0; gen < 2; gen++) {
            totalDocs = ingestBaselineBatch(source, expected, totalDocs, 100);
            client().admin().indices().prepareRefresh(source).get();
        }

        // Both targets fold to identical, exact per-group state.
        assertPollerRunning(legacyView);
        assertPollerRunning(dynView);

        assertFoldEquals(legacyView, def, nsf, expected, totalDocs);
        assertFoldEquals(dynView, def, nsf, expected, totalDocs);

        deleteViewAndAssertPollerStops(dynView);
        deleteViewAndAssertPollerStops(legacyView);
    }

    // ══════════════════════════════════════════════════════════════════════
    // Helpers
    // ══════════════════════════════════════════════════════════════════════

    /** Create a composite (parquet primary + lucene secondary) remote-backed source index. */
    private void createCompositeSource(String name, String... mapping) {
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(name)
                .setSettings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        .put("index.pluggable.dataformat.enabled", true)
                        .put("index.pluggable.dataformat", "composite")
                        .put("index.composite.primary_data_format", "parquet")
                        .putList("index.composite.secondary_data_formats", "lucene")
                        .put("index.composite.merge_on_refresh_max_size", "0b")
                        .put("index.refresh_interval", "1s")
                        .put("index.derived.enabled", false)
                )
                .setMapping(mapping)
        );
        ensureGreen(name);
        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setPersistentSettings(Settings.builder().put("cluster.pluggable.dataformat", "composite"))
            .get();
    }

    /** Create a materialized-view target the "old way": named legacy definition, no descriptor. */
    private void createLegacyTarget(String target, String source) {
        assertAcked(
            client().admin()
                .indices()
                .prepareCreate(target)
                .setSettings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        .put("index.pluggable.dataformat.enabled", true)
                        .put("index.pluggable.dataformat", "composite")
                        .put("index.composite.primary_data_format", "parquet")
                        .putList("index.composite.secondary_data_formats", "lucene")
                        .put("index.derived.enabled", true)
                        .put(DerivedIndexBinding.KEY_DATA_FORMAT, MVDataFormat.NAME)
                        .put("index.mv.definition", "pull_count_sum")
                        .putList("index.mv.state_fields", "RegionID", "cnt", "adv")
                        .put("index.mv.state_merge_enabled", true)
                        .put(MVPullSettings.PULL_INTERVAL.getKey(), "100ms")
                        .put(DerivedIndexBinding.KEY_SOURCE_NAME, source)
                        .put(DerivedIndexBinding.KEY_DEFINITION_ID, "pull_count_sum")
                )
                .setMapping("RegionID", "type=long", "cnt", "type=long", "adv", "type=long")
        );
        ensureGreen(target);
    }

    /** Ingest a deterministic baseline batch (region=i%8, adv=i%5); returns the new total doc count. */
    private int ingestBaselineBatch(String source, Map<List<String>, long[]> expected, int startDoc, int count) {
        int total = startDoc;
        for (int i = 0; i < count; i++) {
            long region = total % 8;
            long adv = total % 5;
            client().prepareIndex(source).setSource("RegionID", region, "AdvEngineID", adv).get();
            List<String> key = List.of(Long.toString(region));
            long[] a = expected.computeIfAbsent(key, k -> new long[2]);
            a[0] += 1;
            a[1] += adv;
            total++;
        }
        return total;
    }

    /** Invoke transport {@link MVValidateAction} and return the (possibly rejected) response. */
    private MVValidateResponse validate(String source, MVDefinitionDescriptor descriptor) {
        String json = MVDefinitionResolver.serialize(descriptor);
        return client().execute(MVValidateAction.INSTANCE, new MVValidateRequest(source, json, null, null)).actionGet();
    }

    /** Validate, asserting success, and return the response (carrying native state fields). */
    private MVValidateResponse validateOk(String source, MVDefinitionDescriptor descriptor) {
        MVValidateResponse r = validate(source, descriptor);
        assertTrue("validation must succeed: reason=" + r.reasonCode() + " msg=" + r.message() + " mismatches=" + r.mismatches(), r.isValid());
        assertFalse("native state fields must be reported", r.nativeStateFields().isEmpty());
        return r;
    }

    /** Create a materialized view via transport {@link MVCreateViewAction}; wait for the poller to start. */
    private void createView(String view, String source, MVDefinitionDescriptor descriptor) throws Exception {
        String json = MVDefinitionResolver.serialize(descriptor);
        MVCreateViewResponse resp = client().execute(
            MVCreateViewAction.INSTANCE,
            new MVCreateViewRequest(view, source, json, null, null, view, "100ms")
        ).actionGet();
        assertTrue("view creation must be acknowledged", resp.isAcknowledged());
        ensureGreen(view);
        assertPollerRunning(view);
    }

    /** Delete the target index (stops the derived poller) and assert its poller is gone. */
    private void deleteViewAndAssertPollerStops(String view) throws Exception {
        // Capture the primary node + shard id BEFORE deletion — once the index is
        // gone its routing table and metadata disappear.
        String node = primaryNodeName(view);
        org.opensearch.core.index.shard.ShardId shardId = new org.opensearch.core.index.shard.ShardId(
            getClusterState().metadata().index(view).getIndex(),
            0
        );
        assertAcked(client().admin().indices().prepareDelete(view));
        assertBusy(
            () -> assertNull("poller must stop on target deletion", pullService(node).getPoller(shardId)),
            60,
            TimeUnit.SECONDS
        );
    }

    /** Assert the derived poller for this view's shard 0 is running (per-shard, not node-global). */
    private void assertPollerRunning(String view) throws Exception {
        assertBusy(() -> {
            String node = primaryNodeName(view);
            org.opensearch.core.index.shard.ShardId shardId = new org.opensearch.core.index.shard.ShardId(
                getClusterState().metadata().index(view).getIndex(),
                0
            );
            assertNotNull("MV shard poller must be running for [" + view + "]", pullService(node).getPoller(shardId));
        }, 60, TimeUnit.SECONDS);
    }

    private org.opensearch.index.engine.derived.pull.NodeDerivedPullService pullService(String node) {
        return internalCluster().getInstance(PluginsService.class, node).filterPlugins(MVDataFormatPlugin.class).get(0).pullService();
    }

    /**
     * Assert the MV catalog has caught up to the full source prefix and folds to
     * exactly {@code expected} per group.
     */
    private void assertFoldEquals(
        String view,
        MVCompiledDefinition def,
        List<MVDefinitionValidator.StateField> nsf,
        Map<List<String>, long[]> expected,
        long expectedDocs
    ) throws Exception {
        assertBusy(() -> {
            PublishedState ps = published(view);
            assertFalse("MV catalog must contain mv_state files", ps.files().isEmpty());
            assertEquals("catalog watermark must cover exactly the source prefix", expectedDocs - 1L, ps.watermark().seqNo());
            Map<List<String>, long[]> actual = fold(def, nsf, ps.files());
            assertEquals("group cardinality", expected.size(), actual.size());
            for (Map.Entry<List<String>, long[]> e : expected.entrySet()) {
                long[] got = actual.get(e.getKey());
                assertNotNull("missing MV group " + e.getKey() + " (actual keys=" + actual.keySet() + ")", got);
                assertArrayEquals("folded state for group " + e.getKey(), e.getValue(), got);
            }
        }, 90, TimeUnit.SECONDS);
    }

    /**
     * Fold the state files through DataFusion using the ENGINE-REPORTED physical
     * state-field names, returning group-key tuple -> folded aggregate outputs.
     * AVG contributes two outputs (count, sum); every other aggregate one.
     */
    private Map<List<String>, long[]> fold(MVCompiledDefinition def, List<MVDefinitionValidator.StateField> nsf, List<String> stateFiles) {
        int ng = def.groupKeys().size();
        String sql = buildFoldSql(def, nsf);
        int outCols = 0;
        for (AggregateSpec agg : def.aggregates()) {
            outCols += agg.function() == AggregateSpec.AggFunction.AVG ? 2 : 1;
        }
        String output = MVNativeBridge.searchV2(stateFiles, sql);
        Map<List<String>, long[]> actual = new LinkedHashMap<>();
        for (String line : output.lines().filter(v -> v.isBlank() == false).toList()) {
            String[] cells = line.split("\\t");
            assertEquals("unexpected fold row arity: " + line, ng + outCols, cells.length);
            List<String> key = new ArrayList<>(ng);
            for (int i = 0; i < ng; i++) {
                key.add(cells[i].trim());
            }
            long[] vals = new long[outCols];
            for (int j = 0; j < outCols; j++) {
                vals[j] = Long.parseLong(cells[ng + j].trim());
            }
            actual.put(List.copyOf(key), vals);
        }
        return actual;
    }

    /** Build the fold SQL over {@code __MV_STATES__} using physical state names reported by validation. */
    private static String buildFoldSql(MVCompiledDefinition def, List<MVDefinitionValidator.StateField> nsf) {
        int ng = def.groupKeys().size();
        // Group keys are folded/grouped by their ENGINE-reported physical name.
        // For plain columns that equals the alias (e.g. "RegionID"); for a derived
        // (expression) key it is the Partial-stage expression name
        // (e.g. "mv_input.EventTime / Int64(300000)") — the name the state file carries.
        List<String> keyCols = new ArrayList<>(ng);
        for (int i = 0; i < ng; i++) {
            keyCols.add("\"" + nsf.get(i).name() + "\"");
        }
        StringBuilder sb = new StringBuilder("SELECT ");
        sb.append(String.join(", ", keyCols));
        int pos = ng;
        int out = 0;
        for (AggregateSpec agg : def.aggregates()) {
            switch (agg.function()) {
                case COUNT, SUM -> {
                    sb.append(", SUM(\"").append(nsf.get(pos).name()).append("\") AS c").append(out++);
                    pos += 1;
                }
                case MIN -> {
                    sb.append(", MIN(\"").append(nsf.get(pos).name()).append("\") AS c").append(out++);
                    pos += 1;
                }
                case MAX -> {
                    sb.append(", MAX(\"").append(nsf.get(pos).name()).append("\") AS c").append(out++);
                    pos += 1;
                }
                case AVG -> {
                    sb.append(", SUM(\"").append(nsf.get(pos).name()).append("\") AS c").append(out++);
                    sb.append(", SUM(\"").append(nsf.get(pos + 1).name()).append("\") AS c").append(out++);
                    pos += 2;
                }
            }
        }
        sb.append(" FROM __MV_STATES__ GROUP BY ").append(String.join(", ", keyCols));
        sb.append(" ORDER BY ").append(String.join(", ", keyCols));
        return sb.toString();
    }

    /** Read the published MV state files + watermark from the primary's catalog snapshot. */
    private PublishedState published(String view) throws Exception {
        String nodeName = primaryNodeName(view);
        org.opensearch.indices.IndicesService indicesService = internalCluster().getInstance(
            org.opensearch.indices.IndicesService.class,
            nodeName
        );
        org.opensearch.index.shard.IndexShard shard = indicesService.indexServiceSafe(getClusterState().metadata().index(view).getIndex())
            .getShard(0);
        try (
            org.opensearch.common.concurrent.GatedCloseable<org.opensearch.index.engine.exec.coord.CatalogSnapshot> ref = shard
                .getCatalogSnapshot()
        ) {
            List<String> files = ref.get()
                .getSearchableFiles(MVStateDataFormat.NAME)
                .stream()
                .flatMap(fileSet -> fileSet.files().stream().map(file -> Path.of(fileSet.directory()).resolve(file).toString()))
                .sorted()
                .toList();
            String encoded = ref.get().getUserData().get(MVWatermark.key(0));
            assertNotNull("MV catalog must contain watermark", encoded);
            return new PublishedState(files, MVWatermark.decode(encoded));
        }
    }

    private record PublishedState(List<String> files, MVWatermark watermark) {
    }
}
