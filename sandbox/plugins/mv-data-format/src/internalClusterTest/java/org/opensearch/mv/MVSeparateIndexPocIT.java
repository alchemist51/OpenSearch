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
import org.opensearch.action.index.IndexResponse;
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
import org.opensearch.test.MockLogAppender;
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
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 2)
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
            MVStateDataFormatPlugin.class,
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

    public void testShipBeforeCommitAndFoldOnRead() throws Exception {
        createSourceIndex();
        createTargetIndex();
        assertColocated();

        // With the pair colocated (asserted above), the ship must take the
        // LOCAL apply path — never the remote forward. A remote or missing
        // apply event fails the test.
        try (
            MockLogAppender appender = MockLogAppender.createForLoggers(
                LogManager.getLogger("org.opensearch.mv.MVShipStateTransportHandler")
            )
        ) {
            appender.addExpectation(
                new MockLogAppender.SeenEventExpectation(
                    "local ship apply",
                    "org.opensearch.mv.MVShipStateTransportHandler",
                    Level.INFO,
                    "*mv ship-apply path=local*"
                )
            );
            appender.addExpectation(
                new MockLogAppender.UnseenEventExpectation(
                    "no remote forward while colocated",
                    "org.opensearch.mv.MVShipStateTransportHandler",
                    Level.INFO,
                    "*mv ship-apply path=remote*"
                )
            );

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

            appender.assertAllExpectationsMatched();
        }

        // With the pair colocated, every ship must take the LOCAL apply path
        // (no serialization). The log fires from the transport handler before
        // the durable apply, so this also proves the shard-addressed routing.
        // (Asserted via log scan below; the routing decision is logged at INFO.)

        // THE SUPERSET GUARANTEE (design contract): the ack certifies durable
        // AND searchable (the target's snapshot published BEFORE the ack), and
        // the source committed after the ack — the target must already hold
        // the complete folded state with NO explicit target refresh here.
        // (The composite target has no classic _search path; the final fold
        // over its mv_state files IS the read.)
        assertGoldenFoldOverTargetState(TARGET);

        // The target's OWN derived format: every target generation carries an
        // mv_state file set with FOLDED state (the fold definition ran over
        // the shipped rows at the target's refresh).
        assertTargetHasFoldedStateFiles();
    }

    public void testShipFailureFailsTheFlushAndRetryHeals() throws Exception {
        createSourceIndex();
        createTargetIndex();
        assertColocated();

        indexDoc("api", "200", 30);
        client().admin().indices().prepareRefresh(SOURCE).get();
        assertFalse("derived target state must be searchable after the ack", targetStateFiles(TARGET).isEmpty());

        // Break the replication precondition: delete the managed target.
        // Dedicated derived-state replication never auto-creates an index.
        assertAcked(client().admin().indices().prepareDelete(TARGET));
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

        // Heal by recreating the managed derived target, then retry failed
        // allocations until source recovery performs cursor pull catch-up.
        createTargetIndex();
        assertBusy(() -> {
            client().admin().cluster().prepareReroute().setRetryFailed(true).get();
            org.opensearch.action.admin.cluster.health.ClusterHealthResponse health = client().admin()
                .cluster()
                .prepareHealth(TARGET, SOURCE)
                .setWaitForGreenStatus()
                .setTimeout(org.opensearch.common.unit.TimeValue.timeValueSeconds(5))
                .get();
            assertFalse("cluster must settle green after target reopen", health.isTimedOut());
        }, 90, java.util.concurrent.TimeUnit.SECONDS);
        assertBusy(() -> {
            try {
                client().admin().indices().prepareRefresh(SOURCE).get();
            } catch (Exception e) {
                throw new AssertionError("flush still failing after target recreated", e);
            }
        });
        // No target refresh (superset guarantee holds through the heal too).
        assertBusy(() -> {
            java.util.List<String> files = targetStateFiles(TARGET);
            assertFalse("derived state must exist after heal", files.isEmpty());
            String folded = MVNativeBridge.searchV2(files, MVConstants.TARGET_FOLD_SEARCH_SQL);
            assertTrue("web/200 state must arrive after heal, saw: " + folded, folded.contains("web\t200"));
        });
    }

    public void testEmptyRefreshHealsRecreatedTargetFromCommittedCursor() throws Exception {
        createSourceIndex();
        createTargetIndex();
        assertColocated();

        indexDoc("api", "200", 30);
        client().admin().indices().prepareRefresh(SOURCE).get();
        assertFalse("initial derived state must be searchable", targetStateFiles(TARGET).isEmpty());

        // Recreate the target without adding another source document. A
        // no-translog derived target falls back to its committed cursor, so
        // even an otherwise empty source refresh must run range reconciliation.
        assertAcked(client().admin().indices().prepareDelete(TARGET));
        createTargetIndex();
        // Generation is provenance, not range authority. Simulate a
        // conservative/incomparable generation watermark with an empty
        // checkpoint: recovery must still scan by _seq_no and pull the row.
        MVTargetCursorLedger.seed(TARGET, 0, SOURCE, 0, new MVTargetCursorLedger.Cursor(Long.MAX_VALUE, -1L));
        org.opensearch.action.support.broadcast.BroadcastResponse refresh = client().admin().indices().prepareRefresh(SOURCE).get();
        assertEquals("empty source refresh must reconcile the recreated target", 0, refresh.getFailedShards());

        assertBusy(() -> {
            java.util.List<String> files = targetStateFiles(TARGET);
            assertFalse("reconciled target state must exist", files.isEmpty());
            String folded = MVNativeBridge.searchV2(files, MVConstants.TARGET_FOLD_SEARCH_SQL);
            assertTrue("api/200 state must be pulled without a new source write, saw: " + folded, folded.contains("api\t200"));
        });
    }

    /**
     * One finalized state batch, TWO targets: the ref-counted handoff shares
     * the same Arrow buffers across both ships — no destination frees the
     * batch under the other (a refcount bug surfaces as a use-after-free,
     * a double-free IllegalState, or an allocator leak — the test JVM runs
     * with arrow.memory.debug.allocator). Both targets must independently
     * fold to the exact goldens.
     */
    public void testOneBatchShipsToMultipleTargets() throws Exception {
        String target2 = "mv_payments_2";
        createSourceIndexWithTargets(TARGET, target2);
        createTargetIndex(TARGET);
        createTargetIndex(target2);
        assertColocated();

        seedGoldenSegments();

        assertGoldenFoldOverTargetState(TARGET);
        assertGoldenFoldOverTargetState(target2);
    }

    public void testDerivedTargetRejectsUserWritesAndMappingChanges() throws Exception {
        createSourceIndex();
        createTargetIndex();

        org.opensearch.index.engine.DataFormatAwareEngine engine = targetEngine(TARGET);
        assertThat(engine, org.hamcrest.Matchers.instanceOf(org.opensearch.index.engine.DerivedIndexEngine.class));
        assertEquals(0, engine.translogManager().getTranslogStats().estimatedNumberOfOperations());

        Exception writeFailure = expectThrows(
            Exception.class,
            () -> client().prepareIndex(TARGET).setSource("service", "forbidden", "status", "500", "cnt", 1L).get()
        );
        assertTrue(
            "user write must be rejected by the derived engine: " + writeFailure,
            org.opensearch.ExceptionsHelper.stackTrace(writeFailure).contains("use derived-state replication")
        );

        Exception mappingFailure = expectThrows(
            Exception.class,
            () -> client().admin().indices().preparePutMapping(TARGET).setSource("intruder", "type=keyword").get()
        );
        assertTrue(
            "mapping update must be rejected as replication-managed: " + mappingFailure,
            org.opensearch.ExceptionsHelper.stackTrace(mappingFailure).contains("replication-managed mapping")
        );
    }

    /**
     * Final fold over the target's OWN mv_state files == the goldens. This is
     * the composite target's read (production: the analytics engine path).
     */
    private void assertGoldenFoldOverTargetState(String targetIndex) throws Exception {
        java.util.List<String> files = targetStateFiles(targetIndex);
        assertFalse("target [" + targetIndex + "] must have mv_state files", files.isEmpty());
        String result = MVNativeBridge.searchV2(files, MVConstants.TARGET_FOLD_SEARCH_SQL);
        assertEquals(
            "api\t200\t4\t115\t10\t50\n" + "api\t500\t1\t900\t900\t900\n" + "batch\t200\t1\t60\t60\t60\n" + "web\t200\t2\t120\t40\t80\n",
            result
        );
    }

    private java.util.List<String> targetStateFiles(String targetIndex) throws Exception {
        java.util.List<String> files = new java.util.ArrayList<>();
        try (
            org.opensearch.common.concurrent.GatedCloseable<org.opensearch.index.engine.exec.coord.CatalogSnapshot> ref = targetEngine(
                targetIndex
            ).acquireSnapshot()
        ) {
            for (org.opensearch.index.engine.exec.WriterFileSet set : ref.get().getSearchableFiles(MVStateDataFormat.NAME)) {
                for (String f : set.files()) {
                    files.add(java.nio.file.Path.of(set.directory()).resolve(f).toString());
                }
            }
        }
        return files;
    }

    private org.opensearch.index.engine.DataFormatAwareEngine targetEngine(String targetIndex) {
        String nodeName = getClusterState().nodes()
            .get(getClusterState().routingTable().index(targetIndex).shard(0).primaryShard().currentNodeId())
            .getName();
        org.opensearch.indices.IndicesService indicesService = internalCluster().getInstance(
            org.opensearch.indices.IndicesService.class,
            nodeName
        );
        org.opensearch.index.shard.IndexShard shard = indicesService.indexServiceSafe(
            getClusterState().metadata().index(targetIndex).getIndex()
        ).getShard(0);
        return (org.opensearch.index.engine.DataFormatAwareEngine) org.opensearch.index.shard.IndexShardTestCase.getIndexer(shard);
    }

    /**
     * Asserts the target's catalog snapshot carries mv_state file sets — the
     * target's derived format materialized folded state per generation.
     */
    private void assertTargetHasFoldedStateFiles() throws Exception {
        try (
            org.opensearch.common.concurrent.GatedCloseable<org.opensearch.index.engine.exec.coord.CatalogSnapshot> ref = targetEngine(
                TARGET
            ).acquireSnapshot()
        ) {
            java.util.Collection<org.opensearch.index.engine.exec.WriterFileSet> stateSets = ref.get()
                .getSearchableFiles(MVStateDataFormat.NAME);
            assertFalse("target must carry mv_state file sets (folded state per generation)", stateSets.isEmpty());
            long foldedRows = stateSets.stream().mapToLong(org.opensearch.index.engine.exec.WriterFileSet::numRows).sum();
            // Each target generation folds ITS shipped rows: gen1 folds 3 source-
            // gen-1 rows -> 3 groups; gen2 folds 3 source-gen-2 rows -> 3 groups.
            // (Cross-generation folding happens at target MERGE — disabled here.)
            assertEquals("folded state rows across target generations", 6, foldedRows);
        }
    }

    /** Golden dataset: 2 segments, 8 docs -> 6 state rows (3 per generation). */
    private void seedGoldenSegments() {
        indexDoc("api", "200", 30);
        indexDoc("api", "200", 50);
        indexDoc("web", "200", 40);
        indexDoc("api", "500", 900);
        indexDoc("api", "200", 25);
        client().admin().indices().prepareRefresh(SOURCE).get();
        indexDoc("api", "200", 10);
        indexDoc("web", "200", 80);
        indexDoc("batch", "200", 60);
        client().admin().indices().prepareRefresh(SOURCE).get();
    }

    // ── Infrastructure ──────────────────────────────────────────────────────

    private void createSourceIndex() {
        createSourceIndexWithTargets(TARGET);
    }

    private void createSourceIndexWithTargets(String... targets) {
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
                    .putList(MVConstants.SHIP_TARGETS_SETTING, targets)
            )
            .setMapping("service", "type=keyword", "status", "type=keyword", "latency_ms", "type=long")
            .get();
        ensureGreen(SOURCE);
    }

    private void createTargetIndex() {
        createTargetIndex(TARGET);
    }

    private void createPlainTargetIndex(String name) {
        client().admin()
            .indices()
            .prepareCreate(name)
            .setSettings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(MVConstants.COLOCATE_WITH_SETTING, SOURCE)
            )
            .setMapping(MVViewsService.TargetCreator.targetMapping("payments"))
            .get();
        ensureGreen(name);
    }

    private void createTargetIndex(String name) {
        // The target is a first-class derived index: parquet stores the
        // replicated state rows, mv_state folds them, and Lucene is only a
        // query-capability projection (never recovery authority). No target
        // translog or user write path participates.
        client().admin()
            .indices()
            .prepareCreate(name)
            .setSettings(
                Settings.builder()
                    .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
                    .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                    .put(MVConstants.DERIVED_INDEX_SETTING, true)
                    .put("index.append_only.enabled", true)
                    .put("index.pluggable.dataformat.enabled", true)
                    .put("index.pluggable.dataformat", "composite")
                    .put("index.composite.primary_data_format", "parquet")
                    .putList("index.composite.secondary_data_formats", "lucene", "mv_state")
                    .put("index.composite.merge_on_refresh_max_size", "0b")
                    .put(MVConstants.COLOCATE_WITH_SETTING, SOURCE)
            )
            .setMapping(MVViewsService.TargetCreator.targetMapping("payments"))
            .get();
        ensureGreen(name);
    }

    /**
     * The colocation decider must put the target's primary on the node holding
     * the source's primary (ordinal pairing, 2 data nodes make this a real
     * constraint rather than a tautology).
     */
    private void assertColocated() {
        String sourceNode = getClusterState().routingTable().index(SOURCE).shard(0).primaryShard().currentNodeId();
        String targetNode = getClusterState().routingTable().index(TARGET).shard(0).primaryShard().currentNodeId();
        assertEquals("MV target primary must colocate with the source primary", sourceNode, targetNode);
    }

    private void indexDoc(String service, String status, long latencyMs) {
        IndexResponse r = client().prepareIndex()
            .setIndex(SOURCE)
            .setSource("service", service, "status", status, "latency_ms", latencyMs)
            .get();
        assertEquals(RestStatus.CREATED, r.status());
    }
}
