/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.derived.pull;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.TestShardRouting;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.derived.pull.spi.BuildResult;
import org.opensearch.index.engine.derived.pull.spi.DerivedArtifactBuilder;
import org.opensearch.index.engine.derived.pull.spi.DerivedPullFormat;
import org.opensearch.index.engine.derived.pull.spi.DerivedSourceReader;
import org.opensearch.index.engine.derived.pull.spi.DerivedSourceSnapshot;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardState;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.IntFunction;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Defect #26 poller lifecycle: node-wide catch-up pressure is held while
 * rounds are capped or failing, and released the moment the poller is caught
 * up (uncapped success, no new data) or closed. A steady-state poller never
 * claims at all. Rounds are driven manually (mock ThreadPool) so every
 * transition is deterministic.
 */
public class DerivedShardPollerCatchUpPressureTests extends OpenSearchTestCase {

    private Path tempDir;
    private ThreadPool noopThreadPool;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        tempDir = createTempDir("derived_pressure_test");
        noopThreadPool = mock(ThreadPool.class); // schedule() no-ops: rounds run manually
        DerivedCatchUpPressure.clearForTests();
    }

    @Override
    public void tearDown() throws Exception {
        DerivedCatchUpPressure.clearForTests();
        super.tearDown();
    }

    private IndexShard mockPrimaryShard() throws IOException {
        IndexShard shard = mock(IndexShard.class);
        ShardId shardId = new ShardId(new Index("test_mv", UUID.randomUUID().toString()), 0);
        when(shard.shardId()).thenReturn(shardId);
        when(shard.state()).thenReturn(IndexShardState.STARTED);
        ShardRouting routing = TestShardRouting.newShardRouting(
            shardId,
            "node1",
            true,
            org.opensearch.cluster.routing.ShardRoutingState.STARTED
        );
        when(shard.routingEntry()).thenReturn(routing);
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_INDEX_UUID, shardId.getIndex().getUUID())
            .build();
        IndexMetadata metadata = IndexMetadata.builder(shardId.getIndex().getName()).settings(indexSettings).build();
        when(shard.indexSettings()).thenReturn(new IndexSettings(metadata, Settings.EMPTY));
        Path shardDataPath = tempDir.resolve(shardId.getIndex().getUUID()).resolve(Integer.toString(shardId.id()));
        Files.createDirectories(shardDataPath);
        when(shard.shardPath()).thenReturn(new ShardPath(false, shardDataPath, shardDataPath, shardId));
        return shard;
    }

    private static DerivedSourceSnapshot snapshot(long watermark) {
        return new DerivedSourceSnapshot() {
            @Override
            public String shardId() {
                return "test:0";
            }

            @Override
            public long watermark() {
                return watermark;
            }

            @Override
            public Map<String, String> metadata() {
                return Map.of();
            }
        };
    }

    /**
     * Format whose reader serves {@code snapshotRef} and whose builder result
     * is chosen per build invocation (1-based) by {@code resultForBuild}.
     */
    private static DerivedPullFormat scriptedFormat(
        AtomicReference<DerivedSourceSnapshot> snapshotRef,
        IntFunction<BuildResult> resultForBuild
    ) {
        AtomicInteger builds = new AtomicInteger();
        return new DerivedPullFormat() {
            @Override
            public String formatId() {
                return "materialized_view";
            }

            @Override
            public DerivedSourceReader createReader(Settings nodeSettings, IndexSettings indexSettings) {
                return new DerivedSourceReader() {
                    @Override
                    public DerivedSourceSnapshot fetchSnapshot(ShardRouting shard, long sinceWatermark) {
                        DerivedSourceSnapshot snap = snapshotRef.get();
                        return (snap != null && snap.watermark() > sinceWatermark) ? snap : null;
                    }

                    @Override
                    public void downloadToStage(DerivedSourceSnapshot snapshot, Path stageDir) throws IOException {
                        Files.writeString(stageDir.resolve("staged.dat"), "x");
                    }

                    @Override
                    public void close() {}
                };
            }

            @Override
            public DerivedArtifactBuilder createArtifactBuilder(Settings nodeSettings, IndexSettings indexSettings) {
                return new DerivedArtifactBuilder() {
                    @Override
                    public BuildResult build(DerivedSourceSnapshot snapshot, Path stageDir, IndexShard shard) {
                        return resultForBuild.apply(builds.incrementAndGet());
                    }

                    @Override
                    public void close() {}
                };
            }
        };
    }

    private static BuildResult result(boolean success, Map<String, Object> stats) {
        return new BuildResult() {
            @Override
            public boolean success() {
                return success;
            }

            @Override
            public String artifactId() {
                return "artifact";
            }

            @Override
            public Map<String, Object> stats() {
                return stats;
            }
        };
    }

    public void testCappedRoundHoldsPressureUncappedReleases() throws Exception {
        AtomicReference<DerivedSourceSnapshot> snap = new AtomicReference<>(snapshot(100L));
        // build 1: capped at watermark 50 (lag remains) — build 2: uncapped final
        DerivedPullFormat format = scriptedFormat(
            snap,
            build -> build == 1 ? result(true, Map.of("capped", true, "capped_watermark", 50L)) : result(true, Map.of())
        );
        DerivedShardPoller poller = new DerivedShardPoller(mockPrimaryShard(), format, TimeValue.timeValueSeconds(30), noopThreadPool, -1L);
        try {
            poller.run();
            assertTrue("capped round (sustained catch-up) must hold node-wide pressure", DerivedCatchUpPressure.isActive());
            poller.run();
            assertFalse("final uncapped round must release pressure", DerivedCatchUpPressure.isActive());
        } finally {
            poller.close();
        }
    }

    public void testBuildFailureHoldsPressureCloseReleases() throws Exception {
        AtomicReference<DerivedSourceSnapshot> snap = new AtomicReference<>(snapshot(100L));
        DerivedPullFormat format = scriptedFormat(snap, build -> result(false, Map.of()));
        DerivedShardPoller poller = new DerivedShardPoller(mockPrimaryShard(), format, TimeValue.timeValueSeconds(30), noopThreadPool, -1L);
        poller.run();
        assertTrue("failed build (lag remains) must hold pressure across the retry", DerivedCatchUpPressure.isActive());
        poller.close();
        assertFalse("close must resolve a held claim — claims always resolve", DerivedCatchUpPressure.isActive());
    }

    public void testThrowingRoundHoldsPressure() throws Exception {
        AtomicReference<DerivedSourceSnapshot> snap = new AtomicReference<>(snapshot(100L));
        DerivedPullFormat format = scriptedFormat(snap, build -> { throw new RuntimeException("Resources exhausted: pool starved"); });
        DerivedShardPoller poller = new DerivedShardPoller(mockPrimaryShard(), format, TimeValue.timeValueSeconds(30), noopThreadPool, -1L);
        poller.run();
        assertTrue("a throwing round must hold pressure across the backoff", DerivedCatchUpPressure.isActive());
        poller.close();
        assertFalse(DerivedCatchUpPressure.isActive());
    }

    public void testNoNewDataReleasesHeldPressure() throws Exception {
        AtomicReference<DerivedSourceSnapshot> snap = new AtomicReference<>(snapshot(100L));
        DerivedPullFormat format = scriptedFormat(snap, build -> result(true, Map.of("capped", true, "capped_watermark", 50L)));
        DerivedShardPoller poller = new DerivedShardPoller(mockPrimaryShard(), format, TimeValue.timeValueSeconds(30), noopThreadPool, -1L);
        try {
            poller.run();
            assertTrue(DerivedCatchUpPressure.isActive());
            snap.set(null); // source went quiet: next round finds no new data
            poller.run();
            assertFalse("a no-new-data round means caught up — pressure must release", DerivedCatchUpPressure.isActive());
        } finally {
            poller.close();
        }
    }

    public void testSteadyStateNeverClaims() throws Exception {
        AtomicReference<DerivedSourceSnapshot> snap = new AtomicReference<>(snapshot(100L));
        AtomicInteger releaseFires = new AtomicInteger();
        DerivedCatchUpPressure.addOnReleaseListener(releaseFires::incrementAndGet);
        DerivedPullFormat format = scriptedFormat(snap, build -> result(true, Map.of()));
        DerivedShardPoller poller = new DerivedShardPoller(mockPrimaryShard(), format, TimeValue.timeValueSeconds(30), noopThreadPool, -1L);
        try {
            poller.run(); // uncapped success — steady state
            assertFalse(DerivedCatchUpPressure.isActive());
            poller.run(); // caught up — no new data
            assertFalse(DerivedCatchUpPressure.isActive());
            assertEquals("steady state must never claim (so never fire release listeners)", 0, releaseFires.get());
        } finally {
            poller.close();
        }
    }

    /**
     * Defect #28b: pressure that cannot make progress must not hold merges
     * hostage. Builds failing repeatedly with merges already deferred are not
     * starved BY merges — holding forever deadlocks (merges deferred -> native
     * baseline stays high -> builds keep failing -> lag never drops ->
     * pressure never releases). After the third consecutive failure the claim
     * is released so TieredPolicy can consolidate (the observed self-heal
     * mechanism of the pre-#26 runs).
     */
    public void testPressureReleasedAfterConsecutiveFailures() throws Exception {
        AtomicReference<DerivedSourceSnapshot> snap = new AtomicReference<>(snapshot(100L));
        DerivedPullFormat format = scriptedFormat(snap, build -> result(false, Map.of()));
        DerivedShardPoller poller = new DerivedShardPoller(mockPrimaryShard(), format, TimeValue.timeValueSeconds(30), noopThreadPool, -1L);
        try {
            poller.run();
            assertTrue("failure 1 must hold pressure across the retry", DerivedCatchUpPressure.isActive());
            poller.run();
            assertTrue("failure 2 must still hold pressure", DerivedCatchUpPressure.isActive());
            poller.run();
            assertFalse(
                "failure 3 must RELEASE pressure — a build that is not succeeding with merges "
                    + "already deferred must not hold them hostage (defect #28b escape valve)",
                DerivedCatchUpPressure.isActive()
            );
            poller.run();
            assertFalse("further failures must not re-claim", DerivedCatchUpPressure.isActive());
        } finally {
            poller.close();
        }
    }

    /**
     * Defect #28b: the valve re-arms automatically. A successful capped round
     * resets the failure streak and re-claims pressure for the ongoing
     * catch-up burst.
     */
    public void testPressureReArmsOnSuccessAfterFailureRelease() throws Exception {
        AtomicReference<DerivedSourceSnapshot> snap = new AtomicReference<>(snapshot(100L));
        // builds 1-3: fail (valve releases at 3) — build 4: capped success (re-arm)
        DerivedPullFormat format = scriptedFormat(
            snap,
            build -> build <= 3 ? result(false, Map.of()) : result(true, Map.of("capped", true, "capped_watermark", 50L))
        );
        DerivedShardPoller poller = new DerivedShardPoller(mockPrimaryShard(), format, TimeValue.timeValueSeconds(30), noopThreadPool, -1L);
        try {
            poller.run();
            poller.run();
            poller.run();
            assertFalse("valve must have released after 3 consecutive failures", DerivedCatchUpPressure.isActive());
            poller.run(); // capped success: failure streak resets, catch-up continues
            assertTrue("a capped success after the valve released must re-arm pressure", DerivedCatchUpPressure.isActive());
        } finally {
            poller.close();
        }
    }
}
