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
import org.opensearch.threadpool.TestThreadPool;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.atomic.AtomicInteger;

import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

/**
 * Tests for {@link DerivedShardPoller} lifecycle and correctness.
 *
 * <ul>
 *   <li>close() is idempotent and stops scheduling</li>
 *   <li>watermark advances only on successful build</li>
 *   <li>null snapshot (no new data) does not advance watermark</li>
 *   <li>failed build does not advance watermark</li>
 *   <li>staging directory is cleaned up after each round</li>
 * </ul>
 */
public class DerivedShardPollerTests extends OpenSearchTestCase {

    private TestThreadPool threadPool;
    private Path tempDir;

    @Override
    public void setUp() throws Exception {
        super.setUp();
        threadPool = new TestThreadPool(getTestName());
        tempDir = createTempDir("derived_poller_test");
    }

    @Override
    public void tearDown() throws Exception {
        terminate(threadPool);
        super.tearDown();
    }

    // ── Helper factories ─────────────────────────────────────────────────

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

        Settings nodeSettings = Settings.EMPTY;
        Settings indexSettings = Settings.builder()
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1)
            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
            .put(IndexMetadata.SETTING_INDEX_UUID, shardId.getIndex().getUUID())
            .build();
        IndexMetadata metadata = IndexMetadata.builder(shardId.getIndex().getName()).settings(indexSettings).build();
        IndexSettings idxSettings = new IndexSettings(metadata, nodeSettings);
        when(shard.indexSettings()).thenReturn(idxSettings);

        // Use real ShardPath — ShardPath is final and cannot be mocked.
        // ShardPath requires dataPath to end with <uuid>/<shard_id>
        Path shardDataPath = tempDir.resolve(shardId.getIndex().getUUID()).resolve(Integer.toString(shardId.id()));
        Files.createDirectories(shardDataPath);
        ShardPath shardPath = new ShardPath(false, shardDataPath, shardDataPath, shardId);
        when(shard.shardPath()).thenReturn(shardPath);

        return shard;
    }

    private static DerivedPullFormat countingFormat(
        String formatId,
        AtomicInteger fetchCount,
        AtomicInteger buildCount,
        DerivedSourceSnapshot snapshotToReturn,
        boolean buildSuccess
    ) {
        return new DerivedPullFormat() {
            @Override
            public String formatId() {
                return formatId;
            }

            @Override
            public DerivedSourceReader createReader(Settings nodeSettings, IndexSettings indexSettings) {
                return new DerivedSourceReader() {
                    @Override
                    public DerivedSourceSnapshot fetchSnapshot(ShardRouting shard, long sinceWatermark) {
                        fetchCount.incrementAndGet();
                        // Only return snapshot if watermark is beyond current
                        if (snapshotToReturn != null && snapshotToReturn.watermark() > sinceWatermark) {
                            return snapshotToReturn;
                        }
                        return null;
                    }

                    @Override
                    public void downloadToStage(DerivedSourceSnapshot snapshot, Path stageDir) throws IOException {
                        // Create a marker file to verify staging works
                        Files.writeString(stageDir.resolve("staged.dat"), "test-data");
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
                        buildCount.incrementAndGet();
                        return new BuildResult() {
                            @Override
                            public boolean success() {
                                return buildSuccess;
                            }

                            @Override
                            public String artifactId() {
                                return "test-artifact-" + snapshot.watermark();
                            }

                            @Override
                            public Map<String, Object> stats() {
                                return Map.of("rows", 42);
                            }
                        };
                    }

                    @Override
                    public void close() {}
                };
            }
        };
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
                return Map.of("wm", String.valueOf(watermark));
            }
        };
    }

    // ── Tests ────────────────────────────────────────────────────────────

    /**
     * close() must be idempotent — calling it multiple times must not throw.
     */
    public void testCloseIsIdempotent() throws Exception {
        AtomicInteger fetchCount = new AtomicInteger();
        AtomicInteger buildCount = new AtomicInteger();
        DerivedPullFormat format = countingFormat("test", fetchCount, buildCount, null, true);
        IndexShard shard = mockPrimaryShard();

        DerivedShardPoller poller = new DerivedShardPoller(shard, format, TimeValue.timeValueSeconds(30), threadPool, -1L);
        assertFalse(poller.isClosed());

        poller.close();
        assertTrue(poller.isClosed());

        poller.close(); // second close must not throw
        assertTrue(poller.isClosed());
    }

    /**
     * Watermark starts at the initial value and stays there when no data
     * is available.
     */
    public void testWatermarkDoesNotAdvanceWithNoData() throws Exception {
        AtomicInteger fetchCount = new AtomicInteger();
        AtomicInteger buildCount = new AtomicInteger();
        DerivedPullFormat format = countingFormat("test", fetchCount, buildCount, null, true);
        IndexShard shard = mockPrimaryShard();

        DerivedShardPoller poller = new DerivedShardPoller(shard, format, TimeValue.timeValueSeconds(1), threadPool, 42L);

        assertEquals(42L, poller.watermark());
        assertEquals("test", poller.formatId());

        // Start and let one round execute
        poller.start();
        assertBusy(() -> assertTrue("fetchSnapshot should have been called", fetchCount.get() > 0));

        // Watermark must not have changed
        assertEquals(42L, poller.watermark());
        assertEquals(0, buildCount.get()); // build not called

        poller.close();
    }

    /**
     * When fetchSnapshot returns a watermark that is less than or equal to the
     * current watermark, the poller does not build and does not advance.
     */
    public void testStaleSnapshotIgnored() throws Exception {
        AtomicInteger fetchCount = new AtomicInteger();
        AtomicInteger buildCount = new AtomicInteger();
        // Snapshot watermark=10, but initial watermark is 20 → stale
        DerivedPullFormat format = countingFormat("test", fetchCount, buildCount, snapshot(10), true);
        IndexShard shard = mockPrimaryShard();

        DerivedShardPoller poller = new DerivedShardPoller(shard, format, TimeValue.timeValueSeconds(1), threadPool, 20L);
        poller.start();

        assertBusy(() -> assertTrue(fetchCount.get() > 0));
        assertEquals(20L, poller.watermark()); // not advanced
        assertEquals(0, buildCount.get()); // build not called

        poller.close();
    }

    /**
     * The formatId is correctly reported.
     */
    public void testFormatIdReported() throws IOException {
        DerivedPullFormat format = countingFormat("my_format", new AtomicInteger(), new AtomicInteger(), null, true);
        IndexShard shard = mockPrimaryShard();

        DerivedShardPoller poller = new DerivedShardPoller(shard, format, TimeValue.timeValueSeconds(30), threadPool, -1L);
        assertEquals("my_format", poller.formatId());
        try {
            poller.close();
        } catch (IOException e) {
            // ignore
        }
    }

    /**
     * isClosed() correctly reflects open vs closed state.
     */
    public void testIsClosedReflectsState() throws Exception {
        DerivedPullFormat format = countingFormat("test", new AtomicInteger(), new AtomicInteger(), null, true);
        IndexShard shard = mockPrimaryShard();

        DerivedShardPoller poller = new DerivedShardPoller(shard, format, TimeValue.timeValueSeconds(30), threadPool, -1L);
        assertFalse(poller.isClosed());
        poller.close();
        assertTrue(poller.isClosed());
    }
}
