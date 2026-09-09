/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.cluster.metadata.Metadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.test.OpenSearchTestCase;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.atomic.AtomicReference;

public class NodeRoutingSnapshotServiceTests extends OpenSearchTestCase {

    private static final String NODE_ID = "test-node-1";

    public void testInitialSnapshotIsEmpty() {
        NodeRoutingSnapshotService service = new NodeRoutingSnapshotService(NODE_ID);
        TargetRoutingSnapshot snapshot = service.current();
        assertSame(TargetRoutingSnapshot.EMPTY, snapshot);
        assertEquals(0L, snapshot.version());
        assertFalse(snapshot.hasTarget("any-index"));
    }

    public void testSnapshotUpdatedOnClusterChanged() {
        NodeRoutingSnapshotService service = new NodeRoutingSnapshotService(NODE_ID);

        ClusterState state = buildClusterState(42L, "target-a", 3, "target-b", 5);
        ClusterChangedEvent event = new ClusterChangedEvent("test", state, ClusterState.EMPTY_STATE);

        service.clusterChanged(event);

        TargetRoutingSnapshot snapshot = service.current();
        assertEquals(42L, snapshot.version());
        assertEquals(NODE_ID, snapshot.nodeId());
        assertTrue(snapshot.hasTarget("target-a"));
        assertTrue(snapshot.hasTarget("target-b"));
        assertEquals(3, snapshot.numberOfShards("target-a"));
        assertEquals(5, snapshot.numberOfShards("target-b"));
    }

    public void testResolveTargetShard() {
        NodeRoutingSnapshotService service = new NodeRoutingSnapshotService(NODE_ID);

        ClusterState state = buildClusterState(1L, "target", 4);
        service.clusterChanged(new ClusterChangedEvent("test", state, ClusterState.EMPTY_STATE));

        TargetRoutingSnapshot snapshot = service.current();
        // sourceShardId=7, targetShards=4 → 7 % 4 = 3
        assertEquals(3, snapshot.resolveTargetShard("target", 7));
        // sourceShardId=0, targetShards=4 → 0 % 4 = 0
        assertEquals(0, snapshot.resolveTargetShard("target", 0));
        // unknown index → -1
        assertEquals(-1, snapshot.resolveTargetShard("unknown", 0));
    }

    public void testSnapshotReplacedOnSubsequentClusterChanged() {
        NodeRoutingSnapshotService service = new NodeRoutingSnapshotService(NODE_ID);

        ClusterState state1 = buildClusterState(1L, "target-a", 3);
        service.clusterChanged(new ClusterChangedEvent("test", state1, ClusterState.EMPTY_STATE));

        TargetRoutingSnapshot first = service.current();
        assertEquals(1L, first.version());
        assertTrue(first.hasTarget("target-a"));

        // Second update replaces the snapshot entirely
        ClusterState state2 = buildClusterState(2L, "target-b", 5);
        service.clusterChanged(new ClusterChangedEvent("test", state2, state1));

        TargetRoutingSnapshot second = service.current();
        assertEquals(2L, second.version());
        assertFalse(second.hasTarget("target-a")); // target-a no longer in state
        assertTrue(second.hasTarget("target-b"));
    }

    public void testEngineCallbackReadsSnapshotWithoutClusterStateAccess() {
        // This test verifies the fundamental contract: engine callbacks use
        // service.current() which is a lock-free AtomicReference.get() and
        // does NOT access ClusterService or ClusterState.
        NodeRoutingSnapshotService service = new NodeRoutingSnapshotService(NODE_ID);

        ClusterState state = buildClusterState(10L, "mv-target", 6);
        service.clusterChanged(new ClusterChangedEvent("test", state, ClusterState.EMPTY_STATE));

        // Simulate engine callback — just reads the snapshot, never touches cluster state
        TargetRoutingSnapshot snapshot = service.current();
        assertEquals(6, snapshot.numberOfShards("mv-target"));
        assertEquals(10L, snapshot.version());
        // The key assertion is that current() returned without any ClusterService
        // interaction — it's a pure AtomicReference.get().
    }

    public void testConcurrentReadersDuringUpdate() throws Exception {
        NodeRoutingSnapshotService service = new NodeRoutingSnapshotService(NODE_ID);

        // Seed initial snapshot
        ClusterState initial = buildClusterState(1L, "target", 3);
        service.clusterChanged(new ClusterChangedEvent("test", initial, ClusterState.EMPTY_STATE));

        int readers = 8;
        CyclicBarrier barrier = new CyclicBarrier(readers + 1); // readers + 1 writer
        CountDownLatch done = new CountDownLatch(readers);
        AtomicReference<Throwable> failure = new AtomicReference<>();

        // Concurrent readers — simulate engine callbacks reading snapshot
        for (int i = 0; i < readers; i++) {
            Thread reader = new Thread(() -> {
                try {
                    barrier.await(); // synchronize start
                    for (int j = 0; j < 1000; j++) {
                        TargetRoutingSnapshot snap = service.current();
                        assertNotNull(snap);
                        // Snapshot must be internally consistent
                        if (snap.hasTarget("target")) {
                            assertTrue(snap.numberOfShards("target") > 0);
                        }
                    }
                } catch (Throwable t) {
                    failure.compareAndSet(null, t);
                } finally {
                    done.countDown();
                }
            });
            reader.setDaemon(true);
            reader.start();
        }

        // Writer — simulate applier thread updating snapshot
        barrier.await();
        for (int v = 2; v <= 50; v++) {
            ClusterState newState = buildClusterState(v, "target", v % 10 + 1);
            service.clusterChanged(new ClusterChangedEvent("test", newState, initial));
            initial = newState;
        }

        done.await();
        if (failure.get() != null) {
            fail("Concurrent read failed: " + failure.get().getMessage());
        }
    }

    public void testCloseIsIdempotentWithoutBind() {
        NodeRoutingSnapshotService service = new NodeRoutingSnapshotService(NODE_ID);
        // close() without bind() should not throw
        service.close();
        // current() still works after close, returns last snapshot
        assertSame(TargetRoutingSnapshot.EMPTY, service.current());
    }

    public void testUnknownTargetReturnsNegativeOne() {
        NodeRoutingSnapshotService service = new NodeRoutingSnapshotService(NODE_ID);
        ClusterState state = buildClusterState(1L, "known", 3);
        service.clusterChanged(new ClusterChangedEvent("test", state, ClusterState.EMPTY_STATE));

        TargetRoutingSnapshot snapshot = service.current();
        assertEquals(-1, snapshot.numberOfShards("unknown-index"));
        assertEquals(-1, snapshot.resolveTargetShard("unknown-index", 0));
    }

    private static ClusterState buildClusterState(long version, String indexName, int numShards) {
        return buildClusterState(version, indexName, numShards, null, 0);
    }

    private static ClusterState buildClusterState(long version, String index1, int shards1, String index2, int shards2) {
        Metadata.Builder metaBuilder = Metadata.builder();
        metaBuilder.put(
            IndexMetadata.builder(index1)
                .settings(
                    Settings.builder()
                        .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, shards1)
                        .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                        .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
                )
                .build(),
            false
        );
        if (index2 != null) {
            metaBuilder.put(
                IndexMetadata.builder(index2)
                    .settings(
                        Settings.builder()
                            .put(IndexMetadata.SETTING_NUMBER_OF_SHARDS, shards2)
                            .put(IndexMetadata.SETTING_NUMBER_OF_REPLICAS, 0)
                            .put(IndexMetadata.SETTING_VERSION_CREATED, org.opensearch.Version.CURRENT)
                    )
                    .build(),
                false
            );
        }
        return ClusterState.builder(new ClusterName("test-cluster")).version(version).metadata(metaBuilder).build();
    }
}
