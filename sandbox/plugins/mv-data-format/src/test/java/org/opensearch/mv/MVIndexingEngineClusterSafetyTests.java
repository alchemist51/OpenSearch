/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import org.opensearch.test.OpenSearchTestCase;

import java.util.Map;

/**
 * Verifies that {@link MVIndexingEngine} engine callbacks do NOT call
 * {@code clusterService.state()} — a known deadlock pattern in OpenSearch
 * when run on the cluster-applier thread.
 *
 * <p>These tests provide a safety net: if someone re-introduces a direct
 * cluster state access in an engine callback, these tests will fail.
 */
public class MVIndexingEngineClusterSafetyTests extends OpenSearchTestCase {

    /**
     * Verifies that {@code afterCommit} on the ship path reads the routing
     * snapshot (supplied by {@link NodeRoutingSnapshotService}) rather than
     * calling {@code clusterService.state()}.
     *
     * <p>We construct an engine with a poison {@code clusterServiceSupplier}
     * that throws if invoked during afterCommit, and a valid routing snapshot
     * supplier that returns target shard info. If afterCommit accesses cluster
     * state directly, the test fails with AssertionError.
     */
    private org.opensearch.index.shard.ShardPath createShardPath(String indexName, String indexUuid, int shardId) throws Exception {
        java.nio.file.Path tempDir = createTempDir();
        java.nio.file.Path dataPath = tempDir.resolve(indexUuid).resolve(Integer.toString(shardId));
        java.nio.file.Files.createDirectories(dataPath);
        return new org.opensearch.index.shard.ShardPath(
            false,
            dataPath,
            dataPath,
            new org.opensearch.core.index.shard.ShardId(indexName, indexUuid, shardId)
        );
    }

    public void testAfterCommitDoesNotCallClusterServiceState() throws Exception {
        // Poison: if afterCommit ever calls clusterServiceSupplier.get().state(),
        // this will blow up.
        java.util.function.Supplier<org.opensearch.cluster.service.ClusterService> poisonSupplier = () -> {
            throw new AssertionError(
                "SAFETY VIOLATION: afterCommit must NOT call clusterServiceSupplier.get() — "
                    + "use routingSnapshotSupplier instead to avoid applier-thread deadlock"
            );
        };

        // Valid routing snapshot with target info
        TargetRoutingSnapshot snapshot = new TargetRoutingSnapshot(Map.of("mv-target", 3), 42L, "test-node");

        org.opensearch.index.shard.ShardPath shardPath = createShardPath("source-index", "_na_", 0);

        // Client that logs the async call but doesn't actually ship
        java.util.concurrent.atomic.AtomicBoolean clientCalled = new java.util.concurrent.atomic.AtomicBoolean(false);

        MVIndexingEngine engine = new MVIndexingEngine(
            shardPath,
            "source-index",
            MVDefinitionSpec.SOURCE,
            MVDataFormat.INSTANCE,
            "payments",
            java.util.List.of("mv-target"),
            () -> null, // client is null — afterCommit should gracefully return
            poisonSupplier,
            false,
            () -> snapshot
        );

        // afterCommit with null client just logs and returns (the client==null guard fires first)
        // The key assertion: the poison clusterServiceSupplier was NOT invoked.
        engine.afterCommit(10L);

        // No exception = afterCommit did NOT touch clusterServiceSupplier
    }

    /**
     * Verifies that {@code afterCommit} with a valid client uses the routing
     * snapshot to resolve the target shard, not cluster state.
     */
    public void testAfterCommitUsesRoutingSnapshotForShardResolution() throws Exception {
        java.util.function.Supplier<org.opensearch.cluster.service.ClusterService> poisonSupplier = () -> {
            throw new AssertionError("SAFETY VIOLATION: must not access cluster state from engine callback");
        };

        TargetRoutingSnapshot snapshot = new TargetRoutingSnapshot(Map.of("mv-target", 4), 99L, "test-node");

        org.opensearch.index.shard.ShardPath shardPath = createShardPath("source-index", "_na_", 5);

        // Track what target shard the action was invoked with
        java.util.concurrent.atomic.AtomicInteger capturedTargetShard = new java.util.concurrent.atomic.AtomicInteger(-1);

        // Create a mock-ish client — we only need execute() to capture the request
        org.opensearch.transport.client.Client mockClient = new org.opensearch.test.client.NoOpClient(getTestName()) {
            @Override
            @SuppressWarnings("unchecked")
            public <
                Request extends org.opensearch.action.ActionRequest,
                Response extends org.opensearch.core.action.ActionResponse> void doExecute(
                    org.opensearch.action.ActionType<Response> action,
                    Request request,
                    org.opensearch.core.action.ActionListener<Response> listener
                ) {
                if (request instanceof MVSourceCommitAction.Request commitReq) {
                    capturedTargetShard.set(commitReq.targetShard());
                    listener.onResponse((Response) new MVSourceCommitAction.Response(commitReq.committedCheckpoint()));
                } else {
                    listener.onFailure(new UnsupportedOperationException("unexpected action"));
                }
            }
        };

        MVIndexingEngine engine = new MVIndexingEngine(
            shardPath,
            "source-index",
            MVDefinitionSpec.SOURCE,
            MVDataFormat.INSTANCE,
            "payments",
            java.util.List.of("mv-target"),
            () -> mockClient,
            poisonSupplier,
            false,
            () -> snapshot
        );

        engine.afterCommit(100L);

        // sourceShardId=5, targetShards=4 → 5 % 4 = 1
        assertEquals(1, capturedTargetShard.get());

        mockClient.close();
    }

    /**
     * Verifies that when the routing snapshot does not know about a target
     * index, afterCommit gracefully skips it (logs error, does not crash).
     */
    public void testAfterCommitSkipsUnknownTargetGracefully() throws Exception {
        java.util.function.Supplier<org.opensearch.cluster.service.ClusterService> poisonSupplier = () -> {
            throw new AssertionError("SAFETY VIOLATION: must not access cluster state from engine callback");
        };

        // Empty snapshot — no targets known
        TargetRoutingSnapshot emptySnapshot = TargetRoutingSnapshot.EMPTY;

        org.opensearch.index.shard.ShardPath shardPath = createShardPath("source-index", "_na_", 0);

        java.util.concurrent.atomic.AtomicBoolean clientExecuted = new java.util.concurrent.atomic.AtomicBoolean(false);
        org.opensearch.transport.client.Client mockClient = new org.opensearch.test.client.NoOpClient(getTestName()) {
            @Override
            @SuppressWarnings("unchecked")
            public <
                Request extends org.opensearch.action.ActionRequest,
                Response extends org.opensearch.core.action.ActionResponse> void doExecute(
                    org.opensearch.action.ActionType<Response> action,
                    Request request,
                    org.opensearch.core.action.ActionListener<Response> listener
                ) {
                clientExecuted.set(true);
                listener.onFailure(new UnsupportedOperationException("should not be called"));
            }
        };

        MVIndexingEngine engine = new MVIndexingEngine(
            shardPath,
            "source-index",
            MVDefinitionSpec.SOURCE,
            MVDataFormat.INSTANCE,
            "payments",
            java.util.List.of("mv-target"),
            () -> mockClient,
            poisonSupplier,
            false,
            () -> emptySnapshot
        );

        // Should not throw — target is unknown so afterCommit logs and skips
        engine.afterCommit(50L);

        // Client should NOT have been invoked since the target was unknown
        assertFalse("Client should not be called for unknown target", clientExecuted.get());

        mockClient.close();
    }

    /**
     * Structural verification: scan MVIndexingEngine source for any remaining
     * direct {@code .state()} calls in engine callback methods. This is a
     * belt-and-suspenders safety check.
     */
    public void testNoClusterStateCallsInEngineCallbackSource() throws Exception {
        // Read the source file to verify no .state() calls remain in callback methods
        String sourceFile = "src/main/java/org/opensearch/mv/MVIndexingEngine.java";
        java.nio.file.Path sourcePath = java.nio.file.Path.of(sourceFile);

        // If we can't find the source file at the expected path, try relative to the plugin root
        if (!java.nio.file.Files.exists(sourcePath)) {
            // Skip the source-scan test when running outside the source tree
            // (e.g., from a build artifact). The other tests in this class
            // provide runtime safety verification via the poison supplier.
            return;
        }

        String source = java.nio.file.Files.readString(sourcePath);

        // afterCommit must not contain a direct .state() call
        // We check the substring between "void afterCommit" and the next @Override
        int afterCommitStart = source.indexOf("void afterCommit(");
        assertTrue("afterCommit method not found", afterCommitStart > 0);
        int afterCommitEnd = source.indexOf("@Override", afterCommitStart + 1);
        if (afterCommitEnd < 0) afterCommitEnd = source.length();
        String afterCommitBody = source.substring(afterCommitStart, afterCommitEnd);

        assertFalse(
            "afterCommit must not call .state() directly — use routingSnapshotSupplier instead",
            afterCommitBody.contains(".state()")
        );
    }
}
