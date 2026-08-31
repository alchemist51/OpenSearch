/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import java.util.Collections;
import java.util.Map;
import java.util.Set;

/**
 * Immutable point-in-time snapshot of target index routing information needed
 * by engine callbacks. Engine callbacks (e.g. {@code afterCommit}) run on the
 * cluster-applier thread and <strong>must not</strong> call
 * {@code clusterService.state()} — doing so is a known deadlock pattern in
 * OpenSearch. Instead, engine callbacks read this snapshot which is published
 * lock-free via {@link NodeRoutingSnapshotService}.
 *
 * <p>This class captures the number of shards per target index (used for
 * ordinal shard mapping: {@code sourceShardId % targetNumberOfShards}) and
 * the cluster state version at capture time for diagnostics.
 *
 * @opensearch.experimental
 */
public final class TargetRoutingSnapshot {

    /** Empty snapshot: no targets known yet. */
    public static final TargetRoutingSnapshot EMPTY = new TargetRoutingSnapshot(Map.of(), 0L, "");

    /** Target index name → number of shards. */
    private final Map<String, Integer> targetShardCounts;

    /** Cluster state version this snapshot was taken from. */
    private final long version;

    /** Node ID this snapshot was captured on. */
    private final String nodeId;

    public TargetRoutingSnapshot(Map<String, Integer> targetShardCounts, long version, String nodeId) {
        this.targetShardCounts = Map.copyOf(targetShardCounts);
        this.version = version;
        this.nodeId = nodeId;
    }

    /**
     * Returns whether this snapshot knows about the given target index.
     */
    public boolean hasTarget(String indexName) {
        return targetShardCounts.containsKey(indexName);
    }

    /**
     * Returns the set of known target index names.
     */
    public Set<String> targetIndexNames() {
        return Collections.unmodifiableSet(targetShardCounts.keySet());
    }

    /**
     * Returns the number of shards for the given target index, or {@code -1}
     * if the target is unknown.
     */
    public int numberOfShards(String indexName) {
        return targetShardCounts.getOrDefault(indexName, -1);
    }

    /**
     * Resolves the target shard ordinal for the given source shard by applying
     * modular mapping: {@code sourceShardId % targetNumberOfShards}.
     *
     * @return the target shard ID, or {@code -1} if the target index is unknown
     */
    public int resolveTargetShard(String targetIndex, int sourceShardId) {
        int numShards = numberOfShards(targetIndex);
        if (numShards <= 0) {
            return -1;
        }
        return sourceShardId % numShards;
    }

    /**
     * Cluster state version at the time this snapshot was captured.
     */
    public long version() {
        return version;
    }

    /**
     * Node ID this snapshot was captured on.
     */
    public String nodeId() {
        return nodeId;
    }

    @Override
    public String toString() {
        return "TargetRoutingSnapshot{targets=" + targetShardCounts.keySet() + ", version=" + version + ", nodeId=" + nodeId + "}";
    }
}
