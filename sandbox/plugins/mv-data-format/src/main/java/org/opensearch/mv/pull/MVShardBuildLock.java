/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv.pull;

import org.opensearch.core.index.shard.ShardId;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.ReentrantLock;

/**
 * One lock per target shard, shared by the round builder and the compaction
 * merger. Both draw from the node's shared native memory pool, and the memory
 * guard gates that pool on the process's jemalloc-resident bytes, so a
 * compaction never runs concurrently with a round build of the same shard —
 * the peak native memory is the larger of the two jobs, not their sum. The
 * merge scheduler's threads simply wait; rounds of different shards are
 * independent.
 */
final class MVShardBuildLock {

    private static final Map<ShardId, ReentrantLock> LOCKS = new ConcurrentHashMap<>();

    private MVShardBuildLock() {}

    static ReentrantLock forShard(ShardId shardId) {
        return LOCKS.computeIfAbsent(shardId, k -> new ReentrantLock(true));
    }
}
