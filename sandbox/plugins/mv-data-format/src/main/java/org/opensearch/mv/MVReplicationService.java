/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.Nullable;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.shard.IndexEventListener;
import org.opensearch.index.shard.IndexShard;

import java.io.Closeable;

/**
 * Noop tracking and checkpoint request support for the MV pull path.
 *
 * <p>In the request-driven model, the TARGET drives checkpoint acquisition
 * by sending {@link MVCheckpointRequestAction} to the source primary every
 * poll round. The source-side handler
 * ({@link MVCheckpointRequestTransportHandler}) resolves the shard, reads
 * the catalog, scopes files and noops, and replies — all in request scope.</p>
 *
 * <p>This service's remaining responsibilities:</p>
 * <ul>
 *   <li>Owns the {@link MVNoopTracker} — the in-memory store of seqNos that
 *       consumed a sequence number but produced no parquet row (failed index
 *       ops, deletes).</li>
 *   <li>Registers {@link MVNoopIndexingListener} on source indices via
 *       {@code onIndexModule} (done in {@link MVDataFormatPlugin}).</li>
 *   <li>Cleans up noop tracker data when source shards close (via
 *       {@link IndexEventListener#afterIndexShardClosed}).</li>
 * </ul>
 *
 * <p>Noop eviction is triggered by the checkpoint request handler
 * ({@link MVCheckpointRequestTransportHandler}) which tracks per-source-shard
 * minimum requested watermarks and evicts below that threshold on each
 * handled request.</p>
 *
 * @opensearch.experimental
 */
public final class MVReplicationService implements IndexEventListener, Closeable {

    private static final Logger logger = LogManager.getLogger(MVReplicationService.class);

    private final MVNoopTracker noopTracker;
    private volatile boolean closed;

    public MVReplicationService(MVNoopTracker noopTracker) {
        this.noopTracker = noopTracker;
    }

    // ── IndexEventListener: cleanup noop data on shard close ─────────────

    @Override
    public void afterIndexShardClosed(
        ShardId shardId,
        @Nullable IndexShard indexShard,
        org.opensearch.common.settings.Settings indexSettings
    ) {
        if (noopTracker != null) {
            noopTracker.removeShard(shardId);
            logger.debug("mv_replication: cleaned up noop data for closed shard [{}]", shardId);
        }
    }

    // ── Lifecycle ────────────────────────────────────────────────────────

    @Override
    public void close() {
        closed = true;
        logger.info("mv_replication: closed (noop tracking only)");
    }

    // ── Accessors ────────────────────────────────────────────────────────

    public MVNoopTracker noopTracker() {
        return noopTracker;
    }

    public boolean isClosed() {
        return closed;
    }
}
