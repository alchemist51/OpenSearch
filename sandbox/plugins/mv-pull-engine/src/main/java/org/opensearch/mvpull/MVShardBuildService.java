/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mvpull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.Nullable;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.shard.IndexEventListener;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardState;
import org.opensearch.mv.MVStateDataFormat;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

/** Owns one {@link MVArtifactPoller} for each locally-started MV primary shard. */
final class MVShardBuildService implements IndexEventListener, Closeable {

    private static final Logger logger = LogManager.getLogger(MVShardBuildService.class);

    private final MVPullSettings.Services services;
    private final Map<ShardId, MVArtifactPoller> pollers = new HashMap<>();
    private boolean closed;

    MVShardBuildService(MVPullSettings.Services services) {
        this.services = services;
    }

    @Override
    public synchronized void afterIndexShardStarted(IndexShard indexShard) {
        reconcile(indexShard);
    }

    @Override
    public synchronized void indexShardStateChanged(
        IndexShard indexShard,
        @Nullable IndexShardState previousState,
        IndexShardState currentState,
        @Nullable String reason
    ) {
        reconcile(indexShard);
    }

    @Override
    public synchronized void shardRoutingChanged(
        IndexShard indexShard,
        @Nullable org.opensearch.cluster.routing.ShardRouting oldRouting,
        org.opensearch.cluster.routing.ShardRouting newRouting
    ) {
        reconcile(indexShard);
    }

    @Override
    public synchronized void beforeIndexShardClosed(ShardId shardId, @Nullable IndexShard indexShard, Settings indexSettings) {
        stop(shardId);
    }

    private void reconcile(IndexShard shard) {
        if (closed || isEligiblePrimary(shard) == false) {
            stop(shard.shardId());
            return;
        }
        if (pollers.containsKey(shard.shardId())) {
            return;
        }
        try {
            MVArtifactPoller poller = new MVArtifactPoller(shard, services);
            pollers.put(shard.shardId(), poller);
            poller.start();
            logger.info("mv_pull started build service for primary shard [{}]", shard.shardId());
        } catch (Exception e) {
            logger.error("mv_pull failed to start build service for shard [" + shard.shardId() + "]", e);
        }
    }

    private static boolean isEligiblePrimary(IndexShard shard) {
        Settings settings = shard.indexSettings().getSettings();
        if (MVPullSettings.SOURCE_INDEX.exists(settings) == false
            || shard.state() != IndexShardState.STARTED
            || shard.routingEntry().primary() == false) {
            return false;
        }
        String primaryFormat = settings.get("index.composite.primary_data_format");
        if (MVStateDataFormat.NAME.equals(primaryFormat) == false) {
            logger.debug(
                "mv_pull target [{}] must use primary data format [{}] but was [{}]",
                shard.shardId(),
                MVStateDataFormat.NAME,
                primaryFormat
            );
            return false;
        }
        return true;
    }

    private void stop(ShardId shardId) {
        MVArtifactPoller poller = pollers.remove(shardId);
        if (poller != null) {
            try {
                poller.close();
            } catch (IOException e) {
                logger.warn("mv_pull failed closing poller for shard [" + shardId + "]", e);
            }
            logger.info("mv_pull stopped build service for shard [{}]", shardId);
        }
    }

    synchronized int activePollers() {
        return pollers.size();
    }

    @Override
    public synchronized void close() {
        closed = true;
        for (ShardId shardId : java.util.List.copyOf(pollers.keySet())) {
            stop(shardId);
        }
    }
}
