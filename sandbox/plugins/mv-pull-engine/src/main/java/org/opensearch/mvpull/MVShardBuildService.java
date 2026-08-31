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
import org.opensearch.cluster.metadata.DerivedIndexBinding;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.Nullable;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.shard.IndexEventListener;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardState;
import org.opensearch.mv.MVStateDataFormat;
import org.opensearch.threadpool.ThreadPool;

import java.io.Closeable;
import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Owns one {@link MVArtifactPoller} for each locally-started MV primary shard.
 *
 * <h2>Thread safety contract</h2>
 * <p>All {@link IndexEventListener} callbacks fire on the cluster-applier thread.
 * Calling {@link org.opensearch.cluster.service.ClusterService#state()} from
 * that thread triggers {@code assertNotCalledFromClusterStateApplier} and
 * terminates the node. Therefore, every callback only <em>enqueues</em> a
 * lightweight reconciliation task onto {@link ThreadPool.Names#GENERIC}; the
 * actual poller construction (which resolves the source via cluster state)
 * runs safely off the applier thread.
 *
 * <p>The {@code synchronized} lock on the internal {@code reconcileOnGeneric}
 * method serializes poller lifecycle mutations. A queued task re-checks
 * {@link #isEligiblePrimary} inside the lock to handle close/state races.
 */
final class MVShardBuildService implements IndexEventListener, Closeable {

    private static final Logger logger = LogManager.getLogger(MVShardBuildService.class);

    private final MVPullSettings.Services services;
    private final Map<ShardId, MVArtifactPoller> pollers = new HashMap<>();
    /** ShardIds whose reconciliation is already enqueued but not yet executed. */
    private final Set<ShardId> pendingReconcile = new java.util.HashSet<>();
    private boolean closed;

    MVShardBuildService(MVPullSettings.Services services) {
        this.services = services;
    }

    // ── IndexEventListener callbacks (cluster-applier thread) ────────────
    // NEVER call ClusterService.state() here. Only enqueue.

    @Override
    public void afterIndexShardStarted(IndexShard indexShard) {
        enqueueReconcile(indexShard);
    }

    @Override
    public void indexShardStateChanged(
        IndexShard indexShard,
        @Nullable IndexShardState previousState,
        IndexShardState currentState,
        @Nullable String reason
    ) {
        enqueueReconcile(indexShard);
    }

    @Override
    public void shardRoutingChanged(
        IndexShard indexShard,
        @Nullable org.opensearch.cluster.routing.ShardRouting oldRouting,
        org.opensearch.cluster.routing.ShardRouting newRouting
    ) {
        enqueueReconcile(indexShard);
    }

    @Override
    public void beforeIndexShardClosed(ShardId shardId, @Nullable IndexShard indexShard, Settings indexSettings) {
        // Stop is safe on any thread — it only closes the poller.
        synchronized (this) {
            pendingReconcile.remove(shardId);
            stop(shardId);
        }
    }

    // ── Async reconciliation ─────────────────────────────────────────────

    /**
     * Enqueue a reconciliation task onto GENERIC. De-duplicates: if a task
     * for this shard is already pending, skip. The shard reference is
     * captured; the task re-validates eligibility inside the synchronized
     * block to handle close/state races.
     */
    private void enqueueReconcile(IndexShard shard) {
        synchronized (this) {
            if (closed) {
                return;
            }
            if (pendingReconcile.add(shard.shardId()) == false) {
                return; // already queued
            }
        }
        try {
            services.threadPool().executor(ThreadPool.Names.GENERIC).execute(() -> reconcileOnGeneric(shard));
        } catch (Exception e) {
            synchronized (this) {
                pendingReconcile.remove(shard.shardId());
            }
            if (closed == false) {
                logger.error("mv_pull failed to enqueue reconcile for shard [" + shard.shardId() + "]", e);
            }
        }
    }

    /**
     * Runs on GENERIC thread — safe to call {@code ClusterService.state()}.
     * Re-checks eligibility and shard state under the lock.
     */
    private synchronized void reconcileOnGeneric(IndexShard shard) {
        pendingReconcile.remove(shard.shardId());
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

    /**
     * Eligibility check for pull-model MV primaries.
     *
     * <p>Requires:
     * <ul>
     *   <li>Index is OPEN (closed indices can briefly instantiate shards in
     *       STARTED state during restart/recovery)</li>
     *   <li>Shard is STARTED and primary</li>
     *   <li>Primary data format is {@code mv_state}</li>
     *   <li>A complete {@link DerivedIndexBinding} is present in IndexMetadata
     *       (public source name + server-enriched UUID/topology). Missing
     *       binding fails closed.</li>
     * </ul>
     *
     * <p><b>Note:</b> The deprecated {@code index.mv_pull.source_index} setting
     * is NOT checked here. All pull targets must have a {@link DerivedIndexBinding}
     * injected by {@link org.opensearch.cluster.metadata.MetadataCreateIndexService}
     * at creation time.
     */
    private static boolean isEligiblePrimary(IndexShard shard) {
        // ── Closed-index guard ────────────────────────────────────────────
        IndexMetadata metadata = shard.indexSettings().getIndexMetadata();
        if (metadata == null || metadata.getState() != IndexMetadata.State.OPEN) {
            return false;
        }

        if (shard.state() != IndexShardState.STARTED || shard.routingEntry().primary() == false) {
            return false;
        }

        Settings settings = shard.indexSettings().getSettings();
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

        // ── Binding gate: fail closed without a complete DerivedIndexBinding ──
        DerivedIndexBinding binding = metadata.getDerivedIndexBinding();
        if (binding == null) {
            logger.debug("mv_pull target [{}] has no DerivedIndexBinding — not eligible", shard.shardId());
            return false;
        }
        binding.validateTargetTopology(shard.indexSettings().getNumberOfShards());

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
        pendingReconcile.clear();
        for (ShardId shardId : java.util.List.copyOf(pollers.keySet())) {
            stop(shardId);
        }
    }
}
