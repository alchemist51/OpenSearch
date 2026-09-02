/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.derived.pull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.metadata.DerivedIndexBinding;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.lifecycle.AbstractLifecycleComponent;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.engine.derived.pull.spi.DerivedPullFormat;
import org.opensearch.index.shard.IndexEventListener;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.index.shard.IndexShardState;
import org.opensearch.threadpool.ThreadPool;

import java.io.IOException;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Node-level service that manages exactly one {@link DerivedShardPoller} per
 * locally-started eligible primary shard.
 *
 * <p>This is the single owner of pull-based derived data lifecycle on a node.
 * Format-specific behavior is injected via registered {@link DerivedPullFormat}
 * implementations — this class never imports or references any MV, DataFusion,
 * Parquet, SegmentInfos, or other format-specific types.
 *
 * <h2>Thread safety</h2>
 * <p>{@link IndexEventListener} callbacks fire on the cluster-applier thread.
 * Calling {@link org.opensearch.cluster.service.ClusterService#state()} from
 * that thread is forbidden. Therefore, all callbacks only <em>enqueue</em>
 * lightweight reconciliation tasks onto {@link ThreadPool.Names#GENERIC}.
 *
 * <p>The {@code synchronized} block in {@link #reconcileOnGeneric} serializes
 * poller lifecycle mutations. Enqueued tasks re-check eligibility inside the
 * lock to handle close/state races.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class NodeDerivedPullService extends AbstractLifecycleComponent implements IndexEventListener {

    private static final Logger logger = LogManager.getLogger(NodeDerivedPullService.class);

    /** Default poll interval when the index setting is not specified. */
    private static final TimeValue DEFAULT_INTERVAL = TimeValue.timeValueMillis(200);

    private final ThreadPool threadPool;
    private final Map<String, DerivedPullFormat> formatRegistry;
    private final Map<ShardId, DerivedShardPoller> pollers = new HashMap<>();
    private final Set<ShardId> pendingReconcile = new HashSet<>();
    private volatile boolean closed;

    /**
     * Creates the node-level pull service.
     *
     * @param threadPool the node's thread pool
     * @param formats    registered derived pull formats (keyed by formatId)
     */
    public NodeDerivedPullService(ThreadPool threadPool, Map<String, DerivedPullFormat> formats) {
        this.threadPool = threadPool;
        this.formatRegistry = new ConcurrentHashMap<>(formats);
    }

    /**
     * Convenience constructor for plugin registration with a list of formats.
     */
    public NodeDerivedPullService(ThreadPool threadPool, List<DerivedPullFormat> formats) {
        this(threadPool, toMap(formats));
    }

    private static Map<String, DerivedPullFormat> toMap(List<DerivedPullFormat> formats) {
        Map<String, DerivedPullFormat> map = new HashMap<>();
        for (DerivedPullFormat f : formats) {
            DerivedPullFormat prev = map.put(f.formatId(), f);
            if (prev != null) {
                throw new IllegalArgumentException("Duplicate DerivedPullFormat registration for formatId [" + f.formatId() + "]");
            }
        }
        return map;
    }

    /**
     * Registers an additional format at runtime (e.g. from a plugin's
     * createComponents). Thread-safe.
     */
    public void registerFormat(DerivedPullFormat format) {
        DerivedPullFormat prev = formatRegistry.putIfAbsent(format.formatId(), format);
        if (prev != null) {
            throw new IllegalArgumentException("DerivedPullFormat [" + format.formatId() + "] is already registered");
        }
        logger.info("derived_pull registered format [{}]", format.formatId());
    }

    // ── IndexEventListener callbacks (cluster-applier thread) ────────────

    /**
     * Fires once the shard reaches STARTED — the definitive signal that the
     * shard is eligible.  Always forces a fresh reconcile even if a prior
     * intermediate-state reconcile already ran and found the shard
     * not-yet-STARTED.
     */
    @Override
    public void afterIndexShardStarted(IndexShard indexShard) {
        enqueueReconcile(indexShard, true);
    }

    @Override
    public void indexShardStateChanged(
        IndexShard indexShard,
        @Nullable IndexShardState previousState,
        IndexShardState currentState,
        @Nullable String reason
    ) {
        // Only reconcile for terminal states; intermediate transitions
        // (RECOVERING, POST_RECOVERY) are covered by afterIndexShardStarted.
        if (currentState == IndexShardState.STARTED || currentState == IndexShardState.CLOSED) {
            enqueueReconcile(indexShard, false);
        }
    }

    @Override
    public void shardRoutingChanged(
        IndexShard indexShard,
        @Nullable org.opensearch.cluster.routing.ShardRouting oldRouting,
        org.opensearch.cluster.routing.ShardRouting newRouting
    ) {
        enqueueReconcile(indexShard, false);
    }

    @Override
    public void beforeIndexShardClosed(ShardId shardId, @Nullable IndexShard indexShard, Settings indexSettings) {
        synchronized (this) {
            pendingReconcile.remove(shardId);
            stopPoller(shardId);
        }
    }

    // ── Async reconciliation ─────────────────────────────────────────────

    /**
     * Enqueue a reconcile task onto GENERIC.
     *
     * @param force when true, bypass the dedup guard so that a fresh
     *              reconcile runs even if a prior (possibly stale) one
     *              is already queued.  Used by {@code afterIndexShardStarted}
     *              to guarantee the definitive STARTED state is seen.
     */
    private void enqueueReconcile(IndexShard shard, boolean force) {
        synchronized (this) {
            if (closed) {
                return;
            }
            if (force) {
                pendingReconcile.add(shard.shardId());   // always (re-)mark pending
            } else if (pendingReconcile.add(shard.shardId()) == false) {
                return; // already queued
            }
        }
        try {
            threadPool.executor(ThreadPool.Names.GENERIC).execute(() -> reconcileOnGeneric(shard));
        } catch (Exception e) {
            synchronized (this) {
                pendingReconcile.remove(shard.shardId());
            }
            if (closed == false) {
                logger.error("derived_pull failed to enqueue reconcile for shard [{}]", shard.shardId(), e);
            }
        }
    }

    private synchronized void reconcileOnGeneric(IndexShard shard) {
        pendingReconcile.remove(shard.shardId());
        if (closed) {
            return;
        }

        String formatId = eligibleFormatId(shard);
        if (formatId == null) {
            stopPoller(shard.shardId());
            return;
        }

        // Already running and healthy?
        DerivedShardPoller existing = pollers.get(shard.shardId());
        if (existing != null) {
            if (existing.isClosed() == false) {
                return; // healthy poller already active
            }
            // Stale/closed poller — clean it up and start a fresh one
            pollers.remove(shard.shardId());
            logger.info("derived_pull replaced stale closed poller for shard [{}]", shard.shardId());
        }

        DerivedPullFormat format = formatRegistry.get(formatId);
        if (format == null) {
            logger.debug("derived_pull: no registered format [{}] for shard [{}]; skipping", formatId, shard.shardId());
            return;
        }

        try {
            DerivedShardPoller poller = new DerivedShardPoller(
                shard,
                format,
                DEFAULT_INTERVAL,
                threadPool,
                -1L // TODO: recover watermark from shard commit userData
            );
            pollers.put(shard.shardId(), poller);
            poller.start();
            logger.info("derived_pull started poller for shard [{}] format [{}]", shard.shardId(), formatId);
        } catch (Exception e) {
            logger.error("derived_pull failed to start poller for shard [{}] format [{}]", shard.shardId(), formatId, e);
        }
    }

    /**
     * Determines if a shard is eligible for pull-based derived data and
     * returns the derived data-format category (== the registered
     * {@link DerivedPullFormat#formatId()}), or {@code null} if not eligible.
     *
     * <p>Eligibility criteria (category-driven — no string match against
     * {@code primary/secondary_data_formats}):
     * <ul>
     *   <li>Index is OPEN</li>
     *   <li>Shard is STARTED and primary</li>
     *   <li>A {@link DerivedIndexBinding} exists (source identity)</li>
     *   <li>The canonical {@code index.derived.data_format} category is set and
     *       resolves to a registered {@link DerivedPullFormat} on this node.
     *       If the category is set but unregistered, the shard fails closed
     *       (no poller) — the derived category is the sole control-plane
     *       signal.</li>
     * </ul>
     */
    String eligibleFormatId(IndexShard shard) {
        IndexMetadata metadata = shard.indexSettings().getIndexMetadata();
        if (metadata == null || metadata.getState() != IndexMetadata.State.OPEN) {
            return null;
        }
        if (shard.state() != IndexShardState.STARTED || shard.routingEntry().primary() == false) {
            return null;
        }

        // Must have a DerivedIndexBinding (source identity).
        DerivedIndexBinding binding = metadata.getDerivedIndexBinding();
        if (binding == null) {
            return null;
        }

        // Resolve the canonical derived data-format category. This is the ONLY
        // eligibility signal — the derived category owns the control plane and
        // is never inferred from the composite primary/secondary format lists.
        String category = DerivedIndexBinding.dataFormatCategory(shard.indexSettings().getSettings());
        if (category == null) {
            return null;
        }
        if (formatRegistry.containsKey(category) == false) {
            logger.warn(
                "derived_pull: shard [{}] declares derived category [{}] with no registered DerivedPullFormat "
                    + "(registered: {}); no poller started",
                shard.shardId(),
                category,
                formatRegistry.keySet()
            );
            return null;
        }
        return category;
    }

    private void stopPoller(ShardId shardId) {
        DerivedShardPoller poller = pollers.remove(shardId);
        if (poller != null) {
            try {
                poller.close();
            } catch (IOException e) {
                logger.warn("derived_pull failed to close poller for shard [{}]", shardId, e);
            }
            logger.info("derived_pull stopped poller for shard [{}]", shardId);
        }
    }

    /** Returns the number of active pollers (visible for testing). */
    public synchronized int activePollers() {
        return pollers.size();
    }

    /** Returns the poller for a shard, or {@code null}. Visible for testing. */
    @Nullable
    public synchronized DerivedShardPoller getPoller(ShardId shardId) {
        return pollers.get(shardId);
    }

    /** Returns the set of registered format IDs. */
    public Set<String> registeredFormats() {
        return Set.copyOf(formatRegistry.keySet());
    }

    // ── Lifecycle ────────────────────────────────────────────────────────

    @Override
    protected void doStart() {
        logger.info("derived_pull NodeDerivedPullService starting with formats: {}", formatRegistry.keySet());
    }

    @Override
    protected void doStop() {
        // Stop is a soft signal; close does the actual cleanup
    }

    @Override
    protected void doClose() {
        synchronized (this) {
            closed = true;
            pendingReconcile.clear();
            for (ShardId shardId : List.copyOf(pollers.keySet())) {
                stopPoller(shardId);
            }
        }
        logger.info("derived_pull NodeDerivedPullService closed");
    }
}
