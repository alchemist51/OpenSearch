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
import org.opensearch.common.settings.Setting;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.shard.IndexEventListener;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.transport.client.Client;

import java.io.Closeable;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * External checkpoint publisher that observes local primary source shards
 * and pushes adverts to bound MV target shards when new data is available.
 *
 * <h2>Design: scheduled sampler (option b)</h2>
 *
 * <p>A plugin-safe per-shard {@code ReferenceManager.RefreshListener} was
 * investigated first (option a). However, {@code IndexShard}'s internal
 * refresh listener list is populated during shard construction
 * ({@code buildInternalRefreshListeners}) and is not extensible from a
 * plugin's {@code onIndexModule}. {@code IndexModule} exposes
 * {@code addIndexEventListener} and {@code addIndexOperationListener} but
 * NOT a refresh-listener hook. Therefore, a clean per-shard refresh
 * callback is impossible without modifying engine config or subclassing
 * the engine — both violate the architectural invariant that the source
 * engine must remain completely unchanged.</p>
 *
 * <p>Instead, this service runs a lightweight scheduled tick on the GENERIC
 * thread pool (default 1 second, configurable via
 * {@code mv_pull.checkpoint_publish_interval}). Each tick iterates LOCAL
 * primary source shards that have bound MV targets (resolved from
 * {@link NodeRoutingSnapshotService#sourceToTargets()}), reads each
 * shard's {@code getProcessedLocalCheckpoint()} (a lock-free volatile
 * read — no remote IO), and publishes via {@link MVCheckpointPublisher}
 * only when the checkpoint has advanced since the last published value.</p>
 *
 * <p><b>SAFETY:</b> this service never calls
 * {@code clusterService.state()} — all routing data comes from the
 * lock-free {@link NodeRoutingSnapshotService} maintained by a
 * {@code ClusterStateListener}. Safe to run from any thread.</p>
 *
 * <p><b>Lifecycle:</b> created in {@code createComponents} (has access to
 * {@code Client}, {@code ThreadPool}, {@code NodeRoutingSnapshotService}),
 * started once, closed on plugin close. Implements {@link IndexEventListener}
 * to track source shard starts and closes for its local registry.</p>
 *
 * @opensearch.experimental
 */
public final class MVReplicationService implements IndexEventListener, Closeable {

    private static final Logger logger = LogManager.getLogger(MVReplicationService.class);

    /**
     * Node-scope interval for the checkpoint publish sampler tick.
     * Default 1 second — sufficient for near-real-time MV tracking with
     * negligible overhead (one volatile read per tracked source shard).
     */
    public static final Setting<TimeValue> CHECKPOINT_PUBLISH_INTERVAL = Setting.timeSetting(
        "mv_pull.checkpoint_publish_interval",
        TimeValue.timeValueSeconds(1),
        TimeValue.timeValueMillis(100),
        Setting.Property.NodeScope
    );

    /** Tracked local source shard state: the shard reference + publisher. */
    static final class TrackedShard {
        final IndexShard shard;
        final MVCheckpointPublisher publisher;

        TrackedShard(IndexShard shard, MVCheckpointPublisher publisher) {
            this.shard = shard;
            this.publisher = publisher;
        }
    }

    private final Client client;
    private final NodeRoutingSnapshotService routingService;
    private final ThreadPool threadPool;
    private final TimeValue interval;

    /** ShardId → tracked shard state. Lock-free via ConcurrentHashMap. */
    private final ConcurrentHashMap<ShardId, TrackedShard> trackedShards = new ConcurrentHashMap<>();

    /** Per-shard last-published checkpoint — publish only on advance. */
    private final ConcurrentHashMap<ShardId, AtomicLong> lastPublished = new ConcurrentHashMap<>();

    /** Scheduler handle for the tick — null until started. */
    private volatile Scheduler.Cancellable scheduledTick;
    private volatile boolean closed;

    // ── Metrics ──────────────────────────────────────────────────────────

    private final AtomicLong tickCount = new AtomicLong();
    private final AtomicLong publishCount = new AtomicLong();
    private final AtomicLong skipCount = new AtomicLong();

    public MVReplicationService(Client client, NodeRoutingSnapshotService routingService, ThreadPool threadPool, TimeValue interval) {
        this.client = client;
        this.routingService = routingService;
        this.threadPool = threadPool;
        this.interval = interval;
    }

    /**
     * Starts the scheduled sampler. Call once after construction.
     */
    public void start() {
        if (closed) {
            throw new IllegalStateException("MVReplicationService is already closed");
        }
        this.scheduledTick = threadPool.scheduleWithFixedDelay(this::tick, interval, ThreadPool.Names.GENERIC);
        logger.info("mv_replication: started checkpoint publisher with interval [{}]", interval);
    }

    // ── IndexEventListener: track source shard lifecycle ─────────────────

    @Override
    public void afterIndexShardStarted(IndexShard indexShard) {
        if (closed) return;
        // Only track primary source shards — target shards and replicas are not sources.
        if (!indexShard.routingEntry().primary()) return;

        // Check if this index has bound MV targets via the routing snapshot.
        // This is a cheap ConcurrentHashMap lookup.
        String indexName = indexShard.shardId().getIndexName();
        Map<String, List<NodeRoutingSnapshotService.BoundTarget>> srcToTgt = routingService.sourceToTargets();
        List<NodeRoutingSnapshotService.BoundTarget> targets = srcToTgt.get(indexName);
        if (targets == null || targets.isEmpty()) {
            return; // Not an MV source — ignore
        }

        ShardId shardId = indexShard.shardId();
        String indexUuid = shardId.getIndex().getUUID();
        MVCheckpointPublisher publisher = new MVCheckpointPublisher(
            client,
            indexName,
            indexUuid,
            shardId.id(),
            routingService
        );
        TrackedShard tracked = new TrackedShard(indexShard, publisher);
        trackedShards.put(shardId, tracked);
        lastPublished.putIfAbsent(shardId, new AtomicLong(-1L));
        logger.info("mv_replication: tracking source shard [{}] with {} bound target(s)", shardId, targets.size());
    }

    @Override
    public void afterIndexShardClosed(
        ShardId shardId,
        @org.opensearch.common.Nullable IndexShard indexShard,
        org.opensearch.common.settings.Settings indexSettings
    ) {
        TrackedShard removed = trackedShards.remove(shardId);
        if (removed != null) {
            lastPublished.remove(shardId);
            logger.info("mv_replication: stopped tracking source shard [{}]", shardId);
        }
    }

    // ── Scheduled tick ───────────────────────────────────────────────────

    /**
     * Called every {@link #interval} on the GENERIC thread pool. Iterates
     * all tracked local primary source shards and publishes checkpoints
     * for any that have advanced.
     */
    void tick() {
        if (closed || trackedShards.isEmpty()) return;
        tickCount.incrementAndGet();

        // Re-check sourceToTargets each tick — targets may appear/disappear
        // dynamically as MV indices are created/deleted.
        Map<String, List<NodeRoutingSnapshotService.BoundTarget>> srcToTgt = routingService.sourceToTargets();

        for (Map.Entry<ShardId, TrackedShard> entry : trackedShards.entrySet()) {
            ShardId shardId = entry.getKey();
            TrackedShard tracked = entry.getValue();

            // Re-verify this source still has targets
            List<NodeRoutingSnapshotService.BoundTarget> targets = srcToTgt.get(shardId.getIndexName());
            if (targets == null || targets.isEmpty()) {
                continue;
            }

            // Skip if the shard is no longer started or no longer primary
            try {
                if (!tracked.shard.routingEntry().primary() || !tracked.shard.routingEntry().active()) {
                    continue;
                }
            } catch (Exception e) {
                // Shard may be closed concurrently
                continue;
            }

            // Read the processed local checkpoint — a lock-free volatile read.
            // This is the same data MVIndexingEngine.refresh() had access to.
            long currentCheckpoint;
            try {
                currentCheckpoint = tracked.shard.getProcessedLocalCheckpoint();
            } catch (Exception e) {
                // Shard may be closing
                continue;
            }

            // Check advance: only publish when checkpoint has moved forward
            AtomicLong lastPub = lastPublished.get(shardId);
            if (lastPub == null) continue;
            long last = lastPub.get();
            if (currentCheckpoint <= last) {
                skipCount.incrementAndGet();
                continue;
            }

            // CAS to avoid concurrent publishes for the same advance
            if (!lastPub.compareAndSet(last, currentCheckpoint)) {
                continue; // Another tick already published a newer value
            }

            // Publish: fire-and-forget to all bound targets.
            // We pass empty file lists — the push is a lightweight notification
            // that tells the target "source has data up to maxSeqNo=X; pull it".
            // The target's poller resolves actual files from remote store.
            tracked.publisher.publish(
                currentCheckpoint,
                tracked.shard.getOperationPrimaryTerm(),
                0L,          // infosVersion: not needed for notification-only publish
                List.of(),   // parquetFiles: target resolves from remote store
                List.of()    // fileSizes: not needed
            );
            publishCount.incrementAndGet();

            if (logger.isTraceEnabled()) {
                logger.trace(
                    "mv_replication: published checkpoint {} for source shard [{}] (advanced from {})",
                    currentCheckpoint,
                    shardId,
                    last
                );
            }
        }
    }

    // ── Lifecycle ────────────────────────────────────────────────────────

    @Override
    public void close() {
        closed = true;
        Scheduler.Cancellable tick = scheduledTick;
        if (tick != null) {
            tick.cancel();
        }
        trackedShards.clear();
        lastPublished.clear();
        logger.info("mv_replication: closed (ticks={}, publishes={}, skips={})", tickCount.get(), publishCount.get(), skipCount.get());
    }

    // ── Accessors for tests ──────────────────────────────────────────────

    /** Number of shards currently being tracked. */
    public int trackedShardCount() {
        return trackedShards.size();
    }

    /** Total ticks executed. */
    public long tickCount() {
        return tickCount.get();
    }

    /** Total checkpoint publishes (advances). */
    public long publishCount() {
        return publishCount.get();
    }

    /** Total tick iterations where checkpoint hadn't advanced (skipped). */
    public long skipCount() {
        return skipCount.get();
    }

    /** Whether the service is closed. */
    public boolean isClosed() {
        return closed;
    }
}
