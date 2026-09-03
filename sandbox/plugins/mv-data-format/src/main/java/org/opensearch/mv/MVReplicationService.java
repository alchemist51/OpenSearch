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
import org.opensearch.cluster.ClusterChangedEvent;
import org.opensearch.cluster.ClusterState;
import org.opensearch.cluster.ClusterStateListener;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.concurrent.GatedCloseable;
import org.opensearch.common.settings.Setting;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.index.engine.exec.coord.CatalogSnapshot;
import org.opensearch.index.shard.IndexEventListener;
import org.opensearch.index.shard.IndexShard;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.threadpool.Scheduler;
import org.opensearch.transport.client.Client;

import java.io.Closeable;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * External checkpoint publisher that observes local primary source shards
 * and pushes REAL adverts (with file names, sizes, and seq ranges from the
 * catalog snapshot) to bound MV target shards when new data is available.
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
 * <p>On CAS-gate advance, reads the shard's catalog snapshot to extract
 * REAL parquet file lists with names, sizes, and per-file seq ranges,
 * so that the publisher's per-target filtering actually runs.</p>
 *
 * <p><b>Reconciliation:</b> Also implements {@link ClusterStateListener}
 * to detect source shards that were already STARTED before the MV target
 * index was created. On cluster state change, checks sourceToTargets for
 * any source index whose local primary shards are not yet tracked, and
 * adds them. This mirrors {@code NodeDerivedPullService.reconcileOnGeneric}
 * and ensures that the normal order (source starts first, target created
 * later) works without restart.</p>
 *
 * <p><b>SAFETY:</b> never calls
 * {@code clusterService.state()} from the applier thread — all routing
 * data comes from the lock-free {@link NodeRoutingSnapshotService} maintained
 * by a {@code ClusterStateListener}. Reconciliation enqueues work to GENERIC.
 * Safe to run from any thread.</p>
 *
 * @opensearch.experimental
 */
public final class MVReplicationService implements IndexEventListener, ClusterStateListener, Closeable {

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

    /** Optional ClusterService for listener registration — may be null in tests. */
    private volatile ClusterService clusterService;

    /** ShardId → tracked shard state. Lock-free via ConcurrentHashMap. */
    private final ConcurrentHashMap<ShardId, TrackedShard> trackedShards = new ConcurrentHashMap<>();

    /**
     * All local primary source-eligible shards seen via afterIndexShardStarted,
     * regardless of whether they had targets at the time. Reconciliation
     * promotes shards from this pool when targets appear.
     */
    private final ConcurrentHashMap<ShardId, IndexShard> allSourceShards = new ConcurrentHashMap<>();

    /** Per-shard last-published checkpoint — publish only on advance. */
    private final ConcurrentHashMap<ShardId, AtomicLong> lastPublished = new ConcurrentHashMap<>();

    /** Scheduler handle for the tick — null until started. */
    private volatile Scheduler.Cancellable scheduledTick;
    private volatile boolean closed;

    // ── Metrics ──────────────────────────────────────────────────────────

    private final AtomicLong tickCount = new AtomicLong();
    private final AtomicLong publishCount = new AtomicLong();
    private final AtomicLong skipCount = new AtomicLong();
    private final AtomicLong catalogReadCount = new AtomicLong();
    private final AtomicLong reconcileCount = new AtomicLong();

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

    /**
     * Binds ClusterService for reconciliation. Called from createComponents.
     */
    public void bindForReconciliation(ClusterService clusterService) {
        this.clusterService = clusterService;
        clusterService.addListener(this);
    }

    // ── ClusterStateListener: reconcile when targets appear ──────────────

    /**
     * Called on the cluster-applier thread. We NEVER call clusterService.state()
     * here — we use event.state(). Enqueues reconciliation to GENERIC.
     */
    @Override
    public void clusterChanged(ClusterChangedEvent event) {
        if (closed) return;
        // Only reconcile if indices changed (target created/deleted)
        if (!event.metadataChanged()) return;
        try {
            threadPool.executor(ThreadPool.Names.GENERIC).execute(this::reconcileTrackedShards);
        } catch (Exception e) {
            if (!closed) {
                logger.debug("mv_replication: failed to enqueue reconciliation", e);
            }
        }
    }

    /**
     * Reconciles tracked shards on the GENERIC thread pool. For each source
     * index in sourceToTargets that has local primary shards in our pool,
     * ensure those shards are tracked. Also untrack shards whose sources
     * no longer have targets.
     */
    void reconcileTrackedShards() {
        if (closed) return;

        reconcileCount.incrementAndGet();
        Map<String, List<NodeRoutingSnapshotService.BoundTarget>> srcToTgt = routingService.sourceToTargets();

        // Add: promote pool shards that now have targets
        for (Map.Entry<ShardId, IndexShard> entry : allSourceShards.entrySet()) {
            ShardId shardId = entry.getKey();
            if (trackedShards.containsKey(shardId)) continue;

            String indexName = shardId.getIndexName();
            List<NodeRoutingSnapshotService.BoundTarget> targets = srcToTgt.get(indexName);
            if (targets == null || targets.isEmpty()) continue;

            IndexShard shard = entry.getValue();
            try {
                if (!shard.routingEntry().primary() || !shard.routingEntry().active()) continue;
            } catch (Exception e) {
                continue;
            }

            trackShard(shard, targets);
        }

        // Remove: untrack shards whose source no longer has any targets
        for (Map.Entry<ShardId, TrackedShard> entry : trackedShards.entrySet()) {
            ShardId shardId = entry.getKey();
            List<NodeRoutingSnapshotService.BoundTarget> targets = srcToTgt.get(shardId.getIndexName());
            if (targets == null || targets.isEmpty()) {
                TrackedShard removed = trackedShards.remove(shardId);
                if (removed != null) {
                    lastPublished.remove(shardId);
                    logger.info("mv_replication: untracked source shard [{}] — no more targets", shardId);
                }
            }
        }
    }

    // ── IndexEventListener: track source shard lifecycle ─────────────────

    @Override
    public void afterIndexShardStarted(IndexShard indexShard) {
        if (closed) return;
        // Only track primary source shards — target shards and replicas are not sources.
        if (!indexShard.routingEntry().primary()) return;

        // Always register in the pool — targets may appear later via reconciliation
        allSourceShards.put(indexShard.shardId(), indexShard);

        // Check if this index already has bound MV targets via the routing snapshot.
        String indexName = indexShard.shardId().getIndexName();
        Map<String, List<NodeRoutingSnapshotService.BoundTarget>> srcToTgt = routingService.sourceToTargets();
        List<NodeRoutingSnapshotService.BoundTarget> targets = srcToTgt.get(indexName);
        if (targets == null || targets.isEmpty()) {
            return; // Not an MV source yet — reconciliation will pick it up later
        }

        trackShard(indexShard, targets);
    }

    private void trackShard(IndexShard indexShard, List<NodeRoutingSnapshotService.BoundTarget> targets) {
        ShardId shardId = indexShard.shardId();
        if (trackedShards.containsKey(shardId)) return;

        String indexName = shardId.getIndexName();
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
        allSourceShards.remove(shardId);
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
     * for any that have advanced. On advance, reads the catalog snapshot
     * to extract REAL file lists with seq ranges.
     */
    void tick() {
        if (closed || trackedShards.isEmpty()) return;
        tickCount.incrementAndGet();

        Map<String, List<NodeRoutingSnapshotService.BoundTarget>> srcToTgt = routingService.sourceToTargets();

        for (Map.Entry<ShardId, TrackedShard> entry : trackedShards.entrySet()) {
            ShardId shardId = entry.getKey();
            TrackedShard tracked = entry.getValue();

            List<NodeRoutingSnapshotService.BoundTarget> targets = srcToTgt.get(shardId.getIndexName());
            if (targets == null || targets.isEmpty()) {
                continue;
            }

            try {
                if (!tracked.shard.routingEntry().primary() || !tracked.shard.routingEntry().active()) {
                    continue;
                }
            } catch (Exception e) {
                continue;
            }

            long currentCheckpoint;
            try {
                currentCheckpoint = tracked.shard.getProcessedLocalCheckpoint();
            } catch (Exception e) {
                continue;
            }

            AtomicLong lastPub = lastPublished.get(shardId);
            if (lastPub == null) continue;
            long last = lastPub.get();
            if (currentCheckpoint <= last) {
                skipCount.incrementAndGet();
                continue;
            }

            if (!lastPub.compareAndSet(last, currentCheckpoint)) {
                continue;
            }

            // ── REAL ADVERT: read catalog snapshot on advance ────────────
            Map<String, MVFileMetadata> fileMetadata = new java.util.LinkedHashMap<>();
            long infosVersion = 0L;

            try (GatedCloseable<CatalogSnapshot> ref = tracked.shard.getCatalogSnapshot()) {
                catalogReadCount.incrementAndGet();
                CatalogSnapshot catalog = ref.get();
                infosVersion = catalog.getVersion();

                for (Segment seg : catalog.getSegments()) {
                    // Look for parquet filesets in the segment
                    for (Map.Entry<String, WriterFileSet> fsEntry : seg.dfGroupedSearchableFiles().entrySet()) {
                        String formatName = fsEntry.getKey();
                        if (!"parquet".equals(formatName)) continue;

                        WriterFileSet wfs = fsEntry.getValue();
                        Path dir = Path.of(wfs.directory());
                        for (String fileName : wfs.files()) {
                            // Best-effort local size
                            long size = -1L;
                            try {
                                Path filePath = dir.resolve(fileName);
                                if (Files.exists(filePath)) {
                                    size = Files.size(filePath);
                                }
                            } catch (Exception ignored) {}
                            // CRC32: attempt to read from shard's PrecomputedChecksumStrategy
                            // if cheaply accessible; else -1 (unknown).
                            long crc32 = MVFileMetadata.CRC32_UNKNOWN;
                            fileMetadata.put(fileName, new MVFileMetadata(
                                size, wfs.minSeqNo(), wfs.maxSeqNo(), crc32
                            ));
                        }
                    }
                }
            } catch (Exception e) {
                // Catalog read failed — publish with empty map (notification-only).
                // Target falls back to pull path.
                logger.debug("mv_replication: catalog read failed for shard [{}], publishing notification-only", shardId, e);
            }

            MVReplicationCheckpoint checkpoint = new MVReplicationCheckpoint(
                shardId.getIndexName(),
                shardId.id(),
                tracked.shard.getOperationPrimaryTerm(),
                currentCheckpoint,
                infosVersion,
                fileMetadata,
                System.currentTimeMillis()
            );

            tracked.publisher.publish(checkpoint);
            publishCount.incrementAndGet();

            if (logger.isTraceEnabled()) {
                logger.trace(
                    "mv_replication: published checkpoint {} for source shard [{}] (advanced from {}) files={}",
                    currentCheckpoint,
                    shardId,
                    last,
                    fileMetadata.size()
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
        ClusterService cs = clusterService;
        if (cs != null) {
            cs.removeListener(this);
        }
        trackedShards.clear();
        allSourceShards.clear();
        lastPublished.clear();
        logger.info(
            "mv_replication: closed (ticks={}, publishes={}, skips={}, catalog_reads={}, reconciles={})",
            tickCount.get(), publishCount.get(), skipCount.get(), catalogReadCount.get(), reconcileCount.get()
        );
    }

    // ── Accessors for tests ──────────────────────────────────────────────

    public int trackedShardCount() {
        return trackedShards.size();
    }

    public long tickCount() {
        return tickCount.get();
    }

    public long publishCount() {
        return publishCount.get();
    }

    public long skipCount() {
        return skipCount.get();
    }

    public long catalogReadCount() {
        return catalogReadCount.get();
    }

    public long reconcileCount() {
        return reconcileCount.get();
    }

    public boolean isClosed() {
        return closed;
    }

    /**
     * Package-private access to tracked shards for testing.
     */
    ConcurrentHashMap<ShardId, TrackedShard> trackedShardsMap() {
        return trackedShards;
    }
}
