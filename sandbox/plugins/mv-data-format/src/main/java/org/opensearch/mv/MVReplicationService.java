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
 * when targets need data.</p>
 *
 * <h2>Advert maxSeqNo (Defect 1 fix)</h2>
 *
 * <p>The checkpoint's maxSeqNo is derived from the catalog — the maximum
 * over all parquet filesets' maxSeqNo values. This ensures the advert
 * never claims seqNos that its file manifest does not cover. If ALL
 * filesets have unknown seq ranges (legacy, all -1), we fall back to the
 * processed local checkpoint (no worse than before). If the computed
 * advertMax &lt;= 0 or no files exist, publish is skipped.</p>
 *
 * <h2>Per-target lag publish (Defect 2 fix)</h2>
 *
 * <p>Instead of a global CAS-advance gate on lastPublished, per-shard state
 * tracks {@code lastObservedProcessed} and {@code lastAdvertMax}. The catalog
 * is read and a checkpoint is rebuilt when ANY of these conditions is true:</p>
 * <pre>
 *   (A) processed > lastObservedProcessed  — possible new data
 *   (B) processed > lastAdvertMax           — refresh may have closed the gap
 *                                             between processed and file coverage
 *   (C) publisher.anyTargetBehind(lastAdvertMax) — retry path: at least one
 *                                             target hasn't confirmed receipt
 * </pre>
 * <p>The publisher's per-target skip logic makes re-sends idempotent: targets
 * whose watermark >= advertMax receive nothing. A lost push is retried every
 * tick until the target's response confirms its watermark.</p>
 *
 * <p><b>Reconciliation:</b> Also implements {@link ClusterStateListener}
 * to detect source shards that were already STARTED before the MV target
 * index was created. On cluster state change, checks sourceToTargets for
 * any source index whose local primary shards are not yet tracked, and
 * adds them.</p>
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

    /**
     * Per-shard tracked state. Holds the shard reference, publisher, and
     * tick-local bookkeeping for the rebuild condition.
     *
     * <p>All fields except publisher are mutated only by tick() which runs
     * on a single-threaded scheduleWithFixedDelay — no CAS needed.</p>
     */
    static final class TrackedShard {
        final IndexShard shard;
        final MVCheckpointPublisher publisher;

        /**
         * Last processedLocalCheckpoint observed by tick(). Used to detect
         * new data. Updated after each tick iteration.
         */
        long lastObservedProcessed = -1L;

        /**
         * maxSeqNo of the last checkpoint built from the catalog. Represents
         * the highest seq coverage we last advertised. A refresh that adds
         * file coverage without a new processed advance is detected when
         * processed > lastAdvertMax.
         */
        long lastAdvertMax = -1L;

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

    /** Scheduler handle for the tick — null until started. */
    private volatile Scheduler.Cancellable scheduledTick;
    private volatile boolean closed;

    // ── Metrics ──────────────────────────────────────────────────────────

    private final AtomicLong tickCount = new AtomicLong();
    private final AtomicLong publishCount = new AtomicLong();
    private final AtomicLong skipCount = new AtomicLong();
    private final AtomicLong catalogReadCount = new AtomicLong();
    private final AtomicLong reconcileCount = new AtomicLong();
    private final AtomicLong unknownRangeSkipCount = new AtomicLong();

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
            logger.info("mv_replication: stopped tracking source shard [{}]", shardId);
        }
    }

    // ── Scheduled tick ───────────────────────────────────────────────────

    /**
     * Called every {@link #interval} on the GENERIC thread pool. Iterates
     * all tracked local primary source shards and publishes checkpoints
     * when targets need data.
     *
     * <p><b>Rebuild condition (per-shard):</b></p>
     * <p>Read the catalog and build a new checkpoint when ANY of:</p>
     * <pre>
     *   (A) processed > lastObservedProcessed  — new data committed
     *   (B) processed > lastAdvertMax           — refresh may have added
     *       file coverage that closes the gap between processed and what
     *       files covered last time (e.g. parquet flush lagged behind
     *       processed advance)
     *   (C) publisher.anyTargetBehind(lastAdvertMax) — at least one target
     *       hasn't confirmed receipt of the last advert; re-send is
     *       idempotent via per-target watermark skip in the publisher
     * </pre>
     *
     * <p>After building: lastObservedProcessed = currentProcessed;
     * lastAdvertMax = checkpoint.maxSeqNo. The publisher's per-target skip
     * ensures at-parity targets receive nothing.</p>
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

            long currentProcessed;
            try {
                currentProcessed = tracked.shard.getProcessedLocalCheckpoint();
            } catch (Exception e) {
                continue;
            }

            // ── Rebuild condition ────────────────────────────────────────
            // (A) New data: processed advanced since last observation
            boolean newData = currentProcessed > tracked.lastObservedProcessed;
            // (B) Refresh-closes-gap: processed is ahead of what files covered
            //     last time — a refresh may have flushed new parquet files
            boolean refreshClosedGap = currentProcessed > tracked.lastAdvertMax;
            // (C) Retry: at least one target hasn't confirmed the last advert
            boolean targetsBehind = tracked.publisher.anyTargetBehind(tracked.lastAdvertMax);

            if (!newData && !refreshClosedGap && !targetsBehind) {
                skipCount.incrementAndGet();
                continue;
            }

            // Update observation before catalog read
            tracked.lastObservedProcessed = currentProcessed;

            // ── REAL ADVERT: read catalog snapshot ───────────────────────
            Map<String, MVFileMetadata> fileMetadata = new java.util.LinkedHashMap<>();
            long infosVersion = 0L;
            long catalogAdvertMax = -1L;
            boolean anyUnknownRange = false;

            try (GatedCloseable<CatalogSnapshot> ref = tracked.shard.getCatalogSnapshot()) {
                catalogReadCount.incrementAndGet();
                CatalogSnapshot catalog = ref.get();
                infosVersion = catalog.getVersion();

                for (Segment seg : catalog.getSegments()) {
                    for (Map.Entry<String, WriterFileSet> fsEntry : seg.dfGroupedSearchableFiles().entrySet()) {
                        String formatName = fsEntry.getKey();
                        if (!"parquet".equals(formatName)) continue;

                        WriterFileSet wfs = fsEntry.getValue();
                        Path dir = Path.of(wfs.directory());
                        for (String fileName : wfs.files()) {
                            long size = -1L;
                            try {
                                Path filePath = dir.resolve(fileName);
                                if (Files.exists(filePath)) {
                                    size = Files.size(filePath);
                                }
                            } catch (Exception ignored) {}

                            // All writers populate ranges now. If a fileset
                            // has an unknown range, that is a BUG — log WARN
                            // and skip publish (never overclaim).
                            if (wfs.maxSeqNo() < 0) {
                                anyUnknownRange = true;
                            }

                            long crc32 = MVFileMetadata.CRC32_UNKNOWN;
                            fileMetadata.put(fileName, new MVFileMetadata(
                                size, wfs.minSeqNo(), wfs.maxSeqNo(), crc32
                            ));

                            // Track max seqNo across all filesets for advertMax
                            if (wfs.maxSeqNo() >= 0) {
                                if (wfs.maxSeqNo() > catalogAdvertMax) {
                                    catalogAdvertMax = wfs.maxSeqNo();
                                }
                            }
                        }
                    }
                }
            } catch (Exception e) {
                // Catalog read failure: skip this tick iteration with WARN,
                // next tick retries. Never send an empty-manifest advert.
                logger.warn(
                    "mv_replication: catalog read failed for shard [{}], skipping tick (will retry next interval): {}",
                    shardId,
                    e.getMessage()
                );
                continue;
            }

            // ── Unknown range = BUG: WARN and skip, never overclaim ──────
            if (anyUnknownRange) {
                unknownRangeSkipCount.incrementAndGet();
                logger.warn(
                    "mv_replication: shard [{}] has fileset(s) with unknown seq range — this is a bug "
                        + "(all writers must populate ranges). Skipping publish to avoid overclaim.",
                    shardId
                );
                continue;
            }

            // ── Determine advertMax ──────────────────────────────────────
            // advertMax comes from catalog file ranges, not
            // processedLocalCheckpoint. This prevents overclaiming seqNos
            // that are processed but not yet flushed to parquet.
            long advertMax;
            if (catalogAdvertMax > 0) {
                advertMax = catalogAdvertMax;
            } else {
                // No files or no coverable data — skip publish
                logger.trace("mv_replication: shard [{}] no coverable files, skipping publish", shardId);
                continue;
            }

            tracked.lastAdvertMax = advertMax;

            MVReplicationCheckpoint checkpoint = new MVReplicationCheckpoint(
                shardId.getIndexName(),
                shardId.id(),
                tracked.shard.getOperationPrimaryTerm(),
                advertMax,
                infosVersion,
                fileMetadata,
                System.currentTimeMillis()
            );

            int sent = tracked.publisher.publish(checkpoint);
            if (sent > 0) {
                publishCount.incrementAndGet();
            }

            if (logger.isTraceEnabled()) {
                logger.trace(
                    "mv_replication: published checkpoint advertMax={} for source shard [{}] "
                        + "(processed={}, catalogMax={}, sent={}, files={})",
                    advertMax,
                    shardId,
                    currentProcessed,
                    catalogAdvertMax,
                    sent,
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
        logger.info(
            "mv_replication: closed (ticks={}, publishes={}, skips={}, catalog_reads={}, reconciles={}, unknown_range_skips={})",
            tickCount.get(), publishCount.get(), skipCount.get(), catalogReadCount.get(), reconcileCount.get(), unknownRangeSkipCount.get()
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

    public long unknownRangeSkipCount() {
        return unknownRangeSkipCount.get();
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
