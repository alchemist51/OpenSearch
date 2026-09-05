/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import org.opensearch.index.engine.dataformat.IndexingExecutionEngine;
import org.opensearch.index.engine.dataformat.Merger;
import org.opensearch.index.engine.dataformat.RefreshInput;
import org.opensearch.index.engine.dataformat.RefreshResult;
import org.opensearch.index.engine.dataformat.Writer;
import org.opensearch.index.engine.dataformat.WriterConfig;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.mv.merge.DataFusionMVStateMergeStrategy;
import org.opensearch.mv.merge.MVMergeExecutor;
import org.opensearch.mv.merge.MVMergeStrategy;
import org.opensearch.mv.merge.NoOpMVMergeStrategy;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * POC(mv) per-shard indexing engine for the derived materialized-view format.
 * Pass-through refresh (parquet-engine pattern). Merge scheduling is owned
 * by the standard data-format framework; this engine supplies an MV-specific
 * strategy for each serving mode.
 */
public final class MVIndexingEngine implements IndexingExecutionEngine<org.opensearch.index.engine.dataformat.DataFormat, MVDocumentInput> {

    /** Definition this engine's writers maintain (SOURCE raw defn or TARGET fold). */
    private final MVDefinitionSpec spec;

    /** The derived format this engine serves (materialized_view on sources, mv_state on targets). */
    private final org.opensearch.index.engine.dataformat.DataFormat format;

    private final ShardPath shardPath;
    private final String tableName;
    private final String sourceIndexName;
    /** Target MV indices for the separate-index ship path; empty = embedded mode. */
    private final java.util.List<String> shipTargets;
    private final java.util.function.Supplier<org.opensearch.transport.client.Client> clientSupplier;
    /**
     * @deprecated Ship-path legacy wiring. The pull-based flow uses
     * {@link #routingSnapshotSupplier} instead of querying cluster state from
     * engine callbacks. Retained for backward-compatible test coverage only.
     * TODO: remove once ship-path is fully replaced by pull-based flow.
     */
    @Deprecated
    private final java.util.function.Supplier<org.opensearch.cluster.service.ClusterService> clusterServiceSupplier;
    /**
     * Lock-free routing snapshot supplier — engine callbacks read this instead
     * of calling {@code clusterService.state()} which would deadlock on the
     * cluster-applier thread. See {@link NodeRoutingSnapshotService} for the
     * safety reasoning.
     */
    private final java.util.function.Supplier<TargetRoutingSnapshot> routingSnapshotSupplier;
    /** Standard data-format merger backed by the selected MV strategy. */
    private final Merger merger;
    /** Definition name from index.mv.definition — the ship fingerprint (D30 I6). */
    private final String definitionName;
    /** Fold checkpoint tracker (D30 I1): contiguity watermark over folded seq-nos. Created at onEngineOpen. */
    private volatile org.opensearch.index.seqno.LocalCheckpointTracker foldTracker;
    /** Translog fsync handle bound by the owning engine (D30 I2 — the phantom-op gate). */
    private volatile java.util.concurrent.Callable<Void> translogSync;
    /** Active source catalog; authoritative list of parquet files to reconcile. */
    private volatile java.util.function.Supplier<
        org.opensearch.common.concurrent.GatedCloseable<org.opensearch.index.engine.exec.coord.CatalogSnapshot>> catalogSnapshotSupplier;
    /**
     * Replay skip bound (D30 phase 3): the last commit's LOCAL CHECKPOINT.
     * Catch-up folds cover ops ≤ bound from committed parquet; translog
     * replay covers ops > bound via the normal ship path; the MV writer
     * skips ops ≤ bound so nothing folds twice and nothing is lost (ops in
     * the non-contiguous (checkpoint, maxSeqNo] commit tail replay normally).
     */
    private volatile long recoverySkipBound = -1L;
    /** Source no-ops awaiting a zero-row target coverage announcement. */
    private final java.util.concurrent.ConcurrentSkipListSet<Long> pendingNoOps = new java.util.concurrent.ConcurrentSkipListSet<>();
    /** Exact source no-op coverage retained in source commit metadata for target recovery. */
    private final java.util.concurrent.atomic.AtomicReference<MVSourceSeqCoverage> knownNoOps =
        new java.util.concurrent.atomic.AtomicReference<>(MVSourceSeqCoverage.EMPTY);

    public MVIndexingEngine(
        ShardPath shardPath,
        String indexName,
        MVDefinitionSpec spec,
        org.opensearch.index.engine.dataformat.DataFormat format,
        String definitionName,
        java.util.List<String> shipTargets,
        java.util.function.Supplier<org.opensearch.transport.client.Client> clientSupplier,
        java.util.function.Supplier<org.opensearch.cluster.service.ClusterService> clusterServiceSupplier,
        boolean stateMergeEnabled
    ) {
        this(
            shardPath,
            indexName,
            spec,
            format,
            definitionName,
            shipTargets,
            clientSupplier,
            clusterServiceSupplier,
            stateMergeEnabled,
            () -> TargetRoutingSnapshot.EMPTY
        );
    }

    public MVIndexingEngine(
        ShardPath shardPath,
        String indexName,
        MVDefinitionSpec spec,
        org.opensearch.index.engine.dataformat.DataFormat format,
        String definitionName,
        java.util.List<String> shipTargets,
        java.util.function.Supplier<org.opensearch.transport.client.Client> clientSupplier,
        java.util.function.Supplier<org.opensearch.cluster.service.ClusterService> clusterServiceSupplier,
        boolean stateMergeEnabled,
        java.util.function.Supplier<TargetRoutingSnapshot> routingSnapshotSupplier
    ) {
        this(
            shardPath,
            indexName,
            spec,
            format,
            definitionName,
            shipTargets,
            clientSupplier,
            clusterServiceSupplier,
            stateMergeEnabled,
            routingSnapshotSupplier,
            null,
            0L
        );
    }

    /**
     * Stage 4: primary constructor. {@code mergeDefinition} is the compiled MV
     * definition resolved by the shared {@link MVDefinitionResolver} (persisted
     * descriptor first, else legacy {@code compiledFor}). The plugin resolves it
     * from the target's settings and passes it here so the target-side state
     * merge honors a persisted descriptor. When {@code null} (legacy/test
     * callers), the engine falls back to {@link MVCompiledDefinition#compiledFor}
     * on {@code definitionName}, preserving prior behavior.
     */
    public MVIndexingEngine(
        ShardPath shardPath,
        String indexName,
        MVDefinitionSpec spec,
        org.opensearch.index.engine.dataformat.DataFormat format,
        String definitionName,
        java.util.List<String> shipTargets,
        java.util.function.Supplier<org.opensearch.transport.client.Client> clientSupplier,
        java.util.function.Supplier<org.opensearch.cluster.service.ClusterService> clusterServiceSupplier,
        boolean stateMergeEnabled,
        java.util.function.Supplier<TargetRoutingSnapshot> routingSnapshotSupplier,
        MVCompiledDefinition mergeDefinition,
        long mergeRuntimePtr
    ) {
        this.spec = spec;
        this.format = format;
        this.definitionName = definitionName;
        this.shardPath = shardPath;
        this.sourceIndexName = indexName;
        this.tableName = indexName.replace('-', '_').replace('.', '_');
        this.shipTargets = shipTargets == null ? java.util.List.of() : java.util.List.copyOf(shipTargets);
        this.clientSupplier = clientSupplier;
        this.clusterServiceSupplier = clusterServiceSupplier;
        this.routingSnapshotSupplier = routingSnapshotSupplier;
        MVMergeStrategy mergeStrategy;
        if (this.shipTargets.isEmpty() == false) {
            mergeStrategy = new NoOpMVMergeStrategy();
        } else if (stateMergeEnabled && MVStateDataFormat.NAME.equals(format.name())) {
            // Generic sorted merge: the definition is consulted ONLY for the
            // group-key COUNT (plain config — how many leading columns form
            // the sort key). Physical column names are read from the input
            // files at merge time. Fold semantics stay at query time.
            MVCompiledDefinition compiledDef = mergeDefinition;
            if (compiledDef == null) {
                try {
                    compiledDef = MVCompiledDefinition.compiledFor(definitionName);
                } catch (Exception e) {
                    compiledDef = null;
                }
            }
            if (compiledDef != null && mergeRuntimePtr != 0) {
                mergeStrategy = new DataFusionMVStateMergeStrategy(
                    format,
                    shardPath,
                    compiledDef.groupKeys().size(),
                    mergeRuntimePtr
                );
            } else {
                // Fail closed: without the key count or the shared runtime,
                // do not merge rather than merge incorrectly/unpooled.
                org.apache.logging.log4j.LogManager.getLogger(MVIndexingEngine.class)
                    .error(
                        "mv merge: state merge disabled for [{}] — {}",
                        definitionName,
                        compiledDef == null ? "group-key count unavailable" : "shared runtime pointer is 0"
                    );
                mergeStrategy = new NoOpMVMergeStrategy();
            }
        } else {
            mergeStrategy = new NoOpMVMergeStrategy();
        }
        this.merger = new MVMergeExecutor(mergeStrategy);
        try {
            Files.createDirectories(shardPath.getDataPath().resolve(getDataFormat().name()));
        } catch (IOException e) {
            throw new RuntimeException("failed to create mv dir", e);
        }
    }

    @Override
    public void onEngineOpen(long maxSeqNo, long localCheckpoint) {
        // Source side: seed contiguity at the committed checkpoint. Operations
        // at or below it are represented by authoritative committed parquet;
        // source-refresh reconciliation repairs any exact target complement.
        this.foldTracker = new org.opensearch.index.seqno.LocalCheckpointTracker(maxSeqNo, localCheckpoint);
        // Phase 3 replay skip bound: catch-up covers ops <= localCheckpoint
        // from committed parquet; translog replay covers ops above it.
        this.recoverySkipBound = localCheckpoint;
        if (shipTargets.isEmpty() == false) {
            String encodedNoOps = readOwnCommitUserData().get(MVConstants.SOURCE_NOOP_COVERAGE_KEY);
            knownNoOps.set(
                encodedNoOps == null ? MVSourceSeqCoverage.EMPTY : MVSourceSeqCoverage.decode(encodedNoOps).through(localCheckpoint)
            );
        }
        // Target side: seed the exact cursor ledger from our own last commit
        // so certification never regresses across restarts.
        if (MVStateDataFormat.NAME.equals(format.name())) {
            MVTargetCursorLedger.resetTarget(sourceIndexName, shardPath.getShardId().id());
            for (Map.Entry<String, String> e : readOwnCursorEntries().entrySet()) {
                String sourceKey = e.getKey(); // "<sourceIndex>.<sourceShard>"
                int dot = sourceKey.lastIndexOf('.');
                if (dot <= 0) {
                    continue;
                }
                MVTargetCursorLedger.seed(
                    sourceIndexName,
                    shardPath.getShardId().id(),
                    sourceKey.substring(0, dot),
                    Integer.parseInt(sourceKey.substring(dot + 1)),
                    MVTargetCursorLedger.Cursor.decode(e.getValue()),
                    MVTargetCursorLedger.decodeCommitCoverage(e.getValue())
                );
            }
        }
    }

    @Override
    public void onNoOp(long seqNo) {
        if (shipTargets.isEmpty() == false && seqNo >= 0) {
            MVSourceSeqCoverage oneNoOp = MVSourceSeqCoverage.ofSeqNos(java.util.List.of(seqNo));
            knownNoOps.updateAndGet(coverage -> coverage.union(oneNoOp));
            if (seqNo > recoverySkipBound) {
                pendingNoOps.add(seqNo);
            }
        }
        org.opensearch.index.seqno.LocalCheckpointTracker t = foldTracker;
        if (t != null) {
            t.markSeqNoAsProcessed(seqNo);
        }
    }

    @Override
    public void bindTranslogSync(java.util.concurrent.Callable<Void> translogSync) {
        this.translogSync = translogSync;
    }

    @Override
    public void bindCatalogSnapshotSupplier(
        java.util.function.Supplier<
            org.opensearch.common.concurrent.GatedCloseable<org.opensearch.index.engine.exec.coord.CatalogSnapshot>> catalogSnapshotSupplier
    ) {
        this.catalogSnapshotSupplier = catalogSnapshotSupplier;
    }

    /** Fold tracker accessor for this format's writers (null until engine open). */
    org.opensearch.index.seqno.LocalCheckpointTracker foldTracker() {
        return foldTracker;
    }

    /** Translog fsync gate for the ship path (null until bound). */
    java.util.concurrent.Callable<Void> translogSync() {
        return translogSync;
    }

    /** Phase 3 replay skip bound: ops ≤ this are catch-up-covered; the MV writer must skip them. */
    long recoverySkipBound() {
        return recoverySkipBound;
    }

    String definitionName() {
        return definitionName;
    }

    private Map<String, String> readOwnCommitUserData() {
        try (org.apache.lucene.store.FSDirectory dir = org.apache.lucene.store.FSDirectory.open(shardPath.getDataPath().resolve("index"))) {
            return Map.copyOf(org.apache.lucene.index.SegmentInfos.readLatestCommit(dir).getUserData());
        } catch (Exception e) {
            return Map.of();
        }
    }

    /** Reads mv.cursor.* entries from this shard's own last commit user data. */
    private Map<String, String> readOwnCursorEntries() {
        Map<String, String> out = new HashMap<>();
        for (Map.Entry<String, String> e : readOwnCommitUserData().entrySet()) {
            if (e.getKey().startsWith(MVConstants.CURSOR_KEY_PREFIX)) {
                out.put(e.getKey().substring(MVConstants.CURSOR_KEY_PREFIX.length()), e.getValue());
            }
        }
        return out;
    }

    @Override
    public Writer<MVDocumentInput> createWriter(WriterConfig config) {
        MVStateShipper shipper = null;
        if (shipTargets.isEmpty() == false) {
            org.opensearch.transport.client.Client client = clientSupplier.get();
            if (client == null) {
                throw new IllegalStateException("mv ship targets " + shipTargets + " configured but node client not initialized");
            }
            shipper = new MVStateShipper(client, shipTargets, sourceIndexName, shardPath.getShardId(), routingSnapshotSupplier.get(), spec);
        }
        return new MVWriter(config.writerGeneration(), shardPath, tableName, spec, getDataFormat(), shipper, this);
    }

    /** Reconciles every target before this source refresh advances visibility. */
    @Override
    public void beforeRefresh() {
        announceNoOps();
        reconcileTargets();
    }

    private void announceNoOps() {
        if (pendingNoOps.isEmpty() || shipTargets.isEmpty()) {
            return;
        }
        java.util.List<Long> announced = java.util.List.copyOf(pendingNoOps);
        MVSourceSeqCoverage coverage = MVSourceSeqCoverage.ofSeqNos(announced);
        java.util.concurrent.Callable<Void> fsyncGate = translogSync;
        if (fsyncGate != null) {
            try {
                fsyncGate.call();
            } catch (Exception e) {
                throw new IllegalStateException("mv no-op announcement: source translog fsync failed", e);
            }
        }
        org.opensearch.transport.client.Client client = clientSupplier.get();
        if (client == null) {
            throw new IllegalStateException("mv no-op announcement: node client is not initialized");
        }
        long maxNoOp = announced.get(announced.size() - 1);
        long generation = Long.MIN_VALUE + maxNoOp;
        MVStateShipper shipper = new MVStateShipper(
            client,
            shipTargets,
            sourceIndexName,
            shardPath.getShardId(),
            routingSnapshotSupplier.get(),
            spec
        );
        try {
            shipper.replicateCoverageOnly(
                new DerivedStateReplicator.BatchCoordinates(
                    generation,
                    foldTracker == null ? -1L : foldTracker.getProcessedCheckpoint(),
                    maxNoOp,
                    definitionName,
                    "noops-" + coverage.encode(),
                    coverage
                )
            );
        } catch (IOException e) {
            throw new IllegalStateException("mv no-op announcement failed for " + coverage, e);
        }
        pendingNoOps.removeAll(announced);
    }

    private void reconcileTargets() {
        if (shipTargets.isEmpty()) {
            return;
        }
        org.opensearch.transport.client.Client client = clientSupplier.get();
        if (client == null) {
            throw new IllegalStateException("mv targets configured but node client is not initialized");
        }
        catchUpTargets(client);
    }

    /**
     * Source-refresh reconciliation retains the target's exact published
     * coverage and ships only its complement through the source fold bound
     * from the active authoritative parquet catalog. Holding the catalog
     * reference excludes unreferenced pre-merge inputs while retaining the
     * selected files through the query. Target durability is separate: later
     * source commits asynchronously cap and trigger target commits without
     * participating in this publication path. Normal source translog replay
     * covers the uncommitted tail above {@link #recoverySkipBound}.
     */
    private void catchUpTargets(org.opensearch.transport.client.Client client) {
        org.opensearch.index.seqno.LocalCheckpointTracker tracker = foldTracker;
        long bound = tracker == null ? recoverySkipBound : Math.max(recoverySkipBound, tracker.getProcessedCheckpoint());
        if (bound < 0) {
            return;
        }
        java.util.function.Supplier<
            org.opensearch.common.concurrent.GatedCloseable<org.opensearch.index.engine.exec.coord.CatalogSnapshot>> snapshotSupplier =
                catalogSnapshotSupplier;
        if (snapshotSupplier == null) {
            throw new IllegalStateException("mv catch-up: source catalog is not initialized");
        }

        try (var snapshotRef = snapshotSupplier.get()) {
            java.util.List<org.opensearch.index.engine.exec.MonoFileWriterSet> parquetFiles = snapshotRef.get()
                .getSearchableFiles("parquet")
                .stream()
                .map(org.opensearch.index.engine.exec.MonoFileWriterSet::from)
                .sorted(
                    java.util.Comparator.comparingLong(org.opensearch.index.engine.exec.MonoFileWriterSet::writerGeneration)
                        .thenComparing(org.opensearch.index.engine.exec.MonoFileWriterSet::file)
                )
                .toList();
            Path stagedCatalog = Files.createTempDirectory(shardPath.getDataPath(), "mv-catch-up-");
            try {
                for (int i = 0; i < parquetFiles.size(); i++) {
                    var file = parquetFiles.get(i);
                    // Resolve through the staged directory's own filesystem
                    // provider (tests wrap shard paths in a filter provider;
                    // Path.of would produce an incompatible default path).
                    Files.createLink(
                        stagedCatalog.resolve(String.format(java.util.Locale.ROOT, "%020d-%04d.parquet", file.writerGeneration(), i)),
                        stagedCatalog.getFileSystem().getPath(file.directory(), file.file())
                    );
                }

                for (String target : shipTargets) {
                    TargetClaim targetClaim = readTargetCursorWithRetry(client, target);
                    MVTargetCursorLedger.Cursor cursor = targetClaim.cursor();
                    java.util.List<MVSourceSeqCoverage.Range> missing = targetClaim.coverage().missingThrough(bound);
                    if (missing.isEmpty()) {
                        org.apache.logging.log4j.LogManager.getLogger(MVIndexingEngine.class)
                            .info("mv catch-up: target [{}] exactly covers source operations through {}", target, bound);
                        continue;
                    }
                    MVSourceSeqCoverage missingCoverage = MVSourceSeqCoverage.ofRanges(missing);
                    MVSourceSeqCoverage provenNoOps = missingCoverage.intersection(knownNoOps.get().through(bound));
                    MVSourceSeqCoverage dataCoverage = missingCoverage.subtract(provenNoOps);
                    MVStateShipper targetReplicator = new MVStateShipper(
                        client,
                        java.util.List.of(target),
                        sourceIndexName,
                        shardPath.getShardId(),
                        routingSnapshotSupplier.get(),
                        spec
                    );
                    long total;
                    long recoveryGeneration = cursor.certifiedGeneration() == Long.MAX_VALUE
                        ? Long.MAX_VALUE
                        : cursor.certifiedGeneration() + 1L;
                    String batchIdentity = "recovery-" + missingCoverage.encode();
                    DerivedStateReplicator.BatchCoordinates coordinates = new DerivedStateReplicator.BatchCoordinates(
                        recoveryGeneration,
                        missingCoverage.floor(),
                        missingCoverage.maxClaimedSeqNo(),
                        definitionName,
                        batchIdentity,
                        missingCoverage
                    );
                    if (dataCoverage.equals(MVSourceSeqCoverage.EMPTY)) {
                        try {
                            total = targetReplicator.replicateCoverageOnly(coordinates);
                        } catch (IOException e) {
                            throw new IllegalStateException(
                                "mv catch-up: no-op coverage replication failed for target [" + target + "]",
                                e
                            );
                        }
                    } else if (parquetFiles.isEmpty() == false) {
                        String predicate = recoveryPredicate(dataCoverage.ranges());
                        String filteredSql = spec.sql()
                            .replace(
                                "FROM " + MVConstants.INPUT_TABLE,
                                "FROM (SELECT * FROM " + MVConstants.INPUT_TABLE + " WHERE " + predicate + ") AS " + MVConstants.INPUT_TABLE
                            );
                        try (org.apache.arrow.memory.RootAllocator allocator = new org.apache.arrow.memory.RootAllocator()) {
                            try (
                                org.apache.arrow.c.ArrowArray array = org.apache.arrow.c.ArrowArray.allocateNew(allocator);
                                org.apache.arrow.c.ArrowSchema schema = org.apache.arrow.c.ArrowSchema.allocateNew(allocator)
                            ) {
                                long rows;
                                try {
                                    rows = MVNativeBridge.buildArrow(
                                        stagedCatalog.toString(),
                                        MVConstants.INPUT_TABLE,
                                        filteredSql,
                                        array.memoryAddress(),
                                        schema.memoryAddress()
                                    );
                                } catch (RuntimeException noRows) {
                                    if (noRows.getMessage() != null && noRows.getMessage().contains("partial produced no batches")) {
                                        rows = 0;
                                    } else {
                                        throw noRows;
                                    }
                                }
                                requireRecoveryRows(dataCoverage, rows);
                                org.apache.arrow.vector.VectorSchemaRoot batch = org.apache.arrow.c.Data.importVectorSchemaRoot(
                                    allocator,
                                    array,
                                    schema,
                                    null
                                );
                                total = targetReplicator.replicate(batch, coordinates);
                            }
                        } catch (IOException e) {
                            throw new IllegalStateException("mv catch-up: replication failed for target [" + target + "]", e);
                        }
                    } else {
                        throw new IllegalStateException(
                            "mv catch-up: unexplained source-data hole " + dataCoverage + " has no active source parquet"
                        );
                    }
                    org.apache.logging.log4j.LogManager.getLogger(MVIndexingEngine.class)
                        .info(
                            "mv catch-up: target [{}] replicated {} state rows from {} files for exact complement {} (proven no-ops {})",
                            target,
                            total,
                            parquetFiles.size(),
                            missingCoverage,
                            provenNoOps
                        );
                }
            } finally {
                try (java.util.stream.Stream<Path> files = Files.list(stagedCatalog)) {
                    for (Path file : files.toList()) {
                        Files.deleteIfExists(file);
                    }
                } finally {
                    Files.deleteIfExists(stagedCatalog);
                }
            }
        } catch (IOException e) {
            throw new IllegalStateException("mv catch-up: source catalog staging or release failed", e);
        }
    }

    static String recoveryPredicate(java.util.List<MVSourceSeqCoverage.Range> missing) {
        return missing.stream()
            .map(range -> "(\"_seq_no\" >= " + range.start() + " AND \"_seq_no\" <= " + range.end() + ")")
            .collect(java.util.stream.Collectors.joining(" OR "));
    }

    static void requireRecoveryRows(MVSourceSeqCoverage dataCoverage, long rows) {
        if (rows == 0 && dataCoverage.equals(MVSourceSeqCoverage.EMPTY) == false) {
            throw new IllegalStateException("mv catch-up: unexplained source-data hole " + dataCoverage + " produced no rows");
        }
    }

    private record TargetClaim(MVTargetCursorLedger.Cursor cursor, MVSourceSeqCoverage coverage) {
    }

    /** Waits for the colocated target primary and reads its live exact published claim. */
    private TargetClaim readTargetCursorWithRetry(org.opensearch.transport.client.Client client, String target) {
        long deadline = System.nanoTime() + java.util.concurrent.TimeUnit.SECONDS.toNanos(90);
        while (true) {
            try {
                MVCursorAction.Response response = client.execute(
                    MVCursorAction.INSTANCE,
                    new MVCursorAction.Request(target, shardPath.getShardId().id(), sourceIndexName, shardPath.getShardId().id())
                ).actionGet();
                return new TargetClaim(
                    new MVTargetCursorLedger.Cursor(response.certifiedGeneration(), response.checkpoint()),
                    response.sourceCoverage()
                );
            } catch (Exception e) {
                String trace = org.opensearch.ExceptionsHelper.stackTrace(e);
                if (trace.contains("does not exist") || System.nanoTime() >= deadline) {
                    throw new IllegalStateException("mv cursor read failed for target [" + target + "]", e);
                }
                try {
                    Thread.sleep(2000);
                } catch (InterruptedException interrupted) {
                    Thread.currentThread().interrupt();
                    throw new IllegalStateException("mv cursor read interrupted for target [" + target + "]", interrupted);
                }
            }
        }
    }

    @Override
    public boolean retainCatalogSnapshot(org.opensearch.index.engine.exec.coord.CatalogSnapshot snapshot) {
        return false;
    }

    /** Cursor candidates written by beforeCommit and published by afterCommit. */
    private volatile Map<String, MVTargetCursorLedger.Cursor> pendingCursorCommit = Map.of();
    private volatile Map<String, MVSourceSeqCoverage> pendingCoverageCommit = Map.of();

    @Override
    public java.util.Map<String, String> beforeCommit(long committedLocalCheckpoint) throws IOException {
        if (shipTargets.isEmpty() == false) {
            MVSourceSeqCoverage durableNoOps = knownNoOps.get().through(committedLocalCheckpoint);
            return durableNoOps.equals(MVSourceSeqCoverage.EMPTY)
                ? Map.of()
                : Map.of(MVConstants.SOURCE_NOOP_COVERAGE_KEY, durableNoOps.encode());
        }
        if (MVStateDataFormat.NAME.equals(format.name()) == false) {
            // Embedded source formats have no target-owned cursor metadata.
            return Map.of();
        }
        Map<String, MVTargetCursorLedger.Cursor> candidates = MVTargetCursorLedger.commitCandidatesForTarget(
            sourceIndexName,
            shardPath.getShardId().id()
        );
        Map<String, MVTargetCursorLedger.Cursor> selectedCursors = new HashMap<>();
        Map<String, MVSourceSeqCoverage> exactClaims = new HashMap<>();
        Map<String, String> cursorMeta = new HashMap<>();
        for (Map.Entry<String, MVTargetCursorLedger.Cursor> entry : candidates.entrySet()) {
            int separator = entry.getKey().lastIndexOf('.');
            if (separator <= 0) {
                continue;
            }
            MVSourceSeqCoverage exactClaim = MVTargetCursorLedger.certifiedCoverage(
                sourceIndexName,
                shardPath.getShardId().id(),
                entry.getKey().substring(0, separator),
                Integer.parseInt(entry.getKey().substring(separator + 1))
            );
            long sourceCommitCap = MVTargetCursorLedger.sourceCommitCap(
                sourceIndexName,
                shardPath.getShardId().id(),
                entry.getKey().substring(0, separator),
                Integer.parseInt(entry.getKey().substring(separator + 1))
            );
            if (exactClaim.maxClaimedSeqNo() > sourceCommitCap) {
                throw new IOException(
                    "mv target commit refused: published claim through "
                        + exactClaim.maxClaimedSeqNo()
                        + " exceeds source committed checkpoint "
                        + sourceCommitCap
                        + " for ["
                        + entry.getKey()
                        + "]"
                );
            }
            MVSourceSeqCoverage cappedClaim = exactClaim.through(sourceCommitCap);
            MVTargetCursorLedger.Cursor selectedCursor = cappedClaim.maxClaimedSeqNo() < 0
                ? MVTargetCursorLedger.Cursor.NONE
                : new MVTargetCursorLedger.Cursor(entry.getValue().certifiedGeneration(), cappedClaim.floor());
            selectedCursors.put(entry.getKey(), selectedCursor);
            exactClaims.put(entry.getKey(), cappedClaim);
            if (selectedCursor.certifiedGeneration() >= 0) {
                cursorMeta.put(
                    MVConstants.CURSOR_KEY_PREFIX + entry.getKey(),
                    MVTargetCursorLedger.encodeCommit(selectedCursor, cappedClaim)
                );
            }
        }
        pendingCursorCommit = Map.copyOf(selectedCursors);
        pendingCoverageCommit = Map.copyOf(exactClaims);
        return cursorMeta;
    }

    @Override
    public void afterCommit(long committedLocalCheckpoint) {
        if (shipTargets.isEmpty() == false) {
            org.opensearch.transport.client.Client client = clientSupplier.get();
            if (client == null) {
                org.apache.logging.log4j.LogManager.getLogger(MVIndexingEngine.class)
                    .error("mv source commit: node client unavailable for checkpoint {}", committedLocalCheckpoint);
                return;
            }
            for (String target : shipTargets) {
                // SAFETY: read routing snapshot instead of clusterService.state()
                // to avoid deadlock on the cluster-applier thread. See
                // NodeRoutingSnapshotService for the safety reasoning.
                TargetRoutingSnapshot routingSnapshot = routingSnapshotSupplier.get();
                int targetShard = routingSnapshot.resolveTargetShard(target, shardPath.getShardId().id());
                if (targetShard < 0) {
                    org.apache.logging.log4j.LogManager.getLogger(MVIndexingEngine.class)
                        .error("mv source commit: target [{}] does not exist for checkpoint {}", target, committedLocalCheckpoint);
                    continue;
                }
                client.execute(
                    MVSourceCommitAction.INSTANCE,
                    new MVSourceCommitAction.Request(
                        target,
                        targetShard,
                        sourceIndexName,
                        shardPath.getShardId().id(),
                        committedLocalCheckpoint
                    ),
                    org.opensearch.core.action.ActionListener.wrap(
                        response -> org.apache.logging.log4j.LogManager.getLogger(MVIndexingEngine.class)
                            .debug("mv source commit: target [{}] capped through {}", target, response.committedCheckpoint()),
                        failure -> org.apache.logging.log4j.LogManager.getLogger(MVIndexingEngine.class)
                            .error("mv source commit: asynchronous target signal failed for [" + target + "]", failure)
                    )
                );
            }
            return;
        }
        Map<String, MVTargetCursorLedger.Cursor> committed = pendingCursorCommit;
        Map<String, MVSourceSeqCoverage> committedCoverage = pendingCoverageCommit;
        pendingCursorCommit = Map.of();
        pendingCoverageCommit = Map.of();
        for (Map.Entry<String, MVTargetCursorLedger.Cursor> entry : committed.entrySet()) {
            int separator = entry.getKey().lastIndexOf('.');
            if (separator <= 0) {
                continue;
            }
            MVTargetCursorLedger.markCommitted(
                sourceIndexName,
                shardPath.getShardId().id(),
                entry.getKey().substring(0, separator),
                Integer.parseInt(entry.getKey().substring(separator + 1)),
                entry.getValue(),
                committedCoverage.getOrDefault(entry.getKey(), MVSourceSeqCoverage.contiguous(entry.getValue().checkpoint()))
            );
        }
    }

    @Override
    public boolean commitReady() {
        if (shipTargets.isEmpty() == false || MVStateDataFormat.NAME.equals(format.name()) == false) {
            return true;
        }
        return MVTargetCursorLedger.allPublishedWithinSourceCommitCaps(sourceIndexName, shardPath.getShardId().id());
    }

    @Override
    public boolean isMergeEligible(java.util.List<org.opensearch.index.engine.exec.Segment> segments) {
        if (shipTargets.isEmpty() == false || MVStateDataFormat.NAME.equals(format.name()) == false) {
            return true;
        }
        // Conservative whole-snapshot proof: when the exact published claim
        // equals the exact durable claim, every segment in the current catalog
        // is from the committed snapshot. If any refreshed batch is newer,
        // reject every candidate rather than guessing which input contains it.
        return MVTargetCursorLedger.allAppliedCommitted(sourceIndexName, shardPath.getShardId().id());
    }

    @Override
    public Merger getMerger() {
        return merger;
    }

    @Override
    public RefreshResult refresh(RefreshInput refreshInput) {
        if (refreshInput == null) {
            return new RefreshResult(List.of());
        }
        if (shipTargets.isEmpty() && MVStateDataFormat.NAME.equals(format.name())) {
            // DataFormatAwareEngine invokes this while holding refreshLock.
            // The writer files in refreshInput therefore publish in the same
            // critical section that makes their staged source cursor claims
            // eligible for the next target commit.
            MVTargetCursorLedger.promoteAll(sourceIndexName, shardPath.getShardId().id());
        }
        List<Segment> segments = new ArrayList<>();
        segments.addAll(refreshInput.existingSegments());
        segments.addAll(refreshInput.writerFiles());
        return new RefreshResult(List.copyOf(segments));
    }

    @Override
    public long getNextWriterGeneration() {
        throw new UnsupportedOperationException("generation is owned by DataFormatAwareEngine");
    }

    @Override
    public org.opensearch.index.engine.dataformat.DataFormat getDataFormat() {
        return format;
    }

    @Override
    public long getHeapBytesUsed() {
        return 0;
    }

    @Override
    public long getNativeBytesUsed() {
        return 0;
    }

    @Override
    public Map<String, Collection<String>> deleteFiles(Map<String, Collection<String>> filesToDelete) {
        Map<String, Collection<String>> failed = new HashMap<>();
        String formatName = format.name();
        Collection<String> mvFiles = filesToDelete.get(formatName);
        if (mvFiles == null) {
            return failed;
        }
        Path dir = shardPath.getDataPath().resolve(formatName);
        List<String> failures = new ArrayList<>();
        for (String f : mvFiles) {
            try {
                Files.deleteIfExists(dir.resolve(f));
            } catch (IOException e) {
                failures.add(f);
            }
        }
        if (failures.isEmpty() == false) {
            failed.put(formatName, failures);
        }
        return failed;
    }

    @Override
    public MVDocumentInput newDocumentInput() {
        return new MVDocumentInput(spec);
    }

    @Override
    public org.opensearch.index.engine.exec.commit.IndexStoreProvider getProvider() {
        return null;
    }

    @Override
    public void close() {}
}
