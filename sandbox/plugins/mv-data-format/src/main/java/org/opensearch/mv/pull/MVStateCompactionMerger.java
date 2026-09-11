/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv.pull;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.metadata.DerivedIndexBinding;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.FieldTypeCapabilities;
import org.opensearch.index.engine.dataformat.MergeInput;
import org.opensearch.index.engine.dataformat.MergeResult;
import org.opensearch.index.engine.dataformat.Merger;
import org.opensearch.index.engine.exec.MonoFileWriterSet;
import org.opensearch.index.engine.exec.Segment;
import org.opensearch.index.engine.exec.WriterFileSet;
import org.opensearch.mv.MVCompiledDefinition;
import org.opensearch.mv.MVConstants;
import org.opensearch.mv.MVDefinitionResolver;
import org.opensearch.mv.MVDefinitionValidator;
import org.opensearch.mv.MVGroupByOrdering;

import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.UUID;

/**
 * Definition-aware compaction for a pull MV target shard: the engine's merge
 * scheduler picks published generations with its ordinary tiered policy and
 * hands them here; the merger folds their state rows by group key through the
 * native {@code PartialReduce} path and returns ONE artifact that carries the
 * identical Partial-stage schema, ordering and footer contract as a built
 * generation, so the read, upload and recovery paths cannot tell them apart.
 *
 * <p>What it is not: it never folds to answers (the output stays a partial
 * state), never touches the source, the watermark or the checkpoint protocol
 * (the catalog's userData is untouched by a merge), and never runs at the
 * same time as a round build of the same shard ({@link MVShardBuildLock}):
 * both draw from the node's shared native memory pool.</p>
 *
 * <p>Registered per node through
 * {@link org.opensearch.index.engine.derived.pull.spi.DerivedStateMergers} for
 * the {@code materialized_view} category; created once per target shard by
 * the engine.</p>
 */
public final class MVStateCompactionMerger implements Merger {

    private static final Logger logger = LogManager.getLogger(MVStateCompactionMerger.class);

    /** Key of the state fileset inside a segment; equality is by name. */
    private static final DataFormat STATE_FORMAT = new DataFormat() {
        @Override
        public String name() {
            return MVConstants.STATE_ARTIFACT_FORMAT;
        }

        @Override
        public long priority() {
            return 0;
        }

        @Override
        public Set<FieldTypeCapabilities> supportedFields() {
            return Set.of();
        }
    };

    private final IndexSettings indexSettings;
    private final ShardId shardId;
    private final Path formatDirectory;
    private final MVPullSettings.Services services;
    private final MVCompiledDefinition compiledDefinition;
    private final MVGroupByOrdering ordering;
    private final String partialSql;
    private final String sourceIndexName;
    private volatile MVBuildRuntime runtime;
    /** Input sets whose fold failed: generations -> failure time; refused fast until the cooldown has passed. */
    private final Map<String, Long> failedInputSets = new java.util.concurrent.ConcurrentHashMap<>();

    /** How long a failed input set is refused before the scheduler may retry it. */
    static final long FAILED_SET_COOLDOWN_MS = Long.getLong("opensearch.mv_pull.compaction.failed_set_cooldown_ms", 10 * 60_000L);

    /**
     * Largest input set (bytes on disk) one compaction accepts; the fold of a
     * bigger set risks exhausting the native memory pool that is sized for one
     * round build. The target's {@code index.merge.policy.max_merged_segment}
     * should sit below this so the policy never proposes a refused set.
     */
    static final long MAX_INPUT_BYTES = Long.getLong("opensearch.mv_pull.compaction.max_input_bytes", 768L * 1024 * 1024);

    /**
     * Largest input set (state rows) one compaction accepts. The fold hashes
     * every input row and then sorts the output IN MEMORY (the node's spill
     * limit may be 0), so rows — not bytes — drive the native memory: about
     * 2 KB per row for this definition's wide state, against the compaction's
     * private 12 GB pool. The target's {@code index.merge.policy.max_merged_segment}
     * (about 45 bytes per state row on disk) should be set so the policy's
     * proposals stay under this cap.
     */
    static final long MAX_INPUT_ROWS = Long.getLong("opensearch.mv_pull.compaction.max_input_rows", 4_000_000L);

    public MVStateCompactionMerger(IndexSettings indexSettings, ShardId shardId, Path shardDataPath, MVPullSettings.Services services) {
        this.indexSettings = Objects.requireNonNull(indexSettings, "indexSettings");
        this.shardId = Objects.requireNonNull(shardId, "shardId");
        this.formatDirectory = Objects.requireNonNull(shardDataPath, "shardDataPath").resolve(MVConstants.STATE_ARTIFACT_FORMAT);
        this.services = Objects.requireNonNull(services, "services");
        Settings settings = indexSettings.getSettings();
        this.compiledDefinition = MVDefinitionResolver.resolve(settings);
        this.ordering = compiledDefinition.groupByOrdering();
        this.partialSql = compiledDefinition.buildPartialSql(MVConstants.INPUT_TABLE);
        this.sourceIndexName = settings.get(DerivedIndexBinding.KEY_SOURCE_NAME);
    }

    @Override
    public MergeResult merge(MergeInput mergeInput) throws IOException {
        List<Segment> segments = mergeInput.segments();
        if (segments.isEmpty()) {
            throw new IllegalArgumentException("compaction needs at least one segment");
        }
        long newGeneration = mergeInput.newWriterGeneration();
        List<String> inputs = new ArrayList<>(segments.size());
        long inputBytes = 0L;
        long inputRows = 0L;
        for (Segment segment : segments) {
            WriterFileSet fileSet = segment.dfGroupedSearchableFiles().get(MVConstants.STATE_ARTIFACT_FORMAT);
            if (fileSet == null || fileSet.files().isEmpty()) {
                throw new IOException(
                    "compaction of shard [" + shardId + "]: segment generation " + segment.generation() + " has no state artifact"
                );
            }
            for (String file : fileSet.files()) {
                Path path = Path.of(fileSet.directory()).resolve(file);
                if (Files.isRegularFile(path) == false) {
                    throw new IOException("compaction of shard [" + shardId + "]: state artifact missing on disk: " + path);
                }
                inputs.add(path.toAbsolutePath().toString());
                inputBytes += Files.size(path);
            }
            inputRows += fileSet.numRows();
        }
        String inputKey = segments.stream()
            .map(Segment::generation)
            .sorted()
            .map(g -> Long.toString(g))
            .collect(java.util.stream.Collectors.joining(","));
        if (inputBytes > MAX_INPUT_BYTES || inputRows > MAX_INPUT_ROWS) {
            throw new IOException(
                "compaction of shard ["
                    + shardId
                    + "]: input set of "
                    + inputBytes
                    + " bytes / "
                    + inputRows
                    + " rows exceeds the cap of "
                    + MAX_INPUT_BYTES
                    + " bytes / "
                    + MAX_INPUT_ROWS
                    + " rows (generations "
                    + inputKey
                    + "); lower index.merge.policy.max_merged_segment on the target"
            );
        }
        Long failedAt = failedInputSets.get(inputKey);
        if (failedAt != null) {
            long ageMs = System.currentTimeMillis() - failedAt;
            if (ageMs < FAILED_SET_COOLDOWN_MS) {
                throw new IOException(
                    "compaction of shard ["
                        + shardId
                        + "]: input set (generations "
                        + inputKey
                        + ") failed "
                        + ageMs
                        + " ms ago; retried after "
                        + FAILED_SET_COOLDOWN_MS
                        + " ms"
                );
            }
            failedInputSets.remove(inputKey);
        }

        String sourceSchema = sourceSchemaWire();
        Files.createDirectories(formatDirectory);
        String fileName = MVConstants.stateFileName(newGeneration);
        Path completed = formatDirectory.resolve(fileName);
        Path temporary = formatDirectory.resolve(fileName + ".compact-" + UUID.randomUUID());

        // Runs on the merge scheduler's thread; never concurrently with the
        // poller's round build of this shard (same native memory pool).
        java.util.concurrent.locks.ReentrantLock lock = MVShardBuildLock.forShard(shardId);
        long tWait = System.nanoTime();
        lock.lock();
        long waitedMs = (System.nanoTime() - tWait) / 1_000_000;
        long t0 = System.nanoTime();
        MVBuildMetrics.INSTANCE.recordCompactionStarted();
        boolean success = false;
        try {
            MVBuildRuntime.ArtifactResult result = runtime().compactStateArtifact(
                sourceSchema,
                MVConstants.INPUT_TABLE,
                partialSql,
                inputs,
                temporary.toString(),
                ordering
            );
            moveCompletedArtifact(temporary, completed);
            long outputBytes = Files.size(completed);
            long durationMs = (System.nanoTime() - t0) / 1_000_000;
            WriterFileSet out = MonoFileWriterSet.of(
                formatDirectory.toAbsolutePath().toString(),
                newGeneration,
                fileName,
                result.rowCount()
            );
            MVBuildMetrics.INSTANCE.recordCompactionCompleted(segments.size(), inputBytes, result.rowCount(), outputBytes, durationMs);
            success = true;
            logger.info(
                "mv_pull COMPACTED shard=[{}] generation={} inputs={} input_rows={} input_bytes={} output_rows={} output_bytes={} "
                    + "compact_ms={} lock_wait_ms={} input_generations={}",
                shardId,
                newGeneration,
                segments.size(),
                inputRows,
                inputBytes,
                result.rowCount(),
                outputBytes,
                durationMs,
                waitedMs,
                segments.stream().map(s -> Long.toString(s.generation())).collect(java.util.stream.Collectors.joining(","))
            );
            return MergeResult.folding(Map.of(STATE_FORMAT, out));
        } finally {
            lock.unlock();
            if (success == false) {
                MVBuildMetrics.INSTANCE.recordCompactionFailed();
                failedInputSets.put(inputKey, System.currentTimeMillis());
                Files.deleteIfExists(temporary);
            }
        }
    }

    /**
     * The SOURCE schema the definition SQL is planned against (schema only —
     * no source data is read): taken from the source index's mapping in the
     * cluster state, exactly as view validation does.
     */
    private String sourceSchemaWire() throws IOException {
        if (sourceIndexName == null || sourceIndexName.isEmpty()) {
            throw new IOException("compaction of shard [" + shardId + "]: target declares no source index");
        }
        IndexMetadata source = services.clusterService().state().metadata().index(sourceIndexName);
        if (source == null) {
            throw new IOException(
                "compaction of shard [" + shardId + "]: source index [" + sourceIndexName + "] is not in the cluster state"
            );
        }
        return MVDefinitionValidator.sourceSchemaWire(source);
    }

    private MVBuildRuntime runtime() {
        MVBuildRuntime rt = runtime;
        if (rt == null) {
            synchronized (this) {
                rt = runtime;
                if (rt == null) {
                    Settings settings = indexSettings.getSettings();
                    rt = new MVBuildRuntime(
                        services.dataFusionRuntimePtr(),
                        MVBuildRuntime.MV_SPILL_BUDGET_BYTES.get(settings),
                        MVBuildRuntime.MV_SPILL_FILE_COUNT_LIMIT.get(settings),
                        MVBuildRuntime.MV_BUILD_MEMORY_ESTIMATE.get(settings),
                        services.parentCircuitBreaker()
                    );
                    runtime = rt;
                }
            }
        }
        return rt;
    }

    private static void moveCompletedArtifact(Path temporary, Path completed) throws IOException {
        try {
            Files.move(temporary, completed, StandardCopyOption.ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException unsupported) {
            Files.move(temporary, completed);
        }
    }
}
