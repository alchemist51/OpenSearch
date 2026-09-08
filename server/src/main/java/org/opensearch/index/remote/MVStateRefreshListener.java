/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.remote;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.lucene.search.ReferenceManager;
import org.opensearch.core.index.shard.ShardId;

import java.io.IOException;
import java.nio.file.DirectoryStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Post-refresh listener that uploads sealed MV partial files to remote store.
 *
 * <p>Registered on source shards that have {@code mv_definitions} in their
 * IndexMetadata customData. After each refresh (which triggers finalize_writer
 * → Rust seal → local partial files), this listener:</p>
 * <ol>
 *   <li>Scans the local {@code <shardData>/mv_state/<mvId>/} directories for
 *       sealed partial files.</li>
 *   <li>Calls {@link MVStateRemoteManager#uploadGeneration} (data files first,
 *       manifest LAST) for each MV.</li>
 *   <li>Updates the in-memory {@link MVCheckpoint} ONLY after manifest commit
 *       (defect-#30 invariant).</li>
 *   <li>Cleans up local partial files after successful upload.</li>
 * </ol>
 *
 * <p>Failure → retry next refresh. Ingest is never blocked.</p>
 *
 * @opensearch.internal
 */
public class MVStateRefreshListener implements ReferenceManager.RefreshListener {

    private static final Logger logger = LogManager.getLogger(MVStateRefreshListener.class);

    private final ShardId shardId;
    private final Path shardDataPath;
    private final MVStateRemoteManager remoteManager;
    private final long primaryTerm;
    private final long defMetadataVersion;
    private final Map<String, String> mvDefinitions; // mvId -> definitionHash
    private final AtomicReference<MVCheckpoint> lastCheckpoint;

    /** Generation counters per MV, used for upload naming. */
    private final Map<String, Long> genCounters;

    public MVStateRefreshListener(
        ShardId shardId,
        Path shardDataPath,
        MVStateRemoteManager remoteManager,
        long primaryTerm,
        long defMetadataVersion,
        Map<String, String> mvDefinitions
    ) {
        this.shardId = shardId;
        this.shardDataPath = shardDataPath;
        this.remoteManager = remoteManager;
        this.primaryTerm = primaryTerm;
        this.defMetadataVersion = defMetadataVersion;
        this.mvDefinitions = Collections.unmodifiableMap(mvDefinitions);
        this.lastCheckpoint = new AtomicReference<>(
            new MVCheckpoint(shardId, primaryTerm, -1, defMetadataVersion, Map.of())
        );
        this.genCounters = new HashMap<>();
    }

    /**
     * Initialize generation counters from remote manifests (restart/promotion).
     */
    public void initFromRemote() throws IOException {
        Map<String, MVCheckpoint.MVPartialEntry> entries = new HashMap<>();
        long maxSeqNo = -1;

        for (Map.Entry<String, String> def : mvDefinitions.entrySet()) {
            String mvId = def.getKey();
            long resumeGen = remoteManager.resumeGeneration(String.valueOf(shardId.id()), mvId);
            genCounters.put(mvId, resumeGen);
            logger.info("MV gen resume: shard={} mvId={} startGen={}", shardId, mvId, resumeGen);

            // Rebuild checkpoint from latest manifest
            List<String> manifests = remoteManager.listManifests(String.valueOf(shardId.id()), mvId);
            if (!manifests.isEmpty()) {
                MVStateManifest manifest = remoteManager.readManifest(
                    String.valueOf(shardId.id()), mvId, manifests.get(0)
                );
                List<MVCheckpoint.FileInfo> files = manifest.files().stream()
                    .map(f -> new MVCheckpoint.FileInfo(f.name(), f.length(), f.checksum()))
                    .toList();
                long sizeBytes = manifest.files().stream().mapToLong(MVStateManifest.FileEntry::length).sum();
                entries.put(mvId, new MVCheckpoint.MVPartialEntry(
                    manifest.generation(), manifest.maxSeqNo(), files, 0, sizeBytes
                ));
                if (manifest.maxSeqNo() > maxSeqNo) {
                    maxSeqNo = manifest.maxSeqNo();
                }
            }
        }

        lastCheckpoint.set(new MVCheckpoint(shardId, primaryTerm, maxSeqNo, defMetadataVersion, entries));
    }

    /**
     * Return the generation to use for writer registration (resume point).
     */
    public long getStartGeneration(String mvId) {
        return genCounters.getOrDefault(mvId, 1L);
    }

    /**
     * Get the last committed checkpoint (derived from remote manifests only).
     */
    public MVCheckpoint getLastCheckpoint() {
        return lastCheckpoint.get();
    }

    @Override
    public void beforeRefresh() {
        // no-op
    }

    @Override
    public void afterRefresh(boolean didRefresh) {
        if (!didRefresh) {
            return;
        }
        for (Map.Entry<String, String> def : mvDefinitions.entrySet()) {
            String mvId = def.getKey();
            String defHash = def.getValue();
            try {
                uploadMVPartials(mvId, defHash);
            } catch (Exception e) {
                // Never fail refresh — retry next cycle
                logger.warn("MV upload failed for mvId={} shard={}, will retry: {}", mvId, shardId, e.getMessage());
            }
        }
    }

    private void uploadMVPartials(String mvId, String defHash) throws IOException {
        Path mvDir = shardDataPath.resolve("mv_state").resolve(mvId);
        if (!Files.isDirectory(mvDir)) {
            return;
        }

        // Collect sealed partial files
        List<Path> partialFiles = new ArrayList<>();
        try (DirectoryStream<Path> stream = Files.newDirectoryStream(mvDir, "_mv_partial.*.parquet")) {
            for (Path p : stream) {
                partialFiles.add(p);
            }
        }

        if (partialFiles.isEmpty()) {
            return;
        }

        // Determine generation: use the gen counter (which is ahead by 1 after last seal)
        long gen = genCounters.getOrDefault(mvId, 1L);

        // Upload: data files first, manifest LAST
        long maxSeqNo = -1; // TODO: pass from engine context in future
        MVStateManifest manifest = remoteManager.uploadGeneration(
            String.valueOf(shardId.id()),
            mvId,
            primaryTerm,
            gen,
            maxSeqNo,
            defMetadataVersion,
            defHash,
            partialFiles
        );

        // Update gen counter for next cycle
        genCounters.put(mvId, gen + 1);

        // ── Defect-#30 invariant: update checkpoint ONLY after manifest committed ──
        MVCheckpoint current = lastCheckpoint.get();
        Map<String, MVCheckpoint.MVPartialEntry> newEntries = new HashMap<>(current.entries());
        List<MVCheckpoint.FileInfo> fileInfos = manifest.files().stream()
            .map(f -> new MVCheckpoint.FileInfo(f.name(), f.length(), f.checksum()))
            .toList();
        long sizeBytes = manifest.files().stream().mapToLong(MVStateManifest.FileEntry::length).sum();
        long rowCount = 0; // TODO: pass from seal result
        newEntries.put(mvId, new MVCheckpoint.MVPartialEntry(
            manifest.generation(), manifest.maxSeqNo(), fileInfos, rowCount, sizeBytes
        ));
        lastCheckpoint.set(new MVCheckpoint(
            shardId, primaryTerm, Math.max(current.maxSeqNo(), manifest.maxSeqNo()),
            defMetadataVersion, newEntries
        ));

        // Clean up local partial files after successful upload
        for (Path p : partialFiles) {
            try {
                Files.deleteIfExists(p);
            } catch (IOException e) {
                logger.debug("Failed to delete uploaded partial: {}", p);
            }
        }

        logger.info(
            "MV upload complete: mvId={} shard={} gen={} files={} manifest={}",
            mvId, shardId, gen, partialFiles.size(), manifest
        );
    }
}
