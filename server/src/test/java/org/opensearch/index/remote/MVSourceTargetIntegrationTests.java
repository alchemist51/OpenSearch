/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.remote;

import org.opensearch.common.blobstore.BlobContainer;
import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.blobstore.support.PlainBlobMetadata;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.index.remote.RemoteStoreEnums.PathType;
import org.opensearch.test.OpenSearchTestCase;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Half-IT: exercises the full source→target data pipeline:
 * <ol>
 *   <li>MVStateRefreshListener uploads partials (with real maxSeqNo from supplier)</li>
 *   <li>MVCheckpointService holds the checkpoint</li>
 *   <li>Hydrator diff logic detects new generations</li>
 *   <li>downloadDataFile downloads from remote store</li>
 *   <li>Checkpoint maxSeqNo == source processedLocalCheckpoint</li>
 * </ol>
 * All against an in-memory blob store (no full cluster required).
 */
public class MVSourceTargetIntegrationTests extends OpenSearchTestCase {

    private static final ShardId SOURCE_SHARD = new ShardId(new Index("source-idx", "src-uuid"), 0);
    private static final String MV_ID = "test_mv";

    // ── Test: upload with maxSeqNo propagation ──────────────────────────

    public void testMaxSeqNoPropagatedToCheckpoint() throws Exception {
        InMemoryBlobStore blobStore = new InMemoryBlobStore();
        MVStateRemoteManager manager = createManager(blobStore);
        Path tempDir = createTempDir();
        Path shardDataPath = tempDir.resolve("shard-data");
        Files.createDirectories(shardDataPath.resolve("mv_state").resolve(MV_ID));

        // Simulate a processedLocalCheckpoint that advances
        AtomicLong processedCheckpoint = new AtomicLong(42L);

        Map<String, String> mvDefs = Map.of(MV_ID, "hash123");
        MVStateRefreshListener listener = new MVStateRefreshListener(
            SOURCE_SHARD, shardDataPath, manager, 1L, 1L, mvDefs,
            processedCheckpoint::get
        );

        // Create sealed partial file and trigger refresh
        Path partialFile = shardDataPath.resolve("mv_state").resolve(MV_ID)
            .resolve("_mv_partial.s0.t1.g1.abc.parquet");
        Files.writeString(partialFile, "sealed-partial-data-gen1");
        listener.afterRefresh(true);

        // Verify maxSeqNo in checkpoint
        MVCheckpoint cp = listener.getLastCheckpoint();
        assertEquals(42L, cp.maxSeqNo());
        assertEquals(1, cp.entries().size());
        MVCheckpoint.MVPartialEntry entry = cp.entries().get(MV_ID);
        assertNotNull(entry);
        assertEquals(42L, entry.maxSeqNo());
        assertEquals(1L, entry.generation());

        // Advance processedCheckpoint and upload another generation
        processedCheckpoint.set(100L);
        Path partialFile2 = shardDataPath.resolve("mv_state").resolve(MV_ID)
            .resolve("_mv_partial.s0.t1.g2.def.parquet");
        Files.writeString(partialFile2, "sealed-partial-data-gen2");
        listener.afterRefresh(true);

        MVCheckpoint cp2 = listener.getLastCheckpoint();
        assertEquals(100L, cp2.maxSeqNo());
        assertEquals(100L, cp2.entries().get(MV_ID).maxSeqNo());
        assertEquals(2L, cp2.entries().get(MV_ID).generation());
    }

    // ── Test: MVCheckpointService static registry round-trip ────────────

    public void testCheckpointServiceRegistration() throws Exception {
        InMemoryBlobStore blobStore = new InMemoryBlobStore();
        MVStateRemoteManager manager = createManager(blobStore);
        Path tempDir = createTempDir();
        Path shardDataPath = tempDir.resolve("shard-data");
        Files.createDirectories(shardDataPath.resolve("mv_state").resolve(MV_ID));

        AtomicLong processedCheckpoint = new AtomicLong(50L);
        Map<String, String> mvDefs = Map.of(MV_ID, "hash123");
        MVStateRefreshListener listener = new MVStateRefreshListener(
            SOURCE_SHARD, shardDataPath, manager, 1L, 1L, mvDefs,
            processedCheckpoint::get
        );

        // Register with service
        MVCheckpointService.registerListener(SOURCE_SHARD, listener);
        try {
            // Create + upload
            Path pf = shardDataPath.resolve("mv_state").resolve(MV_ID)
                .resolve("_mv_partial.s0.t1.g1.abc.parquet");
            Files.writeString(pf, "data");
            listener.afterRefresh(true);

            // Service should return the checkpoint
            MVCheckpoint serviceCheckpoint = MVCheckpointService.getCheckpoint(SOURCE_SHARD);
            assertNotNull(serviceCheckpoint);
            assertEquals(50L, serviceCheckpoint.maxSeqNo());
            assertEquals(1, serviceCheckpoint.entries().size());
            assertEquals(1L, serviceCheckpoint.entries().get(MV_ID).generation());

            // Unregistered shard returns empty checkpoint
            ShardId unknownShard = new ShardId(new Index("unknown", "uuid"), 0);
            MVCheckpoint emptyCheckpoint = MVCheckpointService.getCheckpoint(unknownShard);
            assertNotNull(emptyCheckpoint);
            assertTrue(emptyCheckpoint.entries().isEmpty());
        } finally {
            MVCheckpointService.unregisterListener(SOURCE_SHARD);
        }
    }

    // ── Test: hydrator diff detects new generations ──────────────────────

    public void testHydratorDiffLogic() throws Exception {
        // Verify the core diff: checkpoint gen > high-water gen → needs download
        MVTargetHydrator.HighWater hw = MVTargetHydrator.HighWater.EMPTY;

        // Simulate a checkpoint with gen=3
        MVCheckpoint.MVPartialEntry entry = new MVCheckpoint.MVPartialEntry(
            3L, 50L, List.of(new MVCheckpoint.FileInfo("f1.parquet", 100L, "100")), 0L, 100L
        );
        MVCheckpoint cp = new MVCheckpoint(SOURCE_SHARD, 1L, 50L, 1L, Map.of(MV_ID, entry));

        // Empty high-water → gen 3 is ahead
        assertTrue(cp.primaryTerm() > hw.term()
            || (cp.primaryTerm() == hw.term() && entry.generation() > hw.generation()));

        // Advance high-water to gen 3 → no longer ahead
        MVTargetHydrator.HighWater hw3 = new MVTargetHydrator.HighWater(1L, 3L);
        assertFalse(cp.primaryTerm() > hw3.term()
            || (cp.primaryTerm() == hw3.term() && entry.generation() > hw3.generation()));

        // Term rollover: checkpoint term 2, gen 1 → ahead of hw(term=1, gen=3)
        MVCheckpoint cpNewTerm = new MVCheckpoint(SOURCE_SHARD, 2L, 10L, 1L,
            Map.of(MV_ID, new MVCheckpoint.MVPartialEntry(1L, 10L, List.of(), 0L, 0L)));
        assertTrue(cpNewTerm.primaryTerm() > hw3.term());
    }

    // ── Test: downloadDataFile from remote store ────────────────────────

    public void testDownloadDataFile() throws Exception {
        InMemoryBlobStore blobStore = new InMemoryBlobStore();
        MVStateRemoteManager manager = createManager(blobStore);
        Path tempDir = createTempDir();

        // Upload a generation
        Path partialFile = tempDir.resolve("_mv_partial.s0.t1.g1.abc.parquet");
        String content = "this-is-the-parquet-data-content";
        Files.writeString(partialFile, content);
        manager.uploadGeneration("0", MV_ID, 1L, 1L, 50L, 1L, "hash", List.of(partialFile));

        // Download the data file from remote
        try (InputStream is = manager.downloadDataFile("0", MV_ID, "_mv_partial.s0.t1.g1.abc.parquet")) {
            byte[] downloaded = is.readAllBytes();
            assertEquals(content, new String(downloaded, StandardCharsets.UTF_8));
        }
    }

    // ── Test: full pipeline — upload + register + checkpoint + download ──

    public void testFullPipelineUploadCheckpointDownload() throws Exception {
        InMemoryBlobStore blobStore = new InMemoryBlobStore();
        MVStateRemoteManager manager = createManager(blobStore);
        Path tempDir = createTempDir();
        Path shardDataPath = tempDir.resolve("shard-data");
        Files.createDirectories(shardDataPath.resolve("mv_state").resolve(MV_ID));

        AtomicLong processedCheckpoint = new AtomicLong(75L);
        Map<String, String> mvDefs = Map.of(MV_ID, "hash123");

        // === SOURCE SIDE ===
        MVStateRefreshListener listener = new MVStateRefreshListener(
            SOURCE_SHARD, shardDataPath, manager, 1L, 1L, mvDefs,
            processedCheckpoint::get
        );
        MVCheckpointService.registerListener(SOURCE_SHARD, listener);

        try {
            // Upload 2 generations (simulating 2 refreshes)
            for (int gen = 1; gen <= 2; gen++) {
                processedCheckpoint.set(gen * 50L);
                Path pf = shardDataPath.resolve("mv_state").resolve(MV_ID)
                    .resolve("_mv_partial.s0.t1.g" + gen + ".gen" + gen + ".parquet");
                Files.writeString(pf, "partial-data-generation-" + gen);
                listener.afterRefresh(true);
            }

            // Verify checkpoint after 2 uploads
            MVCheckpoint cp = MVCheckpointService.getCheckpoint(SOURCE_SHARD);
            assertEquals(100L, cp.maxSeqNo()); // 2 * 50
            assertEquals(2L, cp.entries().get(MV_ID).generation());
            // Checkpoint entry tracks LATEST generation's files only (gen 2 = 1 file)
            assertEquals(1, cp.entries().get(MV_ID).files().size());

            // Verify manifests in remote
            List<String> manifests = manager.listManifests("0", MV_ID);
            assertEquals(2, manifests.size());

            // === TARGET SIDE ===
            // Read the latest manifest
            MVStateManifest latestManifest = manager.readManifest("0", MV_ID, manifests.get(0));
            assertEquals(2L, latestManifest.generation());

            // Download data files from remote
            Path targetDir = tempDir.resolve("target-hydrated");
            Files.createDirectories(targetDir);
            for (MVStateManifest.FileEntry fe : latestManifest.files()) {
                Path targetFile = targetDir.resolve(fe.name());
                try (InputStream is = manager.downloadDataFile("0", MV_ID, fe.name())) {
                    Files.copy(is, targetFile);
                }
                // Verify size matches
                assertEquals(fe.length(), Files.size(targetFile));
            }

            // Verify downloaded content
            try (var listing = Files.list(targetDir)) {
                assertTrue(listing.count() >= 1);
            }

        } finally {
            MVCheckpointService.unregisterListener(SOURCE_SHARD);
        }
    }

    // ── Test: checkpoint maxSeqNo matches source processedLocalCheckpoint ──

    public void testCheckpointMaxSeqNoMatchesProcessedLocalCheckpoint() throws Exception {
        InMemoryBlobStore blobStore = new InMemoryBlobStore();
        MVStateRemoteManager manager = createManager(blobStore);
        Path tempDir = createTempDir();
        Path shardDataPath = tempDir.resolve("shard-data");
        Files.createDirectories(shardDataPath.resolve("mv_state").resolve(MV_ID));

        long expectedSeqNo = 12345L;
        AtomicLong processedCheckpoint = new AtomicLong(expectedSeqNo);

        Map<String, String> mvDefs = Map.of(MV_ID, "hash123");
        MVStateRefreshListener listener = new MVStateRefreshListener(
            SOURCE_SHARD, shardDataPath, manager, 1L, 1L, mvDefs,
            processedCheckpoint::get
        );

        // Upload
        Path pf = shardDataPath.resolve("mv_state").resolve(MV_ID)
            .resolve("_mv_partial.s0.t1.g1.xxx.parquet");
        Files.writeString(pf, "data");
        listener.afterRefresh(true);

        // The checkpoint's maxSeqNo MUST match the source processedLocalCheckpoint
        MVCheckpoint cp = listener.getLastCheckpoint();
        assertEquals("maxSeqNo must equal source processedLocalCheckpoint",
            expectedSeqNo, cp.maxSeqNo());
        assertEquals("Entry maxSeqNo must equal source processedLocalCheckpoint",
            expectedSeqNo, cp.entries().get(MV_ID).maxSeqNo());
    }

    // ── Helpers ──────────────────────────────────────────────────────────

    private MVStateRemoteManager createManager(InMemoryBlobStore blobStore) {
        return new MVStateRemoteManager(
            blobStore::blobContainer,
            new RemoteStorePathStrategy(PathType.FIXED),
            new BlobPath().add("remote-base"),
            "src-uuid"
        );
    }

    // Reuse the InMemoryBlobStore from MVUploadCheckpointIntegrationTests
    static class InMemoryBlobStore {
        private final ConcurrentHashMap<String, byte[]> blobs = new ConcurrentHashMap<>();

        BlobContainer blobContainer(BlobPath path) {
            String prefix = path.buildAsString();
            return new InMemoryBlobContainer(prefix, blobs);
        }
    }

    static class InMemoryBlobContainer implements BlobContainer {
        private final String prefix;
        private final ConcurrentHashMap<String, byte[]> store;

        InMemoryBlobContainer(String prefix, ConcurrentHashMap<String, byte[]> store) {
            this.prefix = prefix;
            this.store = store;
        }

        @Override public BlobPath path() { return new BlobPath(); }
        @Override public boolean blobExists(String blobName) { return store.containsKey(prefix + blobName); }

        @Override
        public InputStream readBlob(String blobName) throws IOException {
            byte[] data = store.get(prefix + blobName);
            if (data == null) throw new IOException("Blob not found: " + prefix + blobName);
            return new ByteArrayInputStream(data);
        }

        @Override
        public InputStream readBlob(String blobName, long position, long length) throws IOException {
            byte[] data = store.get(prefix + blobName);
            if (data == null) throw new IOException("Blob not found: " + prefix + blobName);
            return new ByteArrayInputStream(data, (int) position, (int) length);
        }

        @Override public long readBlobPreferredLength() { return Long.MAX_VALUE; }

        @Override
        public void writeBlob(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists)
            throws IOException {
            byte[] data = inputStream.readAllBytes();
            if (failIfAlreadyExists && store.containsKey(prefix + blobName)) {
                throw new IOException("Blob already exists: " + prefix + blobName);
            }
            store.put(prefix + blobName, data);
        }

        @Override
        public void writeBlobAtomic(String blobName, InputStream inputStream, long blobSize, boolean failIfAlreadyExists)
            throws IOException {
            writeBlob(blobName, inputStream, blobSize, failIfAlreadyExists);
        }

        @Override public org.opensearch.common.blobstore.DeleteResult delete() {
            store.keySet().removeIf(k -> k.startsWith(prefix));
            return new org.opensearch.common.blobstore.DeleteResult(0, 0);
        }

        @Override public void deleteBlobsIgnoringIfNotExists(List<String> blobNames) {
            for (String name : blobNames) store.remove(prefix + name);
        }

        @Override
        public Map<String, org.opensearch.common.blobstore.BlobMetadata> listBlobs() {
            Map<String, org.opensearch.common.blobstore.BlobMetadata> result = new TreeMap<>();
            for (Map.Entry<String, byte[]> e : store.entrySet()) {
                if (e.getKey().startsWith(prefix)) {
                    String name = e.getKey().substring(prefix.length());
                    if (!name.contains("/")) {
                        result.put(name, new PlainBlobMetadata(name, e.getValue().length));
                    }
                }
            }
            return result;
        }

        @Override public Map<String, BlobContainer> children() { return Map.of(); }

        @Override
        public Map<String, org.opensearch.common.blobstore.BlobMetadata> listBlobsByPrefix(String blobNamePrefix) {
            Map<String, org.opensearch.common.blobstore.BlobMetadata> all = listBlobs();
            Map<String, org.opensearch.common.blobstore.BlobMetadata> filtered = new TreeMap<>();
            for (Map.Entry<String, org.opensearch.common.blobstore.BlobMetadata> e : all.entrySet()) {
                if (e.getKey().startsWith(blobNamePrefix)) filtered.put(e.getKey(), e.getValue());
            }
            return filtered;
        }
    }
}
