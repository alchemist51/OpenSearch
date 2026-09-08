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
import org.opensearch.index.remote.RemoteStoreEnums.DataCategory;
import org.opensearch.index.remote.RemoteStoreEnums.DataType;
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

/**
 * Half-IT: exercises MVStateRemoteManager upload → MVStateRefreshListener
 * checkpoint update → crash-window invariant (data without manifest = invisible),
 * all against an in-memory blob store (no full cluster required).
 */
public class MVUploadCheckpointIntegrationTests extends OpenSearchTestCase {

    private static final ShardId SHARD = new ShardId(new Index("source-idx", "idx-uuid"), 0);
    private static final String MV_ID = "test_mv";

    // ── Test: full upload → checkpoint visible ──────────────────────────

    public void testUploadAndCheckpointVisible() throws Exception {
        InMemoryBlobStore blobStore = new InMemoryBlobStore();
        MVStateRemoteManager manager = createManager(blobStore);
        Path tempDir = createTempDir();

        // Create fake sealed partial files
        Path partialFile = tempDir.resolve("_mv_partial.s0.t1.g1.abc.parquet");
        Files.writeString(partialFile, "fake-parquet-data-for-testing");

        // Upload generation (data files first, manifest LAST)
        MVStateManifest manifest = manager.uploadGeneration(
            "0", MV_ID, 1L, 1L, 50L, 1L, "hash123", List.of(partialFile)
        );

        assertNotNull(manifest);
        assertEquals(1L, manifest.generation());
        assertEquals(1L, manifest.primaryTerm());
        assertEquals(50L, manifest.maxSeqNo());
        assertEquals(1, manifest.files().size());
        assertEquals("_mv_partial.s0.t1.g1.abc.parquet", manifest.files().get(0).name());

        // Verify manifest exists in remote store
        List<String> manifests = manager.listManifests("0", MV_ID);
        assertEquals(1, manifests.size());

        // Verify generation resume
        long resumeGen = manager.resumeGeneration("0", MV_ID);
        assertEquals(2L, resumeGen);
    }

    // ── Test: crash-window — data without manifest = invisible ──────────

    public void testCrashWindowDataWithoutManifestInvisible() throws Exception {
        InMemoryBlobStore blobStore = new InMemoryBlobStore();
        MVStateRemoteManager manager = createManager(blobStore);
        Path tempDir = createTempDir();

        // Upload gen 1 fully (data + manifest)
        Path partialFile1 = tempDir.resolve("_mv_partial.s0.t1.g1.abc.parquet");
        Files.writeString(partialFile1, "gen1-data");
        manager.uploadGeneration("0", MV_ID, 1L, 1L, 50L, 1L, "hash1", List.of(partialFile1));

        // Now simulate a crash-window: upload data files for gen 2 WITHOUT manifest
        // (directly write to data path, skip manifest)
        BlobPath dataPath = resolveDataPath(blobStore);
        BlobContainer dataContainer = blobStore.blobContainer(dataPath);
        byte[] fakeData = "gen2-data".getBytes(StandardCharsets.UTF_8);
        dataContainer.writeBlob("_mv_partial.s0.t1.g2.xyz.parquet",
            new ByteArrayInputStream(fakeData), fakeData.length, false);

        // Checkpoint should still report gen 1 (no gen 2 manifest)
        long resumeGen = manager.resumeGeneration("0", MV_ID);
        assertEquals("Resume should be gen 2 (1 + 1) since only gen 1 has a manifest", 2L, resumeGen);

        // Verify only gen 1 manifest exists
        List<String> manifests = manager.listManifests("0", MV_ID);
        assertEquals(1, manifests.size());
        long latestGen = MVStateManifest.parseGeneration(manifests.get(0));
        assertEquals(1L, latestGen);
    }

    // ── Test: listener checkpoint update follows manifest commit ─────────

    public void testListenerCheckpointUpdatedOnlyAfterManifest() throws Exception {
        InMemoryBlobStore blobStore = new InMemoryBlobStore();
        MVStateRemoteManager manager = createManager(blobStore);
        Path tempDir = createTempDir();
        Path shardDataPath = tempDir.resolve("shard-data");
        Files.createDirectories(shardDataPath.resolve("mv_state").resolve(MV_ID));

        Map<String, String> mvDefs = Map.of(MV_ID, "hash123");
        MVStateRefreshListener listener = new MVStateRefreshListener(
            SHARD, shardDataPath, manager, 1L, 1L, mvDefs
        );

        // Initially: empty checkpoint
        MVCheckpoint initial = listener.getLastCheckpoint();
        assertEquals(0, initial.entries().size());

        // Create a sealed partial file (simulating Rust seal output)
        Path partialFile = shardDataPath.resolve("mv_state").resolve(MV_ID)
            .resolve("_mv_partial.s0.t1.g1.abc.parquet");
        Files.writeString(partialFile, "sealed-partial-data");

        // Trigger afterRefresh (simulating a refresh event)
        listener.afterRefresh(true);

        // Checkpoint should now have gen 1
        MVCheckpoint afterUpload = listener.getLastCheckpoint();
        assertEquals(1, afterUpload.entries().size());
        MVCheckpoint.MVPartialEntry entry = afterUpload.entries().get(MV_ID);
        assertNotNull(entry);
        assertEquals(1L, entry.generation());
        assertEquals(1, entry.files().size());

        // Local file should be cleaned up
        assertFalse("Local partial should be deleted after upload", Files.exists(partialFile));
    }

    // ── Test: no-op refresh doesn't trigger upload ──────────────────────

    public void testNoOpRefreshSkipped() throws Exception {
        InMemoryBlobStore blobStore = new InMemoryBlobStore();
        MVStateRemoteManager manager = createManager(blobStore);
        Path tempDir = createTempDir();
        Path shardDataPath = tempDir.resolve("shard-data");
        Files.createDirectories(shardDataPath.resolve("mv_state").resolve(MV_ID));

        Map<String, String> mvDefs = Map.of(MV_ID, "hash123");
        MVStateRefreshListener listener = new MVStateRefreshListener(
            SHARD, shardDataPath, manager, 1L, 1L, mvDefs
        );

        // afterRefresh(false) should be a no-op
        listener.afterRefresh(false);
        List<String> manifests = manager.listManifests("0", MV_ID);
        assertTrue(manifests.isEmpty());
    }

    // ── Test: empty directory = no upload ────────────────────────────────

    public void testEmptyDirectoryNoUpload() throws Exception {
        InMemoryBlobStore blobStore = new InMemoryBlobStore();
        MVStateRemoteManager manager = createManager(blobStore);
        Path tempDir = createTempDir();
        Path shardDataPath = tempDir.resolve("shard-data");
        Files.createDirectories(shardDataPath.resolve("mv_state").resolve(MV_ID));

        Map<String, String> mvDefs = Map.of(MV_ID, "hash123");
        MVStateRefreshListener listener = new MVStateRefreshListener(
            SHARD, shardDataPath, manager, 1L, 1L, mvDefs
        );

        // afterRefresh with no partial files should not upload
        listener.afterRefresh(true);
        List<String> manifests = manager.listManifests("0", MV_ID);
        assertTrue("No files = no manifests", manifests.isEmpty());
    }

    // ── Test: multiple refresh cycles → incrementing generations ─────────

    public void testMultipleRefreshCyclesIncrementGeneration() throws Exception {
        InMemoryBlobStore blobStore = new InMemoryBlobStore();
        MVStateRemoteManager manager = createManager(blobStore);
        Path tempDir = createTempDir();
        Path shardDataPath = tempDir.resolve("shard-data");
        Files.createDirectories(shardDataPath.resolve("mv_state").resolve(MV_ID));

        Map<String, String> mvDefs = Map.of(MV_ID, "hash123");
        MVStateRefreshListener listener = new MVStateRefreshListener(
            SHARD, shardDataPath, manager, 1L, 1L, mvDefs
        );

        // Refresh cycle 1
        Path p1 = shardDataPath.resolve("mv_state").resolve(MV_ID)
            .resolve("_mv_partial.s0.t1.g1.aaa.parquet");
        Files.writeString(p1, "data1");
        listener.afterRefresh(true);

        MVCheckpoint cp1 = listener.getLastCheckpoint();
        assertEquals(1L, cp1.entries().get(MV_ID).generation());

        // Refresh cycle 2
        Path p2 = shardDataPath.resolve("mv_state").resolve(MV_ID)
            .resolve("_mv_partial.s0.t1.g2.bbb.parquet");
        Files.writeString(p2, "data2");
        listener.afterRefresh(true);

        MVCheckpoint cp2 = listener.getLastCheckpoint();
        assertEquals(2L, cp2.entries().get(MV_ID).generation());

        // Resume should be gen 3
        assertEquals(3L, manager.resumeGeneration("0", MV_ID));
    }

    // ── Test: initFromRemote rebuilds checkpoint ────────────────────────

    public void testInitFromRemoteRebuildsCheckpoint() throws Exception {
        InMemoryBlobStore blobStore = new InMemoryBlobStore();
        MVStateRemoteManager manager = createManager(blobStore);
        Path tempDir = createTempDir();

        // Pre-populate remote with 2 generations
        Path f1 = tempDir.resolve("g1.parquet");
        Files.writeString(f1, "gen1");
        manager.uploadGeneration("0", MV_ID, 1L, 1L, 50L, 1L, "h1", List.of(f1));
        Path f2 = tempDir.resolve("g2.parquet");
        Files.writeString(f2, "gen2");
        manager.uploadGeneration("0", MV_ID, 1L, 2L, 100L, 1L, "h2", List.of(f2));

        // Create a fresh listener and init from remote
        Path shardDataPath = tempDir.resolve("shard-data");
        Files.createDirectories(shardDataPath);
        Map<String, String> mvDefs = Map.of(MV_ID, "hash123");
        MVStateRefreshListener listener = new MVStateRefreshListener(
            SHARD, shardDataPath, manager, 1L, 1L, mvDefs
        );
        listener.initFromRemote();

        // Checkpoint should reflect gen 2
        MVCheckpoint cp = listener.getLastCheckpoint();
        assertEquals(1, cp.entries().size());
        assertEquals(2L, cp.entries().get(MV_ID).generation());

        // Start gen should be 3
        assertEquals(3L, listener.getStartGeneration(MV_ID));
    }

    // ── Test: isAheadOf with rebuilt checkpoints ────────────────────────

    public void testIsAheadOfWithRebuiltCheckpoints() throws Exception {
        InMemoryBlobStore blobStore = new InMemoryBlobStore();
        MVStateRemoteManager manager = createManager(blobStore);
        Path tempDir = createTempDir();

        // Upload gen 1 and gen 5 for the same MV
        Path f = tempDir.resolve("f.parquet");
        Files.writeString(f, "data");
        manager.uploadGeneration("0", MV_ID, 1L, 1L, 10L, 1L, "h", List.of(f));
        manager.uploadGeneration("0", MV_ID, 1L, 5L, 50L, 1L, "h", List.of(f));

        // Rebuild from remote
        MVCheckpoint rebuilt = MVCheckpoint.rebuildFromManifests(
            SHARD, 1L, 1L, manager, Map.of(MV_ID, "h")
        );

        assertEquals(5L, rebuilt.entries().get(MV_ID).generation());

        // Compare: gen 5 should be ahead of gen 3
        MVCheckpoint older = new MVCheckpoint(SHARD, 1L, 30L, 1L,
            Map.of(MV_ID, new MVCheckpoint.MVPartialEntry(3L, 30L, List.of(), 0, 0)));

        assertTrue(rebuilt.isAheadOf(older, MV_ID));
        assertFalse(older.isAheadOf(rebuilt, MV_ID));
    }

    // ── Helpers: in-memory blob store ────────────────────────────────────

    private MVStateRemoteManager createManager(InMemoryBlobStore blobStore) {
        return new MVStateRemoteManager(
            blobStore::blobContainer,
            new RemoteStorePathStrategy(PathType.FIXED),
            new BlobPath().add("remote-base"),
            "idx-uuid"
        );
    }

    private BlobPath resolveDataPath(InMemoryBlobStore blobStore) {
        // Match the FIXED path: remote-base/<indexUUID>/<shard>/mv_state/<mvId>/data/
        return new BlobPath().add("remote-base").add("idx-uuid").add("0")
            .add("mv_state").add(MV_ID).add("data");
    }

    /**
     * In-memory blob store for testing. Stores blobs in a map keyed by full path.
     */
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

        @Override
        public BlobPath path() {
            return new BlobPath();
        }

        @Override
        public boolean blobExists(String blobName) {
            return store.containsKey(prefix + blobName);
        }

        @Override
        public InputStream readBlob(String blobName) throws IOException {
            byte[] data = store.get(prefix + blobName);
            if (data == null) {
                throw new IOException("Blob not found: " + prefix + blobName);
            }
            return new ByteArrayInputStream(data);
        }

        @Override
        public InputStream readBlob(String blobName, long position, long length) throws IOException {
            byte[] data = store.get(prefix + blobName);
            if (data == null) {
                throw new IOException("Blob not found: " + prefix + blobName);
            }
            return new ByteArrayInputStream(data, (int) position, (int) length);
        }

        @Override
        public long readBlobPreferredLength() {
            return Long.MAX_VALUE;
        }

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

        @Override
        public org.opensearch.common.blobstore.DeleteResult delete() {
            store.keySet().removeIf(k -> k.startsWith(prefix));
            return new org.opensearch.common.blobstore.DeleteResult(0, 0);
        }

        @Override
        public void deleteBlobsIgnoringIfNotExists(List<String> blobNames) {
            for (String name : blobNames) {
                store.remove(prefix + name);
            }
        }

        @Override
        public Map<String, org.opensearch.common.blobstore.BlobMetadata> listBlobs() {
            Map<String, org.opensearch.common.blobstore.BlobMetadata> result = new TreeMap<>();
            for (Map.Entry<String, byte[]> e : store.entrySet()) {
                if (e.getKey().startsWith(prefix)) {
                    String name = e.getKey().substring(prefix.length());
                    if (!name.contains("/")) { // only direct children
                        result.put(name, new PlainBlobMetadata(name, e.getValue().length));
                    }
                }
            }
            return result;
        }

        @Override
        public Map<String, org.opensearch.common.blobstore.BlobContainer> children() {
            return Map.of();
        }

        @Override
        public Map<String, org.opensearch.common.blobstore.BlobMetadata> listBlobsByPrefix(String blobNamePrefix) {
            Map<String, org.opensearch.common.blobstore.BlobMetadata> all = listBlobs();
            Map<String, org.opensearch.common.blobstore.BlobMetadata> filtered = new TreeMap<>();
            for (Map.Entry<String, org.opensearch.common.blobstore.BlobMetadata> e : all.entrySet()) {
                if (e.getKey().startsWith(blobNamePrefix)) {
                    filtered.put(e.getKey(), e.getValue());
                }
            }
            return filtered;
        }
    }
}
