/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.remote;

import org.opensearch.common.blobstore.BlobPath;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.index.remote.RemoteStoreEnums.DataCategory;
import org.opensearch.index.remote.RemoteStoreEnums.DataType;
import org.opensearch.index.remote.RemoteStoreEnums.PathType;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.List;

public class MVStateRemoteTests extends OpenSearchTestCase {

    // ── Piece 2: DataCategory.MV_STATE tests ────────────────────────────

    public void testMVStateDataCategorySupportedTypes() {
        DataCategory mvState = DataCategory.MV_STATE;
        assertEquals("mv_state", mvState.getName());
        assertTrue(mvState.isSupportedDataType(DataType.DATA));
        assertTrue(mvState.isSupportedDataType(DataType.METADATA));
        assertFalse(mvState.isSupportedDataType(DataType.LOCK_FILES));
    }

    // ── Piece 2: MVStatePathInput path composition tests ────────────────

    public void testMVStatePathInputFixed() {
        BlobPath basePath = new BlobPath().add("base");
        String indexUUID = "idx-uuid-123";
        String shardId = "0";
        String mvId = "my_mv";

        MVStatePathInput.Builder builder = MVStatePathInput.builder();
        builder.mvId(mvId);
        builder.basePath(basePath);
        builder.indexUUID(indexUUID);
        builder.shardId(shardId);
        builder.dataCategory(DataCategory.MV_STATE);
        builder.dataType(DataType.DATA);
        MVStatePathInput pathInput = builder.build();

        BlobPath result = PathType.FIXED.path(pathInput, null);
        String path = result.buildAsString();
        // Expected: base/<indexUUID>/<shard>/mv_state/<mvId>/data/
        assertEquals("base/idx-uuid-123/0/mv_state/my_mv/data/", path);
    }

    public void testMVStatePathInputFixedMetadata() {
        BlobPath basePath = new BlobPath().add("base");
        MVStatePathInput.Builder builder = MVStatePathInput.builder();
        builder.mvId("my_mv");
        builder.basePath(basePath);
        builder.indexUUID("idx-uuid-123");
        builder.shardId("0");
        builder.dataCategory(DataCategory.MV_STATE);
        builder.dataType(DataType.METADATA);
        MVStatePathInput pathInput = builder.build();

        BlobPath result = PathType.FIXED.path(pathInput, null);
        assertEquals("base/idx-uuid-123/0/mv_state/my_mv/metadata/", result.buildAsString());
    }

    public void testMVStatePathInputHashedPrefix() {
        BlobPath basePath = new BlobPath().add("base");
        MVStatePathInput.Builder builder = MVStatePathInput.builder();
        builder.mvId("my_mv");
        builder.basePath(basePath);
        builder.indexUUID("idx-uuid-123");
        builder.shardId("0");
        builder.dataCategory(DataCategory.MV_STATE);
        builder.dataType(DataType.DATA);
        MVStatePathInput pathInput = builder.build();

        // Hashed prefix prepends a hash — just verify it contains the expected suffix
        BlobPath result = PathType.HASHED_PREFIX.path(
            pathInput,
            RemoteStoreEnums.PathHashAlgorithm.FNV_1A_BASE64
        );
        String path = result.buildAsString();
        assertTrue("Path should contain mv_state segment: " + path, path.contains("mv_state/my_mv/data"));
        assertTrue("Path should contain index UUID: " + path, path.contains("idx-uuid-123"));
    }

    // ── Piece 3: MVStateManifest round-trip tests ───────────────────────

    public void testManifestSerializeDeserialize() throws IOException {
        List<MVStateManifest.FileEntry> files = List.of(
            new MVStateManifest.FileEntry("_mv_partial.s0.t1.g1.abc.parquet", 1024L, "1024"),
            new MVStateManifest.FileEntry("_mv_partial.s0.t1.g1.def.parquet", 2048L, "2048")
        );
        MVStateManifest original = new MVStateManifest(1L, 5L, 100L, 2L, "hash123", files);

        // Serialize
        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        BytesReference bytes = out.bytes();

        // Deserialize
        MVStateManifest restored = new MVStateManifest(bytes.streamInput());

        assertEquals(original, restored);
        assertEquals(1L, restored.primaryTerm());
        assertEquals(5L, restored.generation());
        assertEquals(100L, restored.maxSeqNo());
        assertEquals(2L, restored.defMetadataVersion());
        assertEquals("hash123", restored.definitionHash());
        assertEquals(2, restored.files().size());
        assertEquals("_mv_partial.s0.t1.g1.abc.parquet", restored.files().get(0).name());
    }

    public void testManifestEmptyFiles() throws IOException {
        MVStateManifest original = new MVStateManifest(1L, 1L, 0L, 1L, "hash", List.of());

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        MVStateManifest restored = new MVStateManifest(out.bytes().streamInput());

        assertEquals(original, restored);
        assertTrue(restored.files().isEmpty());
    }

    // ── Piece 4: Generation resume from manifest file name ──────────────

    public void testManifestFileNameParsing() {
        String name = MVStateManifest.buildFileName(3L, 42L, "some-uuid");
        long gen = MVStateManifest.parseGeneration(name);
        long term = MVStateManifest.parsePrimaryTerm(name);

        assertEquals(42L, gen);
        assertEquals(3L, term);
    }

    public void testManifestFileNameOrdering() {
        // Newer (higher gen) should sort BEFORE older (lower gen)
        String older = MVStateManifest.buildFileName(1L, 10L, "aaa");
        String newer = MVStateManifest.buildFileName(1L, 20L, "bbb");

        // Inverted encoding: newer has lower inverted gen → sorts first
        assertTrue("Newer manifest should sort before older: " + newer + " vs " + older,
            newer.compareTo(older) < 0);
    }

    public void testManifestFileNameHigherTermSortsFirst() {
        String lowerTerm = MVStateManifest.buildFileName(1L, 5L, "aaa");
        String higherTerm = MVStateManifest.buildFileName(2L, 5L, "bbb");

        // Higher term has lower inverted value → sorts first
        assertTrue("Higher term should sort first", higherTerm.compareTo(lowerTerm) < 0);
    }
}
