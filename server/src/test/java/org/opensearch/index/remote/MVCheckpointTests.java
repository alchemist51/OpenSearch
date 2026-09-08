/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.remote;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Unit tests for {@link MVCheckpoint}: Writeable round-trip, isAheadOf matrix
 * (generation advance, term rollover, unknown mvId), rebuild-from-manifests.
 */
public class MVCheckpointTests extends OpenSearchTestCase {

    private static final ShardId SHARD = new ShardId(new Index("test-index", "uuid-1"), 0);

    // ── Writeable round-trip ─────────────────────────────────────────────

    public void testWriteableRoundTrip() throws IOException {
        List<MVCheckpoint.FileInfo> files = List.of(
            new MVCheckpoint.FileInfo("_mv_partial.s0.t1.g5.abc.parquet", 1024L, "1024"),
            new MVCheckpoint.FileInfo("_mv_partial.s0.t1.g5.def.parquet", 2048L, "2048")
        );
        MVCheckpoint.MVPartialEntry entry = new MVCheckpoint.MVPartialEntry(5L, 100L, files, 42L, 3072L);
        Map<String, MVCheckpoint.MVPartialEntry> entries = Map.of("mv1", entry);

        MVCheckpoint original = new MVCheckpoint(SHARD, 3L, 100L, 2L, entries);

        // Serialize
        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);

        // Deserialize
        StreamInput in = out.bytes().streamInput();
        MVCheckpoint restored = new MVCheckpoint(in);

        assertEquals(original, restored);
        assertEquals(SHARD, restored.shardId());
        assertEquals(3L, restored.primaryTerm());
        assertEquals(100L, restored.maxSeqNo());
        assertEquals(2L, restored.defMetadataVersion());
        assertEquals(1, restored.entries().size());

        MVCheckpoint.MVPartialEntry restoredEntry = restored.entries().get("mv1");
        assertNotNull(restoredEntry);
        assertEquals(5L, restoredEntry.generation());
        assertEquals(100L, restoredEntry.maxSeqNo());
        assertEquals(2, restoredEntry.files().size());
        assertEquals(42L, restoredEntry.rowCount());
        assertEquals(3072L, restoredEntry.sizeBytes());
        assertEquals("_mv_partial.s0.t1.g5.abc.parquet", restoredEntry.files().get(0).name());
    }

    public void testWriteableRoundTripEmpty() throws IOException {
        MVCheckpoint original = new MVCheckpoint(SHARD, 1L, -1L, 0L, Map.of());

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        MVCheckpoint restored = new MVCheckpoint(out.bytes().streamInput());

        assertEquals(original, restored);
        assertTrue(restored.entries().isEmpty());
    }

    public void testWriteableRoundTripMultipleMVs() throws IOException {
        Map<String, MVCheckpoint.MVPartialEntry> entries = new HashMap<>();
        entries.put("mv1", new MVCheckpoint.MVPartialEntry(
            10L, 500L, List.of(new MVCheckpoint.FileInfo("f1.parquet", 100L, "100")), 1000L, 100L
        ));
        entries.put("mv2", new MVCheckpoint.MVPartialEntry(
            7L, 300L, List.of(), 0L, 0L
        ));

        MVCheckpoint original = new MVCheckpoint(SHARD, 2L, 500L, 5L, entries);

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        MVCheckpoint restored = new MVCheckpoint(out.bytes().streamInput());

        assertEquals(original, restored);
        assertEquals(2, restored.entries().size());
        assertEquals(10L, restored.entries().get("mv1").generation());
        assertEquals(7L, restored.entries().get("mv2").generation());
    }

    // ── isAheadOf matrix ─────────────────────────────────────────────────

    public void testIsAheadOfGenerationAdvance() {
        MVCheckpoint older = checkpoint(1L, "mv1", 5L);
        MVCheckpoint newer = checkpoint(1L, "mv1", 10L);

        assertTrue("Higher generation is ahead", newer.isAheadOf(older, "mv1"));
        assertFalse("Lower generation is not ahead", older.isAheadOf(newer, "mv1"));
    }

    public void testIsAheadOfSameGeneration() {
        MVCheckpoint a = checkpoint(1L, "mv1", 5L);
        MVCheckpoint b = checkpoint(1L, "mv1", 5L);

        assertFalse("Same generation is not ahead", a.isAheadOf(b, "mv1"));
    }

    public void testIsAheadOfTermRollover() {
        // Higher term always wins, regardless of generation
        MVCheckpoint oldTerm = checkpoint(1L, "mv1", 100L);
        MVCheckpoint newTerm = checkpoint(2L, "mv1", 1L);

        assertTrue("Higher term wins even with lower gen", newTerm.isAheadOf(oldTerm, "mv1"));
        assertFalse("Lower term loses even with higher gen", oldTerm.isAheadOf(newTerm, "mv1"));
    }

    public void testIsAheadOfUnknownMvId() {
        MVCheckpoint cp = checkpoint(1L, "mv1", 5L);
        MVCheckpoint other = checkpoint(1L, "mv1", 3L);

        // Unknown mvId in "this" → false
        assertFalse("Unknown mvId in source is not ahead", cp.isAheadOf(other, "mv_unknown"));
    }

    public void testIsAheadOfUnknownMvIdInOther() {
        MVCheckpoint cp = checkpoint(1L, "mv1", 5L);
        MVCheckpoint other = checkpoint(1L, "mv2", 3L);

        // "this" has mv1, other doesn't → ahead
        assertTrue("Having the MV when other doesn't = ahead", cp.isAheadOf(other, "mv1"));
    }

    public void testIsAheadOfNullOther() {
        MVCheckpoint cp = checkpoint(1L, "mv1", 5L);

        assertTrue("Ahead of null checkpoint", cp.isAheadOf(null, "mv1"));
    }

    public void testIsAheadOfUnknownMvIdWithNullOther() {
        MVCheckpoint cp = checkpoint(1L, "mv1", 5L);

        assertFalse("Unknown mvId is not ahead even of null", cp.isAheadOf(null, "mv_unknown"));
    }

    // ── Equality / hashCode ──────────────────────────────────────────────

    public void testEquality() {
        MVCheckpoint a = checkpoint(1L, "mv1", 5L);
        MVCheckpoint b = checkpoint(1L, "mv1", 5L);
        assertEquals(a, b);
        assertEquals(a.hashCode(), b.hashCode());
    }

    public void testInequalityDifferentGen() {
        MVCheckpoint a = checkpoint(1L, "mv1", 5L);
        MVCheckpoint b = checkpoint(1L, "mv1", 6L);
        assertNotEquals(a, b);
    }

    // ── FileInfo round-trip ──────────────────────────────────────────────

    public void testFileInfoWriteableRoundTrip() throws IOException {
        MVCheckpoint.FileInfo original = new MVCheckpoint.FileInfo("test.parquet", 4096L, "abc123");

        BytesStreamOutput out = new BytesStreamOutput();
        original.writeTo(out);
        MVCheckpoint.FileInfo restored = new MVCheckpoint.FileInfo(out.bytes().streamInput());

        assertEquals(original, restored);
        assertEquals("test.parquet", restored.name());
        assertEquals(4096L, restored.length());
        assertEquals("abc123", restored.checksum());
    }

    // ── Helpers ──────────────────────────────────────────────────────────

    private static MVCheckpoint checkpoint(long term, String mvId, long gen) {
        MVCheckpoint.MVPartialEntry entry = new MVCheckpoint.MVPartialEntry(
            gen, gen * 10, List.of(), 0L, 0L
        );
        return new MVCheckpoint(SHARD, term, gen * 10, 1L, Map.of(mvId, entry));
    }
}
