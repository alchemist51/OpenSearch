/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

/**
 * Tests for {@link MVArrowIpcSchemaReader} and the physical ordering identity
 * derivation that fixes the compaction ordering-identity mismatch between
 * Java (which uses SQL aliases) and Rust (which uses DataFusion's Partial-
 * aggregate physical column names from the Arrow IPC schema).
 */
public class MVArrowIpcSchemaReaderTests extends OpenSearchTestCase {

    /**
     * Verify that physicalOrderingIdentity substitutes the physical column
     * names into the identity string, replacing logical aliases.
     *
     * This is the core invariant: when an expression group key like
     * {@code floor(EventTime/300000) AS event_bucket} produces a Partial
     * aggregate output column named {@code mv_input.EventTime / Int64(300000)},
     * the merge params must use that physical name — NOT the alias
     * {@code event_bucket} — so the Rust merge_state_streams identity
     * comparison succeeds.
     */
    public void testPhysicalOrderingIdentitySubstitutesExpressionKeyNames() {
        MVCompiledDefinition def = MVCompiledDefinition.of(
            List.of(
                GroupKey.ofExpression("event_bucket", GroupKey.ColumnType.LONG,
                    "\"EventTime\" / 300000", "EventTime"),
                GroupKey.of("URL", GroupKey.ColumnType.KEYWORD),
                GroupKey.of("UserID", GroupKey.ColumnType.LONG)
            ),
            List.of(AggregateSpec.count("cnt"))
        );

        MVGroupByOrdering ordering = def.groupByOrdering();

        // The LOGICAL identity uses the alias:
        assertEquals(
            "0:event_bucket:0:0;1:URL:0:0;2:UserID:0:0",
            ordering.orderingIdentity()
        );

        // The PHYSICAL identity substitutes the physical name from the file:
        List<String> physicalNames = List.of(
            "mv_input.EventTime / Int64(300000)", // DataFusion's Partial output
            "URL",       // plain column — same as alias
            "UserID"     // plain column — same as alias
        );
        assertEquals(
            "0:mv_input.EventTime / Int64(300000):0:0;1:URL:0:0;2:UserID:0:0",
            ordering.physicalOrderingIdentity(physicalNames)
        );
    }

    /**
     * physicalOrderingIdentity with plain-only group keys produces the same
     * result as the logical orderingIdentity (no substitution needed).
     */
    public void testPhysicalOrderingIdentityMatchesLogicalForPlainKeys() {
        MVCompiledDefinition def = MVCompiledDefinition.of(
            List.of(
                GroupKey.of("region", GroupKey.ColumnType.LONG),
                GroupKey.of("os", GroupKey.ColumnType.KEYWORD)
            ),
            List.of(AggregateSpec.count("cnt"))
        );

        MVGroupByOrdering ordering = def.groupByOrdering();
        String logical = ordering.orderingIdentity();
        String physical = ordering.physicalOrderingIdentity(List.of("region", "os"));
        assertEquals(logical, physical);
    }

    /**
     * physicalOrderingIdentity rejects mismatched list sizes.
     */
    public void testPhysicalOrderingIdentityRejectsSizeMismatch() {
        MVCompiledDefinition def = MVCompiledDefinition.of(
            List.of(GroupKey.of("k0", GroupKey.ColumnType.LONG)),
            List.of(AggregateSpec.count("cnt"))
        );

        MVGroupByOrdering ordering = def.groupByOrdering();
        expectThrows(
            IllegalArgumentException.class,
            () -> ordering.physicalOrderingIdentity(List.of("a", "b"))
        );
    }

    /**
     * Verify that buildMergeCallParams(referenceFile) produces an ordering
     * identity derived from the file's physical schema, not the logical alias.
     *
     * Creates a minimal valid Arrow IPC file with a known schema, then
     * verifies that the merge call params use the file's field names.
     */
    public void testBuildMergeCallParamsFromFileUsesPhysicalNames() throws IOException {
        // Build a minimal Arrow IPC file with expression-key physical names
        Path tempFile = createTempDir().resolve("test_state.arrow");
        writeMinimalArrowIpcFile(tempFile, List.of(
            "mv_input.EventTime / Int64(300000)",  // expression key physical name
            "URL",                                  // plain key
            "UserID",                               // plain key
            "count(*)[count]"                       // aggregate
        ));

        // Definition with expression group key
        MVCompiledDefinition def = MVCompiledDefinition.of(
            List.of(
                GroupKey.ofExpression("event_bucket", GroupKey.ColumnType.LONG,
                    "\"EventTime\" / 300000", "EventTime"),
                GroupKey.of("URL", GroupKey.ColumnType.KEYWORD),
                GroupKey.of("UserID", GroupKey.ColumnType.LONG)
            ),
            List.of(AggregateSpec.count("cnt"))
        );

        // Zero-arg uses logical alias — will mismatch Rust:
        MVCompiledDefinition.MergeCallParams logicalParams = def.buildMergeCallParams();
        assertTrue(
            "logical identity should use alias 'event_bucket'",
            logicalParams.orderingIdentity().contains("event_bucket")
        );

        // File-based uses physical name — matches Rust:
        MVCompiledDefinition.MergeCallParams physicalParams = def.buildMergeCallParams(tempFile.toString());
        assertEquals(
            "0:mv_input.EventTime / Int64(300000):0:0;1:URL:0:0;2:UserID:0:0",
            physicalParams.orderingIdentity()
        );
    }

    /**
     * Verify that MVArrowIpcSchemaReader correctly reads field names from a
     * valid Arrow IPC file.
     */
    public void testReadFieldNamesFromArrowIpcFile() throws IOException {
        List<String> expectedNames = List.of("col_a", "col_b", "col_c");
        Path tempFile = createTempDir().resolve("schema_test.arrow");
        writeMinimalArrowIpcFile(tempFile, expectedNames);

        List<String> actualNames = MVArrowIpcSchemaReader.readFieldNames(tempFile.toString());
        assertEquals(expectedNames, actualNames);
    }

    /**
     * Verify that readGroupKeyNames returns only the first N fields.
     */
    public void testReadGroupKeyNamesReturnsPrefix() throws IOException {
        List<String> allNames = List.of("key0", "key1", "agg0", "agg1");
        Path tempFile = createTempDir().resolve("prefix_test.arrow");
        writeMinimalArrowIpcFile(tempFile, allNames);

        List<String> keyNames = MVArrowIpcSchemaReader.readGroupKeyNames(tempFile.toString(), 2);
        assertEquals(List.of("key0", "key1"), keyNames);
    }

    /**
     * readGroupKeyNames rejects files with too few fields.
     */
    public void testReadGroupKeyNamesRejectsTooFewFields() throws IOException {
        Path tempFile = createTempDir().resolve("small_test.arrow");
        writeMinimalArrowIpcFile(tempFile, List.of("only_one"));

        expectThrows(
            IOException.class,
            () -> MVArrowIpcSchemaReader.readGroupKeyNames(tempFile.toString(), 3)
        );
    }

    // ── Helpers ──────────────────────────────────────────────────────────

    /**
     * Write a minimal valid Arrow IPC file containing only a schema (no record
     * batches). Uses raw FlatBuffers to avoid needing the arrow-ipc module.
     */
    private static void writeMinimalArrowIpcFile(Path path, List<String> fieldNames) throws IOException {
        // Build Footer FlatBuffer (contains the schema)
        com.google.flatbuffers.FlatBufferBuilder footerBuilder = new com.google.flatbuffers.FlatBufferBuilder(512);

        // Build fields for the schema
        int fieldsOffset = createFieldsInBuilder(footerBuilder, fieldNames);

        // Schema: createSchema(builder, endianness, fieldsVector, customMetadataVector, featuresVector)
        int schemaOffset = org.apache.arrow.flatbuf.Schema.createSchema(
            footerBuilder,
            org.apache.arrow.flatbuf.Endianness.Little,
            fieldsOffset,
            0,  // custom_metadata (none)
            0   // features (none)
        );

        // Empty dictionaries and recordBatches vectors
        org.apache.arrow.flatbuf.Footer.startDictionariesVector(footerBuilder, 0);
        int dictsOffset = footerBuilder.endVector();
        org.apache.arrow.flatbuf.Footer.startRecordBatchesVector(footerBuilder, 0);
        int batchesOffset = footerBuilder.endVector();

        // Footer: createFooter(builder, version, schema, dicts, batches, customMetadata)
        int footerOffset = org.apache.arrow.flatbuf.Footer.createFooter(
            footerBuilder,
            org.apache.arrow.flatbuf.MetadataVersion.V5,
            schemaOffset,
            dictsOffset,
            batchesOffset,
            0  // custom_metadata (none)
        );
        footerBuilder.finish(footerOffset);
        byte[] footerBytes = footerBuilder.sizedByteArray();

        // Build a schema Message for the file body
        com.google.flatbuffers.FlatBufferBuilder msgBuilder = new com.google.flatbuffers.FlatBufferBuilder(512);
        int msgFieldsOffset = createFieldsInBuilder(msgBuilder, fieldNames);
        int msgSchemaOffset = org.apache.arrow.flatbuf.Schema.createSchema(
            msgBuilder,
            org.apache.arrow.flatbuf.Endianness.Little,
            msgFieldsOffset,
            0,
            0
        );
        int messageOffset = org.apache.arrow.flatbuf.Message.createMessage(
            msgBuilder,
            org.apache.arrow.flatbuf.MetadataVersion.V5,
            org.apache.arrow.flatbuf.MessageHeader.Schema,
            msgSchemaOffset,
            0,  // bodyLength
            0   // custom_metadata
        );
        msgBuilder.finish(messageOffset);
        byte[] messageBytes = msgBuilder.sizedByteArray();

        // Assemble the file
        int messagePadded = alignTo8(messageBytes.length);
        int fileSize = 8                           // magic + padding
            + 4 + 4 + messagePadded                // continuation + length + message
            + 4 + 4                                // EOS (continuation + zero length)
            + footerBytes.length                   // footer
            + 4 + 6;                               // footer length + trailing magic
        ByteBuffer file = ByteBuffer.allocate(fileSize);
        file.order(ByteOrder.LITTLE_ENDIAN);

        // Leading magic: ARROW1 + 2 padding bytes
        file.put((byte) 'A').put((byte) 'R').put((byte) 'R').put((byte) 'O')
            .put((byte) 'W').put((byte) '1').put((byte) 0).put((byte) 0);

        // Schema message: continuation(-1) + length + message + padding
        file.putInt(-1);
        file.putInt(messagePadded);
        file.put(messageBytes);
        for (int i = messageBytes.length; i < messagePadded; i++) {
            file.put((byte) 0);
        }

        // EOS marker
        file.putInt(-1);
        file.putInt(0);

        // Footer
        file.put(footerBytes);

        // Footer length + trailing magic
        file.putInt(footerBytes.length);
        file.put((byte) 'A').put((byte) 'R').put((byte) 'R').put((byte) 'O')
            .put((byte) 'W').put((byte) '1');

        file.flip();
        byte[] fileArray = new byte[file.remaining()];
        file.get(fileArray);
        Files.write(path, fileArray, java.nio.file.StandardOpenOption.CREATE_NEW);
    }

    private static int createFieldsInBuilder(
        com.google.flatbuffers.FlatBufferBuilder builder,
        List<String> fieldNames
    ) {
        int[] fieldOffsets = new int[fieldNames.size()];
        for (int i = 0; i < fieldNames.size(); i++) {
            int nameOffset = builder.createString(fieldNames.get(i));
            int typeOffset = org.apache.arrow.flatbuf.Int.createInt(builder, 32, true);
            // createField(builder, name, nullable, typeType, type, dictionary, children, customMetadata)
            fieldOffsets[i] = org.apache.arrow.flatbuf.Field.createField(
                builder,
                nameOffset,
                true,
                org.apache.arrow.flatbuf.Type.Int,
                typeOffset,
                0, // dictionary
                0, // children
                0  // custom_metadata
            );
        }
        return org.apache.arrow.flatbuf.Schema.createFieldsVector(builder, fieldOffsets);
    }

    private static int alignTo8(int size) {
        return (size + 7) & ~7;
    }
}
