/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import org.apache.arrow.flatbuf.Footer;
import org.apache.arrow.flatbuf.Schema;

import java.io.IOException;
import java.io.RandomAccessFile;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * Lightweight reader for Arrow IPC file schemas.
 *
 * <p>Reads only the file footer (last ~1 KB) to extract field names without
 * loading any record batch data. This avoids pulling in the full
 * {@code arrow-ipc} module — only {@code arrow-format} (FlatBuffers schema
 * classes) and {@code flatbuffers-java} are needed, both of which are already
 * on the mv-data-format classpath.</p>
 *
 * <p><b>Why this exists:</b> DataFusion's Partial aggregate stage names its
 * output columns using the physical expression Display form (e.g.
 * {@code mv_input.EventTime / Int64(300000)}), not the SQL alias (e.g.
 * {@code event_bucket}). The merge path's ordering identity must use the
 * same physical names that appear in the actual state file schema. Reading
 * the schema from the file is the GROUND TRUTH — it is authoritative
 * regardless of DataFusion version, expression form, or naming convention
 * changes.</p>
 */
public final class MVArrowIpcSchemaReader {

    // Arrow IPC file magic: "ARROW1" followed by 2 padding bytes
    private static final byte[] ARROW_MAGIC = { 'A', 'R', 'R', 'O', 'W', '1' };
    private static final int MAGIC_SIZE = 6;
    // Footer: ... [footer flatbuffer] [footer_length: i32 LE] [ARROW1]
    private static final int FOOTER_SUFFIX_SIZE = 4 + MAGIC_SIZE; // i32 + magic

    private MVArrowIpcSchemaReader() {}

    /**
     * Read field names from an Arrow IPC file's footer schema.
     *
     * @param path absolute path to the Arrow IPC (.arrow) file
     * @return ordered list of field names from the file schema
     * @throws IOException if the file cannot be read or is not a valid Arrow IPC file
     */
    public static List<String> readFieldNames(String path) throws IOException {
        try (RandomAccessFile raf = new RandomAccessFile(path, "r"); FileChannel channel = raf.getChannel()) {
            long fileSize = channel.size();
            if (fileSize < MAGIC_SIZE + FOOTER_SUFFIX_SIZE) {
                throw new IOException("File too small to be a valid Arrow IPC file: " + path);
            }

            // Read the footer suffix: [footer_length: i32 LE] [ARROW1]
            ByteBuffer suffix = ByteBuffer.allocate(FOOTER_SUFFIX_SIZE);
            suffix.order(ByteOrder.LITTLE_ENDIAN);
            channel.read(suffix, fileSize - FOOTER_SUFFIX_SIZE);
            suffix.flip();

            int footerLength = suffix.getInt();
            // Validate trailing magic
            for (int i = 0; i < MAGIC_SIZE; i++) {
                if (suffix.get() != ARROW_MAGIC[i]) {
                    throw new IOException("Invalid Arrow IPC file (bad trailing magic): " + path);
                }
            }

            if (footerLength <= 0 || footerLength > fileSize - MAGIC_SIZE - FOOTER_SUFFIX_SIZE) {
                throw new IOException("Invalid Arrow IPC footer length (" + footerLength + "): " + path);
            }

            // Read the footer FlatBuffer
            long footerOffset = fileSize - FOOTER_SUFFIX_SIZE - footerLength;
            ByteBuffer footerBuf = ByteBuffer.allocate(footerLength);
            channel.read(footerBuf, footerOffset);
            footerBuf.flip();

            Footer footer = Footer.getRootAsFooter(footerBuf);
            Schema schema = footer.schema();
            if (schema == null) {
                throw new IOException("Arrow IPC footer has no schema: " + path);
            }

            int fieldCount = schema.fieldsLength();
            List<String> names = new ArrayList<>(fieldCount);
            for (int i = 0; i < fieldCount; i++) {
                names.add(schema.fields(i).name());
            }
            return Collections.unmodifiableList(names);
        }
    }

    /**
     * Read group key column names (the first {@code numGroupKeys} fields) from
     * an Arrow IPC state file. These are the PHYSICAL names that the Rust merge
     * engine uses when computing the ordering identity.
     *
     * @param path         path to an Arrow IPC state file
     * @param numGroupKeys number of group-key columns (leading positions)
     * @return the first {@code numGroupKeys} field names from the file schema
     * @throws IOException if the file cannot be read or has fewer fields than expected
     */
    public static List<String> readGroupKeyNames(String path, int numGroupKeys) throws IOException {
        List<String> allNames = readFieldNames(path);
        if (allNames.size() < numGroupKeys) {
            throw new IOException(
                "Arrow IPC file has " + allNames.size() + " fields but expected at least " + numGroupKeys + " group keys: " + path
            );
        }
        return allNames.subList(0, numGroupKeys);
    }
}
