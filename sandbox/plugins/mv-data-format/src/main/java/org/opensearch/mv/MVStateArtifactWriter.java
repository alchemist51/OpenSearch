/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import org.opensearch.index.engine.exec.MonoFileWriterSet;
import org.opensearch.index.engine.exec.WriterFileSet;

import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Objects;
import java.util.UUID;

/**
 * Builds one immutable {@code mv_state} Arrow IPC artifact from a pinned parquet
 * input and returns the file metadata needed for atomic catalog publication.
 *
 * <p>The native writer targets a private temporary path. The completed file is
 * atomically renamed before the {@link WriterFileSet} is returned, so callers
 * can never publish a partially-written artifact. The caller owns cleanup if a
 * later catalog publication fails.
 *
 * @opensearch.internal
 */
public final class MVStateArtifactWriter {

    /** One completed artifact and its physical path. */
    public record Artifact(Path path, WriterFileSet fileSet, long stateRows) {
    }

    public Artifact build(Path parquetInput, String tableName, String filteredSql, Path outputRoot, long writerGeneration)
        throws IOException {
        Objects.requireNonNull(parquetInput, "parquetInput");
        Objects.requireNonNull(tableName, "tableName");
        Objects.requireNonNull(filteredSql, "filteredSql");
        Objects.requireNonNull(outputRoot, "outputRoot");
        if (writerGeneration <= 0L) {
            throw new IllegalArgumentException("writerGeneration must be positive but was [" + writerGeneration + "]");
        }

        Path formatDirectory = outputRoot.resolve(MVStateDataFormat.NAME);
        Files.createDirectories(formatDirectory);
        String fileName = MVConstants.mvFileName(writerGeneration);
        Path completed = formatDirectory.resolve(fileName);
        Path temporary = formatDirectory.resolve(fileName + ".tmp-" + UUID.randomUUID());

        boolean success = false;
        boolean completedCreated = false;
        try {
            long stateRows = MVNativeBridge.buildStateFile(parquetInput.toString(), tableName, filteredSql, temporary.toString());
            if (stateRows <= 0L) {
                throw new IOException("mv_state build produced no state rows for generation [" + writerGeneration + "]");
            }
            moveCompletedArtifact(temporary, completed);
            completedCreated = true;
            WriterFileSet fileSet = MonoFileWriterSet.of(formatDirectory.toAbsolutePath(), writerGeneration, fileName, stateRows);
            success = true;
            return new Artifact(completed, fileSet, stateRows);
        } finally {
            Files.deleteIfExists(temporary);
            if (success == false && completedCreated) {
                Files.deleteIfExists(completed);
            }
        }
    }

    private static void moveCompletedArtifact(Path temporary, Path completed) throws IOException {
        try {
            Files.move(temporary, completed, StandardCopyOption.ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException unsupported) {
            Files.move(temporary, completed);
        }
    }
}
