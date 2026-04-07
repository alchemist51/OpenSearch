/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.plugins;

import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.exec.EngineReaderManager;
import org.opensearch.index.engine.exec.IndexWriterProvider;
import org.opensearch.index.shard.ShardPath;

import java.io.IOException;
import java.util.List;

/**
 * Unified SPI for back-end storage and query engines.
 * <p>
 * Each implementation provides reader lifecycle management (via
 * {@link #createReaderManager}) and declares supported data formats.
 * The type parameter {@code R} carries the reader type, eliminating
 * unsafe casts at the boundary.
 * <p>
 * Plugins that also support the analytics query path should additionally
 * implement {@code SearchExecEngineProvider} from the analytics framework.
 *
 * @param <R> the reader type produced by this backend's reader manager
 * @opensearch.internal
 */
public interface SearchBackEndPlugin<R> {

    /** Unique backend name (e.g., "datafusion", "lucene"). */
    String name();

    /** Returns the data formats this backend can read and query. */
    List<DataFormat> getSupportedFormats();

    /**
     * Creates a reader manager for the given data format and shard.
     * <p>
     * The optional {@link IndexWriterProvider} gives backends access to the
     * shard's IndexWriter for opening NRT readers. Backends that do not need
     * a writer (e.g., DataFusion) may ignore this parameter.
     *
     * @param format the data format
     * @param shardPath the shard path
     * @param indexWriterProvider provider for the shard's IndexWriter, or null if not available
     * @return the reader manager
     * @throws IOException if reader creation fails
     */
    EngineReaderManager<?> createReaderManager(DataFormat format, ShardPath shardPath, IndexWriterProvider indexWriterProvider)
        throws IOException;
}
