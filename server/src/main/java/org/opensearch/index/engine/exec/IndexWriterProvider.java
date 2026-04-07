/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.apache.lucene.index.IndexWriter;
import org.opensearch.common.annotation.ExperimentalApi;

/**
 * Provides read-only access to the shard's {@link IndexWriter} for opening NRT readers.
 * <p>
 * This abstraction exists so that the {@link org.opensearch.plugins.SearchBackEndPlugin}
 * SPI does not leak Lucene types. Implementations wrap a per-shard IndexWriter and
 * expose it only to backends that need it (e.g., Lucene NRT reader managers).
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface IndexWriterProvider {

    /**
     * Returns the IndexWriter for the current shard.
     *
     * @return the IndexWriter, never null
     */
    IndexWriter getIndexWriter();
}
