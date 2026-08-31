/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.derived.pull.spi;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;

/**
 * SPI entry point that ties together a {@link DerivedSourceReader} and a
 * {@link DerivedArtifactBuilder} for a specific derived data format.
 *
 * <p>Format plugins register implementations of this interface with the
 * node-level {@code NodeDerivedPullService}. The generic service resolves
 * the correct format by {@link #formatId()} and delegates all format-specific
 * work to the reader and builder.
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public interface DerivedPullFormat {

    /**
     * Returns the DERIVED DATA-FORMAT CATEGORY this format serves (e.g.
     * {@code "materialized_view"}). This value MUST equal the target index's
     * canonical {@code index.derived.data_format} setting — the node-level
     * {@code NodeDerivedPullService} keys its registry by this id and resolves
     * eligibility directly from the category. It is NOT a
     * {@code secondary_data_formats} entry; the physical target artifact the
     * category stores (e.g. {@code mv_state}) is resolved separately through
     * the {@code DataFormatRegistry}.
     */
    String formatId();

    /**
     * Creates a reader for the given index. Called once per shard poller.
     *
     * @param nodeSettings  node-level settings
     * @param indexSettings index-level settings
     * @return a new reader instance
     */
    DerivedSourceReader createReader(Settings nodeSettings, IndexSettings indexSettings);

    /**
     * Creates an artifact builder for the given index. Called once per shard
     * poller.
     *
     * @param nodeSettings  node-level settings
     * @param indexSettings index-level settings
     * @return a new builder instance
     */
    DerivedArtifactBuilder createArtifactBuilder(Settings nodeSettings, IndexSettings indexSettings);
}
