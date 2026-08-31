/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv.pull;

import org.opensearch.common.settings.Settings;
import org.opensearch.index.IndexSettings;
import org.opensearch.index.engine.derived.pull.spi.DerivedArtifactBuilder;
import org.opensearch.index.engine.derived.pull.spi.DerivedPullFormat;
import org.opensearch.index.engine.derived.pull.spi.DerivedSourceReader;
import org.opensearch.mv.MVDataFormat;

/**
 * MV-specific implementation of the generic {@link DerivedPullFormat} SPI.
 *
 * <p>This adapter bridges the format-agnostic server-level pull service
 * ({@link org.opensearch.index.engine.derived.pull.NodeDerivedPullService})
 * to the MV-specific source reader and artifact builder. It is registered
 * once at plugin startup and resolved by the generic service via
 * {@link #formatId()}.</p>
 *
 * <p>The format ID is the DERIVED DATA-FORMAT CATEGORY
 * {@code materialized_view} — the value the target index declares in the
 * canonical {@code index.derived.data_format} setting. The physical state
 * artifact ({@code mv_state}) is resolved separately through the
 * {@code DataFormatRegistry} and is never listed in the target's
 * {@code index.composite.secondary_data_formats}. The target's primary format
 * remains {@code parquet} (which provides field capabilities such as
 * COLUMNAR_STORAGE for {@code _doc_count}); {@code lucene} may remain an
 * ordinary secondary.</p>
 */
public final class MVDerivedPullFormat implements DerivedPullFormat {

    private final MVPullSettings.Services services;

    public MVDerivedPullFormat(MVPullSettings.Services services) {
        this.services = services;
    }

    @Override
    public String formatId() {
        // The derived category (== index.derived.data_format on the target).
        // NodeDerivedPullService keys its registry and resolves eligibility by
        // this value. It is NOT the physical state-artifact format name
        // (mv_state) — that is resolved via the DataFormatRegistry.
        return MVDataFormat.NAME; // "materialized_view"
    }

    @Override
    public DerivedSourceReader createReader(Settings nodeSettings, IndexSettings indexSettings) {
        return new MVDerivedSourceReader(indexSettings, services);
    }

    @Override
    public DerivedArtifactBuilder createArtifactBuilder(Settings nodeSettings, IndexSettings indexSettings) {
        return new MVDerivedArtifactBuilder(indexSettings, services);
    }
}
