/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import org.opensearch.index.engine.dataformat.DerivedDataFormat;

/**
 * The separate-index MV target's aggregate-state format. It remains a
 * {@link DerivedDataFormat}: row parity and document field capability claims
 * do not apply. Primary use is enabled by the generic derived-only index mode,
 * which accepts externally-built Arrow artifacts instead of documents.
 */
public final class MVStateDataFormat extends DerivedDataFormat {

    public static final String NAME = "mv_state";
    public static final MVStateDataFormat INSTANCE = new MVStateDataFormat();

    private MVStateDataFormat() {}

    @Override
    public String name() {
        return NAME;
    }

    /**
     * The physical target-artifact belongs to the {@code materialized_view}
     * derived category. A derived MV target declares this category via
     * {@code index.derived.data_format}; the composite store then manages the
     * {@code mv_state} artifact for it. The artifact format name is therefore
     * never listed in {@code index.composite.secondary_data_formats}.
     */
    @Override
    public String category() {
        return MVDataFormat.NAME; // "materialized_view"
    }

    @Override
    public boolean isDerivedTargetArtifact() {
        return true;
    }
}
