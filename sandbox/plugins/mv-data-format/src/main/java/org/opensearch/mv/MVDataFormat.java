/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import org.opensearch.index.engine.dataformat.DataFormat;
import org.opensearch.index.engine.dataformat.FieldTypeCapabilities;

import java.util.Set;

/**
 * POC materialized-view data format. A <b>derived</b> secondary format: it never
 * ingests documents (claims no field capabilities); its per-segment files are
 * aggregate state computed from the parquet primary's flushed file.
 */
public final class MVDataFormat extends DataFormat {

    public static final String NAME = "materialized_view";
    public static final MVDataFormat INSTANCE = new MVDataFormat();

    private MVDataFormat() {}

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public long priority() {
        // Never wins a capability claim; always a secondary.
        return Long.MAX_VALUE;
    }

    @Override
    public Set<FieldTypeCapabilities> supportedFields() {
        // Claims nothing: MV reads its input from the primary's flushed file,
        // not from DocumentInput.
        return Set.of();
    }

    @Override
    public boolean exemptFromRowParity() {
        // Aggregated rows: one row per group, not one per document.
        return true;
    }
}
