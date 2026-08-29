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
 * POC materialized-view data format on a SOURCE index: a {@link
 * DerivedDataFormat} whose per-segment output is aggregate state computed
 * from the ingest broadcast (embedded mode) or shipped to a separate MV
 * index before commit (ship mode). The derived-format contract (row-parity
 * exempt, may emit no files, never claims fields) comes from the base type.
 */
public final class MVDataFormat extends DerivedDataFormat {

    public static final String NAME = "materialized_view";
    public static final MVDataFormat INSTANCE = new MVDataFormat();

    private MVDataFormat() {}

    @Override
    public String name() {
        return NAME;
    }
}
