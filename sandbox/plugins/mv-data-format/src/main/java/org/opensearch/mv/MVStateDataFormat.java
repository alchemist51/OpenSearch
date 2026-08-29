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
}
