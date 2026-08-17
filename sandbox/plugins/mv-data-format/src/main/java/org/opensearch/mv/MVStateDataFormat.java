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
 * The separate-index MV TARGET's derived format: maintains per-segment FOLDED
 * state files whose definition is the fold of the shipped state schema
 * ({@link MVDefinitionSpec#TARGET_FOLD}). Shipped state rows arrive as
 * ordinary documents through the target's write path (translog, replication);
 * this format observes the broadcast and materializes folded, group-key-
 * sorted state per target generation — the embedded-MV shape applied to the
 * MV index itself.
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
