/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.shard;

import org.opensearch.index.engine.Engine;

import java.io.IOException;

/** Test-only access to the primary engine no-op path. */
public final class MVNoOpTestHelper {
    private MVNoOpTestHelper() {}

    public static void markPrimaryNoOp(IndexShard shard, long seqNo, String reason) throws IOException {
        shard.getIndexer()
            .noOp(new Engine.NoOp(seqNo, shard.getOperationPrimaryTerm(), Engine.Operation.Origin.PRIMARY, System.nanoTime(), reason));
    }
}
