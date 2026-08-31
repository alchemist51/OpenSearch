/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import org.apache.arrow.vector.VectorSchemaRoot;

import java.io.IOException;

/**
 * Replicates one Arrow batch of derived state plus its source coordinates.
 *
 * <p>The contract is deliberately batch/replication-shaped rather than
 * document/bulk-shaped. The current implementation uses Arrow C Data for the
 * colocated same-JVM path. A remote implementation can encode the same root as
 * Arrow IPC (or stream it with Arrow Flight) without changing fold, cursor,
 * or target-apply semantics.
 */
interface DerivedStateReplicator {

    long replicate(VectorSchemaRoot stateBatch, BatchCoordinates coordinates) throws IOException;

    record BatchCoordinates(long writerGeneration, long foldCheckpoint, long maxSeqNo, String definitionFingerprint, String batchIdentity,
        MVSourceSeqCoverage sourceCoverage) {
        BatchCoordinates(long writerGeneration, long foldCheckpoint, long maxSeqNo, String definitionFingerprint) {
            this(writerGeneration, foldCheckpoint, maxSeqNo, definitionFingerprint, null, MVSourceSeqCoverage.contiguous(foldCheckpoint));
        }

        BatchCoordinates(long writerGeneration, long foldCheckpoint, long maxSeqNo, String definitionFingerprint, String batchIdentity) {
            this(
                writerGeneration,
                foldCheckpoint,
                maxSeqNo,
                definitionFingerprint,
                batchIdentity,
                MVSourceSeqCoverage.contiguous(foldCheckpoint)
            );
        }
    }
}
