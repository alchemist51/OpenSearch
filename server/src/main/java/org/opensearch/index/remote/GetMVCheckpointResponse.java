/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.remote;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.transport.TransportResponse;

import java.io.IOException;
import java.util.Objects;

/**
 * Transport response carrying the MV checkpoint from a source shard.
 *
 * @opensearch.internal
 */
public class GetMVCheckpointResponse extends TransportResponse {

    private final MVCheckpoint checkpoint;

    public GetMVCheckpointResponse(MVCheckpoint checkpoint) {
        this.checkpoint = Objects.requireNonNull(checkpoint);
    }

    public GetMVCheckpointResponse(StreamInput in) throws IOException {
        this.checkpoint = new MVCheckpoint(in);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        checkpoint.writeTo(out);
    }

    public MVCheckpoint checkpoint() {
        return checkpoint;
    }
}
