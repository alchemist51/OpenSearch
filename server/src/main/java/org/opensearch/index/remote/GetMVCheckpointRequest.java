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
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.transport.TransportRequest;

import java.io.IOException;
import java.util.Objects;

/**
 * Transport request to get MV checkpoint from a source shard.
 *
 * @opensearch.internal
 */
public class GetMVCheckpointRequest extends TransportRequest {

    private final ShardId shardId;
    private final String mvIdFilter; // nullable: null = all MVs

    public GetMVCheckpointRequest(ShardId shardId) {
        this(shardId, null);
    }

    public GetMVCheckpointRequest(ShardId shardId, String mvIdFilter) {
        this.shardId = Objects.requireNonNull(shardId);
        this.mvIdFilter = mvIdFilter;
    }

    public GetMVCheckpointRequest(StreamInput in) throws IOException {
        super(in);
        this.shardId = new ShardId(in);
        this.mvIdFilter = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        shardId.writeTo(out);
        out.writeOptionalString(mvIdFilter);
    }

    public ShardId shardId() {
        return shardId;
    }

    public String mvIdFilter() {
        return mvIdFilter;
    }
}
