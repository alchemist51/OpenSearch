/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.action.ActionResponse;
import org.opensearch.common.ParseField;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.StatusToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentParser;
import org.opensearch.rest.RestStatus;
import org.opensearch.rest.action.RestActions;

import java.io.IOException;

import static org.opensearch.rest.RestStatus.OK;

public class UpdatePitResponse extends ActionResponse implements StatusToXContentObject {

    private static final ParseField ID = new ParseField("pit_id");
    private static final ParseField CREATION_TIME = new ParseField("creation_time");
    private static final ParseField EXPIRY_TIME = new ParseField("expiry_time");
    private static final ParseField SUCCESS_FULL = new ParseField("successfull");

    private final String id;
    private final int totalShards;
    private final int successfulShards;
    private final int failedShards;
    private final int skippedShards;
    private final ShardSearchFailure[] shardFailures;

    private final long creationTime;
    private final long expiryTime;
    public UpdatePitResponse(StreamInput in) throws IOException {
        super(in);
        id = in.readString();
        totalShards = in.readVInt();
        successfulShards = in.readVInt();
        failedShards = in.readVInt();
        skippedShards = in.readVInt();
        creationTime = in.readLong();
        expiryTime = in.readLong();
        int size = in.readVInt();
        if(size==0){
            shardFailures = ShardSearchFailure.EMPTY_ARRAY;
        } else {
            shardFailures = new ShardSearchFailure[size];
            for (int i=0; i < shardFailures.length; i++){
                shardFailures[i] = ShardSearchFailure.readShardSearchFailure(in);
            }
        }
    }

    public UpdatePitResponse(
        String id,
        long creationTime,
        long expiryTime,
        int totalShards,
        int successfulShards,
        int skippedShards,
        int failedShards,
        ShardSearchFailure[] shardFailures
    ) {
        this.id = id;
        this.creationTime = creationTime;
        this.expiryTime = expiryTime;
        this.totalShards = totalShards;
        this.successfulShards = successfulShards;
        this.skippedShards = skippedShards;
        this.failedShards = failedShards;
        this.shardFailures = shardFailures;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException{
        out.writeString(id);
        out.writeLong(creationTime);
        out.writeLong(expiryTime);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(ID.getPreferredName(), id);
        RestActions.buildBroadcastShardsHeader(
            builder,
            params,
            getTotalShards(),
            getSuccessfulShards(),
            getSkippedShards(),
            getFailedShards(),
            getShardFailures()
        );
        builder.endObject();
        return builder;
    }

    public int getTotalShards(){
        return totalShards;
    }

    public int getSuccessfulShards(){
        return successfulShards;
    }

    public int getFailedShards(){
        return shardFailures.length;
    }

    public ShardSearchFailure[] getShardFailures() {
        return this.shardFailures;
    }

    public int getSkippedShards(){
        return skippedShards;
    }

    @Override
    public RestStatus status() {
        return RestStatus.status(successfulShards, totalShards, shardFailures);
    }
}
//
