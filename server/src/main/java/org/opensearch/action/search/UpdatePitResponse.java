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

import java.io.IOException;

import static org.opensearch.rest.RestStatus.OK;

public class UpdatePitResponse extends ActionResponse implements StatusToXContentObject {

    private static final ParseField ID = new ParseField("pit_id");
    private static final ParseField CREATION_TIME = new ParseField("creation_time");
    private static final ParseField EXPIRY_TIME = new ParseField("expiry_time");
    private static final ParseField SUCCESS_FULL = new ParseField("successfull");

    private final String id;
    private final long creationTime;
    private final long expiryTime;
    public UpdatePitResponse(StreamInput in) throws IOException {
        super(in);
        id = in.readString();
        creationTime = in.readLong();
        expiryTime = in.readLong();

    }

    public UpdatePitResponse(
        String id,
        long creationTime,
        long expiryTime
    ) {
        this.id = id;
        this.creationTime = creationTime;
        this.expiryTime = expiryTime;
    }

    @Override
    public RestStatus status(){
        return OK;
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

    }
}
