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
import org.opensearch.rest.RestStatus;

import java.io.IOException;

import static org.opensearch.rest.RestStatus.OK;

public class UpdatePitResponse extends ActionResponse implements StatusToXContentObject {

    private static final ParseField ID = new ParseField("pit_id");
    private static final ParseField CREATION_TIME = new ParseField("creation_time");
    private static final ParseField EXPIRY_TIME = new ParseField("expiry_time");
    private static final ParseField SUCCESS_FULL = new ParseField("successfull");

    private final String id;
    private final int totalShards;
    private final int successfullShards;
    private final int failedShards;
    public UpdatePitRequest(StreamInput in) throws IOException {
        super(in);
        id = in.readString();
        totalShards = in.readVInt();
        successfullShards = in.readVInt();
        failedShards = in.readVInt();
    }

    @Override
    public RestStatus status(){
        return OK;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException{
        out.writeString(id);

    }
}
