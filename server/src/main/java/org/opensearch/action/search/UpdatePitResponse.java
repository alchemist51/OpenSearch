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
import org.opensearch.common.xcontent.*;
import org.opensearch.rest.RestStatus;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.opensearch.common.xcontent.ConstructingObjectParser.constructorArg;
import static org.opensearch.rest.RestStatus.NOT_FOUND;
import static org.opensearch.rest.RestStatus.OK;

public class UpdatePitResponse extends ActionResponse implements StatusToXContentObject {
    private final List<UpdatePitResponseInfo> updatePitResults;

    public UpdatePitResponse(List<UpdatePitResponseInfo> updatePitResults){
        this.updatePitResults = updatePitResults;
    }
    public UpdatePitResponse(StreamInput in) throws IOException{
        super(in);
        int size = in.readVInt();
        updatePitResults = new ArrayList<>();
        for (int i=0; i < size; i++) {
            updatePitResults.add(new UpdatePitResponseInfo(in));
        }
    }

    public List<UpdatePitResponseInfo> getUpdatePitResults() {
        return updatePitResults;
    }

    @Override
    public RestStatus status() {
        if (updatePitResults.isEmpty()) return NOT_FOUND;
        return OK;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(updatePitResults.size());
        for (UpdatePitResponseInfo updatePitResult : updatePitResults) {
            updatePitResult.writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.startArray("pits");
        for (UpdatePitResponseInfo response: updatePitResults) {
            response.toXContent(builder,params);
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    private static final ConstructingObjectParser<UpdatePitResponse, Void> PARSER = new ConstructingObjectParser<>(
        "update_pit_response",
        true,
        (Object[] parsedObjects) -> {
            @SuppressWarnings("unchecked")
                List<UpdatePitResponseInfo> updatePitResponseInfoList = (List<UpdatePitResponseInfo>) parsedObjects[0];
            return new UpdatePitResponse(updatePitResponseInfoList);
         }
    );

    static {
        PARSER.declareObjectArray(constructorArg(), UpdatePitResponseInfo.PARSER, new ParseField("pits"));
    }

    public static UpdatePitResponse fromXContent(XContentParser parser) throws IOException {
        return PARSER.parse(parser, null);
    }
}
