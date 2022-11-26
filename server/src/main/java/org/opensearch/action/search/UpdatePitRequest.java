/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.common.xcontent.ToXContentObject;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.common.xcontent.XContentParser;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.opensearch.action.ValidateActions.addValidationError;

public class UpdatePitRequest extends ActionRequest implements ToXContentObject {
// TODO: update the pit request to handle not just array
    private final List<UpdatePitRequestInfo> updatePitRequests;


    public List<UpdatePitRequestInfo> getUpdatePitRequests() {
        return updatePitRequests;
    }

    public UpdatePitRequest(UpdatePitRequestInfo... updatePitRequests){
        this.updatePitRequests = (Arrays.asList(updatePitRequests));
    }

    public UpdatePitRequest(List<UpdatePitRequestInfo> updatePitRequests){
        this.updatePitRequests = updatePitRequests;
    }

    public UpdatePitRequest() {
        this.updatePitRequests = new ArrayList<>();
    }


    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (updatePitRequests == null || updatePitRequests.isEmpty()) {
            validationException = addValidationError("No pit ids specified", validationException);
        }
        return validationException;
    }

    public UpdatePitRequest(StreamInput in) throws IOException {
        super(in);
        int size = in.readVInt();
        updatePitRequests = new ArrayList<>();
        for(int i=0;i<size;i++){
            updatePitRequests.add(new UpdatePitRequestInfo(in));
        }
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVInt(updatePitRequests.size());
        for(UpdatePitRequestInfo updatePitRequest: updatePitRequests) {
            updatePitRequest.writeTo(out);
        }
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.startArray("pits");
        for(UpdatePitRequestInfo requestInfo : updatePitRequests){
            requestInfo.toXContent(builder,params);
        }
        builder.endArray();
        builder.endObject();
        return builder;
    }

    // Req body
    // {
    //   pits: [
    //             {
    //                  pit_id: <>
    //                  keep_alive: <>
    //              }
    //          ]
    // }
    public void fromXContent(XContentParser parser) throws IOException {
        updatePitRequests.clear();
        if(parser.nextToken() != XContentParser.Token.START_OBJECT){
            throw new IllegalArgumentException("Malformed content, must start with an object");
        } else {
            XContentParser.Token token;
            String currentFieldName = null;
            String currentFieldName1 = null;
            while((token = parser.nextToken()) != XContentParser.Token.END_OBJECT){
                if (token == XContentParser.Token.FIELD_NAME){
                    currentFieldName = parser.currentName();
                } else if("pits".equals(currentFieldName)){
                    if(token == XContentParser.Token.START_ARRAY){
                        while(parser.nextToken() != XContentParser.Token.START_OBJECT) {
                            String pit_id =null;
                            String keep_alive = null;
                            if (parser.nextToken() == XContentParser.Token.FIELD_NAME){
                                currentFieldName1 = parser.currentName();
                            }
                            if("pit_id".equals(currentFieldName1)){
                                pit_id = parser.text();
                            } else{
                                throw new IllegalArgumentException("pit_id array element should only contain pit_id " + currentFieldName1);
                            }

                            if (parser.nextToken() == XContentParser.Token.FIELD_NAME){
                                currentFieldName1 = parser.currentName();
                            }
                            if("keep_alive".equals(currentFieldName1)){
                                keep_alive = parser.text();
                            } else{
                                throw new IllegalArgumentException("pit_id array element should only contain pit_id " + currentFieldName1);
                            }
                             updatePitRequests.add(new UpdatePitRequestInfo(pit_id, keep_alive));


                            if(parser.nextToken() !=XContentParser.Token.END_OBJECT){
                                throw new IllegalArgumentException("pit_id array element should only contain pit_id " + currentFieldName1);
                            }
                        }
                    } else {
                        throw new IllegalArgumentException("pit_id array element should only contain pit_id");

                    }
                }

            }
        }
    }
}
