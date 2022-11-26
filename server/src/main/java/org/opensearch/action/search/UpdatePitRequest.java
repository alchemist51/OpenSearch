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
// TODO: update the pit reqyest to handle not just array
    private final List<UpdatePitRequestInfo> updatePitRequests;

    public UpdatePitRequest(StreamInput in) throws IOException {
        super(in);
        int size = in.readVInt();
        updatePitRequests = new ArrayList<>();
        for(int i=0;i<size;i++){
            updatePitRequests.add(new UpdatePitRequestInfo(in));
        }
    }

    public List<UpdatePitRequestInfo> getUpdatePitRequests() {
        return updatePitRequests;
    }

    public UpdatePitRequest(UpdatePitRequestInfo... updatePitRequests){
        this.updatePitRequests = (Arrays.asList(updatePitRequests));
    }

    public UpdatePitRequest(List<UpdatePitRequestInfo> updatePitRequests){
        this.updatePitRequests = updatePitRequests;
    }

    public UpdatePitRequest() {}


    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException validationException = null;
        if (keepAlive == null) {
            validationException = addValidationError("keep alive not specified", validationException);
        }
        return validationException;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.field("keep_alive", keepAlive);
        builder.field("pit_id", pit_id);
        return builder;
    }

    public void fromXContent(XContentParser parser) throws IOException {
        if(parser.nextToken() != XContentParser.Token.START_OBJECT){
            throw new IllegalArgumentException("Malformed content, must start with an object");
        } else {
            XContentParser.Token token;
            String currentFieldName = null;
            while((token = parser.nextToken()) != XContentParser.Token.END_OBJECT){
                if (token == XContentParser.Token.FIELD_NAME){
                    currentFieldName = parser.currentName();
                } else if("keep_alive".equals(currentFieldName)){
                    if(token.isValue() == false){
                        throw new IllegalArgumentException("keep_alive should only contain a time value");
                    }
                    keepAlive = TimeValue.parseTimeValue(parser.text(),"keep_alive");
                }

            }
        }
    }
}
