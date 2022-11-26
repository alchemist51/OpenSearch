/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.common.ParseField;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.common.xcontent.ConstructingObjectParser;
import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.common.xcontent.XContentBuilder;
import org.opensearch.transport.TransportResponse;

import java.io.IOException;

import static org.opensearch.common.xcontent.ConstructingObjectParser.constructorArg;

public class UpdatePitInfo extends TransportResponse implements Writeable, ToXContent {
    private final boolean successful;
    private final String pitId;

    public UpdatePitInfo(boolean successful, String pitId){
        this.successful = successful;
        this.pitId = pitId;
    }

    public UpdatePitInfo(StreamInput in) throws IOException{
        successful = in.readBoolean();
        pitId = in.readString();
    }

    public boolean isSuccessful(){
        return successful;
    }

    public String getPitId(){
        return pitId;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(successful);
        out.writeString(pitId);
    }

    static final ConstructingObjectParser<UpdatePitInfo, Void> PARSER = new ConstructingObjectParser<>(
        "update_pit_info",
        true,
        args -> new UpdatePitInfo((boolean) args[0], (String) args[1])
    );

    static {
        PARSER.declareBoolean(constructorArg(), new ParseField("successful"));
        PARSER.declareString(constructorArg(), new ParseField("pit_id"));
    }

    private static final ParseField SUCCESSFUL = new ParseField("successful");
    private static final ParseField PIT_ID = new ParseField("pit_id");

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(SUCCESSFUL.getPreferredName(), successful);
        builder.field(PIT_ID.getPreferredName(), pitId);
        builder.endObject();
        return builder;
    }
}

