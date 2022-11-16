/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.xcontent.ToXContent;
import org.opensearch.transport.TransportResponse;

import java.io.IOException;

public class UpdatePitInfo extends TransportResponse implements Writable, ToXContent {
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
}
