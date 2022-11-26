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
import org.opensearch.common.xcontent.XContentBuilder;

import java.io.IOException;

public class UpdatePitRequestInfo {
    private final String pitId;
    private final String keepAlive;

    public UpdatePitRequestInfo(String pitId, String keepAlive){
        this.pitId = pitId;
        this.keepAlive = keepAlive;
    }

    public UpdatePitRequestInfo(StreamInput in) throws IOException {
        pitId = in.readString();
        keepAlive = in.readString();
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(pitId);
        out.writeString(keepAlive);
    }

    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject();
        builder.field("pit_id", pitId);
        builder.field("keepAlive", keepAlive);
        builder.endObject();
        return builder;
    }
}
