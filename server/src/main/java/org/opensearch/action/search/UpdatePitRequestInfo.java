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
import org.opensearch.common.unit.TimeValue;

import java.io.IOException;

public class UpdatePitRequestInfo {
    private final String pitId;
    private final TimeValue keepAlive;

    public UpdatePitRequestInfo(String pitId, TimeValue keepAlive){
        this.pitId = pitId;
        this.keepAlive = keepAlive;
    }

    public UpdatePitRequestInfo(StreamInput in) throws IOException {
        pitId = in.readString();
        keepAlive = in.readTimeValue();
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(pitId);
        out.writeTimeValue(keepAlive);
    }
}
