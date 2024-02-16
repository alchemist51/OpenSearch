/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.recovery;


import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

public class PeerRecoveryStats implements Writeable, ToXContentFragment {

    private final long totalRelocation;

    public PeerRecoveryStats(StreamInput in) throws IOException {
        totalRelocation = in.readVLong();
    }

    public PeerRecoveryStats(long totalRelocation) {
        this.totalRelocation = totalRelocation;
    }

    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(totalRelocation);
    }

    public long getTotalRelocation() {
        return totalRelocation;
    }

    private static final String TOTAL_RELOCATION = "total_relocation";

    public XContentBuilder toXContent(XContentBuilder builder, ToXContent.Params params) throws IOException {
        builder.startObject("PeerRecoveryStats");
        builder.field(TOTAL_RELOCATION, totalRelocation);
        return builder.endObject();
    }
}
