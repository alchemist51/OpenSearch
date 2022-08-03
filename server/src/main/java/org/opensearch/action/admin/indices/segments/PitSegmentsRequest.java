/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.segments;

import org.opensearch.action.support.broadcast.BroadcastRequest;
import org.opensearch.common.Strings;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;

/**
 * Transport request for retrieving PITs segment information
 */
public class PitSegmentsRequest extends BroadcastRequest<PitSegmentsRequest> {

    protected boolean verbose = false;
    private Collection<String> pitIds;

    public PitSegmentsRequest() {
        this(Strings.EMPTY_ARRAY);
    }

    public PitSegmentsRequest(StreamInput in) throws IOException {
        super(in);
        pitIds = Arrays.asList(in.readStringArray());
        verbose = in.readBoolean();
    }

    public PitSegmentsRequest(String... indices) {
        super(indices);
        pitIds = Collections.emptyList();
    }

    /**
     * <code>true</code> if detailed information about each segment should be returned,
     * <code>false</code> otherwise.
     */
    public boolean verbose() {
        return verbose;
    }

    /**
     * Sets the <code>verbose</code> option.
     * @see #verbose()
     */
    public void verbose(boolean v) {
        verbose = v;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        if (pitIds == null) {
            out.writeVInt(0);
        } else {
            out.writeStringArray(pitIds.toArray(new String[pitIds.size()]));
        }
        out.writeBoolean(verbose);

    }

    public Collection<String> getPitIds() {
        return pitIds;
    }

    public void setPitIds(Collection<String> pitIds) {
        this.pitIds = pitIds;
    }
}
