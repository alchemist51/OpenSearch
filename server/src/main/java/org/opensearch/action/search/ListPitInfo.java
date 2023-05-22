/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.core.ParseField;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.io.stream.StreamOutput;
import org.opensearch.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ConstructingObjectParser;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

import static org.opensearch.core.xcontent.ConstructingObjectParser.constructorArg;

/**
 * This holds information about pit reader context such as pit id and creation time
 */
public class ListPitInfo implements ToXContentFragment, Writeable {
    private final String pitId;
    private final long creationTime;
    private final long keepAlive;

    private final String[] indices;

    public ListPitInfo(String pitId, long creationTime, long keepAlive, String[] indices) {
        this.pitId = pitId;
        this.creationTime = creationTime;
        this.keepAlive = keepAlive;
        this.indices = indices;
    }

    public ListPitInfo(StreamInput in) throws IOException {
        this.pitId = in.readString();
        this.creationTime = in.readLong();
        this.keepAlive = in.readLong();
        this.indices = in.readStringArray();
    }

    public String getPitId() {
        return pitId;
    }

    public long getCreationTime() {
        return creationTime;
    }

    public String[] getIndices() { return indices; }
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeString(pitId);
        out.writeLong(creationTime);
        out.writeLong(keepAlive);
        out.writeStringArray(indices);
    }

    static final ConstructingObjectParser<ListPitInfo, Void> PARSER = new ConstructingObjectParser<>(
        "list_pit_info",
        true,
        args -> new ListPitInfo((String) args[0], (long) args[1], (long) args[2], (String[]) args[3])
    );

    private static final ParseField CREATION_TIME = new ParseField("creation_time");
    private static final ParseField PIT_ID = new ParseField("pit_id");
    private static final ParseField KEEP_ALIVE = new ParseField("keep_alive");
    private static final ParseField INDICES = new ParseField("indices");
    static {
        PARSER.declareString(constructorArg(), PIT_ID);
        PARSER.declareLong(constructorArg(), CREATION_TIME);
        PARSER.declareLong(constructorArg(), KEEP_ALIVE);
        PARSER.declareStringArray(constructorArg(), INDICES);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field(PIT_ID.getPreferredName(), pitId);
        builder.field(CREATION_TIME.getPreferredName(), creationTime);
        builder.field(KEEP_ALIVE.getPreferredName(), keepAlive);
        builder.field(INDICES.getPreferredName(), indices);
        builder.endObject();
        return builder;
    }

}
