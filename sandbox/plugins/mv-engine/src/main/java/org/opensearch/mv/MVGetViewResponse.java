/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.common.xcontent.StatusToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.List;

/**
 * Response for {@code GET /_mv/views/{name}}. Reports the target's source
 * binding, derived category, and a summary of the persisted descriptor (group
 * keys, aggregate aliases, state fields). {@code 404} when the target index does
 * not exist or is not an MV derived target.
 *
 * <p>Watermark / lag is intentionally omitted in this version: it is per-shard
 * runtime state owned by the derived-pull service and is not cheaply available
 * from cluster state alone. It will be added by the {@code _status} endpoint.
 */
public class MVGetViewResponse extends ActionResponse implements StatusToXContentObject {

    private final boolean found;
    private final String targetIndex;
    private final String sourceIndex;
    private final String dataFormat;
    private final String definitionLabel;
    private final boolean descriptorPresent;
    private final List<String> groupKeys;
    private final List<String> aggregates;
    private final List<String> stateFields;

    public MVGetViewResponse(
        boolean found,
        String targetIndex,
        String sourceIndex,
        String dataFormat,
        String definitionLabel,
        boolean descriptorPresent,
        List<String> groupKeys,
        List<String> aggregates,
        List<String> stateFields
    ) {
        this.found = found;
        this.targetIndex = targetIndex;
        this.sourceIndex = sourceIndex;
        this.dataFormat = dataFormat;
        this.definitionLabel = definitionLabel;
        this.descriptorPresent = descriptorPresent;
        this.groupKeys = groupKeys == null ? List.of() : List.copyOf(groupKeys);
        this.aggregates = aggregates == null ? List.of() : List.copyOf(aggregates);
        this.stateFields = stateFields == null ? List.of() : List.copyOf(stateFields);
    }

    public static MVGetViewResponse notFound(String targetIndex) {
        return new MVGetViewResponse(false, targetIndex, null, null, null, false, null, null, null);
    }

    public MVGetViewResponse(StreamInput in) throws IOException {
        this.found = in.readBoolean();
        this.targetIndex = in.readOptionalString();
        this.sourceIndex = in.readOptionalString();
        this.dataFormat = in.readOptionalString();
        this.definitionLabel = in.readOptionalString();
        this.descriptorPresent = in.readBoolean();
        this.groupKeys = in.readStringList();
        this.aggregates = in.readStringList();
        this.stateFields = in.readStringList();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(found);
        out.writeOptionalString(targetIndex);
        out.writeOptionalString(sourceIndex);
        out.writeOptionalString(dataFormat);
        out.writeOptionalString(definitionLabel);
        out.writeBoolean(descriptorPresent);
        out.writeStringCollection(groupKeys);
        out.writeStringCollection(aggregates);
        out.writeStringCollection(stateFields);
    }

    public boolean isFound() {
        return found;
    }

    @Override
    public RestStatus status() {
        return found ? RestStatus.OK : RestStatus.NOT_FOUND;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("found", found);
        builder.field("view", targetIndex);
        if (found) {
            builder.field("target_index", targetIndex);
            builder.field("source_index", sourceIndex);
            builder.field("data_format", dataFormat);
            builder.field("definition", definitionLabel);
            builder.field("descriptor_present", descriptorPresent);
            builder.startObject("definition_summary");
            builder.field("group_keys", groupKeys);
            builder.field("aggregates", aggregates);
            builder.field("state_fields", stateFields);
            builder.endObject();
            builder.nullField("watermark");
        }
        builder.endObject();
        return builder;
    }
}
