/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.common.xcontent.StatusToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;
import java.util.List;

/**
 * Response for {@code PUT /_mv/views/{name}}. Reports the created target index,
 * its source binding, and the persisted descriptor. HTTP {@code 201 CREATED}
 * when the target index was acknowledged, else {@code 200 OK}.
 */
public class MVCreateViewResponse extends ActionResponse implements StatusToXContentObject {

    private final boolean acknowledged;
    private final String viewName;
    private final String sourceIndex;
    private final String targetIndex;
    private final String descriptorJson;
    private final List<String> stateFields;

    public MVCreateViewResponse(
        boolean acknowledged,
        String viewName,
        String sourceIndex,
        String targetIndex,
        String descriptorJson,
        List<String> stateFields
    ) {
        this.acknowledged = acknowledged;
        this.viewName = viewName;
        this.sourceIndex = sourceIndex;
        this.targetIndex = targetIndex;
        this.descriptorJson = descriptorJson;
        this.stateFields = stateFields == null ? List.of() : List.copyOf(stateFields);
    }

    public MVCreateViewResponse(StreamInput in) throws IOException {
        this.acknowledged = in.readBoolean();
        this.viewName = in.readOptionalString();
        this.sourceIndex = in.readOptionalString();
        this.targetIndex = in.readOptionalString();
        this.descriptorJson = in.readOptionalString();
        this.stateFields = in.readStringList();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(acknowledged);
        out.writeOptionalString(viewName);
        out.writeOptionalString(sourceIndex);
        out.writeOptionalString(targetIndex);
        out.writeOptionalString(descriptorJson);
        out.writeStringCollection(stateFields);
    }

    public boolean isAcknowledged() {
        return acknowledged;
    }

    public String targetIndex() {
        return targetIndex;
    }

    @Override
    public RestStatus status() {
        return acknowledged ? RestStatus.CREATED : RestStatus.OK;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("acknowledged", acknowledged);
        builder.field("view", viewName);
        builder.field("source_index", sourceIndex);
        builder.field("target_index", targetIndex);
        builder.field("state_fields", stateFields);
        if (descriptorJson != null) {
            builder.field("descriptor");
            embedJson(builder, descriptorJson);
        }
        builder.endObject();
        return builder;
    }

    static void embedJson(XContentBuilder builder, String json) throws IOException {
        try (
            XContentParser p = JsonXContent.jsonXContent.createParser(
                NamedXContentRegistry.EMPTY,
                DeprecationHandler.IGNORE_DEPRECATIONS,
                json
            )
        ) {
            p.nextToken();
            builder.copyCurrentStructure(p);
        }
    }
}
