/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import org.opensearch.action.ActionRequest;
import org.opensearch.action.ActionRequestValidationException;
import org.opensearch.action.ValidateActions;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;

/**
 * Transport request for {@code POST /_mv/_validate}. Carries a source index and
 * exactly one definition input: a descriptor object, PPL text, or SQL text.
 *
 * <p>Body shape:
 * <pre>
 * {
 *   "source_index": "clickbench",
 *   "descriptor": { "descriptor_version": 1, "group_keys": [...], "aggregates": [...] }
 *   // OR "ppl": "source=clickbench | stats ..."
 *   // OR "sql": "SELECT ... FROM clickbench GROUP BY ..."
 * }
 * </pre>
 *
 * <p>Only ONE of {@code descriptor} / {@code ppl} / {@code sql} may be present.
 * The descriptor is captured verbatim as compact JSON so the transport action
 * can parse it via {@link MVDefinitionDescriptor#fromXContent}.
 */
public class MVValidateRequest extends ActionRequest {

    static final String F_SOURCE_INDEX = "source_index";
    static final String F_DESCRIPTOR = "descriptor";
    static final String F_PPL = "ppl";
    static final String F_SQL = "sql";

    private final String sourceIndex;
    private final String descriptorJson; // nullable
    private final String ppl;            // nullable
    private final String sql;            // nullable

    public MVValidateRequest(String sourceIndex, String descriptorJson, String ppl, String sql) {
        this.sourceIndex = sourceIndex;
        this.descriptorJson = descriptorJson;
        this.ppl = ppl;
        this.sql = sql;
    }

    public MVValidateRequest(StreamInput in) throws IOException {
        super(in);
        this.sourceIndex = in.readOptionalString();
        this.descriptorJson = in.readOptionalString();
        this.ppl = in.readOptionalString();
        this.sql = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(sourceIndex);
        out.writeOptionalString(descriptorJson);
        out.writeOptionalString(ppl);
        out.writeOptionalString(sql);
    }

    public String sourceIndex() {
        return sourceIndex;
    }

    public String descriptorJson() {
        return descriptorJson;
    }

    public String ppl() {
        return ppl;
    }

    public String sql() {
        return sql;
    }

    /** True when the definition input is query text (PPL or SQL) rather than a descriptor. */
    public boolean hasQueryText() {
        return ppl != null || sql != null;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException e = null;
        if (sourceIndex == null || sourceIndex.isBlank()) {
            e = ValidateActions.addValidationError("[" + F_SOURCE_INDEX + "] is required", e);
        }
        int inputs = 0;
        if (descriptorJson != null) {
            inputs++;
        }
        if (ppl != null) {
            inputs++;
        }
        if (sql != null) {
            inputs++;
        }
        if (inputs == 0) {
            e = ValidateActions.addValidationError("exactly one of [descriptor], [ppl], or [sql] is required", e);
        } else if (inputs > 1) {
            e = ValidateActions.addValidationError("only one of [descriptor], [ppl], or [sql] may be provided", e);
        }
        return e;
    }

    /** Parse a request body into an {@link MVValidateRequest}. */
    public static MVValidateRequest fromXContent(XContentParser parser) throws IOException {
        String sourceIndex = null;
        String descriptorJson = null;
        String ppl = null;
        String sql = null;

        XContentParser.Token token = parser.currentToken();
        if (token == null) {
            token = parser.nextToken();
        }
        if (token != XContentParser.Token.START_OBJECT) {
            throw new IllegalArgumentException("expected START_OBJECT but got [" + token + "]");
        }
        String fieldName = null;
        while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
            if (token == XContentParser.Token.FIELD_NAME) {
                fieldName = parser.currentName();
                continue;
            }
            switch (fieldName) {
                case F_SOURCE_INDEX -> sourceIndex = parser.text();
                case F_DESCRIPTOR -> {
                    if (token != XContentParser.Token.START_OBJECT) {
                        throw new IllegalArgumentException("[" + F_DESCRIPTOR + "] must be an object");
                    }
                    descriptorJson = captureObjectAsJson(parser);
                }
                case F_PPL -> ppl = parser.text();
                case F_SQL -> sql = parser.text();
                default -> throw new IllegalArgumentException("unknown field [" + fieldName + "] in _mv/_validate request");
            }
        }
        return new MVValidateRequest(sourceIndex, descriptorJson, ppl, sql);
    }

    /** Re-serialize the current parser sub-object (positioned at START_OBJECT) as compact JSON. */
    static String captureObjectAsJson(XContentParser parser) throws IOException {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            builder.copyCurrentStructure(parser);
            return BytesReference.bytes(builder).utf8ToString();
        }
    }
}
