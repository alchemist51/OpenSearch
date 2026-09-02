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
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentParser;

import java.io.IOException;

/**
 * Transport request for {@code PUT /_mv/views/{name}}. Carries the view name
 * (from the path), the source index, exactly one definition input (descriptor /
 * PPL / SQL), and optional overrides ({@code target_index}, {@code poll_interval}).
 *
 * <p>Body shape:
 * <pre>
 * {
 *   "source_index": "clickbench",
 *   "descriptor": { ... },        // OR "ppl" / "sql"
 *   "target_index": "clickbench_q9_mv",   // optional; default is the view name
 *   "poll_interval": "1s"                 // optional
 * }
 * </pre>
 */
public class MVCreateViewRequest extends ActionRequest {

    static final String F_SOURCE_INDEX = "source_index";
    static final String F_DESCRIPTOR = "descriptor";
    static final String F_PPL = "ppl";
    static final String F_SQL = "sql";
    static final String F_TARGET_INDEX = "target_index";
    static final String F_POLL_INTERVAL = "poll_interval";

    private final String name;
    private final String sourceIndex;
    private final String descriptorJson; // nullable
    private final String ppl;            // nullable
    private final String sql;            // nullable
    private final String targetIndex;    // nullable -> default
    private final String pollInterval;   // nullable

    public MVCreateViewRequest(
        String name,
        String sourceIndex,
        String descriptorJson,
        String ppl,
        String sql,
        String targetIndex,
        String pollInterval
    ) {
        this.name = name;
        this.sourceIndex = sourceIndex;
        this.descriptorJson = descriptorJson;
        this.ppl = ppl;
        this.sql = sql;
        this.targetIndex = targetIndex;
        this.pollInterval = pollInterval;
    }

    public MVCreateViewRequest(StreamInput in) throws IOException {
        super(in);
        this.name = in.readOptionalString();
        this.sourceIndex = in.readOptionalString();
        this.descriptorJson = in.readOptionalString();
        this.ppl = in.readOptionalString();
        this.sql = in.readOptionalString();
        this.targetIndex = in.readOptionalString();
        this.pollInterval = in.readOptionalString();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeOptionalString(name);
        out.writeOptionalString(sourceIndex);
        out.writeOptionalString(descriptorJson);
        out.writeOptionalString(ppl);
        out.writeOptionalString(sql);
        out.writeOptionalString(targetIndex);
        out.writeOptionalString(pollInterval);
    }

    public String name() {
        return name;
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

    public boolean hasQueryText() {
        return ppl != null || sql != null;
    }

    /**
     * The effective target index name. Defaults to the view {@code name} itself
     * (consistent with {@code GET}/{@code DELETE /_mv/views/{name}} and the
     * {@code definition:name} semantics of {@link MVViewsService}); an explicit
     * {@code target_index} overrides it.
     */
    public String resolvedTargetIndex() {
        if (targetIndex != null && targetIndex.isBlank() == false) {
            return targetIndex;
        }
        return name;
    }

    public String pollInterval() {
        return pollInterval;
    }

    @Override
    public ActionRequestValidationException validate() {
        ActionRequestValidationException e = null;
        if (name == null || name.isBlank()) {
            e = ValidateActions.addValidationError("view [name] is required", e);
        }
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

    /** Parse the request body, taking the view {@code name} from the REST path. */
    public static MVCreateViewRequest fromXContent(String name, XContentParser parser) throws IOException {
        String sourceIndex = null;
        String descriptorJson = null;
        String ppl = null;
        String sql = null;
        String targetIndex = null;
        String pollInterval = null;

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
                    descriptorJson = MVValidateRequest.captureObjectAsJson(parser);
                }
                case F_PPL -> ppl = parser.text();
                case F_SQL -> sql = parser.text();
                case F_TARGET_INDEX -> targetIndex = parser.text();
                case F_POLL_INTERVAL -> pollInterval = parser.text();
                default -> throw new IllegalArgumentException("unknown field [" + fieldName + "] in _mv/views create request");
            }
        }
        return new MVCreateViewRequest(name, sourceIndex, descriptorJson, ppl, sql, targetIndex, pollInterval);
    }
}
