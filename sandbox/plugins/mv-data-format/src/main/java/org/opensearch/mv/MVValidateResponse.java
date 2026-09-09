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
import org.opensearch.mv.MVDefinitionValidator.StateField;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Response for {@code POST /_mv/_validate}.
 *
 * <p>On success ({@link #valid} == true) the response carries the canonical,
 * self-contained descriptor JSON (with its integrity hash), the compiled
 * {@code state_fields}, the physical {@code native_state_fields} the engine
 * would produce, the derived {@code target_mapping}, and the row {@code ordering}
 * contract. On failure it carries a machine-readable {@link #reasonCode}, a
 * human message, and any schema {@link #mismatches}.
 *
 * <p>Implements {@link StatusToXContentObject} so {@code RestToXContentListener}
 * returns {@code 200} for a valid definition and {@code 400} for a rejected one.
 */
public class MVValidateResponse extends ActionResponse implements StatusToXContentObject {

    /** One row-ordering key in the response. */
    public record OrderKey(String column, int stateFieldIndex, String direction, String nulls) {}

    private final boolean valid;

    // Success payload (null/empty on failure).
    private final String descriptorJson;
    private final List<String> stateFields;
    private final List<StateField> nativeStateFields;
    private final Map<String, String> targetMapping;
    private final List<OrderKey> ordering;
    private final long nativeSchemaHash;

    // Failure payload (null/empty on success).
    private final String reasonCode;
    private final String message;
    private final List<String> mismatches;

    private MVValidateResponse(
        boolean valid,
        String descriptorJson,
        List<String> stateFields,
        List<StateField> nativeStateFields,
        Map<String, String> targetMapping,
        List<OrderKey> ordering,
        long nativeSchemaHash,
        String reasonCode,
        String message,
        List<String> mismatches
    ) {
        this.valid = valid;
        this.descriptorJson = descriptorJson;
        this.stateFields = stateFields == null ? List.of() : List.copyOf(stateFields);
        this.nativeStateFields = nativeStateFields == null ? List.of() : List.copyOf(nativeStateFields);
        this.targetMapping = targetMapping == null ? Map.of() : new LinkedHashMap<>(targetMapping);
        this.ordering = ordering == null ? List.of() : List.copyOf(ordering);
        this.nativeSchemaHash = nativeSchemaHash;
        this.reasonCode = reasonCode;
        this.message = message;
        this.mismatches = mismatches == null ? List.of() : List.copyOf(mismatches);
    }

    /** Build a successful (valid) response. */
    public static MVValidateResponse valid(
        String descriptorJson,
        List<String> stateFields,
        List<StateField> nativeStateFields,
        Map<String, String> targetMapping,
        List<OrderKey> ordering,
        long nativeSchemaHash
    ) {
        return new MVValidateResponse(
            true,
            descriptorJson,
            stateFields,
            nativeStateFields,
            targetMapping,
            ordering,
            nativeSchemaHash,
            null,
            null,
            null
        );
    }

    /** Build a rejected (invalid) response. */
    public static MVValidateResponse rejected(String reasonCode, String message, List<String> mismatches) {
        return new MVValidateResponse(false, null, null, null, null, null, 0L, reasonCode, message, mismatches);
    }

    public MVValidateResponse(StreamInput in) throws IOException {
        this.valid = in.readBoolean();
        this.descriptorJson = in.readOptionalString();
        this.stateFields = in.readStringList();
        int nsf = in.readVInt();
        List<StateField> fields = new ArrayList<>(nsf);
        for (int i = 0; i < nsf; i++) {
            fields.add(new StateField(in.readString(), in.readString()));
        }
        this.nativeStateFields = fields;
        int tm = in.readVInt();
        Map<String, String> mapping = new LinkedHashMap<>();
        for (int i = 0; i < tm; i++) {
            mapping.put(in.readString(), in.readString());
        }
        this.targetMapping = mapping;
        int ord = in.readVInt();
        List<OrderKey> order = new ArrayList<>(ord);
        for (int i = 0; i < ord; i++) {
            order.add(new OrderKey(in.readString(), in.readVInt(), in.readString(), in.readString()));
        }
        this.ordering = order;
        this.nativeSchemaHash = in.readLong();
        this.reasonCode = in.readOptionalString();
        this.message = in.readOptionalString();
        this.mismatches = in.readStringList();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeBoolean(valid);
        out.writeOptionalString(descriptorJson);
        out.writeStringCollection(stateFields);
        out.writeVInt(nativeStateFields.size());
        for (StateField f : nativeStateFields) {
            out.writeString(f.name());
            out.writeString(f.arrowToken());
        }
        out.writeVInt(targetMapping.size());
        for (Map.Entry<String, String> e : targetMapping.entrySet()) {
            out.writeString(e.getKey());
            out.writeString(e.getValue());
        }
        out.writeVInt(ordering.size());
        for (OrderKey k : ordering) {
            out.writeString(k.column());
            out.writeVInt(k.stateFieldIndex());
            out.writeString(k.direction());
            out.writeString(k.nulls());
        }
        out.writeLong(nativeSchemaHash);
        out.writeOptionalString(reasonCode);
        out.writeOptionalString(message);
        out.writeStringCollection(mismatches);
    }

    public boolean isValid() {
        return valid;
    }

    public String reasonCode() {
        return reasonCode;
    }

    public String message() {
        return message;
    }

    public List<String> mismatches() {
        return mismatches;
    }

    public List<StateField> nativeStateFields() {
        return nativeStateFields;
    }

    public Map<String, String> targetMapping() {
        return targetMapping;
    }

    public List<OrderKey> ordering() {
        return ordering;
    }

    public List<String> stateFields() {
        return stateFields;
    }

    @Override
    public RestStatus status() {
        return valid ? RestStatus.OK : RestStatus.BAD_REQUEST;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        builder.field("valid", valid);
        if (valid) {
            if (descriptorJson != null) {
                builder.field("descriptor");
                embedJson(builder, descriptorJson);
            }
            builder.field("state_fields", stateFields);
            builder.startArray("native_state_fields");
            for (StateField f : nativeStateFields) {
                builder.startObject().field("name", f.name()).field("arrow_type", f.arrowToken()).endObject();
            }
            builder.endArray();
            builder.startObject("target_mapping");
            for (Map.Entry<String, String> e : targetMapping.entrySet()) {
                builder.field(e.getKey(), e.getValue());
            }
            builder.endObject();
            builder.startArray("ordering");
            for (OrderKey k : ordering) {
                builder.startObject()
                    .field("column", k.column())
                    .field("state_field_index", k.stateFieldIndex())
                    .field("direction", k.direction())
                    .field("nulls", k.nulls())
                    .endObject();
            }
            builder.endArray();
            builder.field("native_schema_hash", String.format(Locale.ROOT, "0x%016x", nativeSchemaHash));
        } else {
            builder.field("reason_code", reasonCode);
            builder.field("message", message);
            if (mismatches.isEmpty() == false) {
                builder.field("mismatches", mismatches);
            }
        }
        builder.endObject();
        return builder;
    }

    /** Embed a raw JSON object string as a structured sub-object in the builder. */
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
