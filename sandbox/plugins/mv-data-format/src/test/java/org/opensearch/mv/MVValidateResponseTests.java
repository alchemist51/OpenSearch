/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.mv.MVDefinitionValidator.StateField;
import org.opensearch.test.OpenSearchTestCase;

import java.util.List;

/** Unit tests for {@link MVValidateResponse} rendering, status mapping, and serialization. */
public class MVValidateResponseTests extends OpenSearchTestCase {

    private static MVCompiledDefinition def() {
        return MVCompiledDefinition.compiledFor("clickbench_100m");
    }

    private static String toJson(MVValidateResponse resp) throws Exception {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            resp.toXContent(builder, ToXContent.EMPTY_PARAMS);
            return BytesReference.bytes(builder).utf8ToString();
        }
    }

    public void testValidResponseRendersAndStatusOk() throws Exception {
        MVCompiledDefinition def = def();
        MVValidateResponse resp = MVValidateResponse.valid(
            MVDefinitionResolver.serialize(def.toDescriptor()),
            def.stateColumnNames(),
            List.of(new StateField("EventTime", "int64"), new StateField("cnt", "int64")),
            def.targetMapping(),
            TransportMVValidateAction.orderingOf(def),
            0xABCDL
        );
        assertEquals(RestStatus.OK, resp.status());
        String json = toJson(resp);
        assertTrue(json.contains("\"valid\":true"));
        assertTrue(json.contains("\"descriptor\""));
        assertTrue(json.contains("\"native_state_fields\""));
        assertTrue(json.contains("\"target_mapping\""));
        assertTrue(json.contains("\"ordering\""));
        assertTrue(json.contains("\"arrow_type\":\"int64\""));
        assertTrue(json.contains("0x000000000000abcd"));
        // ordering carries all group keys, ASC NULLS FIRST
        assertTrue(json.contains("\"direction\":\"ASC\""));
        assertTrue(json.contains("\"nulls\":\"NULLS FIRST\""));
    }

    public void testRejectedResponseRendersAndStatusBadRequest() throws Exception {
        MVValidateResponse resp = MVValidateResponse.rejected(
            MVValidationReasons.SCHEMA_MISMATCH,
            "field [x] type family mismatch",
            List.of("field [x] type family mismatch", "arity mismatch")
        );
        assertEquals(RestStatus.BAD_REQUEST, resp.status());
        String json = toJson(resp);
        assertTrue(json.contains("\"valid\":false"));
        assertTrue(json.contains("\"reason_code\":\"SCHEMA_MISMATCH\""));
        assertTrue(json.contains("\"mismatches\""));
        assertFalse(json.contains("\"descriptor\""));
    }

    public void testWireRoundTripValid() throws Exception {
        MVCompiledDefinition def = def();
        MVValidateResponse resp = MVValidateResponse.valid(
            MVDefinitionResolver.serialize(def.toDescriptor()),
            def.stateColumnNames(),
            List.of(new StateField("EventTime", "int64")),
            def.targetMapping(),
            TransportMVValidateAction.orderingOf(def),
            42L
        );
        MVValidateResponse copy = roundTrip(resp);
        assertTrue(copy.isValid());
        assertEquals(resp.stateFields(), copy.stateFields());
        assertEquals(resp.targetMapping(), copy.targetMapping());
        assertEquals(resp.ordering().size(), copy.ordering().size());
        assertEquals(1, copy.nativeStateFields().size());
    }

    public void testWireRoundTripRejected() throws Exception {
        MVValidateResponse resp = MVValidateResponse.rejected("NATIVE_VALIDATION_REJECTED", "boom", List.of("boom"));
        MVValidateResponse copy = roundTrip(resp);
        assertFalse(copy.isValid());
        assertEquals("NATIVE_VALIDATION_REJECTED", copy.reasonCode());
        assertEquals(List.of("boom"), copy.mismatches());
    }

    private static MVValidateResponse roundTrip(MVValidateResponse resp) throws Exception {
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            resp.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                return new MVValidateResponse(in);
            }
        }
    }
}
