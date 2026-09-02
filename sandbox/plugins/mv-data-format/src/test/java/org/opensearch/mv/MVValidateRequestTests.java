/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.test.OpenSearchTestCase;

/** Unit tests for {@link MVValidateRequest} parsing, validation, and serialization. */
public class MVValidateRequestTests extends OpenSearchTestCase {

    private static String descriptorJson() {
        return MVDefinitionResolver.serialize(MVCompiledDefinition.compiledFor("clickbench_100m").toDescriptor());
    }

    public void testFromXContentDescriptor() throws Exception {
        String body = "{\"source_index\":\"clickbench\",\"descriptor\":" + descriptorJson() + "}";
        try (XContentParser p = createParser(JsonXContent.jsonXContent, body)) {
            MVValidateRequest req = MVValidateRequest.fromXContent(p);
            assertEquals("clickbench", req.sourceIndex());
            assertNotNull(req.descriptorJson());
            assertTrue(req.descriptorJson().contains("group_keys"));
            assertNull(req.ppl());
            assertNull(req.sql());
            assertFalse(req.hasQueryText());
            assertNull(req.validate());
        }
    }

    public void testFromXContentPpl() throws Exception {
        String body = "{\"source_index\":\"clickbench\",\"ppl\":\"source=clickbench | stats count() by RegionID\"}";
        try (XContentParser p = createParser(JsonXContent.jsonXContent, body)) {
            MVValidateRequest req = MVValidateRequest.fromXContent(p);
            assertEquals("clickbench", req.sourceIndex());
            assertTrue(req.hasQueryText());
            assertNotNull(req.ppl());
            assertNull(req.validate());
        }
    }

    public void testValidateMissingSource() {
        MVValidateRequest req = new MVValidateRequest(null, "{}", null, null);
        assertNotNull(req.validate());
    }

    public void testValidateNoInput() {
        MVValidateRequest req = new MVValidateRequest("clickbench", null, null, null);
        assertNotNull(req.validate());
    }

    public void testValidateMultipleInputs() {
        MVValidateRequest req = new MVValidateRequest("clickbench", "{}", "source=x", null);
        assertNotNull(req.validate());
    }

    public void testUnknownFieldRejected() throws Exception {
        String body = "{\"source_index\":\"clickbench\",\"bogus\":true}";
        try (XContentParser p = createParser(JsonXContent.jsonXContent, body)) {
            expectThrows(IllegalArgumentException.class, () -> MVValidateRequest.fromXContent(p));
        }
    }

    public void testWireRoundTrip() throws Exception {
        MVValidateRequest req = new MVValidateRequest("clickbench", descriptorJson(), null, null);
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            req.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                MVValidateRequest copy = new MVValidateRequest(in);
                assertEquals(req.sourceIndex(), copy.sourceIndex());
                assertEquals(req.descriptorJson(), copy.descriptorJson());
                assertEquals(req.ppl(), copy.ppl());
                assertEquals(req.sql(), copy.sql());
            }
        }
    }
}
