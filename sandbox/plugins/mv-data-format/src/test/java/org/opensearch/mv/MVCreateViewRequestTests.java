/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.mv.pull.MVPullSettings;
import org.opensearch.test.OpenSearchTestCase;

/** Unit tests for {@link MVCreateViewRequest} and {@link TransportMVCreateViewAction#buildSettings}. */
public class MVCreateViewRequestTests extends OpenSearchTestCase {

    private static String descriptorJson() {
        return MVDefinitionResolver.serialize(MVCompiledDefinition.compiledFor("clickbench_100m").toDescriptor());
    }

    public void testFromXContentFull() throws Exception {
        String body = "{\"source_index\":\"clickbench\",\"descriptor\":"
            + descriptorJson()
            + ",\"target_index\":\"cb_q9\",\"poll_interval\":\"1s\"}";
        try (XContentParser p = createParser(JsonXContent.jsonXContent, body)) {
            MVCreateViewRequest req = MVCreateViewRequest.fromXContent("q9", p);
            assertEquals("q9", req.name());
            assertEquals("clickbench", req.sourceIndex());
            assertNotNull(req.descriptorJson());
            assertEquals("cb_q9", req.resolvedTargetIndex());
            assertEquals("1s", req.pollInterval());
            assertNull(req.validate());
        }
    }

    public void testResolvedTargetIndexDefaultsToName() throws Exception {
        String body = "{\"source_index\":\"clickbench\",\"descriptor\":" + descriptorJson() + "}";
        try (XContentParser p = createParser(JsonXContent.jsonXContent, body)) {
            MVCreateViewRequest req = MVCreateViewRequest.fromXContent("clickbench_q9", p);
            assertEquals("clickbench_q9", req.resolvedTargetIndex());
        }
    }

    public void testValidateErrors() {
        assertNotNull(new MVCreateViewRequest(null, "s", "{}", null, null, null, null).validate());
        assertNotNull(new MVCreateViewRequest("n", null, "{}", null, null, null, null).validate());
        assertNotNull(new MVCreateViewRequest("n", "s", null, null, null, null, null).validate());
        assertNotNull(new MVCreateViewRequest("n", "s", "{}", "ppl", null, null, null).validate());
        assertNull(new MVCreateViewRequest("n", "s", "{}", null, null, null, null).validate());
    }

    public void testWireRoundTrip() throws Exception {
        MVCreateViewRequest req = new MVCreateViewRequest("q9", "clickbench", descriptorJson(), null, null, "cb_q9", "2s");
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            req.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                MVCreateViewRequest copy = new MVCreateViewRequest(in);
                assertEquals("q9", copy.name());
                assertEquals("clickbench", copy.sourceIndex());
                assertEquals("cb_q9", copy.resolvedTargetIndex());
                assertEquals("2s", copy.pollInterval());
                assertEquals(req.descriptorJson(), copy.descriptorJson());
            }
        }
    }

    public void testBuildSettingsAppliesPollIntervalOverride() {
        MVCompiledDefinition def = MVCompiledDefinition.compiledFor("clickbench_100m");
        String descriptorJson = MVDefinitionResolver.serialize(def.toDescriptor());
        MVCreateViewRequest req = new MVCreateViewRequest("q9", "clickbench", descriptorJson, null, null, "cb_q9", "5s");
        Settings s = TransportMVCreateViewAction.buildSettings(req, 2, def, descriptorJson);
        assertEquals("5s", s.get(MVPullSettings.PULL_INTERVAL.getKey()));
        assertEquals("2", s.get("index.number_of_shards"));
    }

    public void testBuildSettingsWithoutPollIntervalOmitsKey() {
        MVCompiledDefinition def = MVCompiledDefinition.compiledFor("clickbench_100m");
        String descriptorJson = MVDefinitionResolver.serialize(def.toDescriptor());
        MVCreateViewRequest req = new MVCreateViewRequest("q9", "clickbench", descriptorJson, null, null, null, null);
        Settings s = TransportMVCreateViewAction.buildSettings(req, 1, def, descriptorJson);
        assertNull(s.get(MVPullSettings.PULL_INTERVAL.getKey()));
    }
}
