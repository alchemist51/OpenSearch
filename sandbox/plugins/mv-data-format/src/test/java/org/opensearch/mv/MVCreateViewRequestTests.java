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
        // a direct-write target carries no builder-emulation keys at all
        assertNull(s.get(MVPullSettings.PULL_MODE.getKey()));
        assertNull(s.get(MVPullSettings.BUILDER_VIEW.getKey()));
    }

    // ── Builder-shard emulation: builder_view ────────────────────────────────

    public void testFromXContentBuilderView() throws Exception {
        String body = "{\"source_index\":\"clickbench\",\"descriptor\":" + descriptorJson() + ",\"builder_view\":\"cb_q9\"}";
        try (XContentParser p = createParser(JsonXContent.jsonXContent, body)) {
            MVCreateViewRequest req = MVCreateViewRequest.fromXContent("cb_q9_follower", p);
            assertEquals("cb_q9", req.builderView());
            assertNull(req.validate());
        }
    }

    public void testWireRoundTripCarriesBuilderView() throws Exception {
        MVCreateViewRequest req = new MVCreateViewRequest("f1", "clickbench", descriptorJson(), null, null, null, null, "cb_q9");
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            req.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                MVCreateViewRequest copy = new MVCreateViewRequest(in);
                assertEquals("cb_q9", copy.builderView());
                assertEquals("f1", copy.resolvedTargetIndex());
            }
        }
        // the 7-arg constructor means "no leader"
        assertNull(new MVCreateViewRequest("f1", "clickbench", descriptorJson(), null, null, null, null).builderView());
    }

    public void testBuildSettingsStampsHydrateModeForFollowers() {
        MVCompiledDefinition def = MVCompiledDefinition.compiledFor("clickbench_100m");
        String descriptorJson = MVDefinitionResolver.serialize(def.toDescriptor());
        MVCreateViewRequest req = new MVCreateViewRequest("f1", "clickbench", descriptorJson, null, null, null, "1s", "cb_q9");
        Settings s = TransportMVCreateViewAction.buildSettings(req, 3, def, descriptorJson);
        assertEquals(MVPullSettings.MODE_HYDRATE, s.get(MVPullSettings.PULL_MODE.getKey()));
        assertEquals("cb_q9", s.get(MVPullSettings.BUILDER_VIEW.getKey()));
        assertEquals("1s", s.get(MVPullSettings.PULL_INTERVAL.getKey()));
        assertEquals("3", s.get("index.number_of_shards"));
        // the setting itself accepts both modes and nothing else
        assertEquals(MVPullSettings.MODE_HYDRATE, MVPullSettings.PULL_MODE.get(s));
        assertEquals(MVPullSettings.MODE_BUILD, MVPullSettings.PULL_MODE.get(Settings.EMPTY));
        expectThrows(
            IllegalArgumentException.class,
            () -> MVPullSettings.PULL_MODE.get(Settings.builder().put(MVPullSettings.PULL_MODE.getKey(), "push").build())
        );
    }

    public void testHydrateTransportParsesAndDefaultsToPushWithRecoveryPoll() throws Exception {
        MVCompiledDefinition def = MVCompiledDefinition.compiledFor("clickbench_100m");
        String descriptorJson = MVDefinitionResolver.serialize(def.toDescriptor());
        // default: push + 10s recovery poll
        MVCreateViewRequest pushed = new MVCreateViewRequest("f1", "clickbench", descriptorJson, null, null, null, null, "cb_q9");
        Settings s1 = TransportMVCreateViewAction.buildSettings(pushed, 1, def, descriptorJson);
        assertEquals(MVPullSettings.TRANSPORT_PUSH, s1.get(MVPullSettings.HYDRATE_TRANSPORT.getKey()));
        assertEquals("10s", s1.get(MVPullSettings.PULL_INTERVAL.getKey()));
        // explicit poll keeps the default cadence
        String body = "{\"source_index\":\"clickbench\",\"descriptor\":"
            + descriptorJson
            + ",\"builder_view\":\"cb_q9\",\"hydrate_transport\":\"poll\"}";
        try (XContentParser p = createParser(JsonXContent.jsonXContent, body)) {
            MVCreateViewRequest polled = MVCreateViewRequest.fromXContent("f2", p);
            assertEquals("poll", polled.hydrateTransport());
            Settings s2 = TransportMVCreateViewAction.buildSettings(polled, 1, def, descriptorJson);
            assertEquals(MVPullSettings.TRANSPORT_POLL, s2.get(MVPullSettings.HYDRATE_TRANSPORT.getKey()));
            assertNull(s2.get(MVPullSettings.PULL_INTERVAL.getKey()));
        }
        // an explicit poll_interval always wins
        MVCreateViewRequest explicit = new MVCreateViewRequest(
            "f3",
            "clickbench",
            descriptorJson,
            null,
            null,
            null,
            "500ms",
            "cb_q9",
            "push"
        );
        assertEquals(
            "500ms",
            TransportMVCreateViewAction.buildSettings(explicit, 1, def, descriptorJson).get(MVPullSettings.PULL_INTERVAL.getKey())
        );
        // the setting rejects anything but poll|push; direct-write targets carry no transport key
        expectThrows(
            IllegalArgumentException.class,
            () -> MVPullSettings.HYDRATE_TRANSPORT.get(Settings.builder().put(MVPullSettings.HYDRATE_TRANSPORT.getKey(), "smoke").build())
        );
        assertNull(
            TransportMVCreateViewAction.buildSettings(
                new MVCreateViewRequest("q9", "clickbench", descriptorJson, null, null, null, null),
                1,
                def,
                descriptorJson
            ).get(MVPullSettings.HYDRATE_TRANSPORT.getKey())
        );
    }

    public void testLeaderFanoutOptionsParseAndLandInSettings() throws Exception {
        MVCompiledDefinition def = MVCompiledDefinition.compiledFor("clickbench_100m");
        String descriptorJson = MVDefinitionResolver.serialize(def.toDescriptor());
        String body = "{\"source_index\":\"clickbench\",\"descriptor\":"
            + descriptorJson
            + ",\"fanout_concurrency\":4,\"fanout_async\":true}";
        try (XContentParser p = createParser(JsonXContent.jsonXContent, body)) {
            MVCreateViewRequest leader = MVCreateViewRequest.fromXContent("cb_q9", p);
            assertEquals(Integer.valueOf(4), leader.fanoutConcurrency());
            assertEquals(Boolean.TRUE, leader.fanoutAsync());
            Settings s = TransportMVCreateViewAction.buildSettings(leader, 1, def, descriptorJson);
            assertEquals(Integer.valueOf(4), MVPullSettings.FANOUT_CONCURRENCY.get(s));
            assertTrue(MVPullSettings.FANOUT_ASYNC.get(s));
            try (BytesStreamOutput out = new BytesStreamOutput()) {
                leader.writeTo(out);
                try (StreamInput in = out.bytes().streamInput()) {
                    MVCreateViewRequest copy = new MVCreateViewRequest(in);
                    assertEquals(Integer.valueOf(4), copy.fanoutConcurrency());
                    assertEquals(Boolean.TRUE, copy.fanoutAsync());
                }
            }
        }
        // absent -> setting defaults (1, false), and no keys stamped
        Settings d = TransportMVCreateViewAction.buildSettings(
            new MVCreateViewRequest("q9", "clickbench", descriptorJson, null, null, null, null),
            1,
            def,
            descriptorJson
        );
        assertNull(d.get(MVPullSettings.FANOUT_CONCURRENCY.getKey()));
        assertEquals(Integer.valueOf(1), MVPullSettings.FANOUT_CONCURRENCY.get(d));
        assertFalse(MVPullSettings.FANOUT_ASYNC.get(d));
        assertEquals("cheapest_first", MVPullSettings.FANOUT_ORDER.get(d));
    }

    public void testValidateBuilderViewRequiresExistingBuildModeLeaderOnSameSource() {
        String descriptorJson = descriptorJson();
        Settings leaderSettings = Settings.builder()
            .put(org.opensearch.cluster.metadata.DerivedIndexBinding.KEY_SOURCE_NAME, "clickbench")
            .put("index.version.created", org.opensearch.Version.CURRENT)
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .build();
        Settings followerLeaderSettings = Settings.builder()
            .put(leaderSettings)
            .put(MVPullSettings.PULL_MODE.getKey(), MVPullSettings.MODE_HYDRATE)
            .put(MVPullSettings.BUILDER_VIEW.getKey(), "cb_q9")
            .build();
        org.opensearch.cluster.metadata.Metadata metadata = org.opensearch.cluster.metadata.Metadata.builder()
            .put(org.opensearch.cluster.metadata.IndexMetadata.builder("cb_q9").settings(leaderSettings))
            .put(org.opensearch.cluster.metadata.IndexMetadata.builder("cb_f0").settings(followerLeaderSettings))
            .build();

        // no builder_view: nothing to validate
        TransportMVCreateViewAction.validateBuilderView(
            new MVCreateViewRequest("f1", "clickbench", descriptorJson, null, null, null, null),
            metadata
        );
        // happy path: existing build-mode leader on the same source
        TransportMVCreateViewAction.validateBuilderView(
            new MVCreateViewRequest("f1", "clickbench", descriptorJson, null, null, null, null, "cb_q9"),
            metadata
        );
        // missing leader
        assertTrue(
            expectThrows(
                IllegalArgumentException.class,
                () -> TransportMVCreateViewAction.validateBuilderView(
                    new MVCreateViewRequest("f1", "clickbench", descriptorJson, null, null, null, null, "nope"),
                    metadata
                )
            ).getMessage().contains("does not exist")
        );
        // leader bound to another source
        assertTrue(
            expectThrows(
                IllegalArgumentException.class,
                () -> TransportMVCreateViewAction.validateBuilderView(
                    new MVCreateViewRequest("f1", "other_source", descriptorJson, null, null, null, null, "cb_q9"),
                    metadata
                )
            ).getMessage().contains("bound to source")
        );
        // leader that is itself a follower
        assertTrue(
            expectThrows(
                IllegalArgumentException.class,
                () -> TransportMVCreateViewAction.validateBuilderView(
                    new MVCreateViewRequest("f1", "clickbench", descriptorJson, null, null, null, null, "cb_f0"),
                    metadata
                )
            ).getMessage().contains("itself a hydrating follower")
        );
    }
}
