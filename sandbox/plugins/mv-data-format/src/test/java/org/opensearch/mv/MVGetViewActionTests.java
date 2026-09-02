/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import org.opensearch.Version;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.OpenSearchTestCase;

/** Unit tests for {@link TransportMVGetViewAction#describe} over cluster-state index metadata. */
public class MVGetViewActionTests extends OpenSearchTestCase {

    private static IndexMetadata mvTarget(String name) {
        MVCompiledDefinition def = MVCompiledDefinition.compiledFor("clickbench_100m");
        String json = MVDefinitionResolver.serialize(def.toDescriptor());
        Settings settings = Settings.builder()
            .put(MVViewCreation.buildTargetSettings("clickbench", 2, def, json))
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .build();
        return IndexMetadata.builder(name).settings(settings).build();
    }

    private static String toJson(MVGetViewResponse resp) throws Exception {
        try (XContentBuilder builder = XContentFactory.jsonBuilder()) {
            resp.toXContent(builder, ToXContent.EMPTY_PARAMS);
            return BytesReference.bytes(builder).utf8ToString();
        }
    }

    public void testDescribeMvTarget() throws Exception {
        MVGetViewResponse resp = TransportMVGetViewAction.describe("cb_mv", mvTarget("cb_mv"));
        assertTrue(resp.isFound());
        assertEquals(RestStatus.OK, resp.status());
        String json = toJson(resp);
        assertTrue(json.contains("\"found\":true"));
        assertTrue(json.contains("\"source_index\":\"clickbench\""));
        assertTrue(json.contains("\"data_format\":\"" + MVDataFormat.NAME + "\""));
        assertTrue(json.contains("\"descriptor_present\":true"));
        assertTrue(json.contains("\"definition\":\"descriptor\""));
        assertTrue(json.contains("\"group_keys\""));
        assertTrue(json.contains("\"state_fields\""));
    }

    public void testDescribeMissingIsNotFound() {
        MVGetViewResponse resp = TransportMVGetViewAction.describe("missing", null);
        assertFalse(resp.isFound());
        assertEquals(RestStatus.NOT_FOUND, resp.status());
    }

    public void testDescribeNonMvIndexIsNotFound() {
        Settings settings = Settings.builder()
            .put("index.number_of_shards", 1)
            .put("index.number_of_replicas", 0)
            .put(IndexMetadata.SETTING_VERSION_CREATED, Version.CURRENT)
            .build();
        IndexMetadata plain = IndexMetadata.builder("plain").settings(settings).build();
        MVGetViewResponse resp = TransportMVGetViewAction.describe("plain", plain);
        assertFalse(resp.isFound());
    }
}
