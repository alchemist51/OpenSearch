/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import org.opensearch.test.OpenSearchTestCase;

import java.util.LinkedHashMap;
import java.util.Map;

/** Unit tests for {@link MVSourceMappingReader#osTypes}. */
public class MVSourceMappingReaderTests extends OpenSearchTestCase {

    private static Map<String, Object> field(String type) {
        Map<String, Object> m = new LinkedHashMap<>();
        m.put("type", type);
        return m;
    }

    public void testFlatProperties() {
        Map<String, Object> props = new LinkedHashMap<>();
        props.put("EventTime", field("date"));
        props.put("URL", field("keyword"));
        props.put("RegionID", field("long"));
        Map<String, Object> root = Map.of("properties", props);

        Map<String, String> types = MVSourceMappingReader.osTypes(root);
        assertEquals("date", types.get("EventTime"));
        assertEquals("keyword", types.get("URL"));
        assertEquals("long", types.get("RegionID"));
        assertEquals(3, types.size());
    }

    public void testUnwrapsDocContainer() {
        Map<String, Object> props = new LinkedHashMap<>();
        props.put("x", field("integer"));
        Map<String, Object> doc = Map.of("properties", props);
        Map<String, Object> root = Map.of("_doc", doc);

        Map<String, String> types = MVSourceMappingReader.osTypes(root);
        assertEquals("integer", types.get("x"));
        assertEquals(1, types.size());
    }

    public void testNestedObjectDottedPaths() {
        Map<String, Object> inner = new LinkedHashMap<>();
        inner.put("code", field("integer"));
        Map<String, Object> http = new LinkedHashMap<>();
        http.put("properties", inner);
        Map<String, Object> props = new LinkedHashMap<>();
        props.put("http", http);
        Map<String, Object> root = Map.of("properties", props);

        Map<String, String> types = MVSourceMappingReader.osTypes(root);
        assertEquals("integer", types.get("http.code"));
    }

    public void testMultiFieldsNotDescended() {
        // A keyword with a text sub-field under `fields` records only the parent leaf.
        Map<String, Object> fields = new LinkedHashMap<>();
        fields.put("text", field("text"));
        Map<String, Object> url = field("keyword");
        url.put("fields", fields);
        Map<String, Object> props = new LinkedHashMap<>();
        props.put("URL", url);
        Map<String, Object> root = Map.of("properties", props);

        Map<String, String> types = MVSourceMappingReader.osTypes(root);
        assertEquals("keyword", types.get("URL"));
        assertFalse(types.containsKey("URL.text"));
        assertEquals(1, types.size());
    }

    public void testEmpty() {
        assertTrue(MVSourceMappingReader.osTypes(null).isEmpty());
        assertTrue(MVSourceMappingReader.osTypes(Map.of()).isEmpty());
    }
}
