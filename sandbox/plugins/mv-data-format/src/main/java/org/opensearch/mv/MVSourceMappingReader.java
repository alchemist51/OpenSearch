/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Flattens an OpenSearch index mapping (the {@code _doc} {@code sourceAsMap()}
 * form) into an ordered {@code fieldPath -> mappingType} map suitable for
 * {@link MVDefinitionValidator#validate(MVDefinitionDescriptor, Map)}.
 *
 * <p>Only leaf fields with an explicit {@code type} are recorded. Object
 * sub-mappings ({@code properties}) are walked recursively and their leaves are
 * emitted with dotted paths (e.g. {@code http.status}). Multi-fields
 * ({@code fields}) are intentionally NOT descended — an MV definition groups /
 * aggregates over a mapped field, not its analyzer sub-fields — but the parent
 * leaf's own {@code type} is still recorded.
 *
 * <p>Fields whose OpenSearch type has no columnar Arrow representation are still
 * recorded here; {@link MVDefinitionValidator#buildSourceSchemaWire} drops them
 * from the native schema wire (so a definition referencing one fails closed with
 * an "unknown column" native rejection, which is the correct behavior).
 */
public final class MVSourceMappingReader {

    private MVSourceMappingReader() {}

    /**
     * @param sourceAsMap the mapping document as returned by
     *                    {@code MappingMetadata.sourceAsMap()} — either the full
     *                    {@code {"_doc": {"properties": {...}}}} form or the
     *                    already-unwrapped {@code {"properties": {...}}} form.
     * @return ordered fieldPath -> OpenSearch mapping type
     */
    @SuppressWarnings("unchecked")
    public static Map<String, String> osTypes(Map<String, Object> sourceAsMap) {
        Map<String, String> out = new LinkedHashMap<>();
        if (sourceAsMap == null || sourceAsMap.isEmpty()) {
            return out;
        }
        Map<String, Object> root = sourceAsMap;
        // Unwrap a single top-level type container (e.g. "_doc") if present.
        if (root.containsKey("properties") == false && root.size() == 1) {
            Object only = root.values().iterator().next();
            if (only instanceof Map) {
                root = (Map<String, Object>) only;
            }
        }
        Object props = root.get("properties");
        if (props instanceof Map) {
            walk("", (Map<String, Object>) props, out);
        }
        return out;
    }

    @SuppressWarnings("unchecked")
    private static void walk(String prefix, Map<String, Object> properties, Map<String, String> out) {
        for (Map.Entry<String, Object> e : properties.entrySet()) {
            if ((e.getValue() instanceof Map) == false) {
                continue;
            }
            Map<String, Object> field = (Map<String, Object>) e.getValue();
            String path = prefix.isEmpty() ? e.getKey() : prefix + "." + e.getKey();
            Object type = field.get("type");
            if (type instanceof String) {
                out.put(path, (String) type);
            }
            // Recurse into nested/object sub-properties (but never into `fields`).
            Object sub = field.get("properties");
            if (sub instanceof Map) {
                walk(path, (Map<String, Object>) sub, out);
            }
        }
    }
}
