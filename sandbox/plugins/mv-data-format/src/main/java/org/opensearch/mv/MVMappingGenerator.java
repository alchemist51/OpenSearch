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
 * Generates OpenSearch index mappings from an {@link MVCompiledDefinition}.
 *
 * <p>The generated mapping uses ONLY the stable user-visible aliases defined
 * in the compiled definition. DataFusion internal names (e.g.
 * {@code count(Int64(1))[count]}, {@code sum(mv_input.x)[sum]}) are NEVER
 * exposed in the mapping.</p>
 *
 * <p>Group key columns and aggregate aliases are included as top-level
 * fields in the mapping's {@code properties} block. For AVG aggregates,
 * only the user alias ({@code avg_<field>}) appears in the mapping (typed
 * as {@code double}); the internal decomposition into count/sum state
 * columns is not visible to users.</p>
 */
public final class MVMappingGenerator {

    /**
     * Generate an OpenSearch mapping from the compiled definition.
     *
     * <p>Returns a structure suitable for use as the {@code "mappings"} value
     * in an index creation request:
     * <pre>
     * {
     *   "properties": {
     *     "RegionID":       { "type": "long" },
     *     "cnt":            { "type": "long" },
     *     "sum_AdvEngineID":{ "type": "long" },
     *     "avg_ResWidth":   { "type": "double" }
     *   }
     * }
     * </pre>
     *
     * @param definition the compiled MV definition
     * @return mapping structure as nested maps
     */
    public Map<String, Object> generateMapping(MVCompiledDefinition definition) {
        Map<String, Object> properties = new LinkedHashMap<>();

        // Group key columns
        for (GroupKey key : definition.groupKeys()) {
            Map<String, Object> fieldMapping = new LinkedHashMap<>();
            fieldMapping.put("type", key.columnType().osType());
            properties.put(key.name(), fieldMapping);
        }

        // Aggregate columns — user alias only, never DataFusion internals
        for (AggregateSpec agg : definition.aggregates()) {
            Map<String, Object> fieldMapping = new LinkedHashMap<>();
            fieldMapping.put("type", agg.targetMappingType());
            properties.put(agg.userAlias(), fieldMapping);
        }

        Map<String, Object> mapping = new LinkedHashMap<>();
        mapping.put("properties", properties);
        return mapping;
    }

    /**
     * Validate that an existing mapping is compatible with the definition.
     * Returns {@code true} if every field in the definition exists in the
     * mapping with the correct type.
     *
     * @param definition  the compiled MV definition
     * @param mapping     the existing index mapping (the {@code "mappings"} object)
     * @return true if compatible
     */
    @SuppressWarnings("unchecked")
    public boolean isCompatible(MVCompiledDefinition definition, Map<String, Object> mapping) {
        Object propsObj = mapping.get("properties");
        if (propsObj == null || !(propsObj instanceof Map)) {
            return false;
        }
        Map<String, Object> properties = (Map<String, Object>) propsObj;

        for (Map.Entry<String, String> entry : definition.targetMapping().entrySet()) {
            Object fieldObj = properties.get(entry.getKey());
            if (fieldObj == null || !(fieldObj instanceof Map)) {
                return false;
            }
            Map<String, Object> fieldMap = (Map<String, Object>) fieldObj;
            if (!entry.getValue().equals(fieldMap.get("type"))) {
                return false;
            }
        }
        return true;
    }
}
