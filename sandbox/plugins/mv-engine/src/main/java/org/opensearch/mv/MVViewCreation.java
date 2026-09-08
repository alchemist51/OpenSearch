/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import org.opensearch.common.settings.Settings;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Assembles the target index settings and mapping from a compiled definition.
 * Adapted from the POC MVViewCreation: DerivedIndexBinding references replaced
 * with inline setting keys from MVConstants.
 */
public final class MVViewCreation {

    private MVViewCreation() {}

    /**
     * Build the target index settings for a descriptor-driven MV target.
     */
    public static Settings buildTargetSettings(
        String sourceIndex,
        int sourceShards,
        MVCompiledDefinition compiledDef,
        String descriptorJson
    ) {
        return commonTargetSettings(sourceIndex, sourceShards)
            .put(MVConstants.DESCRIPTOR_SETTING, descriptorJson)
            .putList(MVConstants.STATE_FIELDS_SETTING, compiledDef.stateColumnNames())
            .build();
    }

    /**
     * Common target settings shared by all creation paths.
     */
    static Settings.Builder commonTargetSettings(String sourceIndex, int sourceShards) {
        return Settings.builder()
            .put(MVConstants.DERIVED_SOURCE_NAME_SETTING, sourceIndex)
            .put("index.number_of_shards", sourceShards)
            .put("index.number_of_replicas", 0)
            .put("index.refresh_interval", "-1")
            .put(MVConstants.DERIVED_INDEX_SETTING, true)
            .put("index.append_only.enabled", true)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats", "lucene")
            .put(MVConstants.DERIVED_DATA_FORMAT_SETTING, MVConstants.DATA_FORMAT_NAME)
            .put(MVConstants.STATE_MERGE_SETTING, true)
            .put(MVConstants.COLOCATE_WITH_SETTING, sourceIndex);
    }

    /**
     * Target mapping JSON generated from a compiled definition.
     */
    public static String targetMapping(MVCompiledDefinition compiledDef) {
        MVMappingGenerator generator = new MVMappingGenerator();
        Map<String, Object> mapping = generator.generateMapping(compiledDef);

        @SuppressWarnings("unchecked")
        Map<String, Object> properties = (Map<String, Object>) mapping.get("properties");

        // Hidden provenance field
        Map<String, Object> provenance = new LinkedHashMap<>();
        provenance.put("type", "long");
        provenance.put("index", false);
        properties.put("_mv_source_generation", provenance);

        StringBuilder sb = new StringBuilder("{\"dynamic\":\"false\",\"_field_names\":{\"enabled\":false},\"properties\":{");
        boolean first = true;
        for (Map.Entry<String, Object> entry : properties.entrySet()) {
            if (first == false) {
                sb.append(",");
            }
            first = false;
            @SuppressWarnings("unchecked")
            Map<String, Object> fieldMap = (Map<String, Object>) entry.getValue();
            sb.append("\"").append(entry.getKey()).append("\":{");
            boolean firstField = true;
            for (Map.Entry<String, Object> fe : fieldMap.entrySet()) {
                if (firstField == false) {
                    sb.append(",");
                }
                firstField = false;
                sb.append("\"").append(fe.getKey()).append("\":");
                if (fe.getValue() instanceof Boolean) {
                    sb.append(fe.getValue());
                } else {
                    sb.append("\"").append(fe.getValue()).append("\"");
                }
            }
            sb.append("}");
        }
        sb.append("}}");
        return sb.toString();
    }

    /** Default target index name: {@code <source>_mv_<name>}. */
    public static String defaultTargetIndex(String sourceIndex, String viewName) {
        return String.format(Locale.ROOT, "%s_mv_%s", sourceIndex, viewName);
    }

    /** Convenience: state-column names list. */
    public static List<String> stateFields(MVCompiledDefinition compiledDef) {
        return compiledDef.stateColumnNames();
    }
}
