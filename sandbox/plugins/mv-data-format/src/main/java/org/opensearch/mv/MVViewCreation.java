/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import org.opensearch.cluster.metadata.DerivedIndexBinding;
import org.opensearch.common.settings.Settings;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * The single, shared assembler that turns a compiled MV definition into the
 * exact public create-index settings and mapping of a derived
 * {@code materialized_view} target index.
 *
 * <p>Stage&nbsp;4's {@link MVViewsService.TargetCreator} (auto-creation from
 * {@code index.mv.views} on the source) and Stage&nbsp;5's
 * {@code PUT /_mv/views/{name}} REST create path both go through here, so the
 * two creation entry points produce a byte-identical target contract — the
 * same public settings, the same mapping, the same fail-closed
 * {@link MVDefinitionResolver#validateCreation} gate — and can never drift.
 *
 * <h2>The target contract (unchanged from Stage&nbsp;4)</h2>
 * <ul>
 *   <li>{@code index.derived.source.name} = source index</li>
 *   <li>{@code index.number_of_shards} = source shard count (identity binding)</li>
 *   <li>{@code index.number_of_replicas} = 0</li>
 *   <li>{@code index.refresh_interval} = -1 (source-refresh-driven visibility)</li>
 *   <li>{@code index.derived.enabled} = true (first-class derived index)</li>
 *   <li>{@code index.append_only.enabled} = true</li>
 *   <li>{@code index.pluggable.dataformat.enabled} = true,
 *       {@code index.pluggable.dataformat} = composite,
 *       {@code index.composite.primary_data_format} = parquet,
 *       {@code index.composite.secondary_data_formats} = [lucene]</li>
 *   <li>{@code index.derived.data_format} = {@code materialized_view}
 *       (the canonical derived category)</li>
 *   <li>{@code index.mv.descriptor} = the serialized, self-contained descriptor JSON</li>
 *   <li>{@code index.mv.state_fields} = the compiled state-column names</li>
 *   <li>{@code index.mv.colocate_with} = source index</li>
 * </ul>
 *
 * <p>Row/column ordering, the descriptor's integrity hash, and the mapping are
 * all recomputed from the one {@link MVCompiledDefinition}, so definition,
 * mapping and {@code state_fields} agree by construction.
 */
public final class MVViewCreation {

    private MVViewCreation() {}

    /**
     * Build the public create-index settings for a descriptor-driven MV target.
     * The persisted descriptor is authoritative at runtime
     * ({@link MVDefinitionResolver#resolve} reads it first), so no legacy
     * {@code index.mv.definition} / {@code index.derived.definition_id} name is
     * set — the target is self-contained across restarts.
     *
     * @param sourceIndex   the source index name
     * @param sourceShards  the source shard count (identity-bound)
     * @param compiledDef   the compiled definition (single source of mapping,
     *                      state fields, and descriptor)
     * @param descriptorJson the serialized descriptor JSON
     *                       ({@link MVDefinitionResolver#serialize})
     */
    public static Settings buildTargetSettings(
        String sourceIndex,
        int sourceShards,
        MVCompiledDefinition compiledDef,
        String descriptorJson
    ) {
        return commonTargetSettings(sourceIndex, sourceShards).put(MVConstants.DESCRIPTOR_SETTING, descriptorJson)
            .putList(MVConstants.STATE_FIELDS_SETTING, compiledDef.stateColumnNames())
            .build();
    }

    /**
     * The common target settings shared by the descriptor-driven REST path and
     * the legacy named-definition auto-creation path. Callers add the
     * descriptor / state_fields (and, for the legacy path, the definition name)
     * before {@code build()}.
     */
    static Settings.Builder commonTargetSettings(String sourceIndex, int sourceShards) {
        return Settings.builder()
            .put(DerivedIndexBinding.KEY_SOURCE_NAME, sourceIndex)
            .put("index.number_of_shards", sourceShards)
            .put("index.number_of_replicas", 0)
            // Target search visibility is source-refresh-driven.
            .put("index.refresh_interval", "-1")
            // First-class derived index: replication-owned writes only.
            .put(MVConstants.DERIVED_INDEX_SETTING, true)
            .put("index.append_only.enabled", true)
            .put("index.pluggable.dataformat.enabled", true)
            .put("index.pluggable.dataformat", "composite")
            // Parquet stores replicated rows; Lucene is a query-capability projection.
            .put("index.composite.primary_data_format", "parquet")
            .putList("index.composite.secondary_data_formats", "lucene")
            // Canonical DERIVED DATA-FORMAT CATEGORY.
            .put(DerivedIndexBinding.KEY_DATA_FORMAT, MVDataFormat.NAME)
            .put(MVConstants.COLOCATE_WITH_SETTING, sourceIndex);
    }

    /**
     * Derived target mapping generated from a compiled {@link MVCompiledDefinition}
     * via {@link MVMappingGenerator}. Uses stable user-visible aliases (never
     * DataFusion internal names), adds the hidden {@code _mv_source_generation}
     * provenance field, disables {@code _field_names}, and pins
     * {@code dynamic:false} (the composite apply path cannot do dynamic mapping
     * updates).
     *
     * <p>This is the single mapping serializer; {@link MVViewsService.TargetCreator}
     * delegates here so the auto-creation and REST create paths emit an
     * identical mapping.
     */
    public static String targetMapping(MVCompiledDefinition compiledDef) {
        MVMappingGenerator generator = new MVMappingGenerator();
        Map<String, Object> mapping = generator.generateMapping(compiledDef);

        @SuppressWarnings("unchecked")
        Map<String, Object> properties = (Map<String, Object>) mapping.get("properties");
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

    /** Default target index name for a view, mirroring {@code <source>_mv_<name>}. */
    public static String defaultTargetIndex(String sourceIndex, String viewName) {
        return String.format(Locale.ROOT, "%s_mv_%s", sourceIndex, viewName);
    }

    /** Convenience: the ordered state-column (state_fields) list for a definition. */
    public static List<String> stateFields(MVCompiledDefinition compiledDef) {
        return compiledDef.stateColumnNames();
    }
}
