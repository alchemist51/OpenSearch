/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.mapper.MappedFieldType;

import java.util.EnumSet;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;

/**
 * Resolves which data format handles which capabilities for each field type.
 * Uses primary-gets-priority strategy: if the primary format supports a capability
 * for a field type, it wins. Secondary formats only get capabilities the primary can't handle.
 */
@ExperimentalApi
public final class FieldAssignmentResolver {

    private static final Logger logger = LogManager.getLogger(FieldAssignmentResolver.class);

    private FieldAssignmentResolver() {}

    /**
     * Resolves field assignments for all mapped fields.
     *
     * @param registry       the field support registry with all format capabilities
     * @param roleMap        format → engine role mapping
     * @param fieldTypes     all mapped field types from the mapper service
     * @return per-format FieldAssignments
     */
    public static Map<DataFormat, FieldAssignments> resolve(
        FieldSupportRegistry registry,
        Map<DataFormat, EngineRole> roleMap,
        Iterable<MappedFieldType> fieldTypes
    ) {
        // Find primary format
        DataFormat primaryFormat = null;
        for (Map.Entry<DataFormat, EngineRole> entry : roleMap.entrySet()) {
            if (entry.getValue() == EngineRole.PRIMARY) {
                primaryFormat = entry.getKey();
                break;
            }
        }

        // Build per-format assignment maps
        Map<DataFormat, Map<String, Set<FieldCapability>>> perFormatMap = new HashMap<>();
        for (DataFormat format : roleMap.keySet()) {
            perFormatMap.put(format, new HashMap<>());
        }

        for (MappedFieldType fieldType : fieldTypes) {
            // Skip internal metadata fields (e.g. _id, _index, _source) — managed by the engine, not data format plugins
            if (fieldType.typeName().startsWith("_")) {
                continue;
            }
            String typeName = fieldType.typeName();
            resolveField(registry, roleMap, primaryFormat, perFormatMap, fieldType, typeName);
        }

        // Wrap into FieldAssignments
        Map<DataFormat, FieldAssignments> result = new HashMap<>();
        for (Map.Entry<DataFormat, Map<String, Set<FieldCapability>>> entry : perFormatMap.entrySet()) {
            result.put(entry.getKey(), new FieldAssignments(entry.getValue()));
            logger.info("[COMPOSITE_DEBUG] Field assignments for format [{}]:", entry.getKey().name());
            for (Map.Entry<String, Set<FieldCapability>> fieldEntry : entry.getValue().entrySet()) {
                logger.info("[COMPOSITE_DEBUG]   fieldType=[{}] -> capabilities={}", fieldEntry.getKey(), fieldEntry.getValue());
            }
        }
        return result;
    }

    private static void resolveField(
        FieldSupportRegistry registry,
        Map<DataFormat, EngineRole> roleMap,
        DataFormat primaryFormat,
        Map<DataFormat, Map<String, Set<FieldCapability>>> perFormatMap,
        MappedFieldType fieldType,
        String typeName
    ) {
        // Determine which capabilities are required by the mapping
        Set<FieldCapability> required = EnumSet.noneOf(FieldCapability.class);
        if (fieldType.isSearchable()) {
            required.add(FieldCapability.INDEX);
        }
        if (fieldType.hasDocValues()) {
            required.add(FieldCapability.DOC_VALUES);
        }
        if (fieldType.isStored()) {
            required.add(FieldCapability.STORE);
        }

        logger.info("[COMPOSITE_DEBUG] resolveField: field=[{}] type=[{}] required capabilities={} (isSearchable={}, hasDocValues={}, isStored={})",
            fieldType.name(), typeName, required, fieldType.isSearchable(), fieldType.hasDocValues(), fieldType.isStored());

        // For each required capability, assign to primary if it supports it, else to secondary
        for (FieldCapability cap : required) {
            boolean primaryHasCap = primaryFormat != null && registry.hasCapability(typeName, primaryFormat, cap);
            logger.info("[COMPOSITE_DEBUG]   capability [{}]: primary format [{}] hasCapability={}, registry capabilities for type={}",
                cap, primaryFormat != null ? primaryFormat.name() : "null", primaryHasCap,
                primaryFormat != null ? registry.getCapabilities(typeName, primaryFormat) : "N/A");

            if (primaryHasCap) {
                // Primary handles this capability
                perFormatMap.get(primaryFormat)
                    .computeIfAbsent(typeName, k -> EnumSet.noneOf(FieldCapability.class))
                    .add(cap);
                logger.info("[COMPOSITE_DEBUG]   -> assigned [{}] to PRIMARY format [{}]", cap, primaryFormat.name());
            } else {
                // Find a secondary format that supports it
                boolean assignedToSecondary = false;
                for (Map.Entry<DataFormat, EngineRole> entry : roleMap.entrySet()) {
                    DataFormat secondaryFormat = entry.getKey();
                    EngineRole role = entry.getValue();
                    boolean isSecondary = role != EngineRole.PRIMARY;
                    boolean secondaryHasCap = registry.hasCapability(typeName, secondaryFormat, cap);
                    logger.info("[COMPOSITE_DEBUG]   checking secondary format [{}] role={} isSecondary={} hasCapability={} registryCapabilities={}",
                        secondaryFormat.name(), role, isSecondary, secondaryHasCap,
                        registry.getCapabilities(typeName, secondaryFormat));

                    if (isSecondary && secondaryHasCap) {
                        perFormatMap.get(secondaryFormat)
                            .computeIfAbsent(typeName, k -> EnumSet.noneOf(FieldCapability.class))
                            .add(cap);
                        logger.info("[COMPOSITE_DEBUG]   -> assigned [{}] to SECONDARY format [{}]", cap, secondaryFormat.name());
                        assignedToSecondary = true;
                        break;
                    }
                }
                if (!assignedToSecondary) {
                    logger.warn("[COMPOSITE_DEBUG]   -> capability [{}] for field=[{}] type=[{}] NOT assigned to any format!",
                        cap, fieldType.name(), typeName);
                }
            }
        }
    }
}
