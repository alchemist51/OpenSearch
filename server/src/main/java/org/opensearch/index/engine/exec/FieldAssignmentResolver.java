/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

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
            String typeName = fieldType.typeName();
            resolveField(registry, roleMap, primaryFormat, perFormatMap, fieldType, typeName);
        }

        // Wrap into FieldAssignments
        Map<DataFormat, FieldAssignments> result = new HashMap<>();
        for (Map.Entry<DataFormat, Map<String, Set<FieldCapability>>> entry : perFormatMap.entrySet()) {
            result.put(entry.getKey(), new FieldAssignments(entry.getValue()));
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

        // For each required capability, assign to primary if it supports it, else to secondary
        for (FieldCapability cap : required) {
            if (primaryFormat != null && registry.hasCapability(typeName, primaryFormat, cap)) {
                // Primary handles this capability
                perFormatMap.get(primaryFormat)
                    .computeIfAbsent(typeName, k -> EnumSet.noneOf(FieldCapability.class))
                    .add(cap);
            } else {
                // Find a secondary format that supports it
                for (Map.Entry<DataFormat, EngineRole> entry : roleMap.entrySet()) {
                    if (entry.getValue() != EngineRole.PRIMARY
                        && registry.hasCapability(typeName, entry.getKey(), cap)) {
                        perFormatMap.get(entry.getKey())
                            .computeIfAbsent(typeName, k -> EnumSet.noneOf(FieldCapability.class))
                            .add(cap);
                        break;
                    }
                }
            }
        }
    }
}
