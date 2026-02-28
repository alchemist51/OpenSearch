/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec.composite;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.index.engine.exec.DataFormat;
import org.opensearch.index.engine.exec.EngineRole;
import org.opensearch.index.engine.exec.FieldCapability;
import org.opensearch.index.engine.exec.FieldSupportRegistry;
import org.opensearch.index.mapper.MappedFieldType;

import java.util.Map;

/**
 * Stateless validator that checks field-to-capability compatibility using the
 * {@link FieldSupportRegistry} at index creation or mapping update time.
 */
@ExperimentalApi
public final class CompositeFieldValidator {

    private CompositeFieldValidator() {}

    /**
     * Validates that the primary data format has at least one capability
     * registered for every mapped field type.
     * Throws IllegalArgumentException if any field lacks primary coverage.
     */
    public static void validatePrimaryCoverage(
        FieldSupportRegistry registry,
        Map<DataFormat, EngineRole> roleMap,
        Iterable<MappedFieldType> fieldTypes
    ) {
        DataFormat primaryFormat = null;
        for (Map.Entry<DataFormat, EngineRole> entry : roleMap.entrySet()) {
            if (entry.getValue() == EngineRole.PRIMARY) {
                primaryFormat = entry.getKey();
                break;
            }
        }
        if (primaryFormat == null) {
            return;
        }
        for (MappedFieldType fieldType : fieldTypes) {
            if (!registry.hasAnyCapability(fieldType.typeName(), primaryFormat)) {
                throw new IllegalArgumentException(
                    "Field [" + fieldType.name() + "] of type [" + fieldType.typeName()
                        + "] has no capabilities registered for primary data format [" + primaryFormat.name() + "]"
                );
            }
        }
    }

    /**
     * Validates that every field's enabled mapping properties have at least one
     * data format with the corresponding capability:
     *   isSearchable() → INDEX, hasDocValues() → DOC_VALUES, isStored() → STORE.
     * Throws IllegalArgumentException if any property lacks coverage.
     */
    public static void validateMappingPropertyCoverage(
        FieldSupportRegistry registry,
        Iterable<MappedFieldType> fieldTypes
    ) {
        for (MappedFieldType fieldType : fieldTypes) {
            String typeName = fieldType.typeName();
            if (fieldType.isSearchable()) {
                checkCapabilityCoverage(registry, fieldType, typeName, FieldCapability.INDEX, "index");
            }
            if (fieldType.hasDocValues()) {
                checkCapabilityCoverage(registry, fieldType, typeName, FieldCapability.DOC_VALUES, "doc_values");
            }
            if (fieldType.isStored()) {
                checkCapabilityCoverage(registry, fieldType, typeName, FieldCapability.STORE, "store");
            }
        }
    }

    private static void checkCapabilityCoverage(
        FieldSupportRegistry registry,
        MappedFieldType fieldType,
        String typeName,
        FieldCapability requiredCapability,
        String propertyName
    ) {
        for (DataFormat format : registry.allFormats()) {
            if (registry.hasCapability(typeName, format, requiredCapability)) {
                return;
            }
        }
        throw new IllegalArgumentException(
            "Field [" + fieldType.name() + "] of type [" + typeName
                + "] requires [" + requiredCapability + "] capability (mapping property [" + propertyName
                + "]=true) but no data format provides it"
        );
    }
}
