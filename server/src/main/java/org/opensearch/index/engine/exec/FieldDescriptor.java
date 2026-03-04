/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.index.engine.exec;

import org.opensearch.common.annotation.ExperimentalApi;

import java.util.Collections;
import java.util.EnumSet;
import java.util.Objects;
import java.util.Set;

/**
 * Immutable per-field descriptor that carries a field's name, type name, resolved capabilities,
 * and pre-computed boolean flags for O(1) hot-path capability checks.
 *
 * <p>Created by {@link FieldAssignmentResolver} during engine initialization. Each mapped field
 * gets its own descriptor per data format, replacing the previous type-name-keyed lookup.
 */
@ExperimentalApi
public final class FieldDescriptor {

    private final String fieldName;
    private final String typeName;
    private final Set<FieldCapability> assignedCapabilities;
    private final boolean searchable;
    private final boolean hasDocValues;
    private final boolean stored;

    /**
     * Constructs a new FieldDescriptor.
     *
     * @param fieldName            the mapped field name (e.g., "title", "price")
     * @param typeName             the field type name (e.g., "keyword", "long")
     * @param assignedCapabilities the capabilities this format is responsible for on this field
     */
    public FieldDescriptor(String fieldName, String typeName, Set<FieldCapability> assignedCapabilities) {
        this.fieldName = Objects.requireNonNull(fieldName);
        this.typeName = Objects.requireNonNull(typeName);
        this.assignedCapabilities = Collections.unmodifiableSet(EnumSet.copyOf(assignedCapabilities));
        this.searchable = assignedCapabilities.contains(FieldCapability.INDEX);
        this.hasDocValues = assignedCapabilities.contains(FieldCapability.DOC_VALUES);
        this.stored = assignedCapabilities.contains(FieldCapability.STORE);
    }

    /** Returns the mapped field name. */
    public String fieldName() {
        return fieldName;
    }

    /** Returns the field type name. */
    public String typeName() {
        return typeName;
    }

    /** Returns the immutable set of assigned capabilities. */
    public Set<FieldCapability> assignedCapabilities() {
        return assignedCapabilities;
    }

    /** Returns true if the assigned capabilities include {@link FieldCapability#INDEX}. */
    public boolean isSearchable() {
        return searchable;
    }

    /** Returns true if the assigned capabilities include {@link FieldCapability#DOC_VALUES}. */
    public boolean hasDocValues() {
        return hasDocValues;
    }

    /** Returns true if the assigned capabilities include {@link FieldCapability#STORE}. */
    public boolean isStored() {
        return stored;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FieldDescriptor that = (FieldDescriptor) o;
        return fieldName.equals(that.fieldName) && typeName.equals(that.typeName) && assignedCapabilities.equals(that.assignedCapabilities);
    }

    @Override
    public int hashCode() {
        return Objects.hash(fieldName, typeName, assignedCapabilities);
    }

    @Override
    public String toString() {
        return "FieldDescriptor{" + "fieldName='" + fieldName + '\'' + ", typeName='" + typeName + '\'' + ", capabilities=" + assignedCapabilities + '}';
    }
}
