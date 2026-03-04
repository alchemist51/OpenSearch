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
import java.util.Map;
import java.util.Set;

/**
 * Per-format view of field capability assignments resolved by the composite engine.
 * Maps fieldName → FieldDescriptor that this format is responsible for.
 *
 * <p>Used by DocumentInput implementations to decide whether to write a given field.
 * If a field name has no entry, this format should skip it entirely.
 */
@ExperimentalApi
public class FieldAssignments {

    private final Map<String, FieldDescriptor> descriptors;

    public FieldAssignments(Map<String, FieldDescriptor> descriptors) {
        this.descriptors = Map.copyOf(descriptors);
    }

    /**
     * Returns true if this format should handle the given field name.
     */
    public boolean shouldHandle(String fieldName) {
        return descriptors.containsKey(fieldName);
    }

    /**
     * Returns the assigned capabilities for a field name, or empty set if none.
     */
    public Set<FieldCapability> getAssignedCapabilities(String fieldName) {
        FieldDescriptor fd = descriptors.get(fieldName);
        return fd != null ? fd.assignedCapabilities() : Collections.emptySet();
    }

    /**
     * Returns the full FieldDescriptor for a given field name, or null if none.
     */
    public FieldDescriptor getDescriptor(String fieldName) {
        return descriptors.get(fieldName);
    }
}
