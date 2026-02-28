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
 * Maps fieldTypeName → Set of FieldCapability that this format is responsible for.
 *
 * <p>Used by DocumentInput implementations to decide whether to write a given field.
 * If a field type has no entry, this format should skip it entirely.
 */
@ExperimentalApi
public class FieldAssignments {

    /** Sentinel instance that accepts all fields — used in single-format (non-composite) mode. */
    public static final FieldAssignments ACCEPT_ALL = new FieldAssignments(Collections.emptyMap(), true);

    private final Map<String, Set<FieldCapability>> assignments;
    private final boolean acceptAll;

    public FieldAssignments(Map<String, Set<FieldCapability>> assignments) {
        this(assignments, false);
    }

    private FieldAssignments(Map<String, Set<FieldCapability>> assignments, boolean acceptAll) {
        this.assignments = assignments;
        this.acceptAll = acceptAll;
    }

    /**
     * Returns true if this format should handle the given field type.
     */
    public boolean shouldHandle(String fieldTypeName) {
        if (acceptAll) {
            return true;
        }
        return assignments.containsKey(fieldTypeName);
    }

    /**
     * Returns the assigned capabilities for a field type, or empty set if none.
     */
    public Set<FieldCapability> getAssignedCapabilities(String fieldTypeName) {
        if (acceptAll) {
            return Collections.emptySet();
        }
        Set<FieldCapability> caps = assignments.get(fieldTypeName);
        return caps != null ? Collections.unmodifiableSet(caps) : Collections.emptySet();
    }
}
