/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

/**
 * MV constants for the source-side definition layer.
 *
 * <p>Pull/ship/merge/native constants from the POC are omitted — this is the
 * definition-only layer. Constants that reference DerivedIndexBinding or
 * MVDataFormat are replaced with inline strings.</p>
 */
public final class MVConstants {

    private MVConstants() {}

    /** Canonical table name every definition SQL is written against. */
    public static final String INPUT_TABLE = "mv_input";

    // ── customData keys for atomic dual-write in IndexMetadata ────────────

    /**
     * customData key on the SOURCE index: maps mvId → definition descriptor JSON.
     * Written atomically with the target's {@link #TARGET_BINDING_KEY}.
     */
    public static final String SOURCE_DEFINITIONS_KEY = "mv_definitions";

    /**
     * customData key on the TARGET index: a single JSON value carrying
     * {@code {source, mvId, descriptor}}.
     */
    public static final String TARGET_BINDING_KEY = "mv_binding";

    // ── Index settings (subset needed at definition layer) ───────────────

    /** Persisted descriptor JSON on the target index. */
    public static final String DESCRIPTOR_SETTING = "index.mv.descriptor";

    /** Ordered state-column names on the target index. */
    public static final String STATE_FIELDS_SETTING = "index.mv.state_fields";

    /** Colocate-with source hint on the target index. */
    public static final String COLOCATE_WITH_SETTING = "index.mv.colocate_with";

    /** First-class derived index flag. */
    public static final String DERIVED_INDEX_SETTING = "index.derived.enabled";

    /** Derived data-format category (= "materialized_view"). */
    public static final String DERIVED_DATA_FORMAT_SETTING = "index.derived.data_format";

    /** Derived source name setting. */
    public static final String DERIVED_SOURCE_NAME_SETTING = "index.derived.source.name";

    /** The canonical derived data-format category name. */
    public static final String DATA_FORMAT_NAME = "materialized_view";

    /** State-merge enabled setting on the target. */
    public static final String STATE_MERGE_SETTING = "index.mv.state_merge_enabled";
}
