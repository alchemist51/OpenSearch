/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import org.opensearch.index.engine.dataformat.DerivedDataFormat;
import org.opensearch.test.OpenSearchTestCase;

/**
 * Tests for {@link MVStateDataFormat} — the separate-index MV target's
 * aggregate-state format descriptor.
 */
public class MVStateDataFormatTests extends OpenSearchTestCase {

    public void testNameReturnsMvState() {
        assertEquals("mv_state", MVStateDataFormat.INSTANCE.name());
        assertEquals(MVStateDataFormat.NAME, MVStateDataFormat.INSTANCE.name());
    }

    public void testSingletonInstance() {
        assertSame(MVStateDataFormat.INSTANCE, MVStateDataFormat.INSTANCE);
    }

    public void testIsDerivedDataFormat() {
        assertTrue(MVStateDataFormat.INSTANCE instanceof DerivedDataFormat);
    }

    public void testNameConstantMatchesInstance() {
        assertEquals("mv_state", MVStateDataFormat.NAME);
    }

    /**
     * mv_state belongs to the {@code materialized_view} DERIVED DATA-FORMAT
     * CATEGORY and is that category's physical target artifact. This is the
     * contract the DataFormatRegistry uses to resolve
     * index.derived.data_format=materialized_view to the mv_state artifact
     * (never a string match in secondary_data_formats).
     */
    public void testCategoryIsMaterializedView() {
        assertEquals("materialized_view", MVStateDataFormat.INSTANCE.category());
        assertEquals(MVDataFormat.NAME, MVStateDataFormat.INSTANCE.category());
    }

    public void testIsDerivedTargetArtifact() {
        assertTrue("mv_state must be the materialized_view target artifact", MVStateDataFormat.INSTANCE.isDerivedTargetArtifact());
    }

    /**
     * The source-side materialized_view capture format shares the category but
     * is NOT the target artifact — it remains a legal source secondary.
     */
    public void testSourceFormatIsNotTargetArtifact() {
        assertEquals("materialized_view", MVDataFormat.INSTANCE.category());
        assertFalse("materialized_view (source capture) must not be a target artifact", MVDataFormat.INSTANCE.isDerivedTargetArtifact());
    }
}
