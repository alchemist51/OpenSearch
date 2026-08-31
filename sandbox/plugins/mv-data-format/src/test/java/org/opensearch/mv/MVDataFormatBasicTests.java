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
 * Tests for {@link MVDataFormat} — the source-side materialized view derived
 * data format descriptor.
 */
public class MVDataFormatBasicTests extends OpenSearchTestCase {

    public void testNameReturnsMaterializedView() {
        assertEquals("materialized_view", MVDataFormat.INSTANCE.name());
        assertEquals(MVDataFormat.NAME, MVDataFormat.INSTANCE.name());
    }

    public void testSingletonInstance() {
        assertSame(MVDataFormat.INSTANCE, MVDataFormat.INSTANCE);
    }

    public void testIsDerivedDataFormat() {
        assertTrue(MVDataFormat.INSTANCE instanceof DerivedDataFormat);
    }

    public void testNameConstantMatchesInstance() {
        assertEquals("materialized_view", MVDataFormat.NAME);
    }

    public void testSourceAndStateFormatsHaveDifferentNames() {
        assertNotEquals(MVDataFormat.NAME, MVStateDataFormat.NAME);
        assertNotEquals(MVDataFormat.INSTANCE.name(), MVStateDataFormat.INSTANCE.name());
    }
}
