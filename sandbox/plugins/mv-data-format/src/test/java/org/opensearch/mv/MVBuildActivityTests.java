/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import org.opensearch.test.OpenSearchTestCase;

/**
 * Semantics of the defect #23 build-pressure registry: counted claims,
 * guaranteed-resolution floor, and per-shard isolation.
 */
public class MVBuildActivityTests extends OpenSearchTestCase {

    @Override
    public void setUp() throws Exception {
        super.setUp();
        MVBuildActivity.clearForTests();
    }

    @Override
    public void tearDown() throws Exception {
        MVBuildActivity.clearForTests();
        super.tearDown();
    }

    public void testInactiveByDefault() {
        assertFalse(MVBuildActivity.isActive("mv-target", 0));
    }

    public void testMarkThenClear() {
        MVBuildActivity.markActive("mv-target", 0);
        assertTrue(MVBuildActivity.isActive("mv-target", 0));
        MVBuildActivity.clearActive("mv-target", 0);
        assertFalse(MVBuildActivity.isActive("mv-target", 0));
    }

    public void testClaimsAreCounted() {
        MVBuildActivity.markActive("mv-target", 0);
        MVBuildActivity.markActive("mv-target", 0);
        MVBuildActivity.clearActive("mv-target", 0);
        assertTrue("one of two claims cleared — still active", MVBuildActivity.isActive("mv-target", 0));
        MVBuildActivity.clearActive("mv-target", 0);
        assertFalse(MVBuildActivity.isActive("mv-target", 0));
    }

    public void testUnmatchedClearDoesNotPoisonFutureMarks() {
        MVBuildActivity.clearActive("mv-target", 0);
        MVBuildActivity.clearActive("mv-target", 0);
        assertFalse(MVBuildActivity.isActive("mv-target", 0));
        MVBuildActivity.markActive("mv-target", 0);
        assertTrue("mark after unmatched clears must still register", MVBuildActivity.isActive("mv-target", 0));
        MVBuildActivity.clearActive("mv-target", 0);
        assertFalse(MVBuildActivity.isActive("mv-target", 0));
    }

    public void testShardAndIndexIsolation() {
        MVBuildActivity.markActive("mv-target", 0);
        assertFalse("other shard of same index unaffected", MVBuildActivity.isActive("mv-target", 1));
        assertFalse("other index unaffected", MVBuildActivity.isActive("other-target", 0));
        MVBuildActivity.clearActive("mv-target", 0);
    }
}
