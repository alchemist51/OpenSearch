/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mvpull;

import org.opensearch.test.OpenSearchTestCase;

/**
 * Tests that the definition-driven SQL filter generation correctly wraps the
 * {@code clickbench_100m} definition with a seq_no range subquery, preserving
 * the full 45-column SELECT and GROUP BY.
 */
public class MVDefinitionPartialSqlTests extends OpenSearchTestCase {

    public void testClickbench100mFilteredSqlContainsSeqNoRange() {
        String sql = MVArtifactPoller.definitionPartialSql("clickbench_100m", 1000L, 2000L);
        assertTrue("must contain _seq_no > 1000", sql.contains("\"_seq_no\" > 1000"));
        assertTrue("must contain _seq_no <= 2000", sql.contains("\"_seq_no\" <= 2000"));
    }

    public void testClickbench100mFilteredSqlPreservesGroupBy() {
        String sql = MVArtifactPoller.definitionPartialSql("clickbench_100m", 0L, 100L);
        assertTrue(
            "GROUP BY must include all 5 keys",
            sql.contains("GROUP BY \"EventTime\", \"RegionID\", \"OS\", \"CounterID\", \"IsRefresh\"")
        );
    }

    public void testClickbench100mFilteredSqlHasNoGlobalCountStar() {
        String sql = MVArtifactPoller.definitionPartialSql("clickbench_100m", 0L, 100L);
        assertFalse("filtered SQL must not contain COUNT(*)", sql.contains("COUNT(*)"));
    }

    public void testClickbench100mFilteredSqlHasPerFieldCount() {
        String sql = MVArtifactPoller.definitionPartialSql("clickbench_100m", 0L, 100L);
        assertTrue(sql.contains("COUNT(\"AdvEngineID\")"));
        assertTrue(sql.contains("COUNT(\"ResolutionWidth\")"));
        assertTrue(sql.contains("COUNT(\"SendTiming\")"));
    }

    public void testClickbench100mFilteredSqlWrapsFromClause() {
        String sql = MVArtifactPoller.definitionPartialSql("clickbench_100m", 500L, 1500L);
        // The FROM clause must be a subquery filtering mv_input
        assertTrue("must contain subquery form", sql.contains("FROM (SELECT * FROM mv_input WHERE"));
        assertTrue("subquery must alias as mv_input", sql.contains(") AS mv_input"));
    }

    public void testLegacyDefinitionStillWorks() {
        // Verify pull_count_sum (legacy-shape definition) also works through definitionPartialSql
        String sql = MVArtifactPoller.definitionPartialSql("pull_count_sum", 0L, 50L);
        assertTrue(sql.contains("\"_seq_no\" > 0"));
        assertTrue(sql.contains("\"_seq_no\" <= 50"));
        assertTrue(sql.contains("GROUP BY \"RegionID\""));
    }

    public void testUnknownDefinitionThrows() {
        expectThrows(
            IllegalArgumentException.class,
            () -> MVArtifactPoller.definitionPartialSql("nonexistent_def", 0L, 100L)
        );
    }
}
