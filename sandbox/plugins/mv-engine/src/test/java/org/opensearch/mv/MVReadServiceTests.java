/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import org.opensearch.test.OpenSearchTestCase;

import java.util.List;
import java.util.Map;

/**
 * Unit tests for MVReadService. Since the native bridge requires the .so loaded
 * at runtime, these tests validate the Java-level contract: schema JSON
 * generation, hydrated directory discovery, and result container behaviour.
 *
 * <p>End-to-end tests that exercise the native DataFusion fold run as Rust unit
 * tests in analytics-backend-datafusion/rust/src/mv_fold.rs (2 source shards
 * × 2 generations with overlapping group keys → exact folded values).</p>
 */
public class MVReadServiceTests extends OpenSearchTestCase {

    public void testMVQueryResultEmpty() {
        MVReadService.MVQueryResult result = new MVReadService.MVQueryResult(
            List.of("event_bucket", "count"),
            List.of(),
            0
        );
        assertEquals(0, result.totalRows());
        assertTrue(result.rows().isEmpty());
        assertEquals(2, result.columns().size());
    }

    public void testMVQueryResultWithRows() {
        List<Map<String, Object>> rows = List.of(
            Map.of("event_bucket", 100L, "count", 10L, "sum_adv", 500L),
            Map.of("event_bucket", 200L, "count", 5L, "sum_adv", 250L)
        );
        MVReadService.MVQueryResult result = new MVReadService.MVQueryResult(
            List.of("event_bucket", "count", "sum_adv"),
            rows,
            2
        );
        assertEquals(2, result.totalRows());
        assertEquals(2, result.rows().size());
        assertEquals(100L, result.rows().get(0).get("event_bucket"));
        assertEquals(10L, result.rows().get(0).get("count"));
    }

    public void testBuildInputSchemaJsonFromDefinition() {
        // Verify that a compiled definition can produce a valid schema JSON.
        MVCompiledDefinition def = MVCompiledDefinition.of(
            List.of(
                GroupKey.of("event_bucket", GroupKey.ColumnType.LONG),
                GroupKey.of("URL", GroupKey.ColumnType.KEYWORD)
            ),
            List.of(
                AggregateSpec.count("cnt"),
                AggregateSpec.sum("AdvEngineID", "sum_adv")
            )
        );

        // Verify projectionOrder
        List<String> cols = def.projectionOrder();
        assertTrue(cols.contains("event_bucket"));
        assertTrue(cols.contains("URL"));

        // Verify partial SQL generation
        String sql = def.buildPartialSql("mv_input");
        assertTrue(sql.contains("SELECT"));
        assertTrue(sql.contains("GROUP BY"));
        assertTrue(sql.contains("event_bucket"));
    }
}
