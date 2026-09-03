/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import org.opensearch.test.OpenSearchTestCase;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Tests for the {@code clickbench_5m_url} definition and the generic
 * reusable abstractions it exercises:
 * <ul>
 *   <li>{@link GroupKey} derived SQL expression separate from output alias</li>
 *   <li>{@link AggregateSpec#countField(String, String)} per-field COUNT</li>
 *   <li>Expression-aware partial SQL ({@code <expr> AS alias} + repeated
 *       expression in GROUP BY) and materialized-key fold SQL</li>
 *   <li>Exact 43-field projection / state_fields order</li>
 *   <li>Target mapping types, hidden provenance field, {@code dynamic:false},
 *       {@code _field_names} disabled</li>
 *   <li>Stable definition hash and schema validation</li>
 *   <li>Compiler identity between the artifact builder and target creator</li>
 *   <li>Legacy definitions unchanged</li>
 * </ul>
 */
public class MVClickbench5mUrlTests extends OpenSearchTestCase {

    private static final List<String> PREFIXES = List.of("adv", "resw", "resh", "resd", "cip", "rip", "conn", "dns", "fetch", "send");
    private static final List<String> FIELDS = List.of(
        "AdvEngineID",
        "ResolutionWidth",
        "ResolutionHeight",
        "ResolutionDepth",
        "ClientIP",
        "RemoteIP",
        "ConnectTiming",
        "DNSTiming",
        "FetchTiming",
        "SendTiming"
    );

    // ── GroupKey: stable SQL expression separate from output alias ────────

    public void testPlainGroupKeyIsPlainColumnAndDefaultsExpression() {
        GroupKey k = GroupKey.of("CounterID", GroupKey.ColumnType.LONG);
        assertTrue(k.isPlainColumn());
        assertEquals("\"CounterID\"", k.sqlExpression());
        assertEquals("CounterID", k.osFieldPath());
    }

    public void testThreeArgConstructorStillDefaultsExpression() {
        GroupKey k = new GroupKey("region", GroupKey.ColumnType.KEYWORD, "metadata.region");
        assertTrue(k.isPlainColumn());
        assertEquals("\"region\"", k.sqlExpression());
        assertEquals("metadata.region", k.osFieldPath());
    }

    public void testDerivedGroupKeyKeepsExpressionSeparateFromAlias() {
        GroupKey k = GroupKey.ofExpression("event_bucket", GroupKey.ColumnType.LONG, "CAST(\"EventTime\" AS BIGINT) / 300000", "EventTime");
        assertFalse(k.isPlainColumn());
        assertEquals("event_bucket", k.name());
        assertEquals("CAST(\"EventTime\" AS BIGINT) / 300000", k.sqlExpression());
        assertEquals("EventTime", k.osFieldPath());
        assertEquals(GroupKey.ColumnType.LONG, k.columnType());
    }

    // ── AggregateSpec: per-field COUNT ────────────────────────────────────

    public void testCountFieldFactory() {
        AggregateSpec cf = AggregateSpec.countField("AdvEngineID", "adv_cnt");
        assertEquals(AggregateSpec.AggFunction.COUNT, cf.function());
        assertEquals("AdvEngineID", cf.sourceField());
        assertEquals("adv_cnt", cf.userAlias());
        assertEquals("COUNT(\"AdvEngineID\")", cf.partialSqlFragment());
        assertEquals("SUM(\"adv_cnt\")", cf.foldSqlFragment());
        assertEquals("long", cf.targetMappingType());
        assertEquals(1, cf.stateColumns().size());
        assertEquals("adv_cnt", cf.stateColumns().get(0).name());
        assertEquals("long", cf.stateColumns().get(0).physicalType());
    }

    public void testCountStarVsCountFieldDiffer() {
        AggregateSpec star = AggregateSpec.count("cnt");
        AggregateSpec field = AggregateSpec.countField("AdvEngineID", "adv_cnt");
        assertNull(star.sourceField());
        assertEquals("COUNT(*)", star.partialSqlFragment());
        assertNotNull(field.sourceField());
        assertEquals("COUNT(\"AdvEngineID\")", field.partialSqlFragment());
    }

    // ── Compiled definition shape ─────────────────────────────────────────

    public void testCompiledHasThreeGroupKeysInOrder() {
        MVCompiledDefinition def = MVCompiledDefinition.clickbench5mUrl();
        assertEquals(3, def.groupKeys().size());
        assertEquals("event_bucket", def.groupKeys().get(0).name());
        assertEquals(GroupKey.ColumnType.TIMESTAMP, def.groupKeys().get(0).columnType());
        assertFalse(def.groupKeys().get(0).isPlainColumn());
        assertEquals("URL", def.groupKeys().get(1).name());
        assertEquals(GroupKey.ColumnType.KEYWORD, def.groupKeys().get(1).columnType());
        assertEquals("UserID", def.groupKeys().get(2).name());
        assertEquals(GroupKey.ColumnType.LONG, def.groupKeys().get(2).columnType());
    }

    public void testCompiledHas40Aggregates() {
        MVCompiledDefinition def = MVCompiledDefinition.clickbench5mUrl();
        assertEquals(40, def.aggregates().size());
        // Verify SUM/MIN/MAX/COUNT quad per field, in order.
        for (int f = 0; f < FIELDS.size(); f++) {
            int base = f * 4;
            assertEquals(AggregateSpec.AggFunction.SUM, def.aggregates().get(base).function());
            assertEquals(AggregateSpec.AggFunction.MIN, def.aggregates().get(base + 1).function());
            assertEquals(AggregateSpec.AggFunction.MAX, def.aggregates().get(base + 2).function());
            assertEquals(AggregateSpec.AggFunction.COUNT, def.aggregates().get(base + 3).function());
            assertEquals(FIELDS.get(f), def.aggregates().get(base + 3).sourceField()); // per-field COUNT
            assertEquals(PREFIXES.get(f) + "_sum", def.aggregates().get(base).userAlias());
            assertEquals(PREFIXES.get(f) + "_min", def.aggregates().get(base + 1).userAlias());
            assertEquals(PREFIXES.get(f) + "_max", def.aggregates().get(base + 2).userAlias());
            assertEquals(PREFIXES.get(f) + "_cnt", def.aggregates().get(base + 3).userAlias());
        }
    }

    // ── Projection / state_fields order (exactly 43) ──────────────────────

    public void testProjectionOrderIs43Fields() {
        MVCompiledDefinition def = MVCompiledDefinition.clickbench5mUrl();
        List<String> proj = def.projectionOrder();
        assertEquals(43, proj.size());
        assertEquals("event_bucket", proj.get(0));
        assertEquals("URL", proj.get(1));
        assertEquals("UserID", proj.get(2));
        int i = 3;
        for (String p : PREFIXES) {
            assertEquals(p + "_sum", proj.get(i++));
            assertEquals(p + "_min", proj.get(i++));
            assertEquals(p + "_max", proj.get(i++));
            assertEquals(p + "_cnt", proj.get(i++));
        }
        assertEquals(43, i);
    }

    public void testStateColumnNamesEqualsProjectionOrder() {
        MVCompiledDefinition def = MVCompiledDefinition.clickbench5mUrl();
        assertEquals(def.projectionOrder(), def.stateColumnNames());
    }

    // ── Partial SQL: bucket AS alias + repeated expression in GROUP BY ────

    public void testPartialSqlBucketAliasAndGroupBy() {
        String sql = MVCompiledDefinition.clickbench5mUrl().buildPartialSql("mv_input");
        assertTrue(sql, sql.startsWith("SELECT date_bin(INTERVAL '5 minutes', \"EventTime\") AS \"event_bucket\", \"URL\", \"UserID\", "));
        assertTrue(sql, sql.endsWith(" FROM mv_input GROUP BY date_bin(INTERVAL '5 minutes', \"EventTime\"), \"URL\", \"UserID\""));
        // First and last metric blocks exactly.
        assertTrue(sql, sql.contains("SUM(\"AdvEngineID\"), MIN(\"AdvEngineID\"), MAX(\"AdvEngineID\"), COUNT(\"AdvEngineID\")"));
        assertTrue(sql, sql.contains("SUM(\"SendTiming\"), MIN(\"SendTiming\"), MAX(\"SendTiming\"), COUNT(\"SendTiming\")"));
    }

    public void testPartialSqlUsesDateBin5Minutes() {
        // EventTime is a date field. date_bin produces a timestamp bucket, not an
        // integer epoch ordinal — the key lesson from the date-type source bug.
        String sql = MVCompiledDefinition.clickbench5mUrl().buildPartialSql("mv_input");
        assertTrue("must use date_bin", sql.contains("date_bin(INTERVAL '5 minutes', \"EventTime\")"));
        assertFalse("must NOT use epoch arithmetic", sql.contains("/ 300000"));
    }

    public void testPartialSqlSelectHas43Columns() {
        String sql = MVCompiledDefinition.clickbench5mUrl().buildPartialSql("mv_input");
        assertEquals(43, topLevelSelectColumnCount(sql));
    }

    public void testPartialSqlAggregateCounts() {
        String select = selectPortion(MVCompiledDefinition.clickbench5mUrl().buildPartialSql("mv_input"));
        assertEquals(10, countOccurrences(select, "SUM("));
        assertEquals(10, countOccurrences(select, "MIN("));
        assertEquals(10, countOccurrences(select, "MAX("));
        assertEquals(10, countOccurrences(select, "COUNT("));
    }

    // ── Fold SQL: groups by materialized keys, folds states ───────────────

    public void testFoldSqlGroupsByMaterializedKeys() {
        String sql = MVCompiledDefinition.clickbench5mUrl().buildFoldSql("mv_input");
        assertTrue(sql, sql.startsWith("SELECT \"event_bucket\", \"URL\", \"UserID\", "));
        assertTrue(sql, sql.endsWith(" FROM mv_input GROUP BY \"event_bucket\", \"URL\", \"UserID\""));
        assertFalse("fold must not re-evaluate the bucket expression", sql.contains("date_bin("));
        assertFalse("fold must not alias", sql.contains(" AS "));
        assertTrue(sql, sql.contains("SUM(\"adv_sum\"), MIN(\"adv_min\"), MAX(\"adv_max\"), SUM(\"adv_cnt\")"));
        assertTrue(sql, sql.contains("SUM(\"send_sum\"), MIN(\"send_min\"), MAX(\"send_max\"), SUM(\"send_cnt\")"));
    }

    public void testFoldSqlAggregateCounts() {
        String select = selectPortion(MVCompiledDefinition.clickbench5mUrl().buildFoldSql("mv_input"));
        // sums(10) + cnts folded as SUM(10) = 20 SUM; mins=10; maxes=10.
        assertEquals(20, countOccurrences(select, "SUM("));
        assertEquals(10, countOccurrences(select, "MIN("));
        assertEquals(10, countOccurrences(select, "MAX("));
    }

    // ── Target mapping types + hidden provenance field ────────────────────

    public void testCompiledTargetMappingTypes() {
        Map<String, String> mapping = MVCompiledDefinition.clickbench5mUrl().targetMapping();
        assertEquals(43, mapping.size());
        assertEquals("date", mapping.get("event_bucket"));
        assertEquals("keyword", mapping.get("URL"));
        assertEquals("long", mapping.get("UserID"));
        for (String p : PREFIXES) {
            assertEquals("long", mapping.get(p + "_sum"));
            assertEquals("long", mapping.get(p + "_min"));
            assertEquals("long", mapping.get(p + "_max"));
            assertEquals("long", mapping.get(p + "_cnt"));
        }
    }

    public void testTargetMappingJsonHasTypesHiddenFieldAndDynamicFalse() {
        String json = MVViewsService.TargetCreator.targetMapping("clickbench_5m_url");
        assertTrue(json, json.contains("\"dynamic\":\"false\""));
        assertTrue(json, json.contains("\"_field_names\":{\"enabled\":false}"));
        assertTrue(json, json.contains("\"event_bucket\":{\"type\":\"date\"}"));
        assertTrue(json, json.contains("\"URL\":{\"type\":\"keyword\"}"));
        assertTrue(json, json.contains("\"UserID\":{\"type\":\"long\"}"));
        assertTrue(json, json.contains("\"send_cnt\":{\"type\":\"long\"}"));
        // Hidden provenance field, non-indexed.
        assertTrue(json, json.contains("\"_mv_source_generation\":{\"type\":\"long\",\"index\":false}"));
    }

    // ── Hash stability ────────────────────────────────────────────────────

    public void testHashStableAcrossCalls() {
        assertEquals(MVCompiledDefinition.clickbench5mUrl().hash(), MVCompiledDefinition.clickbench5mUrl().hash());
    }

    public void testHashIsSha256Hex() {
        String h = MVCompiledDefinition.clickbench5mUrl().hash();
        assertEquals(64, h.length());
        assertTrue(h.matches("[0-9a-f]{64}"));
    }

    public void testHashDependsOnBucketExpression() {
        // Same alias/type but plain column (no expression) → different hash.
        MVCompiledDefinition withExpr = MVCompiledDefinition.of(
            List.of(GroupKey.ofExpression("event_bucket", GroupKey.ColumnType.LONG, "CAST(\"EventTime\" AS BIGINT) / 300000", "EventTime")),
            List.of(AggregateSpec.count("cnt"))
        );
        MVCompiledDefinition plain = MVCompiledDefinition.of(
            List.of(GroupKey.of("event_bucket", GroupKey.ColumnType.LONG)),
            List.of(AggregateSpec.count("cnt"))
        );
        assertNotEquals(withExpr.hash(), plain.hash());
    }

    // ── Schema validation ─────────────────────────────────────────────────

    public void testSchemaValidationPassesWithFullSchema() {
        MVCompiledDefinition def = MVCompiledDefinition.clickbench5mUrl();
        Map<String, Object> schema = new HashMap<>();
        for (String col : def.projectionOrder()) {
            schema.put(col, "long");
        }
        schema.put("URL", "keyword");
        def.validateSchema(schema); // must not throw
    }

    public void testSchemaValidationFailsOnMissingField() {
        MVCompiledDefinition def = MVCompiledDefinition.clickbench5mUrl();
        Map<String, Object> schema = new HashMap<>();
        schema.put("event_bucket", "long"); // missing the rest
        IllegalStateException ex = expectThrows(IllegalStateException.class, () -> def.validateSchema(schema));
        assertTrue(ex.getMessage(), ex.getMessage().contains("missing field"));
    }

    // ── Compiler identity (builder ≡ target creator) ──────────────────────

    public void testCompiledForMatchesDirectFactory() {
        assertEquals(MVCompiledDefinition.clickbench5mUrl().hash(), MVCompiledDefinition.compiledFor("clickbench_5m_url").hash());
    }

    public void testStateFieldsFromSpecEqualCompiledProjection() {
        // MVViewsService persists state_fields from the spec; the pull builder
        // and search read from the compiled projection. They must be identical.
        List<String> specShipFields = MVDefinitionSpec.source("clickbench_5m_url").shipFields();
        List<String> compiledProjection = MVCompiledDefinition.compiledFor("clickbench_5m_url").stateColumnNames();
        assertEquals(compiledProjection, specShipFields);
        assertEquals(43, specShipFields.size());
    }

    public void testSpecSqlMatchesCompiledPartialSql() {
        assertEquals(
            MVCompiledDefinition.compiledFor("clickbench_5m_url").buildPartialSql(MVConstants.INPUT_TABLE),
            MVDefinitionSpec.source("clickbench_5m_url").sql()
        );
    }

    public void testFoldSpecSqlMatchesCompiledFoldSql() {
        assertEquals(
            MVCompiledDefinition.compiledFor("clickbench_5m_url").buildFoldSql(MVConstants.INPUT_TABLE),
            MVDefinitionSpec.fold("clickbench_5m_url").sql()
        );
    }

    // ── Registration ──────────────────────────────────────────────────────

    public void testRegisteredInSourceFoldAndAllNames() {
        assertSame(MVDefinitionSpec.CLICKBENCH_5M_URL, MVDefinitionSpec.source("clickbench_5m_url"));
        assertSame(MVDefinitionSpec.CLICKBENCH_5M_URL_FOLD, MVDefinitionSpec.fold("clickbench_5m_url"));
        assertTrue(MVDefinitionSpec.allNames().contains("clickbench_5m_url"));
    }

    public void testUrlIsUtf8CaptureColumnAndKeywordKey() {
        // Capture column is UTF8; the compiled group key surfaces keyword.
        MVDefinitionSpec spec = MVDefinitionSpec.CLICKBENCH_5M_URL;
        assertEquals("URL", spec.columns().get(1).name());
        assertEquals(MVDefinitionSpec.ColumnType.UTF8, spec.columns().get(1).type());
        assertEquals(GroupKey.ColumnType.KEYWORD, MVCompiledDefinition.clickbench5mUrl().groupKeys().get(1).columnType());
    }

    // ── Legacy definitions unchanged ──────────────────────────────────────

    public void testLegacyCompiledForMatchesFromLegacySpec() {
        for (String name : List.of("pull_count_sum", "pull_count_sum_userid", "clickbench_q9")) {
            MVCompiledDefinition viaName = MVCompiledDefinition.compiledFor(name);
            MVCompiledDefinition viaSpec = MVCompiledDefinition.fromLegacySpec(MVDefinitionSpec.source(name));
            assertEquals("hash mismatch for " + name, viaSpec.hash(), viaName.hash());
        }
    }

    public void testLegacyPlainKeyPartialSqlUnchanged() {
        // A plain-column definition must emit no alias and no CAST — byte-identical
        // to the pre-change generator.
        MVCompiledDefinition def = MVCompiledDefinition.compiledFor("pull_count_sum");
        String sql = def.buildPartialSql("mv_input");
        assertEquals("SELECT \"RegionID\", COUNT(*), SUM(\"AdvEngineID\") FROM mv_input GROUP BY \"RegionID\"", sql);
        assertFalse(sql.contains(" AS "));
    }

    public void testLegacyHashUnaffectedByExpressionField() {
        // Rebuilding a legacy plain-key definition via the generic of() path
        // yields the same hash as the named legacy compile — proving the new
        // sqlExpression field does not perturb plain-column canonical forms.
        MVCompiledDefinition named = MVCompiledDefinition.compiledFor("pull_count_sum");
        MVCompiledDefinition rebuilt = MVCompiledDefinition.of(
            List.of(GroupKey.of("RegionID", GroupKey.ColumnType.LONG)),
            List.of(AggregateSpec.count("cnt"), AggregateSpec.sum("AdvEngineID", "sum_AdvEngineID"))
        );
        assertEquals(named.hash(), rebuilt.hash());
    }

    // ── helpers ───────────────────────────────────────────────────────────

    private static String selectPortion(String sql) {
        int from = sql.indexOf(" FROM ");
        return sql.substring("SELECT ".length(), from);
    }

    private static int topLevelSelectColumnCount(String sql) {
        String select = selectPortion(sql);
        int depth = 0;
        int commas = 0;
        for (char c : select.toCharArray()) {
            if (c == '(') depth++;
            else if (c == ')') depth--;
            else if (c == ',' && depth == 0) commas++;
        }
        return commas + 1;
    }

    private static int countOccurrences(String str, String sub) {
        int count = 0;
        int idx = 0;
        while ((idx = str.indexOf(sub, idx)) != -1) {
            count++;
            idx += sub.length();
        }
        return count;
    }
}
