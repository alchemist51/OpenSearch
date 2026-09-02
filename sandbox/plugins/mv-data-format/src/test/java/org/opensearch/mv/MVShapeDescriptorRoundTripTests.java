/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.mv;

import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;

/**
 * Round-trip test for the wire JSON that {@code MVShapeMatcher} (analytics-engine, Stage&nbsp;2)
 * emits: parse it with {@link MVDefinitionDescriptor#fromXContent} and rebuild a definition with
 * {@link MVCompiledDefinition#fromDescriptor}.
 *
 * <p>The matcher lives in analytics-engine, which this module does not depend on at compile time,
 * so the descriptor JSON here is written to exactly the shape the matcher produces
 * ({@code descriptor_version} + optional {@code source_language} + ordered {@code group_keys} /
 * {@code aggregates}, with the {@code COUNT}/{@code COUNT_FIELD}/{@code AVG} tokens and derived-key
 * {@code expression} + {@code source_column}, and no {@code definition_hash}). This pins the
 * cross-module contract independently of the matcher implementation.
 */
public class MVShapeDescriptorRoundTripTests extends OpenSearchTestCase {

    /**
     * The exact wire shape emitted by {@code MVShapeMatcher} for
     * {@code stats sum/min/max/count/count(AdvEngineID), avg(ParamPrice)
     * by span(EventTime,300000) as event_bucket, URL, UserID}.
     */
    private static final String MATCHER_JSON = "{"
        + "\"descriptor_version\":1,"
        + "\"source_language\":\"ppl\","
        + "\"group_keys\":["
        + "{\"name\":\"event_bucket\",\"column_type\":\"LONG\","
        + "\"expression\":\"CAST(\\\"EventTime\\\" AS BIGINT) / 300000\",\"source_column\":\"EventTime\"},"
        + "{\"name\":\"URL\",\"column_type\":\"KEYWORD\"},"
        + "{\"name\":\"UserID\",\"column_type\":\"LONG\"}"
        + "],"
        + "\"aggregates\":["
        + "{\"function\":\"SUM\",\"field\":\"AdvEngineID\",\"alias\":\"sum_adv\"},"
        + "{\"function\":\"MIN\",\"field\":\"AdvEngineID\",\"alias\":\"min_adv\"},"
        + "{\"function\":\"MAX\",\"field\":\"AdvEngineID\",\"alias\":\"max_adv\"},"
        + "{\"function\":\"COUNT\",\"alias\":\"cnt\"},"
        + "{\"function\":\"COUNT_FIELD\",\"field\":\"AdvEngineID\",\"alias\":\"cnt_adv\"},"
        + "{\"function\":\"AVG\",\"field\":\"ParamPrice\",\"alias\":\"avg_pp\"}"
        + "]"
        + "}";

    public void testMatcherJsonParsesToDescriptor() throws IOException {
        MVDefinitionDescriptor descriptor = parse(MATCHER_JSON);

        assertEquals(1, descriptor.descriptorVersion());
        assertEquals("ppl", descriptor.sourceLanguage().orElseThrow());
        assertTrue("matcher does not emit a definition hash", descriptor.definitionHash().isEmpty());

        assertEquals(3, descriptor.groupKeys().size());
        MVDefinitionDescriptor.GroupKeyDescriptor bucket = descriptor.groupKeys().get(0);
        assertEquals("event_bucket", bucket.name());
        assertEquals(GroupKey.ColumnType.LONG, bucket.columnType());
        assertEquals("CAST(\"EventTime\" AS BIGINT) / 300000", bucket.expression());
        assertEquals("EventTime", bucket.sourceColumn());

        assertEquals(6, descriptor.aggregates().size());
        assertEquals(MVDefinitionDescriptor.AggFunctionToken.COUNT, descriptor.aggregates().get(3).function());
        assertNull("COUNT(*) carries no field", descriptor.aggregates().get(3).field());
        assertEquals(MVDefinitionDescriptor.AggFunctionToken.COUNT_FIELD, descriptor.aggregates().get(4).function());
        assertEquals("AdvEngineID", descriptor.aggregates().get(4).field());
        assertEquals(MVDefinitionDescriptor.AggFunctionToken.AVG, descriptor.aggregates().get(5).function());
    }

    public void testMatcherJsonRebuildsCompiledDefinition() throws IOException {
        MVDefinitionDescriptor descriptor = parse(MATCHER_JSON);
        MVCompiledDefinition def = MVCompiledDefinition.fromDescriptor(descriptor);

        // Three group keys define the state sort order (Stage 1 groupByOrdering contract).
        assertEquals(3, def.groupKeys().size());
        assertEquals(3, def.groupByOrdering().size());
        assertEquals(3, def.groupByOrdering().keys().size());
        assertEquals(
            java.util.List.of("event_bucket", "URL", "UserID"),
            def.groupByOrdering().keys().stream().map(k -> def.groupKeys().get(k.stateFieldIndex()).name()).toList()
        );

        // Aggregates reconstruct through the same factory path as the typed builders.
        assertEquals(6, def.aggregates().size());
        // AVG expands to two state columns (count + sum); its user alias is compiler-forced to avg_<field>.
        assertTrue(def.stateColumnNames().contains("avg_count_ParamPrice"));
        assertTrue(def.stateColumnNames().contains("avg_sum_ParamPrice"));

        // A byte-stable re-serialization round-trips.
        assertEquals(descriptor, parse(toJson(descriptor)));
    }

    private static String toJson(MVDefinitionDescriptor descriptor) throws IOException {
        org.opensearch.core.xcontent.XContentBuilder builder = org.opensearch.common.xcontent.XContentFactory.jsonBuilder();
        descriptor.toXContent(builder, org.opensearch.core.xcontent.ToXContent.EMPTY_PARAMS);
        return builder.toString();
    }

    private MVDefinitionDescriptor parse(String json) throws IOException {
        return MVDefinitionDescriptor.fromXContent(createParser(JsonXContent.jsonXContent, json));
    }
}
