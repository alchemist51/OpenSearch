/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.be.lucene.serializers;

import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.opensearch.analytics.spi.FieldStorageInfo;
import org.opensearch.be.lucene.CalciteToOSMapperConversionUtils;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.index.query.TermQueryBuilder;

import java.util.List;

/**
 * Serializer for the NOT_EQUALS operator on a single column with a literal value
 * (e.g. {@code tag &lt;&gt; 'hello'} or {@code status &lt;&gt; 200}). The dual to
 * {@link EqualsSerializer} — used when a {@code !=} predicate is delegated to
 * Lucene as a peer (correctness or performance delegation).
 *
 * <p>Expected RexCall shape: {@code &lt;&gt;($colIdx, literal)}. Either operand
 * order is accepted ({@code literal &lt;&gt; $colIdx} also works).
 *
 * <p>OpenSearch's DSL has no native "not equals" leaf — the canonical form is
 * {@code bool { must_not: term {...} }}. We emit that shape directly, which the
 * field's mapper resolves at {@code toQuery(qsc)} time. Type coercion of the
 * literal happens via {@link CalciteToOSMapperConversionUtils} so the mapper
 * sees plain Java types (Number, String, etc.) rather than Calcite's BigDecimal
 * / NlsString wrappers.
 *
 * <p><b>NULL semantics caveat.</b> SQL's {@code col &lt;&gt; literal} returns
 * UNKNOWN (filtered out) when {@code col} is NULL. Lucene's
 * {@code must_not term} excludes only matching docs — non-matching including
 * missing-field docs are kept. The planner-level NOT_EQUALS handling is
 * expected to add an {@code IS NOT NULL} guard upstream when SQL three-valued
 * semantics are required; this serializer only translates the equality
 * inversion.
 */
public class NotEqualsSerializer extends AbstractQuerySerializer {

    @Override
    public QueryBuilder buildQueryBuilder(RexCall call, List<FieldStorageInfo> fieldStorage) {
        if (call.getOperands().size() != 2) {
            throw new IllegalArgumentException("NOT_EQUALS expects 2 operands, got " + call.getOperands().size());
        }
        RexNode left = call.getOperands().get(0);
        RexNode right = call.getOperands().get(1);

        // Either (column, literal) or (literal, column).
        RexInputRef columnRef;
        RexLiteral valueLit;
        if (left instanceof RexInputRef l && right instanceof RexLiteral r) {
            columnRef = l;
            valueLit = r;
        } else if (left instanceof RexLiteral l && right instanceof RexInputRef r) {
            columnRef = r;
            valueLit = l;
        } else {
            throw new IllegalArgumentException(
                "NOT_EQUALS performance-delegation requires (RexInputRef, RexLiteral); got " + left + " <> " + right
            );
        }

        String fieldName = FieldStorageInfo.resolve(fieldStorage, columnRef.getIndex()).getFieldName();
        // Calcite stores literals in canonical types (BigDecimal for ints, NlsString for
        // strings, etc.) that the OpenSearch Mapper can't parse directly. Convert at the
        // Calcite ↔ OpenSearch boundary so the mapper sees plain Java types.
        Object value = CalciteToOSMapperConversionUtils.literalToOpenSearchValue(valueLit);
        TermQueryBuilder term = new TermQueryBuilder(fieldName, value);
        BoolQueryBuilder bool = QueryBuilders.boolQuery();
        bool.mustNot(term);
        return bool;
    }
}
