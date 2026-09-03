/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner.mv;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Join;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.core.Union;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.planner.RelNodeUtils;
import org.opensearch.analytics.planner.mv.MVShapeResult.AggToken;
import org.opensearch.analytics.planner.mv.MVShapeResult.ColumnType;
import org.opensearch.analytics.planner.mv.MVShapeResult.Reason;
import org.opensearch.analytics.planner.rel.AggregateMode;
import org.opensearch.analytics.planner.rel.AnnotatedProjectExpression;
import org.opensearch.analytics.planner.rel.OpenSearchAggregate;
import org.opensearch.analytics.planner.rel.OpenSearchExchangeReducer;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;

/**
 * QTF-style post-CBO shape matcher that decides whether a PPL/SQL-derived Calcite
 * {@link RelNode} plan can be materialized as a materialized view (MV), and — on a
 * match — extracts a language-agnostic {@link MVShapeResult descriptor shape}.
 *
 * <p>It mirrors {@code OpenSearchLateMaterializationRewriter}: a read-only walk that
 * returns a structured result (never throws on an unsupported shape) and emits
 * {@code [MV-SHAPE] matched/rejected} debug logging. The output is byte-shaped to the
 * {@code MVDefinitionDescriptor} wire contract of the {@code mv-data-format} plugin, so
 * a matched plan round-trips through {@code MVDefinitionDescriptor.fromXContent} +
 * {@code MVCompiledDefinition.fromDescriptor} with no source-language coupling.
 *
 * <h2>Accepted shape</h2>
 * An optional trailing {@link Project} (SELECT list / AVG quotient) above an
 * {@link Aggregate}, over an optional deterministic {@link Project} (derived group
 * keys), over a single {@link TableScan}. Distributed plans are normalized: a
 * {@code FINAL} {@link OpenSearchAggregate} is descended through its
 * {@link OpenSearchExchangeReducer} to the {@code PARTIAL} aggregate (which carries the
 * real source fields and group expressions), and leading exchange reducers are skipped.
 *
 * <ul>
 *   <li>at least one group key is <b>required</b> (keys define the state sort order);</li>
 *   <li>aggregates must be in {@code {SUM, MIN, MAX, COUNT(*), COUNT(field), AVG(field)}};</li>
 *   <li>no {@code DISTINCT} / {@code FILTER(WHERE)} / approximate aggregates;</li>
 *   <li>group-key expressions must be in the derived-key whitelist (input refs;
 *       {@code + - * /} over refs and literals; {@code CAST}; {@code FLOOR}/{@code CEIL})
 *       and reference exactly one source column;</li>
 *   <li>group-key types must map to one of {@code {KEYWORD, LONG, INTEGER, DOUBLE}}.</li>
 * </ul>
 *
 * <h2>Rejections</h2>
 * Every disqualifier is reported as an {@link MVShapeResult.Reason} with a message —
 * see {@link MVShapeResult.Reason} for the full set (JOIN, WINDOW, SORT_OR_LIMIT,
 * FILTER_WHERE, DISTINCT_AGG, UNSUPPORTED_AGG, SUBQUERY_OR_MULTI_TABLE,
 * NON_DETERMINISTIC_EXPR, ZERO_GROUP_KEYS, UNSUPPORTED_KEY_EXPR, UNSUPPORTED_TYPE).
 *
 * @opensearch.internal
 */
public final class MVShapeMatcher {

    private static final Logger LOGGER = LogManager.getLogger(MVShapeMatcher.class);

    private MVShapeMatcher() {}

    /** Internal control-flow signal for a rejection discovered mid-walk. */
    private static final class Reject extends RuntimeException {
        private final Reason reason;

        Reject(Reason reason, String message) {
            super(message);
            this.reason = reason;
        }
    }

    /**
     * Match {@code root} against the MV shape, with no source-language provenance.
     *
     * @param root the post-CBO plan root
     * @return a {@link MVShapeResult} — {@code MATCHED} with the descriptor shape, or {@code REJECTED}
     */
    public static MVShapeResult match(RelNode root) {
        return match(root, null, null);
    }

    /**
     * Match {@code root} against the MV shape, embedding the given provenance in the
     * emitted descriptor JSON.
     *
     * @param root           the post-CBO plan root
     * @param sourceLanguage optional source language (e.g. {@code "ppl"}), or {@code null}
     * @param sourceText     optional original source text, or {@code null}
     * @return a {@link MVShapeResult} — {@code MATCHED} with the descriptor shape, or {@code REJECTED}
     */
    public static MVShapeResult match(RelNode root, String sourceLanguage, String sourceText) {
        try {
            MVShapeResult result = doMatch(root, sourceLanguage, sourceText);
            if (result.isMatched()) {
                LOGGER.debug(
                    "[MV-SHAPE] matched: groupKeys={}, aggregates={}",
                    result.groupKeys().size(),
                    result.aggregates().size()
                );
            }
            return result;
        } catch (Reject r) {
            LOGGER.debug("[MV-SHAPE] rejected: reason={}, message={}", r.reason, r.getMessage());
            return MVShapeResult.rejected(r.reason, r.getMessage());
        }
    }

    // ── Core walk ──────────────────────────────────────────────────────

    private static MVShapeResult doMatch(RelNode root, String sourceLanguage, String sourceText) {
        RelNode node = strip(root);

        // 1. Whole-tree disqualifiers, in priority order.
        rejectWholeTreeDisqualifiers(node);

        // 2. Optional trailing Project (SELECT list / AVG quotient) above the aggregate.
        Project topProject = null;
        if (node instanceof Project p) {
            topProject = p;
            node = strip(getInput(p));
        }

        // 3. Aggregate spine. Distributed FINAL -> descend through the ER to PARTIAL.
        if ((node instanceof Aggregate) == false) {
            throw new Reject(Reason.ZERO_GROUP_KEYS, "plan has no GROUP BY aggregation; an MV must group by >= 1 key");
        }
        Aggregate baseAgg = (Aggregate) node;
        if (node instanceof OpenSearchAggregate osa && osa.getMode() == AggregateMode.FINAL) {
            RelNode below = strip(getInput(osa));
            if (below instanceof Aggregate partial) {
                baseAgg = partial;
            }
        }

        // 4. Source side: optional deterministic Project, then a single TableScan.
        RelNode src = strip(getInput(baseAgg));
        Project sourceProject = null;
        if (src instanceof Project sp) {
            sourceProject = sp;
            src = strip(getInput(sp));
        }
        if (src instanceof Filter) {
            throw new Reject(Reason.FILTER_WHERE, "a WHERE/HAVING filter is present");
        }
        if ((src instanceof TableScan) == false) {
            throw new Reject(Reason.SUBQUERY_OR_MULTI_TABLE, "aggregate source is not a single table scan: " + src.getClass().getSimpleName());
        }
        TableScan scan = (TableScan) src;
        RelDataType scanRowType = scan.getRowType();

        // 5. Group keys (GROUP BY order == ascending groupSet order == output order).
        List<Integer> groupOrdinals = baseAgg.getGroupSet().asList();
        if (groupOrdinals.isEmpty()) {
            throw new Reject(Reason.ZERO_GROUP_KEYS, "aggregate has zero group keys; an MV must group by >= 1 key");
        }
        int numGroupKeys = groupOrdinals.size();

        List<MVShapeResult.GroupKey> groupKeys = new ArrayList<>(numGroupKeys);
        for (int k = 0; k < numGroupKeys; k++) {
            int g = groupOrdinals.get(k);
            ColumnType type = mapType(baseAgg.getRowType().getFieldList().get(k).getType());
            String alias = groupAlias(k, topProject, baseAgg);
            if (sourceProject != null) {
                RexNode expr = unwrapAnnotations(sourceProject.getProjects().get(g));
                if (expr instanceof RexInputRef ref) {
                    String os = scanRowType.getFieldList().get(ref.getIndex()).getName();
                    groupKeys.add(new MVShapeResult.GroupKey(alias, type, null, alias.equals(os) ? null : os));
                } else {
                    groupKeys.add(expressionKey(alias, type, expr, scanRowType));
                }
            } else {
                String os = scanRowType.getFieldList().get(g).getName();
                groupKeys.add(new MVShapeResult.GroupKey(alias, type, null, alias.equals(os) ? null : os));
            }
        }

        // 6. Aggregates. With a trailing project, the aggregate outputs surface through it
        //    (and AVG reappears as a division over the SUM/COUNT primitives); without one,
        //    the base aggregate's calls are the result directly.
        List<AggregateCall> aggCalls = baseAgg.getAggCallList();
        List<MVShapeResult.Aggregate> aggregates = new ArrayList<>();
        if (topProject != null) {
            List<RelDataTypeField> outFields = topProject.getRowType().getFieldList();
            for (int i = 0; i < topProject.getProjects().size(); i++) {
                RexNode expr = peelCast(unwrapAnnotations(topProject.getProjects().get(i)));
                String outName = outFields.get(i).getName();
                if (expr instanceof RexInputRef ref) {
                    if (ref.getIndex() < numGroupKeys) {
                        continue; // group-key passthrough — already extracted
                    }
                    int aggIdx = ref.getIndex() - numGroupKeys;
                    aggregates.add(aggregateFromCall(callAt(aggCalls, aggIdx), outName, sourceProject, scanRowType));
                } else if (expr instanceof RexCall call && call.getKind() == SqlKind.DIVIDE) {
                    aggregates.add(reconstructAvg(call, numGroupKeys, aggCalls, outName, sourceProject, scanRowType));
                } else {
                    throw new Reject(
                        Reason.UNSUPPORTED_AGG,
                        "output column [" + outName + "] is a derived expression an MV cannot materialize"
                    );
                }
            }
        } else {
            for (AggregateCall call : aggCalls) {
                aggregates.add(aggregateFromCall(call, aliasFor(call), sourceProject, scanRowType));
            }
        }
        if (aggregates.isEmpty()) {
            throw new Reject(Reason.UNSUPPORTED_AGG, "no aggregate outputs found");
        }

        String json = buildDescriptorJson(groupKeys, aggregates, sourceLanguage, sourceText);
        return MVShapeResult.matched(groupKeys, aggregates, json);
    }

    // ── Whole-tree disqualifiers ────────────────────────────────────────

    private static void rejectWholeTreeDisqualifiers(RelNode node) {
        if (RelNodeUtils.findAllNodes(node, Join.class).isEmpty() == false) {
            throw new Reject(Reason.JOIN, "plan contains a join; MV v1 is single-table only");
        }
        if (RelNodeUtils.findAllNodes(node, Union.class).isEmpty() == false) {
            throw new Reject(Reason.SUBQUERY_OR_MULTI_TABLE, "plan contains a set operation (UNION/INTERSECT/MINUS)");
        }
        for (Project p : RelNodeUtils.findAllNodes(node, Project.class)) {
            if (p.containsOver()) {
                throw new Reject(Reason.WINDOW, "plan contains a window function (OVER)");
            }
        }
        if (RelNodeUtils.findAllNodes(node, Sort.class).isEmpty() == false) {
            throw new Reject(Reason.SORT_OR_LIMIT, "plan contains an ORDER BY / LIMIT (top-N)");
        }
        if (RelNodeUtils.findAllNodes(node, TableScan.class).size() > 1) {
            throw new Reject(Reason.SUBQUERY_OR_MULTI_TABLE, "plan references more than one table");
        }
        if (RelNodeUtils.findAllNodes(node, Filter.class).isEmpty() == false) {
            throw new Reject(Reason.FILTER_WHERE, "plan contains a WHERE/HAVING filter");
        }
    }

    // ── Aggregate extraction ────────────────────────────────────────────

    private static MVShapeResult.Aggregate aggregateFromCall(
        AggregateCall call,
        String alias,
        Project sourceProject,
        RelDataType scanRowType
    ) {
        if (call.isDistinct()) {
            throw new Reject(Reason.DISTINCT_AGG, "DISTINCT aggregate [" + call.getAggregation().getName() + "] not supported");
        }
        if (call.filterArg >= 0) {
            throw new Reject(Reason.DISTINCT_AGG, "aggregate FILTER(WHERE) not supported");
        }
        SqlKind kind = call.getAggregation().getKind();
        String opName = call.getAggregation().getName();
        String safeAlias = (alias == null || alias.isBlank()) ? synthAlias(kind, opName) : alias;
        return switch (kind) {
            case SUM, SUM0 -> new MVShapeResult.Aggregate(AggToken.SUM, requireField(call, sourceProject, scanRowType), safeAlias);
            case MIN -> new MVShapeResult.Aggregate(AggToken.MIN, requireField(call, sourceProject, scanRowType), safeAlias);
            case MAX -> new MVShapeResult.Aggregate(AggToken.MAX, requireField(call, sourceProject, scanRowType), safeAlias);
            case COUNT -> call.getArgList().isEmpty()
                ? new MVShapeResult.Aggregate(AggToken.COUNT, null, safeAlias)
                : new MVShapeResult.Aggregate(AggToken.COUNT_FIELD, requireField(call, sourceProject, scanRowType), safeAlias);
            case AVG -> new MVShapeResult.Aggregate(AggToken.AVG, requireField(call, sourceProject, scanRowType), safeAlias);
            default -> {
                if ("APPROX_COUNT_DISTINCT".equals(opName)) {
                    throw new Reject(Reason.DISTINCT_AGG, "approximate distinct-count aggregate not supported");
                }
                throw new Reject(Reason.UNSUPPORTED_AGG, "unsupported aggregate function [" + opName + "]");
            }
        };
    }

    /**
     * Reconstruct an {@code AVG(field)} from the {@code SUM(field) / COUNT(*)} decomposition
     * that CBO leaves as a division in the trailing project.
     */
    private static MVShapeResult.Aggregate reconstructAvg(
        RexCall division,
        int numGroupKeys,
        List<AggregateCall> aggCalls,
        String alias,
        Project sourceProject,
        RelDataType scanRowType
    ) {
        RexNode left = peelCast(unwrapAnnotations(division.getOperands().get(0)));
        RexNode right = peelCast(unwrapAnnotations(division.getOperands().get(1)));
        if ((left instanceof RexInputRef sumRef) == false || (right instanceof RexInputRef countRef) == false) {
            throw new Reject(Reason.UNSUPPORTED_AGG, "output column [" + alias + "] is a derived expression an MV cannot materialize");
        }
        int sumIdx = ((RexInputRef) left).getIndex() - numGroupKeys;
        int countIdx = ((RexInputRef) right).getIndex() - numGroupKeys;
        AggregateCall sumCall = callAt(aggCalls, sumIdx);
        AggregateCall countCall = callAt(aggCalls, countIdx);
        boolean sumOk = sumCall.getAggregation().getKind() == SqlKind.SUM || sumCall.getAggregation().getKind() == SqlKind.SUM0;
        boolean countOk = countCall.getAggregation().getKind() == SqlKind.COUNT && countCall.getArgList().isEmpty();
        if (sumOk == false || countOk == false) {
            throw new Reject(Reason.UNSUPPORTED_AGG, "output column [" + alias + "] is a derived expression an MV cannot materialize");
        }
        String field = requireField(sumCall, sourceProject, scanRowType);
        return new MVShapeResult.Aggregate(AggToken.AVG, field, alias);
    }

    private static AggregateCall callAt(List<AggregateCall> aggCalls, int idx) {
        if (idx < 0 || idx >= aggCalls.size()) {
            throw new Reject(Reason.UNSUPPORTED_AGG, "aggregate output references a non-aggregate column");
        }
        return aggCalls.get(idx);
    }

    private static String requireField(AggregateCall call, Project sourceProject, RelDataType scanRowType) {
        if (call.getArgList().isEmpty()) {
            throw new Reject(Reason.UNSUPPORTED_AGG, "aggregate [" + call.getAggregation().getName() + "] requires a source field");
        }
        String field = sourceFieldName(call.getArgList().get(0), sourceProject, scanRowType);
        if (field == null) {
            throw new Reject(
                Reason.UNSUPPORTED_AGG,
                "aggregate [" + call.getAggregation().getName() + "] over a derived expression is not supported"
            );
        }
        return field;
    }

    /** Resolve an aggregate/group argument ordinal to its underlying OpenSearch source field name. */
    private static String sourceFieldName(int argOrdinal, Project sourceProject, RelDataType scanRowType) {
        if (sourceProject != null) {
            RexNode e = unwrapAnnotations(sourceProject.getProjects().get(argOrdinal));
            if (e instanceof RexInputRef ref) {
                return scanRowType.getFieldList().get(ref.getIndex()).getName();
            }
            return null; // aggregating over an expression
        }
        return scanRowType.getFieldList().get(argOrdinal).getName();
    }

    // ── Group-key expression handling ───────────────────────────────────

    private static MVShapeResult.GroupKey expressionKey(String alias, ColumnType type, RexNode expr, RelDataType scanRowType) {
        checkDeterministic(expr);
        checkKeyExprWhitelist(expr);
        Set<Integer> refs = collectInputRefs(expr);
        if (refs.size() != 1) {
            throw new Reject(
                Reason.UNSUPPORTED_KEY_EXPR,
                "group-key expression must reference exactly one source column (found " + refs.size() + ")"
            );
        }
        String os = scanRowType.getFieldList().get(refs.iterator().next()).getName();
        String sql = renderSql(expr, scanRowType);

        // Detect date_bin/date_trunc patterns for span key extraction.
        long spanMs = extractSpanIntervalMs(expr);
        if (spanMs > 0) {
            return new MVShapeResult.GroupKey(alias, ColumnType.TIMESTAMP, sql, os, spanMs);
        }
        return new MVShapeResult.GroupKey(alias, type, sql, os, -1);
    }

    /**
     * Try to extract a span interval in milliseconds from a date_bin/date_trunc
     * RexCall. Returns -1 if the expression is not a recognized span pattern.
     */
    private static long extractSpanIntervalMs(RexNode expr) {
        if (expr instanceof RexCall spanCall
            && spanCall.getKind() == SqlKind.OTHER_FUNCTION) {
            String fn = spanCall.getOperator().getName().toUpperCase(java.util.Locale.ROOT);
            if ("DATE_BIN".equals(fn) || "DATE_TRUNC".equals(fn)) {
                if (spanCall.getOperands().isEmpty() == false) {
                    RexNode intervalOp = unwrapAnnotations(spanCall.getOperands().get(0));
                    if (intervalOp instanceof RexLiteral lit) {
                        return intervalLiteralToMs(lit);
                    }
                }
            }
        }
        return -1;
    }

    /** Convert a Calcite interval literal to milliseconds, returning -1 on failure. */
    private static long intervalLiteralToMs(RexLiteral lit) {
        SqlTypeName tn = lit.getTypeName();
        // Calcite represents INTERVAL 'N MINUTES' as a BigDecimal of milliseconds for
        // INTERVAL_DAY_SECOND types, or months for INTERVAL_YEAR_MONTH types.
        if (tn.getFamily() == org.apache.calcite.sql.type.SqlTypeFamily.INTERVAL_DAY_TIME) {
            BigDecimal ms = lit.getValueAs(BigDecimal.class);
            if (ms != null) {
                return ms.longValueExact();
            }
        }
        return -1;
    }

    private static void checkDeterministic(RexNode expr) {
        expr.accept(new RexVisitorImpl<Void>(true) {
            @Override
            public Void visitCall(RexCall call) {
                SqlOperator op = call.getOperator();
                if (op.isDeterministic() == false || op.isDynamicFunction()) {
                    throw new Reject(Reason.NON_DETERMINISTIC_EXPR, "non-deterministic expression [" + op.getName() + "]");
                }
                return super.visitCall(call);
            }
        });
    }

    private static void checkKeyExprWhitelist(RexNode expr) {
        if (expr instanceof RexInputRef || expr instanceof RexLiteral) {
            return;
        }
        if (expr instanceof RexCall call) {
            switch (call.getKind()) {
                case PLUS, MINUS, TIMES, DIVIDE, CAST, FLOOR, CEIL -> {
                    for (RexNode operand : call.getOperands()) {
                        checkKeyExprWhitelist(unwrapAnnotations(operand));
                    }
                    return;
                }
                case OTHER_FUNCTION -> {
                    // Allow date_trunc, date_bin and similar time-bucketing functions.
                    String fnName = call.getOperator().getName().toUpperCase(java.util.Locale.ROOT);
                    if ("DATE_TRUNC".equals(fnName) || "DATE_BIN".equals(fnName)) {
                        return;
                    }
                    throw new Reject(
                        Reason.UNSUPPORTED_KEY_EXPR,
                        "group-key expression uses unsupported function [" + call.getOperator().getName() + "]"
                    );
                }
                default -> throw new Reject(
                    Reason.UNSUPPORTED_KEY_EXPR,
                    "group-key expression uses unsupported operator [" + call.getOperator().getName() + "]"
                );
            }
        }
        throw new Reject(Reason.UNSUPPORTED_KEY_EXPR, "group-key expression is not supported: " + expr);
    }

    /** Render a whitelisted key expression to DataFusion-compatible SQL with quoted source columns. */
    private static String renderSql(RexNode expr, RelDataType scanRowType) {
        if (expr instanceof RexInputRef ref) {
            return "\"" + scanRowType.getFieldList().get(ref.getIndex()).getName() + "\"";
        }
        if (expr instanceof RexLiteral lit) {
            return renderLiteral(lit);
        }
        RexCall call = (RexCall) expr;
        List<RexNode> ops = call.getOperands();
        return switch (call.getKind()) {
            case CAST -> "CAST(" + renderSql(unwrapAnnotations(ops.get(0)), scanRowType) + " AS " + call.getType().getSqlTypeName().getName()
                + ")";
            case FLOOR -> "FLOOR(" + renderSql(unwrapAnnotations(ops.get(0)), scanRowType) + ")";
            case CEIL -> "CEIL(" + renderSql(unwrapAnnotations(ops.get(0)), scanRowType) + ")";
            case PLUS -> binary(ops, "+", scanRowType);
            case MINUS -> binary(ops, "-", scanRowType);
            case TIMES -> binary(ops, "*", scanRowType);
            case DIVIDE -> binary(ops, "/", scanRowType);
            case OTHER_FUNCTION -> renderFunction(call, scanRowType);
            default -> throw new Reject(
                Reason.UNSUPPORTED_KEY_EXPR,
                "group-key expression uses unsupported operator [" + call.getOperator().getName() + "]"
            );
        };
    }

    /**
     * Render a whitelisted function call (date_bin, date_trunc) to DataFusion SQL.
     * Preserves the function name and renders all operands.
     */
    private static String renderFunction(RexCall call, RelDataType scanRowType) {
        String fnName = call.getOperator().getName();
        StringBuilder sb = new StringBuilder(fnName).append("(");
        for (int i = 0; i < call.getOperands().size(); i++) {
            if (i > 0) {
                sb.append(", ");
            }
            sb.append(renderSql(unwrapAnnotations(call.getOperands().get(i)), scanRowType));
        }
        sb.append(")");
        return sb.toString();
    }

    private static String binary(List<RexNode> ops, String op, RelDataType scanRowType) {
        return renderSql(unwrapAnnotations(ops.get(0)), scanRowType) + " " + op + " " + renderSql(unwrapAnnotations(ops.get(1)), scanRowType);
    }

    private static String renderLiteral(RexLiteral lit) {
        // Interval literals: render as INTERVAL 'N unit' for DataFusion compatibility.
        SqlTypeName tn = lit.getTypeName();
        if (tn.getFamily() == org.apache.calcite.sql.type.SqlTypeFamily.INTERVAL_DAY_TIME) {
            BigDecimal ms = lit.getValueAs(BigDecimal.class);
            if (ms != null) {
                return "INTERVAL '" + formatIntervalMs(ms.longValueExact()) + "'";
            }
        }

        Object v = lit.getValue2();
        if (v == null) {
            return "NULL";
        }
        if (v instanceof BigDecimal bd) {
            return bd.stripTrailingZeros().toPlainString();
        }
        if (v instanceof Number n) {
            return n.toString();
        }
        if (v instanceof String s) {
            return "'" + s.replace("'", "''") + "'";
        }
        return v.toString();
    }

    /** Format milliseconds to a human-readable SQL INTERVAL value string. */
    private static String formatIntervalMs(long ms) {
        if (ms % 3600_000L == 0) {
            return (ms / 3600_000L) + " hours";
        }
        if (ms % 60_000L == 0) {
            return (ms / 60_000L) + " minutes";
        }
        if (ms % 1000L == 0) {
            return (ms / 1000L) + " seconds";
        }
        return ms + " milliseconds";
    }

    // ── Type mapping ────────────────────────────────────────────────────

    private static ColumnType mapType(RelDataType type) {
        SqlTypeName t = type.getSqlTypeName();
        return switch (t) {
            case VARCHAR, CHAR -> ColumnType.KEYWORD;
            case BIGINT -> ColumnType.LONG;
            case INTEGER, SMALLINT, TINYINT -> ColumnType.INTEGER;
            case DOUBLE, FLOAT, REAL, DECIMAL -> ColumnType.DOUBLE;
            case TIMESTAMP, TIMESTAMP_WITH_LOCAL_TIME_ZONE, DATE, TIME -> ColumnType.TIMESTAMP;
            default -> throw new Reject(Reason.UNSUPPORTED_TYPE, "unsupported group-key type [" + t + "]");
        };
    }

    // ── Alias helpers ───────────────────────────────────────────────────

    private static String groupAlias(int outputOrdinal, Project topProject, Aggregate baseAgg) {
        if (topProject != null) {
            List<RexNode> projects = topProject.getProjects();
            for (int i = 0; i < projects.size(); i++) {
                RexNode e = peelCast(unwrapAnnotations(projects.get(i)));
                if (e instanceof RexInputRef ref && ref.getIndex() == outputOrdinal) {
                    return topProject.getRowType().getFieldList().get(i).getName();
                }
            }
        }
        return baseAgg.getRowType().getFieldList().get(outputOrdinal).getName();
    }

    private static String aliasFor(AggregateCall call) {
        String name = call.getName();
        if (name == null || name.isBlank()) {
            return synthAlias(call.getAggregation().getKind(), call.getAggregation().getName());
        }
        return name;
    }

    private static String synthAlias(SqlKind kind, String opName) {
        return opName.toLowerCase(java.util.Locale.ROOT) + "_agg";
    }

    // ── RexNode utilities ───────────────────────────────────────────────

    /** Recursively unwrap {@link AnnotatedProjectExpression} wrappers to the underlying expression. */
    private static RexNode unwrapAnnotations(RexNode e) {
        return e.accept(new RexShuttle() {
            @Override
            public RexNode visitCall(RexCall call) {
                if (call instanceof AnnotatedProjectExpression a) {
                    return a.getOriginal().accept(this);
                }
                return super.visitCall(call);
            }
        });
    }

    /** Peel outer {@code CAST(...)} wrappers (CBO type coercion) to expose the inner expression. */
    private static RexNode peelCast(RexNode e) {
        while (e instanceof RexCall call && call.getKind() == SqlKind.CAST) {
            e = unwrapAnnotations(call.getOperands().get(0));
        }
        return e;
    }

    private static Set<Integer> collectInputRefs(RexNode expr) {
        Set<Integer> refs = new LinkedHashSet<>();
        expr.accept(new RexVisitorImpl<Void>(true) {
            @Override
            public Void visitInputRef(RexInputRef ref) {
                refs.add(ref.getIndex());
                return null;
            }
        });
        return refs;
    }

    /** Unwrap HepRelVertex and skip leading exchange reducers to reach the next logical operator. */
    private static RelNode strip(RelNode n) {
        n = RelNodeUtils.unwrapHep(n);
        while (n instanceof OpenSearchExchangeReducer er) {
            n = RelNodeUtils.unwrapHep(er.getInput());
        }
        return n;
    }

    private static RelNode getInput(RelNode singleInput) {
        return singleInput.getInput(0);
    }

    // ── Descriptor JSON emission (MVDefinitionDescriptor wire contract) ──

    private static String buildDescriptorJson(
        List<MVShapeResult.GroupKey> groupKeys,
        List<MVShapeResult.Aggregate> aggregates,
        String sourceLanguage,
        String sourceText
    ) {
        try (XContentBuilder b = XContentBuilder.builder(MediaTypeRegistry.JSON.xContent())) {
            b.startObject();
            b.field("descriptor_version", 1);
            if (sourceLanguage != null) {
                b.field("source_language", sourceLanguage);
            }
            if (sourceText != null) {
                b.field("source_text", sourceText);
            }
            b.startArray("group_keys");
            for (MVShapeResult.GroupKey k : groupKeys) {
                b.startObject();
                b.field("name", k.name());
                b.field("column_type", k.type().name());
                if (k.expression() != null) {
                    b.field("expression", k.expression());
                }
                if (k.sourceColumn() != null) {
                    b.field("source_column", k.sourceColumn());
                }
                if (k.spanIntervalMs() > 0) {
                    b.field("span_interval_ms", k.spanIntervalMs());
                }
                b.endObject();
            }
            b.endArray();
            b.startArray("aggregates");
            for (MVShapeResult.Aggregate a : aggregates) {
                b.startObject();
                b.field("function", a.function().name());
                if (a.field() != null) {
                    b.field("field", a.field());
                }
                b.field("alias", a.alias());
                b.endObject();
            }
            b.endArray();
            b.endObject();
            return b.toString();
        } catch (IOException e) {
            // In-memory JSON assembly does not perform I/O; a failure here is not an "unsupported shape".
            throw new IllegalStateException("failed to build MV descriptor JSON", e);
        }
    }
}
