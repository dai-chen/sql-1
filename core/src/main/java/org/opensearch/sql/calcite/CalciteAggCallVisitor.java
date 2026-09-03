/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder.AggCall;
import org.apache.logging.log4j.util.Strings;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.AggregateFunction;
import org.opensearch.sql.ast.expression.Alias;
import org.opensearch.sql.ast.expression.Function;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.calcite.utils.PlanUtils;
import org.opensearch.sql.expression.function.BuiltinFunctionName;

public class CalciteAggCallVisitor extends AbstractNodeVisitor<AggCall, CalcitePlanContext> {
  private final CalciteRexNodeVisitor rexNodeVisitor;

  public CalciteAggCallVisitor(CalciteRexNodeVisitor rexNodeVisitor) {
    this.rexNodeVisitor = rexNodeVisitor;
  }

  public AggCall analyze(UnresolvedExpression unresolved, CalcitePlanContext context) {
    return unresolved.accept(this, context);
  }

  @Override
  public AggCall visitAlias(Alias node, CalcitePlanContext context) {
    AggCall aggCall = analyze(node.getDelegated(), context);
    // Only OpenSearch SQL uses node.getAlias, OpenSearch PPL uses node.getName.
    return aggCall.as(Strings.isEmpty(node.getAlias()) ? node.getName() : node.getAlias());
  }

  @Override
  public AggCall visitAggregateFunction(AggregateFunction node, CalcitePlanContext context) {
    RexNode field =
        node.getField() == null ? null : rexNodeVisitor.analyze(node.getField(), context);
    List<RexNode> argList = new ArrayList<>();
    for (UnresolvedExpression arg : node.getArgList()) {
      argList.add(rexNodeVisitor.analyze(arg, context));
    }
    // Apply the optional FILTER(WHERE ...) predicate by guarding the aggregated argument rather
    // than by setting AggregateCall.filterArg -- see guardWithFilterCondition.
    final RexNode aggField =
        node.condition() == null ? field : guardWithFilterCondition(field, node, context);
    return BuiltinFunctionName.ofAggregation(node.getFuncName())
        .map(
            functionName ->
                PlanUtils.makeAggCall(context, functionName, node.getDistinct(), aggField, argList))
        .orElseThrow(
            () ->
                new UnsupportedOperationException("Unexpected aggregation: " + node.getFuncName()));
  }

  /**
   * Rewrites {@code agg(x) FILTER(WHERE cond)} into {@code agg(CASE WHEN cond THEN x END)}, and
   * {@code COUNT(*) FILTER(WHERE cond)} into {@code COUNT(CASE WHEN cond THEN 1 END)}.
   *
   * <p>The equivalent {@link AggCall#filter} form sets Calcite's {@code AggregateCall.filterArg},
   * which the analytics-engine route cannot execute: the filter is serialized to Substrait as an
   * extra aggregate input field, and DataFusion then rejects its own plan because the two sides
   * disagree on that field's nullability ("Physical input schema should be the same as the one
   * converted from logical input schema ... field nullability at index N [&lt;pred&gt; IS TRUE]:
   * (physical) true vs (logical) false"). Guarding the argument keeps the predicate inside an
   * ordinary projection, which both backends handle.
   *
   * <p>Semantics are preserved because every aggregate here ignores NULL arguments, so a row whose
   * predicate is false — or NULL, which the {@code ELSE} branch also covers — does not contribute.
   * The one visible difference is {@code SUM} over a group where no row matches: this form yields
   * NULL (SQL standard) whereas {@code filterArg} on the V2 engine yields 0.
   *
   * <p>Note this rewrite is not undone downstream: Calcite's {@code AggregateCaseToFilterRule},
   * which would convert the CASE back into a filter, is not registered in this repository.
   */
  private RexNode guardWithFilterCondition(
      RexNode field, AggregateFunction node, CalcitePlanContext context) {
    RexNode condition = rexNodeVisitor.analyze(node.condition(), context);
    // COUNT(*) carries no field; count a constant so NULL-skipping does the filtering.
    RexNode value = field != null ? field : context.rexBuilder.makeExactLiteral(BigDecimal.ONE);
    return context.rexBuilder.makeCall(
        SqlStdOperatorTable.CASE,
        condition,
        value,
        context.rexBuilder.makeNullLiteral(value.getType()));
  }

  // Visit special UDAFs that are derived from command. For example, patterns command generates
  // brain function.
  @Override
  public AggCall visitFunction(Function node, CalcitePlanContext context) {
    List<RexNode> argList = new ArrayList<>();
    RexNode field =
        node.getFuncArgs().isEmpty()
            ? null
            : rexNodeVisitor.analyze(node.getFuncArgs().get(0), context);
    for (int i = 1; i < node.getFuncArgs().size(); i++) {
      argList.add(rexNodeVisitor.analyze(node.getFuncArgs().get(i), context));
    }
    return BuiltinFunctionName.ofAggregation(node.getFuncName())
        .map(functionName -> PlanUtils.makeAggCall(context, functionName, false, field, argList))
        .orElseThrow(
            () ->
                new UnsupportedOperationException("Unexpected aggregation: " + node.getFuncName()));
  }
}
