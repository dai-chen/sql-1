/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor;

import java.util.Collections;
import java.util.List;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlExplainLevel;
import org.opensearch.analytics.exec.QueryPlanExecutor;
import org.opensearch.sql.ast.statement.ExplainMode;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.executor.pagination.Cursor;
import org.opensearch.sql.planner.physical.PhysicalPlan;

/**
 * Execution engine that hands off RelNode plans to the Analytics engine for execution. Uses {@link
 * QueryPlanExecutor} to execute plans directly in-process (same JVM, shared classloader via plugin
 * extension).
 */
public class AnalyticsExecutionEngine implements ExecutionEngine {

  private final QueryPlanExecutor<RelNode, Iterable<Object[]>> planExecutor;

  public AnalyticsExecutionEngine(QueryPlanExecutor<RelNode, Iterable<Object[]>> planExecutor) {
    this.planExecutor = planExecutor;
  }

  @Override
  public void execute(PhysicalPlan plan, ResponseListener<QueryResponse> listener) {
    listener.onFailure(
        new UnsupportedOperationException("Analytics engine only supports RelNode execution"));
  }

  @Override
  public void execute(
      PhysicalPlan plan, ExecutionContext context, ResponseListener<QueryResponse> listener) {
    listener.onFailure(
        new UnsupportedOperationException("Analytics engine only supports RelNode execution"));
  }

  @Override
  public void explain(PhysicalPlan plan, ResponseListener<ExplainResponse> listener) {
    listener.onFailure(
        new UnsupportedOperationException("Analytics engine only supports RelNode execution"));
  }

  @Override
  public void execute(
      RelNode plan, CalcitePlanContext context, ResponseListener<QueryResponse> listener) {
    try {
      Iterable<Object[]> results = planExecutor.execute(plan, context);

      List<Schema.Column> columns =
          plan.getRowType().getFieldList().stream()
              .map(
                  f ->
                      new Schema.Column(
                          f.getName(),
                          null,
                          OpenSearchTypeFactory.convertRelDataTypeToExprType(f.getType())))
              .toList();
      // TODO: convert Iterable<Object[]> results to List<ExprValue>
      listener.onResponse(
          new QueryResponse(new Schema(columns), Collections.emptyList(), Cursor.None));
    } catch (Exception e) {
      listener.onFailure(e);
    }
  }

  @Override
  public void explain(
      RelNode plan,
      ExplainMode mode,
      CalcitePlanContext context,
      ResponseListener<ExplainResponse> listener) {
    try {
      SqlExplainLevel level =
          mode == ExplainMode.SIMPLE
              ? SqlExplainLevel.NO_ATTRIBUTES
              : SqlExplainLevel.EXPPLAN_ATTRIBUTES;
      String logical = RelOptUtil.toString(plan, level);
      listener.onResponse(new ExplainResponse(new ExplainResponseNodeV2(logical, null, null)));
    } catch (Exception e) {
      listener.onFailure(e);
    }
  }
}
