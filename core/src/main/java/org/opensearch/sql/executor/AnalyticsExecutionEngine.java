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
import org.opensearch.sql.ast.statement.ExplainMode;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.executor.pagination.Cursor;
import org.opensearch.sql.planner.physical.PhysicalPlan;

/**
 * Execution engine that hands off RelNode plans to the Analytics engine via transport action. The
 * Analytics engine executes the plan (e.g., via DataFusion) and returns results.
 *
 * <p>Currently a stub: transport action submission is not yet wired. Returns empty results.
 */
public class AnalyticsExecutionEngine implements ExecutionEngine {

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
      // TODO: Serialize RelNode and submit to Analytics engine via transport action
      // client.execute(AnalyticsQueryAction.INSTANCE, new AnalyticsQueryRequest(relNodeJson),
      //     ActionListener.wrap(response -> listener.onResponse(response), listener::onFailure));

      // TODO: Worker thread
      // Stub: return empty result with schema derived from RelNode
      List<Schema.Column> columns =
          plan.getRowType().getFieldList().stream()
              .map(
                  f ->
                      new Schema.Column(
                          f.getName(),
                          null,
                          OpenSearchTypeFactory.convertRelDataTypeToExprType(f.getType())))
              .toList();
      listener.onResponse(
              // TOOD: no dependency on ExprValue. Java object format response.
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
