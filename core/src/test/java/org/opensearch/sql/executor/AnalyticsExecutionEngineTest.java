/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.util.concurrent.atomic.AtomicReference;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelRecordType;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.ast.statement.ExplainMode;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.common.response.ResponseListener;

class AnalyticsExecutionEngineTest {

  private final AnalyticsExecutionEngine engine = new AnalyticsExecutionEngine();

  @Test
  void executeRelNodeReturnsEmptyResult() {
    RelNode plan = mock(RelNode.class);
    CalcitePlanContext context = mock(CalcitePlanContext.class);
    AtomicReference<ExecutionEngine.QueryResponse> result = new AtomicReference<>();

    engine.execute(
        plan,
        context,
        new ResponseListener<>() {
          @Override
          public void onResponse(ExecutionEngine.QueryResponse response) {
            result.set(response);
          }

          @Override
          public void onFailure(Exception e) {
            throw new AssertionError("Should not fail", e);
          }
        });

    assertNotNull(result.get());
    assertTrue(result.get().getResults().isEmpty());
    assertTrue(result.get().getSchema().getColumns().isEmpty());
  }

  @Test
  void explainRelNodeReturnsLogicalPlan() {
    RelNode plan = mock(RelNode.class);
    RelDataType rowType = new RelRecordType(java.util.List.of());
    org.mockito.Mockito.when(plan.getRowType()).thenReturn(rowType);
    CalcitePlanContext context = mock(CalcitePlanContext.class);
    AtomicReference<ExecutionEngine.ExplainResponse> result = new AtomicReference<>();

    engine.explain(
        plan,
        ExplainMode.STANDARD,
        context,
        new ResponseListener<>() {
          @Override
          public void onResponse(ExecutionEngine.ExplainResponse response) {
            result.set(response);
          }

          @Override
          public void onFailure(Exception e) {
            throw new AssertionError("Should not fail", e);
          }
        });

    assertNotNull(result.get());
    assertNotNull(result.get().getCalcite().getLogical());
  }

  @Test
  void executePhysicalPlanFails() {
    AtomicReference<Exception> error = new AtomicReference<>();

    engine.execute(
        mock(org.opensearch.sql.planner.physical.PhysicalPlan.class),
        new ResponseListener<>() {
          @Override
          public void onResponse(ExecutionEngine.QueryResponse response) {
            throw new AssertionError("Should not succeed");
          }

          @Override
          public void onFailure(Exception e) {
            error.set(e);
          }
        });

    assertNotNull(error.get());
    assertEquals(UnsupportedOperationException.class, error.get().getClass());
  }
}
