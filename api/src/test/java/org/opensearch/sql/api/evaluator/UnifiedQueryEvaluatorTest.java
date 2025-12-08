/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.evaluator;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;
import org.opensearch.sql.api.UnifiedQueryTestBase;
import org.opensearch.sql.common.antlr.SyntaxCheckException;
import org.opensearch.sql.executor.ExecutionEngine;

/** Unit tests for {@link UnifiedQueryEvaluator}. */
public class UnifiedQueryEvaluatorTest extends UnifiedQueryTestBase {

  private UnifiedQueryEvaluator evaluator;

  @Before
  public void setUp() {
    super.setUp();
    evaluator = UnifiedQueryEvaluator.builder().planner(planner).build();
  }

  @Test
  public void testBuilderPattern() {
    UnifiedQueryEvaluator eval = UnifiedQueryEvaluator.builder().planner(planner).build();
    assertNotNull("Evaluator should be created", eval);
  }

  @Test
  public void testSourceQuery() {
    ExecutionEngine.QueryResponse response = evaluator.evaluate("source = employees");

    verifyDataRows(
        response.getResults(),
        new Object[] {1, "Alice", 25, "Engineering"},
        new Object[] {2, "Bob", 35, "Sales"},
        new Object[] {3, "Charlie", 45, "Engineering"},
        new Object[] {4, "Diana", 28, "Marketing"});
  }

  @Test
  public void testWhereQuery() {
    ExecutionEngine.QueryResponse response =
        evaluator.evaluate("source = employees | where age > 30");

    verifyDataRows(
        response.getResults(),
        new Object[] {2, "Bob", 35, "Sales"},
        new Object[] {3, "Charlie", 45, "Engineering"});
  }

  @Test
  public void testStatsQuery() {
    ExecutionEngine.QueryResponse response =
        evaluator.evaluate("source = employees | stats count() by department");

    // Verify we have 3 department groups
    assertEquals(3, response.getResults().size());

    // Verify aggregation results contain required fields
    response
        .getResults()
        .forEach(
            row -> {
              assertTrue(row.tupleValue().containsKey("department"));
              assertTrue(row.tupleValue().containsKey("count()"));
            });
  }

  @Test
  public void testResultConversion() {
    ExecutionEngine.QueryResponse response = evaluator.evaluate("source = employees");

    // Verify results are properly converted to ExprTupleValue
    response
        .getResults()
        .forEach(
            row -> {
              assertNotNull(row.tupleValue());
              assertTrue(row.tupleValue().containsKey("id"));
              assertTrue(row.tupleValue().containsKey("name"));
            });
  }

  @Test(expected = SyntaxCheckException.class)
  public void testSyntaxError() {
    evaluator.evaluate("source = employees | invalid_command");
  }

  @Test(expected = IllegalStateException.class)
  public void testSemanticError() {
    evaluator.evaluate("source = nonexistent_table");
  }

  @Test
  public void testCursorIsNull() {
    ExecutionEngine.QueryResponse response = evaluator.evaluate("source = employees");
    assertEquals(null, response.getCursor());
  }
}
