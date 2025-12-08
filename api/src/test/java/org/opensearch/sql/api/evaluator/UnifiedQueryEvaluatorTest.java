/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.evaluator;

import static org.opensearch.sql.data.type.ExprCoreType.INTEGER;
import static org.opensearch.sql.data.type.ExprCoreType.LONG;
import static org.opensearch.sql.data.type.ExprCoreType.STRING;

import org.junit.Before;
import org.junit.Test;
import org.opensearch.sql.api.UnifiedQueryTestBase;
import org.opensearch.sql.common.antlr.SyntaxCheckException;
import org.opensearch.sql.executor.ExecutionEngine.QueryResponse;

public class UnifiedQueryEvaluatorTest extends UnifiedQueryTestBase
    implements QueryEvaluatorAssertion {

  private UnifiedQueryEvaluator evaluator;

  @Before
  public void setUp() {
    super.setUp();
    evaluator = UnifiedQueryEvaluator.builder().planner(planner).build();
  }

  private QueryEvaluatorAssertion.ResponseAssertion verifyQueryEval(String query) {
    return verify(evaluator.evaluate(query));
  }

  @Test
  public void testSimplyQuery() {
    QueryResponse response = evaluator.evaluate("source = employees | where age > 30");
    verify(response)
        .expectSchema(
            col("id", INTEGER), col("name", STRING), col("age", INTEGER), col("department", STRING))
        .expectData(row(2, "Bob", 35, "Sales"), row(3, "Charlie", 45, "Engineering"));
  }

  @Test
  public void testComplexQuery() {
    QueryResponse response = evaluator.evaluate("source = employees | stats count() by department");
    verify(response)
        .expectSchema(col("count()", LONG), col("department", STRING))
        .expectData(row(2L, "Engineering"), row(1L, "Sales"), row(1L, "Marketing"));
  }

  @Test(expected = SyntaxCheckException.class)
  public void testSyntaxError() {
    verifyQueryEval("source = employees | invalid_command");
  }

  @Test(expected = IllegalStateException.class)
  public void testSemanticError() {
    verifyQueryEval("source = nonexistent_table");
  }
}
