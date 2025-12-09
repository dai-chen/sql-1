/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.runtime;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import org.apache.calcite.rel.RelNode;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.sql.api.UnifiedQueryTestBase;
import org.opensearch.sql.common.antlr.SyntaxCheckException;

public class UnifiedQueryCompilerTest extends UnifiedQueryTestBase implements ResultSetAssertion {

  private UnifiedQueryCompiler compiler;

  @Before
  public void setUp() {
    super.setUp();
    compiler = UnifiedQueryCompiler.builder().context(planner.getContext()).build();
  }

  @Test
  public void testSimpleQuery() throws Exception {
    RelNode plan = planner.plan("source = employees | where age > 30");
    try (PreparedStatement statement = compiler.compile(plan)) {
      ResultSet resultSet = statement.executeQuery();

      verify(resultSet)
          .expectSchema(col("id"), col("name"), col("age"), col("department"))
          .expectData(row(2, "Bob", 35, "Sales"), row(3, "Charlie", 45, "Engineering"));
    }
  }

  @Test
  public void testComplexQuery() throws Exception {
    RelNode plan = planner.plan("source = employees | stats count() by department");
    try (PreparedStatement statement = compiler.compile(plan)) {
      ResultSet resultSet = statement.executeQuery();

      verify(resultSet)
          .expectSchema(col("count()"), col("department"))
          .expectData(row(2L, "Engineering"), row(1L, "Sales"), row(1L, "Marketing"));
    }
  }

  @Test(expected = SyntaxCheckException.class)
  public void testSyntaxError() {
    planner.plan("source = employees | invalid_command");
  }

  @Test(expected = IllegalStateException.class)
  public void testSemanticError() {
    planner.plan("source = nonexistent_table");
  }
}
