/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api;

import static org.junit.Assert.assertEquals;

import org.apache.calcite.rel.RelNode;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.sql.executor.QueryType;

public class UnifiedQueryTranspilerTest extends UnifiedQueryTestBase {

  private UnifiedQueryPlanner planner;
  private UnifiedQueryTranspiler transpiler;

  @Before
  public void setUp() {
    super.setUp();
    planner =
        UnifiedQueryPlanner.builder()
            .language(QueryType.PPL)
            .catalog("catalog", testSchema)
            .defaultNamespace("catalog")
            .build();

    transpiler = new UnifiedQueryTranspiler();
  }

  @Test
  public void testSimpleSourceQuery() {
    String pplQuery = "source = employees";
    RelNode plan = planner.plan(pplQuery);

    TranspileOptions options = TranspileOptions.builder().prettyPrint(false).build();
    String sql = transpiler.toSql(plan, options);

    String expectedSql = "SELECT *\nFROM `catalog`.`employees`";
    assertEquals("Generated SQL should match expected", expectedSql, sql);
  }

  @Test
  public void testQueryWithFieldsProjection() {
    String pplQuery = "source = employees | fields name, age";
    RelNode plan = planner.plan(pplQuery);

    TranspileOptions options = TranspileOptions.builder().prettyPrint(false).build();
    String sql = transpiler.toSql(plan, options);

    String expectedSql = "SELECT `name`, `age`\nFROM `catalog`.`employees`";
    assertEquals("Generated SQL should match expected", expectedSql, sql);
  }

  @Test
  public void testQueryWithFilter() {
    String pplQuery = "source = employees | where age > 30";
    RelNode plan = planner.plan(pplQuery);

    TranspileOptions options = TranspileOptions.builder().prettyPrint(false).build();
    String sql = transpiler.toSql(plan, options);

    String expectedSql = "SELECT *\nFROM `catalog`.`employees`\nWHERE `age` > 30";
    assertEquals("Generated SQL should match expected", expectedSql, sql);
  }

  @Test
  public void testQueryWithAggregation() {
    String pplQuery = "source = employees | stats avg(age) by department";
    RelNode plan = planner.plan(pplQuery);

    TranspileOptions options = TranspileOptions.builder().prettyPrint(false).build();
    String sql = transpiler.toSql(plan, options);

    String expectedSql =
        "SELECT AVG(`age`) `avg(age)`, `department`\n"
            + "FROM `catalog`.`employees`\n"
            + "GROUP BY `department`";
    assertEquals("Generated SQL should match expected", expectedSql, sql);
  }
}
