/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

import org.apache.calcite.rel.RelNode;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.sql.executor.QueryType;

public class UnifiedQueryPlannerSqlTest extends UnifiedQueryTestBase {

  @Override
  @Before
  public void setUp() {
    testSchema =
        new org.apache.calcite.schema.impl.AbstractSchema() {
          @Override
          protected java.util.Map<String, org.apache.calcite.schema.Table> getTableMap() {
            return java.util.Map.of("employees", createEmployeesTable());
          }
        };

    context =
        UnifiedQueryContext.builder()
            .language(QueryType.SQL)
            .catalog(DEFAULT_CATALOG, testSchema)
            .build();
    planner = new UnifiedQueryPlanner(context);
  }

  @Test
  public void testSelectWithFilter() {
    RelNode plan = planner.plan("SELECT id, name FROM catalog.employees WHERE age > 30");
    assertNotNull("Plan should be created for SELECT with filter", plan);
  }

  @Test
  public void testAggregate() {
    RelNode plan =
        planner.plan("SELECT department, count(*) FROM catalog.employees GROUP BY department");
    assertNotNull("Plan should be created for aggregate query", plan);
  }

  @Test
  public void testSortWithLimit() {
    RelNode plan = planner.plan("SELECT * FROM catalog.employees ORDER BY age LIMIT 10");
    assertNotNull("Plan should be created for sort with limit", plan);
  }

  @Test
  public void testCaseToFilterRewrite() {
    RelNode plan =
        planner.plan(
            "SELECT department, SUM(CASE WHEN age > 30 THEN 1 ELSE 0 END)"
                + " FROM catalog.employees GROUP BY department");
    assertNotNull("Plan should be created with CASE-to-FILTER rewrite", plan);
  }

  @Test
  public void testInvalidSqlSyntax() {
    assertThrows(IllegalStateException.class, () -> planner.plan("SELEC invalid syntax"));
  }
}
