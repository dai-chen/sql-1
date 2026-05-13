/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api;

import static org.apache.calcite.sql.type.SqlTypeName.INTEGER;
import static org.apache.calcite.sql.type.SqlTypeName.VARCHAR;

import java.util.Map;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.sql.executor.QueryType;

/**
 * Tests for SQL query planning through the V2 ANTLR parser path. Covers SELECT, WHERE, ORDER BY,
 * JOIN, UNION, and MINUS operations that produce valid RelNode plans.
 */
public class UnifiedQueryPlannerSqlV2Test extends UnifiedQueryTestBase {

  @Override
  protected QueryType queryType() {
    return QueryType.SQL;
  }

  @Before
  @Override
  public void setUp() {
    testSchema =
        new AbstractSchema() {
          @Override
          protected Map<String, Table> getTableMap() {
            return Map.of(
                "employees", createEmployeesTable(),
                "departments", createDepartmentsTable());
          }
        };

    context = contextBuilder().build();
    planner = new UnifiedQueryPlanner(context);
  }

  private Table createDepartmentsTable() {
    return SimpleTable.builder()
        .col("dept_id", INTEGER)
        .col("dept_name", VARCHAR)
        .row(new Object[] {1, "Engineering"})
        .row(new Object[] {2, "Sales"})
        .row(new Object[] {3, "Marketing"})
        .build();
  }

  // === Basic SELECT tests ===

  @Test
  public void selectStar() {
    givenQuery("SELECT * FROM catalog.employees")
        .assertPlan(
            """
            LogicalTableScan(table=[[catalog, employees]])
            """)
        .assertFields("id", "name", "age", "department");
  }

  @Test
  public void testFilter() {
    givenQuery("SELECT * FROM catalog.employees WHERE age > 30")
        .assertPlan(
            """
            LogicalFilter(condition=[>($2, 30)])
              LogicalTableScan(table=[[catalog, employees]])
            """);
  }

  @Test
  public void testOrderBy() {
    givenQuery("SELECT * FROM catalog.employees ORDER BY age")
        .assertPlan(
            """
            LogicalSort(sort0=[$2], dir0=[ASC-nulls-first])
              LogicalTableScan(table=[[catalog, employees]])
            """);
  }

  @Test
  public void testFilterAndOrderBy() {
    givenQuery("SELECT * FROM catalog.employees WHERE name = 'Alice' ORDER BY age")
        .assertPlan(
            """
            LogicalSort(sort0=[$2], dir0=[ASC-nulls-first])
              LogicalFilter(condition=[=($1, 'Alice')])
                LogicalTableScan(table=[[catalog, employees]])
            """);
  }

  // === JOIN tests ===

  @Test
  public void testInnerJoin() {
    givenQuery(
            "SELECT * FROM catalog.employees JOIN catalog.departments"
                + " ON employees.department = departments.dept_name")
        .assertPlanContains("LogicalJoin(condition");
  }

  @Test
  public void testLeftJoin() {
    givenQuery(
            "SELECT * FROM catalog.employees LEFT JOIN catalog.departments"
                + " ON employees.department = departments.dept_name")
        .assertPlanContains("LogicalJoin(condition=")
        .assertPlanContains("joinType=[left]");
  }

  @Test
  public void testRightJoin() {
    givenQuery(
            "SELECT * FROM catalog.employees RIGHT JOIN catalog.departments"
                + " ON employees.department = departments.dept_name")
        .assertPlanContains("LogicalJoin(condition=")
        .assertPlanContains("joinType=[right]");
  }

  @Test
  public void testCrossJoin() {
    givenQuery("SELECT * FROM catalog.employees CROSS JOIN catalog.departments")
        .assertPlanContains("LogicalJoin(condition=[true], joinType=[inner]");
  }

  @Test
  public void testJoinWithFilterAndOrderBy() {
    givenQuery(
            "SELECT * FROM catalog.employees JOIN catalog.departments"
                + " ON employees.department = departments.dept_name"
                + " WHERE employees.age > 30 ORDER BY employees.name")
        .assertPlanContains("LogicalJoin")
        .assertPlanContains("LogicalSort")
        .assertPlanContains("LogicalFilter");
  }

  // === UNION tests ===

  @Test
  public void testUnionAll() {
    givenQuery(
            "SELECT name, age FROM catalog.employees"
                + " UNION ALL SELECT dept_name, dept_id FROM catalog.departments")
        .assertPlanContains("LogicalUnion(all=[true])");
  }

  @Test
  public void testUnion() {
    givenQuery(
            "SELECT name, age FROM catalog.employees"
                + " UNION SELECT dept_name, dept_id FROM catalog.departments")
        .assertPlanContains("LogicalUnion(all=[true])");
  }

  @Test
  public void testMultiWayUnion() {
    givenQuery(
            "SELECT name, age FROM catalog.employees"
                + " UNION ALL SELECT dept_name, dept_id FROM catalog.departments"
                + " UNION ALL SELECT name, age FROM catalog.employees")
        .assertPlanContains("LogicalUnion(all=[true])");
  }

  // === MINUS (EXCEPT) tests ===

  @Test
  public void testMinus() {
    givenQuery(
            "SELECT name, age FROM catalog.employees"
                + " MINUS SELECT dept_name, dept_id FROM catalog.departments")
        .assertPlanContains("LogicalMinus(all=[false])");
  }
}
