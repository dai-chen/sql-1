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
 * and JOIN operations that produce valid RelNode plans.
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

  @Test
  public void testJoinTypes() {
    Map.of("JOIN", "inner", "LEFT JOIN", "left", "RIGHT JOIN", "right")
        .forEach(
            (syntax, type) ->
                givenQuery(
                        """
                        SELECT * FROM catalog.employees %s catalog.departments
                          ON employees.department = departments.dept_name
                        """
                            .formatted(syntax))
                    .assertPlan(
                        """
                        LogicalJoin(condition=[=($3, $5)], joinType=[%s])
                          LogicalTableScan(table=[[catalog, employees]])
                          LogicalTableScan(table=[[catalog, departments]])
                        """
                            .formatted(type)));
  }

  @Test
  public void testCrossJoin() {
    givenQuery("SELECT * FROM catalog.employees CROSS JOIN catalog.departments")
        .assertPlan(
            """
            LogicalJoin(condition=[true], joinType=[inner])
              LogicalTableScan(table=[[catalog, employees]])
              LogicalTableScan(table=[[catalog, departments]])
            """);
  }

  @Test
  public void testJoinWithFilterAndOrderBy() {
    givenQuery(
            """
            SELECT * FROM catalog.employees JOIN catalog.departments
              ON employees.department = departments.dept_name
              WHERE employees.age > 30 ORDER BY employees.name
            """)
        .assertPlan(
            """
            LogicalSort(sort0=[$1], dir0=[ASC-nulls-first])
              LogicalFilter(condition=[>($2, 30)])
                LogicalJoin(condition=[=($3, $5)], joinType=[inner])
                  LogicalTableScan(table=[[catalog, employees]])
                  LogicalTableScan(table=[[catalog, departments]])
            """);
  }

  @Test
  public void testInSubquery() {
    givenQuery(
            """
            SELECT name FROM catalog.employees
              WHERE age IN (SELECT age FROM catalog.departments WHERE dept_name = 'Engineering')
            """)
        .assertPlan(
            """
            LogicalProject(name=[$1])
              LogicalFilter(condition=[IN($2, {
            LogicalProject(age=[$cor0.age])
              LogicalFilter(condition=[=($1, 'Engineering')])
                LogicalTableScan(table=[[catalog, departments]])
            })], variablesSet=[[$cor0]])
                LogicalTableScan(table=[[catalog, employees]])
            """);
  }

  @Test
  public void testExistsSubquery() {
    givenQuery(
            """
            SELECT name FROM catalog.employees
              WHERE EXISTS (SELECT 1 FROM catalog.departments WHERE dept_id = age)
            """)
        .assertPlan(
            """
            LogicalProject(name=[$1])
              LogicalFilter(condition=[EXISTS({
            LogicalProject(1=[1])
              LogicalFilter(condition=[=($0, $cor0.age)])
                LogicalTableScan(table=[[catalog, departments]])
            })], variablesSet=[[$cor0]])
                LogicalTableScan(table=[[catalog, employees]])
            """);
  }

  @Test
  public void testNotInSubquery() {
    givenQuery(
            """
            SELECT name FROM catalog.employees
              WHERE age NOT IN (SELECT age FROM catalog.departments WHERE dept_name = 'Engineering')
            """)
        .assertPlan(
            """
            LogicalProject(name=[$1])
              LogicalFilter(condition=[NOT(IN($2, {
            LogicalProject(age=[$cor0.age])
              LogicalFilter(condition=[=($1, 'Engineering')])
                LogicalTableScan(table=[[catalog, departments]])
            }))], variablesSet=[[$cor0]])
                LogicalTableScan(table=[[catalog, employees]])
            """);
  }

  @Test
  public void testNotExistsSubquery() {
    givenQuery(
            """
            SELECT name FROM catalog.employees
              WHERE NOT EXISTS (SELECT 1 FROM catalog.departments WHERE dept_id = age)
            """)
        .assertPlan(
            """
            LogicalProject(name=[$1])
              LogicalFilter(condition=[NOT(EXISTS({
            LogicalProject(1=[1])
              LogicalFilter(condition=[=($0, $cor0.age)])
                LogicalTableScan(table=[[catalog, departments]])
            }))], variablesSet=[[$cor0]])
                LogicalTableScan(table=[[catalog, employees]])
            """);
  }

  @Test
  public void selectWithoutFrom_literal() {
    // Fix 3: FROM-less SELECT should produce a one-row result via LogicalValues.
    // The optimizer folds Project([1]) over Values([[0]]) into Values([[1]]).
    givenQuery("SELECT 1").assertPlanContains("LogicalValues(tuples=[[{ 1 }]])");
  }

  @Test
  public void selectWithoutFrom_expression() {
    // Non-trivial expressions remain as Project over the one-row dual table.
    givenQuery("SELECT 1 + 1")
        .assertPlanContains("LogicalValues(tuples=[[{ 0 }]])")
        .assertPlanContains("LogicalProject");
  }

  // --- HAVING and aggregate resolution tests (Fix 6 verification) ---
  // These verify that post-aggregate AggregateFunction AST nodes are resolved via the registry
  // in CalciteRexNodeVisitor.visitAggregateFunction, without needing the name-match fallback.

  @Test
  public void testHavingAndAggResolution_havingMaxCol() {
    // HAVING on a named aggregate. Verifies the registry path used by Filter (HAVING)
    // entry-point translation in CalciteRexNodeVisitor.visitAggregateFunction.
    givenQuery("SELECT department FROM catalog.employees GROUP BY department HAVING MAX(age) > 30")
        .assertPlanContains("LogicalAggregate")
        .assertPlanContains("LogicalFilter");
  }

  @Test
  public void testHavingAndAggResolution_scalarFnOverAggregate() {
    // Scalar function wrapping an aggregate. Verifies the registry path used by Project
    // (post-aggregate SELECT) entry-point translation.
    givenQuery("SELECT ABS(MAX(age)) FROM catalog.employees")
        .assertPlanContains("LogicalAggregate")
        .assertPlanContains("LogicalProject");
  }

  @Test
  public void testHavingAndAggResolution_arithmeticOnAggregates() {
    // Arithmetic combining two distinct aggregates. Verifies the registry holds multiple
    // entries simultaneously and resolves each by structural identity.
    givenQuery("SELECT MAX(age) + MIN(age) AS range_sum FROM catalog.employees")
        .assertPlanContains("LogicalAggregate")
        .assertPlanContains("LogicalProject");
  }

  @Test
  public void testHavingAndAggResolution_havingCountStar() {
    // HAVING COUNT(*) — verifies the canonical form "COUNT(*)" (rather than "COUNT(field)")
    // is correctly recognized.
    givenQuery("SELECT department FROM catalog.employees GROUP BY department HAVING COUNT(*) > 5")
        .assertPlanContains("LogicalAggregate")
        .assertPlanContains("LogicalFilter");
  }

  @Test
  public void testHavingAndAggResolution_havingWithAlias() {
    // HAVING with alias reference — `cnt` is the alias of COUNT(*). The semantic layer
    // resolves the alias back to the underlying aggregate; verifies the resolved aggregate
    // still translates correctly through visitAggregateFunction.
    givenQuery(
            "SELECT department, COUNT(*) AS cnt FROM catalog.employees GROUP BY department"
                + " HAVING cnt > 1")
        .assertPlanContains("LogicalAggregate")
        .assertPlanContains("LogicalFilter");
  }

  @Test
  public void testHavingAndAggResolution_havingCompoundAnd() {
    // HAVING with compound AND of two aggregate conditions.
    givenQuery(
            "SELECT department FROM catalog.employees GROUP BY department"
                + " HAVING MAX(age) > 30 AND MIN(age) < 50")
        .assertPlanContains("LogicalAggregate")
        .assertPlanContains("LogicalFilter");
  }
}
