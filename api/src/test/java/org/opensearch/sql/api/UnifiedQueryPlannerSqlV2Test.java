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
  public void testUnionAll() {
    givenQuery(
            """
            SELECT name FROM catalog.employees UNION ALL SELECT dept_name FROM catalog.departments
            """)
        .assertPlan(
            """
            LogicalUnion(all=[true])
              LogicalProject(name=[$1])
                LogicalTableScan(table=[[catalog, employees]])
              LogicalProject(dept_name=[$1])
                LogicalTableScan(table=[[catalog, departments]])
            """);
  }

  @Test
  public void testUnionDistinct() {
    givenQuery(
            """
            SELECT name FROM catalog.employees UNION SELECT dept_name FROM catalog.departments
            """)
        .assertPlan(
            """
            LogicalUnion(all=[false])
              LogicalProject(name=[$1])
                LogicalTableScan(table=[[catalog, employees]])
              LogicalProject(dept_name=[$1])
                LogicalTableScan(table=[[catalog, departments]])
            """);
  }

  @Test
  public void testMultiWayUnion() {
    givenQuery(
            """
            SELECT name FROM catalog.employees
            UNION ALL SELECT dept_name FROM catalog.departments
            UNION ALL SELECT name FROM catalog.employees
            """)
        .assertPlan(
            """
            LogicalUnion(all=[true])
              LogicalProject(name=[$1])
                LogicalTableScan(table=[[catalog, employees]])
              LogicalProject(dept_name=[$1])
                LogicalTableScan(table=[[catalog, departments]])
              LogicalProject(name=[$1])
                LogicalTableScan(table=[[catalog, employees]])
            """);
  }

  @Test
  public void testMultiWayUnionDistinct() {
    givenQuery(
            """
            SELECT name FROM catalog.employees
            UNION SELECT dept_name FROM catalog.departments
            UNION SELECT name FROM catalog.employees
            """)
        .assertPlan(
            """
            LogicalUnion(all=[false])
              LogicalProject(name=[$1])
                LogicalTableScan(table=[[catalog, employees]])
              LogicalProject(dept_name=[$1])
                LogicalTableScan(table=[[catalog, departments]])
              LogicalProject(name=[$1])
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
  public void selectLiteralWithoutFrom() {
    // FROM-less SELECT produces a one-row result via LogicalValues so the downstream
    // Project evaluates over a single row.
    givenQuery("SELECT 1")
        .assertPlan(
            """
            LogicalSort(sort0=[$0], dir0=[ASC])
              LogicalValues(tuples=[[{ 1 }]])
            """);
  }

  @Test
  public void selectExpressionWithoutFrom() {
    givenQuery("SELECT 1 + 1")
        .assertPlan(
            """
            LogicalProject(1 + 1=[+(1:BIGINT, 1:BIGINT)])
              LogicalValues(tuples=[[{ 0 }]])
            """);
  }

  @Test
  public void testGroupByAggregateAlias() {
    givenQuery(
            """
            SELECT department, SUM(age) AS total FROM catalog.employees GROUP BY department
            """)
        .assertPlan(
            """
            LogicalProject(department=[$0], total=[$1])
              LogicalAggregate(group=[{0}], SUM(age)=[CHECKED_LONG_SUM($1)])
                LogicalProject(department=[$3], age=[$2])
                  LogicalTableScan(table=[[catalog, employees]])
            """);
  }

  /**
   * A computed GROUP BY key must be referenced from the group-key column the Aggregate already
   * produces: the base column it was computed from (age) is gone above the Aggregate.
   */
  @Test
  public void testGroupByCaseExpression() {
    givenQuery(
            """
            SELECT CASE WHEN age > 30 THEN 'old' ELSE 'young' END AS g, COUNT(*) AS cnt
              FROM catalog.employees GROUP BY CASE WHEN age > 30 THEN 'old' ELSE 'young' END
            """)
        .assertPlan(
            """
            LogicalProject(g=[$0], cnt=[$1])
              LogicalAggregate(group=[{0}], COUNT(*)=[COUNT()])
                LogicalProject(Case(caseValue=null, whenClauses=[When(condition=>(age, 30), result=old)], elseClause=Optional[young])=[CASE(>($2, 30), 'old':VARCHAR, 'young':VARCHAR)])
                  LogicalTableScan(table=[[catalog, employees]])
            """);
  }

  @Test
  public void testGroupByCastExpression() {
    givenQuery(
            """
            SELECT CAST(age AS STRING) AS c, COUNT(*) AS cnt
              FROM catalog.employees GROUP BY CAST(age AS STRING)
            """)
        .assertPlan(
            """
            LogicalProject(c=[$0], cnt=[$1])
              LogicalAggregate(group=[{0}], COUNT(*)=[COUNT()])
                LogicalProject(Cast(expression=age, convertedType=STRING)=[SAFE_CAST($2)])
                  LogicalTableScan(table=[[catalog, employees]])
            """);
  }

  /** A select item that wraps the group key resolves the key and applies the wrapper on top. */
  @Test
  public void testGroupByExpressionWrappedInSelectItem() {
    givenQuery(
            """
            SELECT UPPER(CASE WHEN age > 30 THEN 'old' ELSE 'young' END) AS g, COUNT(*) AS cnt
              FROM catalog.employees GROUP BY CASE WHEN age > 30 THEN 'old' ELSE 'young' END
            """)
        .assertPlan(
            """
            LogicalProject(g=[UPPER($0)], cnt=[$1])
              LogicalAggregate(group=[{0}], COUNT(*)=[COUNT()])
                LogicalProject(Case(caseValue=null, whenClauses=[When(condition=>(age, 30), result=old)], elseClause=Optional[young])=[CASE(>($2, 30), 'old':VARCHAR, 'young':VARCHAR)])
                  LogicalTableScan(table=[[catalog, employees]])
            """);
  }

  /**
   * {@code And}, {@code Or} and {@code Between} extend {@code UnresolvedExpression} directly for
   * the same reason {@code Case}/{@code Cast}/{@code In}/{@code Not} do, so a group key whose
   * top-level node is one of them needs the same resolution.
   */
  @Test
  public void testGroupByAndExpression() {
    givenQuery(
            """
            SELECT age > 30 AND age < 50 AS g, COUNT(*) AS cnt
              FROM catalog.employees GROUP BY age > 30 AND age < 50
            """)
        .assertPlan(
            """
            LogicalProject(g=[$0], cnt=[$1])
              LogicalAggregate(group=[{0}], COUNT(*)=[COUNT()])
                LogicalProject(And(left=>(age, 30), right=<(age, 50))=[SEARCH($2, Sarg[(30..50)])])
                  LogicalTableScan(table=[[catalog, employees]])
            """);
  }

  @Test
  public void testGroupByOrExpression() {
    givenQuery(
            """
            SELECT age > 30 OR age < 20 AS g, COUNT(*) AS cnt
              FROM catalog.employees GROUP BY age > 30 OR age < 20
            """)
        .assertPlan(
            """
            LogicalProject(g=[$0], cnt=[$1])
              LogicalAggregate(group=[{0}], COUNT(*)=[COUNT()])
                LogicalProject(Or(left=>(age, 30), right=<(age, 20))=[SEARCH($2, Sarg[(-∞..20), (30..+∞)])])
                  LogicalTableScan(table=[[catalog, employees]])
            """);
  }

  @Test
  public void testGroupByBetweenExpression() {
    givenQuery(
            """
            SELECT age BETWEEN 30 AND 40 AS g, COUNT(*) AS cnt
              FROM catalog.employees GROUP BY age BETWEEN 30 AND 40
            """)
        .assertPlan(
            """
            LogicalProject(g=[$0], cnt=[$1])
              LogicalAggregate(group=[{0}], COUNT(*)=[COUNT()])
                LogicalProject(Between(value=age, lowerBound=30, upperBound=40)=[SEARCH($2, Sarg[[30..40]])])
                  LogicalTableScan(table=[[catalog, employees]])
            """);
  }

  /**
   * Regression guard for group-key scope: two aggregates in one query, each grouping by the same
   * computed key. The inner Aggregate's registered ordinal must not leak into the outer one, whose
   * input has a different row type -- it used to resolve there and throw IndexOutOfBoundsException,
   * and would have returned the wrong column had the ordinal been in range.
   */
  @Test
  public void testAggregatesInOneQuerySharingComputedGroupKey() {
    givenQuery(
            """
            SELECT a.g FROM
              (SELECT CASE WHEN age > 30 THEN 'old' ELSE 'young' END AS g, COUNT(*) AS cnt
                 FROM catalog.employees GROUP BY CASE WHEN age > 30 THEN 'old' ELSE 'young' END) a
              JOIN
              (SELECT CASE WHEN age > 30 THEN 'old' ELSE 'young' END AS g, COUNT(*) AS cnt
                 FROM catalog.employees GROUP BY CASE WHEN age > 30 THEN 'old' ELSE 'young' END) b
              ON a.g = b.g
            """)
        .assertFields("a.g");
  }

  /**
   * Regression guard: a select-list literal must stay a literal even when its text equals a column
   * name. Group-key resolution is keyed on the registered group-by expressions, so it can never
   * rebind an unrelated expression to a same-named column.
   */
  @Test
  public void testSelectLiteralMatchingColumnName() {
    givenQuery(
            """
            SELECT name, 'age' AS tag FROM catalog.employees
            """)
        .assertPlan(
            """
            LogicalProject(name=[$1], tag=['age':VARCHAR])
              LogicalTableScan(table=[[catalog, employees]])
            """);
  }

  @Test
  public void testOrderByAggregateAlias() {
    givenQuery(
            """
            SELECT department, COUNT(*) AS cnt FROM catalog.employees
              GROUP BY department ORDER BY cnt DESC LIMIT 3
            """)
        .assertPlan(
            """
            LogicalSort(sort0=[$1], dir0=[DESC-nulls-last])
              LogicalProject(department=[$1], cnt=[$0])
                LogicalSort(sort0=[$0], dir0=[DESC-nulls-last], fetch=[3])
                  LogicalProject(COUNT(*)=[$1], department=[$0])
                    LogicalAggregate(group=[{0}], COUNT(*)=[COUNT()])
                      LogicalProject(department=[$3])
                        LogicalTableScan(table=[[catalog, employees]])
            """);
  }

  @Test
  public void testAliasPreservedInOutputSchema() {
    givenQuery("SELECT COUNT(*) AS cnt FROM catalog.employees").assertFields("cnt");

    givenQuery("SELECT department, COUNT(*) AS cnt FROM catalog.employees GROUP BY department")
        .assertFields("department", "cnt");

    givenQuery("SELECT department, COUNT(*) FROM catalog.employees GROUP BY department")
        .assertFields("department", "COUNT(*)");

    givenQuery("SELECT MAX(age) + MIN(age) AS range_sum FROM catalog.employees")
        .assertFields("range_sum");

    givenQuery("SELECT id, name, age AS years, department FROM catalog.employees")
        .assertFields("id", "name", "years", "department");
  }

  @Test
  public void testHavingMaxCol() {
    givenQuery(
            """
            SELECT department FROM catalog.employees
              GROUP BY department HAVING MAX(age) > 30
            """)
        .assertPlan(
            """
            LogicalProject(department=[$1])
              LogicalFilter(condition=[>($0, 30)])
                LogicalProject(MAX(age)=[$1], department=[$0])
                  LogicalAggregate(group=[{0}], MAX(age)=[MAX($1)])
                    LogicalProject(department=[$3], age=[$2])
                      LogicalTableScan(table=[[catalog, employees]])
            """);
  }

  @Test
  public void testCountStarWithFilter() {
    givenQuery("SELECT COUNT(*) FILTER(WHERE age > 30) FROM catalog.employees")
        .assertPlan(
            """
            LogicalAggregate(group=[{}], COUNT(*) FILTER(WHERE age > 30)=[COUNT() FILTER $0])
              LogicalProject($f1=[>($2, 30)])
                LogicalTableScan(table=[[catalog, employees]])
            """);
  }

  @Test
  public void testFilteredAggregateWithGroupBy() {
    givenQuery(
            """
            SELECT department, SUM(age) FILTER(WHERE age > 30) FROM catalog.employees
              GROUP BY department
            """)
        .assertPlan(
            """
            LogicalAggregate(group=[{0}], SUM(age) FILTER(WHERE age > 30)=[CHECKED_LONG_SUM($1) FILTER $2])
              LogicalProject(department=[$3], age=[$2], $f3=[>($2, 30)])
                LogicalTableScan(table=[[catalog, employees]])
            """);
  }

  @Test
  public void testMultipleFilteredAggregates() {
    givenQuery(
            """
            SELECT MAX(age) FILTER(WHERE age > 30), MIN(age) FILTER(WHERE age < 50)
              FROM catalog.employees
            """)
        .assertPlan(
            """
            LogicalAggregate(group=[{}], MAX(age) FILTER(WHERE age > 30)=[MAX($0) FILTER $1], MIN(age) FILTER(WHERE age < 50)=[MIN($0) FILTER $2])
              LogicalProject(age=[$2], $f4=[>($2, 30)], $f5=[<($2, 50)])
                LogicalTableScan(table=[[catalog, employees]])
            """);
  }

  @Test
  public void testScalarFnOverAggregate() {
    givenQuery("SELECT ABS(MAX(age)) FROM catalog.employees")
        .assertPlan(
            """
            LogicalProject(ABS(MAX(age))=[ABS($0)])
              LogicalAggregate(group=[{}], MAX(age)=[MAX($0)])
                LogicalProject(age=[$2])
                  LogicalTableScan(table=[[catalog, employees]])
            """);
  }

  @Test
  public void testArithmeticOnAggregates() {
    givenQuery("SELECT MAX(age) + MIN(age) AS range_sum FROM catalog.employees")
        .assertPlan(
            """
            LogicalProject(range_sum=[+(CAST($0):BIGINT, CAST($1):BIGINT)])
              LogicalAggregate(group=[{}], MAX(age)=[MAX($0)], MIN(age)=[MIN($0)])
                LogicalProject(age=[$2])
                  LogicalTableScan(table=[[catalog, employees]])
            """);
  }

  @Test
  public void testHavingCountStar() {
    givenQuery(
            """
            SELECT department FROM catalog.employees
              GROUP BY department HAVING COUNT(*) > 5
            """)
        .assertPlan(
            """
            LogicalProject(department=[$1])
              LogicalFilter(condition=[>($0, 5)])
                LogicalProject(COUNT(*)=[$1], department=[$0])
                  LogicalAggregate(group=[{0}], COUNT(*)=[COUNT()])
                    LogicalProject(department=[$3])
                      LogicalTableScan(table=[[catalog, employees]])
            """);
  }

  @Test
  public void testHavingWithAlias() {
    givenQuery(
            """
            SELECT department, COUNT(*) AS cnt FROM catalog.employees
              GROUP BY department HAVING cnt > 1
            """)
        .assertPlan(
            """
            LogicalProject(department=[$1], cnt=[$0])
              LogicalFilter(condition=[>($0, 1)])
                LogicalProject(COUNT(*)=[$1], department=[$0])
                  LogicalAggregate(group=[{0}], COUNT(*)=[COUNT()])
                    LogicalProject(department=[$3])
                      LogicalTableScan(table=[[catalog, employees]])
            """);
  }

  @Test
  public void testHavingCompoundAnd() {
    givenQuery(
            """
            SELECT department FROM catalog.employees
              GROUP BY department HAVING MAX(age) > 30 AND MIN(age) < 50
            """)
        .assertPlan(
            """
            LogicalProject(department=[$2])
              LogicalFilter(condition=[AND(>($0, 30), <($1, 50))])
                LogicalProject(MAX(age)=[$1], MIN(age)=[$2], department=[$0])
                  LogicalAggregate(group=[{0}], MAX(age)=[MAX($1)], MIN(age)=[MIN($1)])
                    LogicalProject(department=[$3], age=[$2])
                      LogicalTableScan(table=[[catalog, employees]])
            """);
  }

  @Test
  public void testCountDistinctWindowWithOrderBy() {
    // No frame printed: RANGE .. CURRENT ROW is Calcite's default for ORDER BY.
    givenQuery(
            """
            SELECT department, COUNT(DISTINCT name) OVER(ORDER BY department) FROM catalog.employees
            """)
        .assertPlan(
            """
            LogicalProject(department=[$3], COUNT(DISTINCT name) OVER(ORDER BY department)=[COUNT(DISTINCT $1) OVER (ORDER BY $3 NULLS FIRST)])
              LogicalTableScan(table=[[catalog, employees]])
            """);
  }

  @Test
  public void testSumWindowWithPartitionAndOrderBy() {
    givenQuery(
            """
            SELECT name, SUM(age) OVER(PARTITION BY department ORDER BY age) FROM catalog.employees
            """)
        .assertPlan(
            """
            LogicalProject(name=[$1], SUM(age) OVER(PARTITION BY department ORDER BY age)=[CHECKED_LONG_SUM($2) OVER (PARTITION BY $3 ORDER BY $2 NULLS FIRST)])
              LogicalTableScan(table=[[catalog, employees]])
            """);
  }

  @Test
  public void testWindowOrderByDefaultsNullsFirst() {
    // Window function ORDER BY without explicit NULLS FIRST/LAST defaults to NULLS FIRST,
    // matching top-level ORDER BY semantics.
    givenQuery(
            """
            SELECT name, ROW_NUMBER() OVER (ORDER BY id) AS rn FROM catalog.employees
            """)
        .assertPlan(
            """
            LogicalProject(name=[$1], rn=[ROW_NUMBER() OVER (ORDER BY $0 NULLS FIRST)])
              LogicalTableScan(table=[[catalog, employees]])
            """);
  }

  @Test
  public void testWindowRank() {
    givenQuery(
            """
            SELECT name, RANK() OVER (ORDER BY age) AS r FROM catalog.employees
            """)
        .assertPlan(
            """
            LogicalProject(name=[$1], r=[RANK() OVER (ORDER BY $2 NULLS FIRST)])
              LogicalTableScan(table=[[catalog, employees]])
            """);
  }

  @Test
  public void testWindowDenseRank() {
    givenQuery(
            """
            SELECT name, DENSE_RANK() OVER (ORDER BY age) AS r FROM catalog.employees
            """)
        .assertPlan(
            """
            LogicalProject(name=[$1], r=[DENSE_RANK() OVER (ORDER BY $2 NULLS FIRST)])
              LogicalTableScan(table=[[catalog, employees]])
            """);
  }

  @Test
  public void testWindowRankPartitionBy() {
    givenQuery(
            """
            SELECT name, RANK() OVER (PARTITION BY department ORDER BY age DESC) AS r
              FROM catalog.employees
            """)
        .assertPlan(
            """
            LogicalProject(name=[$1], r=[RANK() OVER (PARTITION BY $3 ORDER BY $2 DESC NULLS FIRST)])
              LogicalTableScan(table=[[catalog, employees]])
            """);
  }

  @Test
  public void testGroupByExpression() {
    givenQuery("SELECT LENGTH(name), COUNT(*) FROM catalog.employees GROUP BY LENGTH(name)")
        .assertPlan(
            """
            LogicalAggregate(group=[{0}], COUNT(*)=[COUNT()])
              LogicalProject(LENGTH(name)=[CHAR_LENGTH($1)])
                LogicalTableScan(table=[[catalog, employees]])
            """);
  }

  @Test
  public void testHavingOnGroupByExpression() {
    givenQuery(
            "SELECT COUNT(*) FROM catalog.employees GROUP BY LENGTH(name) HAVING LENGTH(name) > 3")
        .assertPlan(
            """
            LogicalProject(COUNT(*)=[$0])
              LogicalFilter(condition=[>($1, 3)])
                LogicalProject(COUNT(*)=[$1], LENGTH(name)=[$0])
                  LogicalAggregate(group=[{0}], COUNT(*)=[COUNT()])
                    LogicalProject(LENGTH(name)=[CHAR_LENGTH($1)])
                      LogicalTableScan(table=[[catalog, employees]])
            """);
  }

  @Test
  public void testOrderByGroupByExpression() {
    givenQuery(
            """
            SELECT LENGTH(name) FROM catalog.employees GROUP BY LENGTH(name) ORDER BY LENGTH(name)
            """)
        .assertPlan(
            """
            LogicalSort(sort0=[$0], dir0=[ASC-nulls-first])
              LogicalAggregate(group=[{0}])
                LogicalProject(LENGTH(name)=[CHAR_LENGTH($1)])
                  LogicalTableScan(table=[[catalog, employees]])
            """);
  }

  @Test
  public void testWindowOverGroupByWithLimit() {
    givenQuery(
            """
            SELECT department, COUNT(*) AS cnt, ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) AS rn
              FROM catalog.employees GROUP BY department LIMIT 3
            """)
        .assertPlan(
            """
            LogicalSort(fetch=[3])
              LogicalProject(department=[$0], cnt=[$1], rn=[ROW_NUMBER() OVER (ORDER BY $1 DESC NULLS FIRST)])
                LogicalAggregate(group=[{0}], COUNT(*)=[COUNT()])
                  LogicalProject(department=[$3])
                    LogicalTableScan(table=[[catalog, employees]])
            """);
  }

  @Test
  public void testWindowOverGroupByOrderByWindowAlias() {
    givenQuery(
            """
            SELECT department, COUNT(*) AS cnt, ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) AS rn
              FROM catalog.employees GROUP BY department ORDER BY rn LIMIT 3
            """)
        .assertPlan(
            """
            LogicalSort(sort0=[$2], dir0=[ASC-nulls-first], fetch=[3])
              LogicalProject(department=[$0], cnt=[$1], rn=[ROW_NUMBER() OVER (ORDER BY $1 DESC NULLS FIRST)])
                LogicalAggregate(group=[{0}], COUNT(*)=[COUNT()])
                  LogicalProject(department=[$3])
                    LogicalTableScan(table=[[catalog, employees]])
            """);
  }

  @Test
  public void testWindowOverGroupByOrderByWindowAliasWithoutLimit() {
    givenQuery(
            """
            SELECT department, COUNT(*) AS cnt, ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) AS rn
              FROM catalog.employees GROUP BY department ORDER BY rn
            """)
        .assertPlan(
            """
            LogicalSort(sort0=[$2], dir0=[ASC-nulls-first])
              LogicalProject(department=[$0], cnt=[$1], rn=[ROW_NUMBER() OVER (ORDER BY $1 DESC NULLS FIRST)])
                LogicalAggregate(group=[{0}], COUNT(*)=[COUNT()])
                  LogicalProject(department=[$3])
                    LogicalTableScan(table=[[catalog, employees]])
            """);
  }

  @Test
  public void testMultipleWindowFunctionsOrderByWindowAlias() {
    givenQuery(
            """
            SELECT department, COUNT(*) AS cnt, ROW_NUMBER() OVER (ORDER BY COUNT(*) DESC) AS rn,
                   ROW_NUMBER() OVER (ORDER BY department) AS rn2
              FROM catalog.employees GROUP BY department ORDER BY rn LIMIT 3
            """)
        .assertPlan(
            """
            LogicalSort(sort0=[$2], dir0=[ASC-nulls-first], fetch=[3])
              LogicalProject(department=[$0], cnt=[$1], rn=[ROW_NUMBER() OVER (ORDER BY $1 DESC NULLS FIRST)], rn2=[ROW_NUMBER() OVER (ORDER BY $0 NULLS FIRST)])
                LogicalAggregate(group=[{0}], COUNT(*)=[COUNT()])
                  LogicalProject(department=[$3])
                    LogicalTableScan(table=[[catalog, employees]])
            """);
  }
}
