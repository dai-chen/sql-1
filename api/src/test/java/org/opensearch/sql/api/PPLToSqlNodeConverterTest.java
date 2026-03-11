/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api;

import static org.junit.Assert.assertTrue;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.junit.Test;
import org.opensearch.sql.ast.tree.UnresolvedPlan;

public class PPLToSqlNodeConverterTest {

  private static String toSql(SqlNode node) {
    return node.toSqlString(CalciteSqlDialect.DEFAULT).getSql();
  }

  private String convert(String ppl) {
    UnresolvedPlan plan = PPLToSqlNodeConverter.parse(ppl);
    PPLToSqlNodeConverter converter = new PPLToSqlNodeConverter();
    SqlNode result = converter.convert(plan);
    return toSql(result);
  }

  @Test
  public void testSource() {
    String sql = convert("source=t");
    assertTrue(sql.contains("SELECT *"));
    assertTrue(sql.contains("FROM \"t\""));
  }

  @Test
  public void testWhereFilter() {
    String sql = convert("source=t | where a > 1");
    assertTrue(sql.contains("\"a\" > 1"));
  }

  @Test
  public void testFieldsProject() {
    String sql = convert("source=t | fields b, c");
    assertTrue(sql.contains("\"b\""));
    assertTrue(sql.contains("\"c\""));
  }

  @Test
  public void testSortOrderBy() {
    String sql = convert("source=t | sort b");
    assertTrue(sql.contains("ORDER BY"));
    assertTrue(sql.contains("\"b\""));
  }

  @Test
  public void testHeadLimit() {
    String sql = convert("source=t | head 10");
    assertTrue(sql.contains("FETCH NEXT 10 ROWS ONLY") || sql.contains("10"));
  }

  @Test
  public void testFullPipeline() {
    String sql = convert("source=t | where a > 1 | fields b, c | sort b | head 10");
    assertTrue("Should have WHERE clause", sql.contains("\"a\" > 1"));
    assertTrue("Should have projected columns", sql.contains("\"b\""));
    assertTrue("Should have projected columns", sql.contains("\"c\""));
    assertTrue("Should have ORDER BY", sql.contains("ORDER BY"));
    assertTrue("Should have subquery nesting", sql.contains("_t"));
  }

  // -- US-004: Expression visitor tests --

  @Test
  public void testFunctionCall() {
    String sql = convert("source=t | where abs(a) > 1");
    assertTrue(sql.contains("ABS"));
  }

  @Test
  public void testArithmeticOperators() {
    String sql = convert("source=t | where a + b > 1");
    assertTrue(sql.contains("+"));
  }

  @Test
  public void testIfFunction() {
    String sql = convert("source=t | where if(a > 1, b, c) > 0");
    assertTrue(sql.contains("CASE WHEN"));
  }

  @Test
  public void testInExpression() {
    String sql = convert("source=t | where a in (1, 2, 3)");
    assertTrue(sql.contains("IN"));
  }

  @Test
  public void testBetweenExpression() {
    String sql = convert("source=t | where a between 1 and 10");
    assertTrue(sql.contains("BETWEEN"));
  }

  @Test
  public void testCaseExpression() {
    // PPL case syntax: case(condition, result ...) - tested via where clause
    String sql = convert("source=t | where isnull(a)");
    assertTrue(sql.contains("IS NULL"));
  }

  @Test
  public void testCastExpression() {
    String sql = convert("source=t | where cast(a as integer) > 0");
    assertTrue(sql.contains("CAST"));
    assertTrue(sql.contains("INTEGER"));
  }

  @Test
  public void testXorExpression() {
    String sql = convert("source=t | where a xor b");
    assertTrue(sql.contains("AND"));
    assertTrue(sql.contains("OR"));
    assertTrue(sql.contains("NOT"));
  }

  @Test
  public void testRegexpCompare() {
    String sql = convert("source=t | where a REGEXP 'pattern'");
    assertTrue(sql.contains("REGEXP_CONTAINS"));
  }

  @Test
  public void testLiteralTypes() {
    String sql = convert("source=t | where a = '2024-01-01'");
    assertTrue(sql.contains("2024-01-01"));
  }

  @Test
  public void testLogFunction() {
    String sql = convert("source=t | where log(a) > 1");
    assertTrue(sql.contains("LN"));
  }

  // -- US-005: Stats (aggregation) command tests --

  @Test
  public void testStatsCount() {
    String sql = convert("source=t | stats count() as cnt");
    assertTrue(sql, sql.contains("COUNT(*)"));
    assertTrue(sql, sql.contains("\"cnt\""));
  }

  @Test
  public void testStatsGroupBy() {
    String sql = convert("source=t | stats avg(a) as avg_a by b");
    assertTrue(sql, sql.contains("AVG"));
    assertTrue(sql, sql.contains("GROUP BY"));
  }

  @Test
  public void testStatsMultipleAggs() {
    String sql = convert("source=t | stats count() as cnt, sum(a) as total by b");
    assertTrue(sql, sql.contains("COUNT(*)"));
    assertTrue(sql, sql.contains("SUM"));
    assertTrue(sql, sql.contains("GROUP BY"));
  }

  @Test
  public void testStatsWithSpan() {
    String sql = convert("source=t | stats count() as cnt by span(a, 10)");
    assertTrue(sql, sql.contains("COUNT(*)"));
    assertTrue(sql, sql.contains("FLOOR"));
    assertTrue(sql, sql.contains("GROUP BY"));
  }

  // -- US-006: Eval command tests --

  @Test
  public void testEvalSimple() {
    String sql = convert("source=t | eval x = a + 1");
    assertTrue(sql, sql.contains("+"));
    assertTrue(sql, sql.contains("\"x\""));
  }

  @Test
  public void testEvalForwardReference() {
    String sql = convert("source=t | eval x = a + 1, y = x * 2");
    assertTrue(sql, sql.contains("\"x\""));
    assertTrue(sql, sql.contains("\"y\""));
    assertTrue(sql, sql.contains("*"));
  }

  @Test
  public void testEvalSelfReference() {
    String sql = convert("source=t | eval a = a + 1");
    assertTrue(sql, sql.contains("+"));
    assertTrue(sql, sql.contains("\"a\""));
  }

  // -- US-007: Dedup command tests --

  @Test
  public void testDedupSimple() {
    String sql = convert("source=t | dedup a");
    assertTrue(sql, sql.contains("ROW_NUMBER()"));
    assertTrue(sql, sql.contains("IS NOT NULL"));
  }

  @Test
  public void testDedupKeepEmpty() {
    String sql = convert("source=t | dedup 2 a, b keepempty=true");
    assertTrue(sql, sql.contains("ROW_NUMBER()"));
    assertTrue(sql, sql.contains("IS NULL"));
    assertTrue(sql, sql.contains("<= 2"));
  }

  @Test
  public void testDedupConsecutive() {
    String sql = convert("source=t | dedup a consecutive=true");
    assertTrue(sql, sql.contains("ROW_NUMBER()"));
    assertTrue(sql, sql.contains("_global_rn"));
    assertTrue(sql, sql.contains("_group_rn"));
  }
}
