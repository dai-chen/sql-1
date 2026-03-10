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
}
