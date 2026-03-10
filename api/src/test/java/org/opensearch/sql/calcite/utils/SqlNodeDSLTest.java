/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.opensearch.sql.calcite.utils.SqlNodeDSL.*;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.junit.Test;

public class SqlNodeDSLTest {

  private static String toSql(SqlNode node) {
    return node.toSqlString(CalciteSqlDialect.DEFAULT).getSql();
  }

  @Test
  public void testSelectStarFromTable() {
    SqlNode sql = select(star()).from(table("t")).build();
    String result = toSql(sql);
    assertTrue(result.contains("SELECT *"));
    assertTrue(result.contains("FROM \"t\""));
  }

  @Test
  public void testNestedSubquery() {
    SqlNode inner =
        select(star())
            .from(table("t"))
            .where(gt(identifier("a"), literal(1)))
            .build();
    SqlNode outer = select(identifier("b")).from(subquery(inner, "_t1")).build();
    String result = toSql(outer);
    assertTrue(result.contains("SELECT \"b\""));
    assertTrue(result.contains("\"_t1\""));
    assertTrue(result.contains("\"a\" > 1"));
  }

  @Test
  public void testOrderByWithLimit() {
    SqlNode sql =
        select(identifier("x"))
            .from(table("t"))
            .orderBy(desc(identifier("x")))
            .limit(intLiteral(10))
            .offset(intLiteral(5))
            .build();
    String result = toSql(sql);
    assertTrue(result.contains("ORDER BY"));
    assertTrue(result.contains("DESC"));
    // Calcite uses ANSI syntax: OFFSET ... ROWS FETCH NEXT ... ROWS ONLY
    assertTrue(result.contains("OFFSET"));
    assertTrue(result.contains("FETCH"));
  }

  @Test
  public void testAggregationWithGroupBy() {
    SqlNode sql =
        select(identifier("dept"), as(countStar(), "cnt"))
            .from(table("emp"))
            .groupBy(identifier("dept"))
            .build();
    String result = toSql(sql);
    assertTrue(result.contains("COUNT(*)"));
    assertTrue(result.contains("\"cnt\""));
    assertTrue(result.contains("GROUP BY"));
  }

  @Test
  public void testExpressionHelpers() {
    // comparison
    assertEquals("\"a\" > 1", toSql(gt(identifier("a"), literal(1))));
    assertEquals("\"a\" < 1", toSql(lt(identifier("a"), literal(1))));
    assertEquals("\"a\" = 'x'", toSql(eq(identifier("a"), literal("x"))));
    assertEquals("\"a\" <> 'y'", toSql(neq(identifier("a"), literal("y"))));

    // logical
    String andResult = toSql(and(gt(identifier("a"), literal(1)), lt(identifier("b"), literal(2))));
    assertTrue(andResult.contains("AND"));

    String orResult = toSql(or(gt(identifier("a"), literal(1)), lt(identifier("b"), literal(2))));
    assertTrue(orResult.contains("OR"));

    // as
    assertTrue(toSql(as(identifier("x"), "y")).contains("\"y\""));

    // not
    assertTrue(toSql(not(eq(identifier("a"), literal(1)))).contains("NOT"));

    // isNull / isNotNull
    assertTrue(toSql(isNull(identifier("a"))).contains("IS NULL"));
    assertTrue(toSql(isNotNull(identifier("a"))).contains("IS NOT NULL"));

    // like
    assertTrue(toSql(like(identifier("a"), literal("%foo%"))).contains("LIKE"));

    // arithmetic
    assertTrue(toSql(plus(identifier("a"), literal(1))).contains("+"));
    assertTrue(toSql(minus(identifier("a"), literal(1))).contains("-"));
    assertTrue(toSql(times(identifier("a"), literal(2))).contains("*"));
    assertTrue(toSql(divide(identifier("a"), literal(2))).contains("/"));
    assertTrue(toSql(mod(identifier("a"), literal(2))).contains("MOD"));
  }
}
