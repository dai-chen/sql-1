/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.spec.aggregate;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.babel.SqlBabelParserImpl;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.junit.Test;

/** Unit tests for {@link AvgTemporalCastRewriter}. */
public class AvgTemporalCastRewriterTest {

  private static final SqlParser.Config PARSER_CONFIG =
      SqlParser.config()
          .withParserFactory(SqlBabelParserImpl.FACTORY)
          .withLex(Lex.BIG_QUERY)
          .withConformance(SqlConformanceEnum.BABEL);

  @Test
  public void testAvgCastTimestamp_rewrittenToBigint() throws Exception {
    String sql = rewrite("SELECT avg(CAST(col AS timestamp)) FROM t");
    assertContains(sql, "AVG(CAST(`col` AS BIGINT))");
  }

  @Test
  public void testAvgCastDate_rewrittenToBigint() throws Exception {
    String sql = rewrite("SELECT avg(CAST(col AS date)) FROM t");
    assertContains(sql, "AVG(CAST(`col` AS BIGINT))");
  }

  @Test
  public void testAvgCastTime_rewrittenToBigint() throws Exception {
    String sql = rewrite("SELECT avg(CAST(col AS time)) FROM t");
    assertContains(sql, "AVG(CAST(`col` AS BIGINT))");
  }

  @Test
  public void testAvgBareColumn_untouched() throws Exception {
    String sql = rewrite("SELECT avg(col) FROM t");
    assertNotContains(sql, "BIGINT");
  }

  @Test
  public void testAvgCastInteger_untouched() throws Exception {
    String sql = rewrite("SELECT avg(CAST(col AS integer)) FROM t");
    assertNotContains(sql, "BIGINT");
    assertContains(sql, "INTEGER");
  }

  @Test
  public void testSumCastTimestamp_untouched() throws Exception {
    String sql = rewrite("SELECT sum(CAST(col AS timestamp)) FROM t");
    assertNotContains(sql, "BIGINT");
    assertContains(sql, "TIMESTAMP");
  }

  @Test
  public void testAvgTimestampFunction_rewrittenToBigint() throws Exception {
    String sql = rewrite("SELECT avg(timestamp(CAST(col AS VARCHAR))) FROM t");
    assertContains(sql, "AVG(CAST(CAST(`col` AS VARCHAR) AS BIGINT))");
  }

  @Test
  public void testAvgCastTimestampInWindow_rewrittenToBigint() throws Exception {
    String sql = rewrite("SELECT avg(CAST(datetime0 AS timestamp)) OVER (PARTITION BY col) FROM t");
    assertContains(sql, "AVG(CAST(`datetime0` AS BIGINT))");
  }

  private static String rewrite(String sql) throws Exception {
    SqlNode node = SqlParser.create(sql, PARSER_CONFIG).parseStmt();
    node = node.accept(AvgTemporalCastRewriter.INSTANCE);
    return node.toString().replaceAll("\\n", " ");
  }

  private static void assertContains(String actual, String expected) {
    assertTrue(
        "Expected to contain: " + expected + "\nActual: " + actual, actual.contains(expected));
  }

  private static void assertNotContains(String actual, String unexpected) {
    assertFalse(
        "Expected NOT to contain: " + unexpected + "\nActual: " + actual,
        actual.contains(unexpected));
  }
}
