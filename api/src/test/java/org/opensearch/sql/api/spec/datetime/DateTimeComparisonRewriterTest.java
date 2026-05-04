/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.spec.datetime;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.babel.SqlBabelParserImpl;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.junit.Test;

/**
 * Unit tests for {@link DateTimeComparisonRewriter}. Uses the production Babel parser config and
 * chains {@link DateTimeLiteralRewriter} first (as in production) so that function-call style
 * literals are converted to typed literals before the comparison rewriter runs.
 */
public class DateTimeComparisonRewriterTest {

  /** Match production parser config in UnifiedSqlSpec. */
  private static final SqlParser.Config PARSER_CONFIG =
      SqlParser.config()
          .withParserFactory(SqlBabelParserImpl.FACTORY)
          .withLex(Lex.BIG_QUERY)
          .withConformance(SqlConformanceEnum.BABEL);

  @Test
  public void testTimestampEqualsDate_castDateToTimestamp() throws Exception {
    String sql =
        rewriteToString(
            "SELECT * FROM t WHERE TIMESTAMP('2020-09-16 00:00:00') = DATE('2020-09-16')");
    assertContains(sql, "CAST(DATE '2020-09-16' AS TIMESTAMP)");
    assertNotContains(sql, "CAST(TIMESTAMP");
  }

  @Test
  public void testDateEqualsTimestamp_castDateToTimestamp() throws Exception {
    String sql =
        rewriteToString(
            "SELECT * FROM t WHERE DATE('2020-09-16') = TIMESTAMP('2020-09-16 00:00:00')");
    assertContains(sql, "CAST(DATE '2020-09-16' AS TIMESTAMP)");
    assertNotContains(sql, "CAST(TIMESTAMP");
  }

  @Test
  public void testTimestampGreaterThanTime_castTimeToTimestamp() throws Exception {
    String sql =
        rewriteToString(
            "SELECT * FROM t WHERE TIMESTAMP('2020-09-16 10:20:30') > TIME('10:20:30')");
    assertContains(sql, "CAST(TIME '10:20:30' AS TIMESTAMP)");
    assertNotContains(sql, "CAST(TIMESTAMP");
  }

  @Test
  public void testTimeGreaterThanOrEqualDate_castBothToTimestamp() throws Exception {
    String sql = rewriteToString("SELECT * FROM t WHERE TIME('10:20:30') >= DATE('2020-09-16')");
    assertContains(sql, "CAST(TIME '10:20:30' AS TIMESTAMP)");
    assertContains(sql, "CAST(DATE '2020-09-16' AS TIMESTAMP)");
  }

  @Test
  public void testSameType_noRewrite() throws Exception {
    String sql =
        rewriteToString(
            "SELECT * FROM t WHERE TIMESTAMP('2020-09-16 00:00:00') = TIMESTAMP('2020-09-16"
                + " 10:20:30')");
    assertNotContains(sql, "CAST");
  }

  @Test
  public void testNonComparisonCall_untouched() throws Exception {
    String sql = rewriteToString("SELECT UPPER(name) FROM t");
    assertNotContains(sql, "CAST");
  }

  @Test
  public void testDateLessThanTimestamp() throws Exception {
    String sql =
        rewriteToString(
            "SELECT * FROM t WHERE DATE('2020-09-16') < TIMESTAMP('2020-09-16 00:00:00')");
    assertContains(sql, "CAST(DATE '2020-09-16' AS TIMESTAMP)");
  }

  @Test
  public void testTimeLessThanOrEqualTimestamp() throws Exception {
    String sql =
        rewriteToString(
            "SELECT * FROM t WHERE TIME('10:20:30') <= TIMESTAMP('2020-09-16 10:20:30')");
    assertContains(sql, "CAST(TIME '10:20:30' AS TIMESTAMP)");
    assertNotContains(sql, "CAST(TIMESTAMP");
  }

  private static String rewriteToString(String sql) throws Exception {
    SqlNode node = SqlParser.create(sql, PARSER_CONFIG).parseStmt();
    // Chain rewriters in production order: literals first, then comparisons.
    node = node.accept(DateTimeLiteralRewriter.INSTANCE);
    node = node.accept(DateTimeComparisonRewriter.INSTANCE);
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
