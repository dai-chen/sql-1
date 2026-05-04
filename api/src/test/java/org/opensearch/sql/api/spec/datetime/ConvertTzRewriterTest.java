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

/** Unit tests for {@link ConvertTzRewriter}. */
public class ConvertTzRewriterTest {

  private static final SqlParser.Config PARSER_CONFIG =
      SqlParser.config()
          .withParserFactory(SqlBabelParserImpl.FACTORY)
          .withLex(Lex.BIG_QUERY)
          .withConformance(SqlConformanceEnum.BABEL);

  @Test
  public void testConvertTzReordersArguments() throws Exception {
    String sql = rewrite("SELECT convert_tz('2020-01-01', '+00:00', '+09:00') FROM t");
    assertTrue("Should contain CONVERT_TIMEZONE", sql.contains("CONVERT_TIMEZONE"));
    // Verify arg reorder: from_tz, to_tz come before datetime
    assertTrue(
        "from_tz should precede datetime", sql.indexOf("+00:00") < sql.indexOf("2020-01-01"));
  }

  @Test
  public void testConvertTzCaseInsensitive() throws Exception {
    String sql = rewrite("SELECT CONVERT_TZ('2020-01-01', '+00:00', '+09:00') FROM t");
    assertTrue("Should rewrite uppercase variant", sql.contains("CONVERT_TIMEZONE"));
  }

  @Test
  public void testWrongArityUntouched() throws Exception {
    String sql = rewrite("SELECT convert_tz('2020-01-01', '+00:00') FROM t");
    assertTrue("2-arg call should be left as convert_tz", sql.contains("convert_tz"));
    assertFalse("Should NOT rewrite to CONVERT_TIMEZONE", sql.contains("CONVERT_TIMEZONE"));
  }

  @Test
  public void testDifferentFunctionUntouched() throws Exception {
    String sql = rewrite("SELECT upper('hello') FROM t");
    assertFalse(
        "Non-matching function should not become CONVERT_TIMEZONE",
        sql.contains("CONVERT_TIMEZONE"));
  }

  @Test
  public void testStringLiteralDtIsCastToTimestamp() throws Exception {
    String sql = rewrite("SELECT convert_tz('2020-01-01 00:00:00', '+00:00', '+09:00') FROM t");
    assertTrue("Should contain CAST", sql.contains("CAST"));
    assertTrue("Should contain AS TIMESTAMP", sql.contains("AS TIMESTAMP"));
    assertTrue(
        "CAST should wrap the datetime literal",
        sql.contains("CAST('2020-01-01 00:00:00' AS TIMESTAMP)"));
  }

  @Test
  public void testIdentifierDtIsNotCast() throws Exception {
    String sql = rewrite("SELECT convert_tz(ts_col, '+00:00', '+09:00') FROM t");
    assertTrue("Should contain CONVERT_TIMEZONE", sql.contains("CONVERT_TIMEZONE"));
    assertFalse("Identifier operand should NOT be wrapped in CAST", sql.contains("CAST"));
    assertTrue("Identifier should appear as-is", sql.contains("ts_col"));
  }

  @Test
  public void testNestedConvertTz() throws Exception {
    String sql =
        rewrite(
            "SELECT convert_tz(convert_tz('2020-01-01', '+00:00', '+05:00'), '+05:00', '+09:00')"
                + " FROM t");
    // Both inner and outer should be rewritten
    assertFalse("No convert_tz should remain", sql.toLowerCase().contains("convert_tz("));
    assertTrue("Should contain CONVERT_TIMEZONE", sql.contains("CONVERT_TIMEZONE"));
  }

  private static String rewrite(String sql) throws Exception {
    SqlNode node = SqlParser.create(sql, PARSER_CONFIG).parseStmt();
    node = node.accept(ConvertTzRewriter.INSTANCE);
    return node.toString().replaceAll("\\n", " ");
  }
}
