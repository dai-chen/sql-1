/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.spec.cast;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.babel.SqlBabelParserImpl;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.junit.Test;

/** Unit tests for {@link StringTypeNameRewriter}. */
public class StringTypeNameRewriterTest {

  private static final SqlParser.Config PARSER_CONFIG =
      SqlParser.config()
          .withParserFactory(SqlBabelParserImpl.FACTORY)
          .withLex(Lex.BIG_QUERY)
          .withConformance(SqlConformanceEnum.BABEL);

  @Test
  public void testCastAsStringRewrittenToVarchar() throws Exception {
    String sql = rewrite("SELECT CAST(x AS STRING) FROM t");
    assertTrue("CAST AS STRING should become CAST AS VARCHAR", sql.contains("VARCHAR"));
    assertFalse("STRING should not remain", sql.contains("STRING"));
  }

  @Test
  public void testCastAsVarcharUntouched() throws Exception {
    String sql = rewrite("SELECT CAST(x AS VARCHAR) FROM t");
    assertTrue("VARCHAR should remain", sql.contains("VARCHAR"));
  }

  @Test
  public void testCastAsIntegerUntouched() throws Exception {
    String sql = rewrite("SELECT CAST(x AS INTEGER) FROM t");
    assertTrue("INTEGER should remain", sql.contains("INTEGER"));
    assertFalse("Should not introduce VARCHAR", sql.contains("VARCHAR"));
  }

  @Test
  public void testNonCastCallUntouched() throws Exception {
    String sql = rewrite("SELECT upper(x) FROM t");
    assertFalse("Non-CAST call should not introduce VARCHAR", sql.contains("VARCHAR"));
  }

  @Test
  public void testNestedCastAsString() throws Exception {
    String sql = rewrite("SELECT CAST(CAST(x AS STRING) AS INTEGER) FROM t");
    assertTrue("Inner CAST AS STRING should become VARCHAR", sql.contains("VARCHAR"));
    assertFalse("STRING should not remain", sql.contains("STRING"));
  }

  private static String rewrite(String sql) throws Exception {
    SqlNode node = SqlParser.create(sql, PARSER_CONFIG).parseStmt();
    node = node.accept(StringTypeNameRewriter.INSTANCE);
    return node.toString().replaceAll("\\n", " ");
  }
}
