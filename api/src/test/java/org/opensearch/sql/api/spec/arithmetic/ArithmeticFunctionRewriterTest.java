/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.spec.arithmetic;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.calcite.config.Lex;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.babel.SqlBabelParserImpl;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.junit.Test;

/** Unit tests for {@link ArithmeticFunctionRewriter}. */
public class ArithmeticFunctionRewriterTest {

  private static final SqlParser.Config PARSER_CONFIG =
      SqlParser.config()
          .withParserFactory(SqlBabelParserImpl.FACTORY)
          .withLex(Lex.BIG_QUERY)
          .withConformance(SqlConformanceEnum.BABEL);

  @Test
  public void testAddRewrittenToPlus() throws Exception {
    String sql = rewrite("SELECT add(a, b) FROM t");
    assertTrue("add(a,b) should become a + b", sql.contains("+"));
  }

  @Test
  public void testSubtractRewrittenToMinus() throws Exception {
    String sql = rewrite("SELECT subtract(a, b) FROM t");
    assertTrue("subtract(a,b) should become a - b", sql.contains("-"));
  }

  @Test
  public void testMultiplyRewritten() throws Exception {
    String sql = rewrite("SELECT multiply(a, b) FROM t");
    assertTrue("multiply(a,b) should become a * b", sql.contains("*"));
  }

  @Test
  public void testDivideRewritten() throws Exception {
    String sql = rewrite("SELECT divide(a, b) FROM t");
    assertTrue("divide(a,b) should become a / b", sql.contains("/"));
  }

  @Test
  public void testModulusRewritten() throws Exception {
    String sql = rewrite("SELECT modulus(a, b) FROM t");
    assertTrue("modulus(a,b) should become MOD(a, b)", sql.contains("MOD"));
  }

  @Test
  public void testWrongArityUntouched() throws Exception {
    String sql = rewrite("SELECT add(a, b, c) FROM t");
    // 3-arg add should NOT be rewritten to +
    assertFalse("3-arg add should not become +", sql.contains("+"));
  }

  @Test
  public void testDifferentFunctionUntouched() throws Exception {
    String sql = rewrite("SELECT upper(a) FROM t");
    assertFalse("Non-matching function should not be rewritten", sql.contains("+"));
  }

  @Test
  public void testNestedArithmetic() throws Exception {
    String sql = rewrite("SELECT add(multiply(a, b), c) FROM t");
    assertTrue("Nested multiply should become *", sql.contains("*"));
    assertTrue("Outer add should become +", sql.contains("+"));
  }

  private static String rewrite(String sql) throws Exception {
    SqlNode node = SqlParser.create(sql, PARSER_CONFIG).parseStmt();
    node = node.accept(ArithmeticFunctionRewriter.INSTANCE);
    return node.toString().replaceAll("\\n", " ");
  }
}
