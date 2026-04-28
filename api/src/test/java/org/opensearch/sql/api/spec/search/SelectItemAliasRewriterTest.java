/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.spec.search;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.calcite.avatica.util.Casing;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.parser.SqlParser;
import org.junit.Test;

/** Unit tests for {@link SelectItemAliasRewriter}. */
public class SelectItemAliasRewriterTest {

  /** Match production parser config in UnifiedQueryContext. */
  private static final SqlParser.Config PARSER_CONFIG =
      SqlParser.Config.DEFAULT.withUnquotedCasing(Casing.UNCHANGED);

  @Test
  public void testUnnamedAggregateGetsExpressionText() throws Exception {
    SqlNode result = rewrite("SELECT COUNT(*) FROM t");
    assertContains(result, "AS `COUNT(*)`");
  }

  @Test
  public void testMultipleUnnamedAggregates() throws Exception {
    SqlNode result = rewrite("SELECT SUM(a), COUNT(*), AVG(b) FROM t");
    // Alias text uses bare identifiers (no internal backticks) via toSqlString with
    // quoteAllIdentifiers(false). The alias identifier itself is still backtick-quoted by
    // the default toString() unparser.
    assertContains(result, "AS `SUM(a)`");
    assertContains(result, "AS `COUNT(*)`");
    assertContains(result, "AS `AVG(b)`");
  }

  @Test
  public void testUnnamedArithmeticExpression() throws Exception {
    SqlNode result = rewrite("SELECT 1 + 1 FROM t");
    assertContains(result, "AS `1 + 1`");
  }

  @Test
  public void testUnnamedFunctionCall() throws Exception {
    SqlNode result = rewrite("SELECT UPPER(name) FROM t");
    assertContains(result, "AS `UPPER(name)`");
  }

  @Test
  public void testExplicitlyAliasedExpressionUntouched() throws Exception {
    SqlNode result = rewrite("SELECT COUNT(*) AS cnt FROM t");
    String actual = flatten(result);
    assertContains(result, "AS `cnt`");
    assertFalse("Should not double-wrap", actual.contains("AS `COUNT(*)`"));
  }

  @Test
  public void testSimpleColumnReferenceUntouched() throws Exception {
    String sql = "SELECT name FROM t";
    String before = flatten(parse(sql));
    String after = flatten(rewrite(sql));
    assertEquals(before, after);
  }

  @Test
  public void testStarUntouched() throws Exception {
    String sql = "SELECT * FROM t";
    String before = flatten(parse(sql));
    String after = flatten(rewrite(sql));
    assertEquals(before, after);
  }

  @Test
  public void testQualifiedStarUntouched() throws Exception {
    String sql = "SELECT t.* FROM t";
    String before = flatten(parse(sql));
    String after = flatten(rewrite(sql));
    assertEquals(before, after);
  }

  @Test
  public void testMixedItems() throws Exception {
    SqlNode result = rewrite("SELECT id, COUNT(*), name AS n FROM t");
    String actual = flatten(result);
    assertContains(result, "AS `COUNT(*)`");
    assertFalse("id should not be aliased", actual.contains("AS `id`"));
    assertTrue("name AS n should remain", actual.contains("AS `n`"));
  }

  @Test
  public void testSubqueryInFromAlsoRewritten() throws Exception {
    SqlNode result = rewrite("SELECT x FROM (SELECT COUNT(*) FROM t) s");
    assertContains(result, "AS `COUNT(*)`");
  }

  @Test
  public void testUnionBothBranchesRewritten() throws Exception {
    SqlNode result = rewrite("SELECT COUNT(*) FROM t UNION SELECT COUNT(*) FROM u");
    String actual = flatten(result);
    // Both branches should have the alias
    int first = actual.indexOf("AS `COUNT(*)`");
    assertTrue("First branch should be rewritten", first >= 0);
    assertTrue("Second branch should be rewritten", actual.indexOf("AS `COUNT(*)`", first + 1) > 0);
  }

  @Test
  public void testNestedFunctionCalls() throws Exception {
    SqlNode result = rewrite("SELECT UPPER(TRIM(name)) FROM t");
    assertContains(result, "UPPER(TRIM(BOTH ' ' FROM `name`))");
  }

  @Test
  public void testAllUnchangedReturnsSameSelectList() throws Exception {
    String sql = "SELECT * FROM t";
    SqlSelect parsed = (SqlSelect) parse(sql);
    String beforeSelectList = parsed.getSelectList().toString();
    SqlSelect result = (SqlSelect) parsed.accept(SelectItemAliasRewriter.INSTANCE);
    assertEquals(beforeSelectList, result.getSelectList().toString());
    assertFalse("No AS should be added", flatten(result).contains(" AS "));
  }

  private static SqlNode rewrite(String sql) throws Exception {
    return parse(sql).accept(SelectItemAliasRewriter.INSTANCE);
  }

  private static SqlNode parse(String sql) throws Exception {
    return SqlParser.create(sql, PARSER_CONFIG).parseStmt();
  }

  private static String flatten(SqlNode node) {
    return node.toString().replaceAll("\\n", " ");
  }

  private static void assertContains(SqlNode node, String expected) {
    String actual = node.toString().replaceAll("\\n", " ");
    assertTrue(
        "Expected to contain: " + expected + "\nActual: " + actual, actual.contains(expected));
  }
}
