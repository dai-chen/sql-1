/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.SchemaPlus;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.sql.calcite.parser.OpenSearchSqlParserImpl;
import org.opensearch.sql.calcite.parser.StarExceptReplaceRewriter;
import org.opensearch.sql.calcite.parser.SqlStarExceptReplace;
import org.opensearch.sql.executor.QueryType;

public class StarExceptReplaceTest extends UnifiedQueryTestBase {

  @Override
  @Before
  public void setUp() {
    super.setUp();
    try {
      context.close();
    } catch (Exception e) {
      /* ignore */
    }

    context =
        UnifiedQueryContext.builder()
            .language(QueryType.SQL)
            .conformance(SqlConformanceEnum.DEFAULT)
            .catalog(DEFAULT_CATALOG, testSchema)
            .defaultNamespace(DEFAULT_CATALOG)
            .build();
    planner = new UnifiedQueryPlanner(context);
  }

  @Test
  public void testStandardSelectStarParsesWithCustomParser() {
    RelNode plan = planner.plan("SELECT * FROM employees");
    assertNotNull("SELECT * should parse with custom parser", plan);
  }

  @Test
  public void testStandardSelectColumnsParsesWithCustomParser() {
    RelNode plan = planner.plan("SELECT id, name FROM employees");
    assertNotNull("SELECT columns should parse with custom parser", plan);
  }

  @Test
  public void testStandardSelectWithWhereParsesWithCustomParser() {
    RelNode plan = planner.plan("SELECT * FROM employees WHERE age > 30");
    assertNotNull("SELECT with WHERE should parse with custom parser", plan);
  }

  @Test
  public void testUnparseExceptOnly() {
    SqlNode star = SqlIdentifier.star(SqlParserPos.ZERO);
    SqlNodeList except =
        new SqlNodeList(
            java.util.List.of(
                new SqlIdentifier("a", SqlParserPos.ZERO),
                new SqlIdentifier("b", SqlParserPos.ZERO)),
            SqlParserPos.ZERO);
    SqlStarExceptReplace node =
        new SqlStarExceptReplace(SqlParserPos.ZERO, star, except, null);
    SqlPrettyWriter writer = new SqlPrettyWriter();
    node.unparse(writer, 0, 0);
    assertEquals("* EXCEPT (\"a\", \"b\")", writer.toString());
  }

  @Test
  public void testUnparseReplaceOnly() {
    SqlNode star = SqlIdentifier.star(SqlParserPos.ZERO);
    SqlNode expr =
        new SqlBasicCall(
            SqlStdOperatorTable.PLUS,
            new SqlNode[] {
              new SqlIdentifier("x", SqlParserPos.ZERO),
              SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO)
            },
            SqlParserPos.ZERO);
    SqlNode asCall =
        new SqlBasicCall(
            SqlStdOperatorTable.AS,
            new SqlNode[] {expr, new SqlIdentifier("c", SqlParserPos.ZERO)},
            SqlParserPos.ZERO);
    SqlNodeList replace =
        new SqlNodeList(java.util.List.of(asCall), SqlParserPos.ZERO);
    SqlStarExceptReplace node =
        new SqlStarExceptReplace(SqlParserPos.ZERO, star, null, replace);
    SqlPrettyWriter writer = new SqlPrettyWriter();
    node.unparse(writer, 0, 0);
    assertEquals("* REPLACE (\"x\" + 1 AS \"c\")", writer.toString());
  }

  @Test
  public void testUnparseExceptAndReplace() {
    SqlNode star = SqlIdentifier.star(SqlParserPos.ZERO);
    SqlNodeList except =
        new SqlNodeList(
            java.util.List.of(new SqlIdentifier("a", SqlParserPos.ZERO)),
            SqlParserPos.ZERO);
    SqlNode expr =
        new SqlBasicCall(
            SqlStdOperatorTable.PLUS,
            new SqlNode[] {
              new SqlIdentifier("x", SqlParserPos.ZERO),
              SqlLiteral.createExactNumeric("1", SqlParserPos.ZERO)
            },
            SqlParserPos.ZERO);
    SqlNode asCall =
        new SqlBasicCall(
            SqlStdOperatorTable.AS,
            new SqlNode[] {expr, new SqlIdentifier("c", SqlParserPos.ZERO)},
            SqlParserPos.ZERO);
    SqlNodeList replace =
        new SqlNodeList(java.util.List.of(asCall), SqlParserPos.ZERO);
    SqlStarExceptReplace node =
        new SqlStarExceptReplace(SqlParserPos.ZERO, star, except, replace);
    SqlPrettyWriter writer = new SqlPrettyWriter();
    node.unparse(writer, 0, 0);
    assertEquals("* EXCEPT (\"a\") REPLACE (\"x\" + 1 AS \"c\")", writer.toString());
  }

  // --- Parse tests (Task 3) ---

  private SqlParser.Config parserConfig() {
    return SqlParser.Config.DEFAULT
        .withParserFactory(OpenSearchSqlParserImpl.FACTORY);
  }

  private SqlNode parse(String sql) throws SqlParseException {
    return SqlParser.create(sql, parserConfig()).parseQuery();
  }

  private SqlStarExceptReplace parseAndGetExceptReplace(String sql) throws SqlParseException {
    SqlNode node = parse(sql);
    assertTrue("Expected SqlSelect, got " + node.getClass(), node instanceof SqlSelect);
    SqlSelect select = (SqlSelect) node;
    SqlNode firstItem = select.getSelectList().get(0);
    assertTrue(
        "Expected SqlStarExceptReplace, got " + firstItem.getClass(),
        firstItem instanceof SqlStarExceptReplace);
    return (SqlStarExceptReplace) firstItem;
  }

  @Test
  public void testParseSelectStarExcept() throws SqlParseException {
    SqlStarExceptReplace node = parseAndGetExceptReplace("SELECT * EXCEPT(a, b) FROM t");
    assertNotNull(node.getExcept());
    assertEquals(2, node.getExcept().size());
    assertEquals("A", ((SqlIdentifier) node.getExcept().get(0)).getSimple());
    assertEquals("B", ((SqlIdentifier) node.getExcept().get(1)).getSimple());
  }

  @Test
  public void testParseSelectStarReplace() throws SqlParseException {
    SqlStarExceptReplace node = parseAndGetExceptReplace("SELECT * REPLACE(x + 1 AS c) FROM t");
    assertNotNull(node.getReplace());
    assertEquals(1, node.getReplace().size());
  }

  @Test
  public void testParseSelectStarExceptAndReplace() throws SqlParseException {
    SqlStarExceptReplace node =
        parseAndGetExceptReplace("SELECT * EXCEPT(a) REPLACE(b + 1 AS b) FROM t");
    assertNotNull(node.getExcept());
    assertEquals(1, node.getExcept().size());
    assertNotNull(node.getReplace());
    assertEquals(1, node.getReplace().size());
  }

  @Test
  public void testParseSelectStarWithoutExceptReplaceStillWorks() throws SqlParseException {
    SqlNode node = parse("SELECT * FROM t");
    assertTrue(node instanceof SqlSelect);
    SqlSelect select = (SqlSelect) node;
    // Plain * should remain a SqlIdentifier, not SqlStarExceptReplace
    assertTrue(
        "Plain * should be SqlIdentifier",
        select.getSelectList().get(0) instanceof SqlIdentifier);
  }

  @Test
  public void testParseSelectStarExceptDoesNotBreakSetExcept() throws SqlParseException {
    // EXCEPT as set operation should still work
    SqlNode node = parse("SELECT * FROM t EXCEPT SELECT * FROM t");
    assertNotNull(node);
  }

  @Test
  public void testParseQualifiedStarExcept() throws SqlParseException {
    SqlStarExceptReplace node = parseAndGetExceptReplace("SELECT t.* EXCEPT(a) FROM t");
    assertNotNull(node.getExcept());
    assertEquals(1, node.getExcept().size());
    // Star should be qualified
    assertTrue(node.getStar() instanceof SqlIdentifier);
    SqlIdentifier star = (SqlIdentifier) node.getStar();
    assertTrue(star.isStar());
  }

  // --- Rewriter tests (Task 4) ---

  private StarExceptReplaceRewriter createRewriter() {
    SchemaPlus rootSchema = CalciteSchema.createRootSchema(true, false).plus();
    rootSchema.add(DEFAULT_CATALOG, testSchema);
    return new StarExceptReplaceRewriter(rootSchema.getSubSchema(DEFAULT_CATALOG));
  }

  @Test
  public void testRewriteSelectStarExcept() throws SqlParseException {
    SqlNode parsed = parse("SELECT * EXCEPT(age) FROM employees");
    SqlNode rewritten = createRewriter().rewrite(parsed);
    SqlSelect select = (SqlSelect) rewritten;
    assertEquals(3, select.getSelectList().size());
    for (SqlNode item : select.getSelectList()) {
      assertTrue(!(item instanceof SqlStarExceptReplace));
    }
  }

  @Test
  public void testRewriteSelectStarExceptMultipleColumns() throws SqlParseException {
    SqlNode parsed = parse("SELECT * EXCEPT(age, department) FROM employees");
    SqlNode rewritten = createRewriter().rewrite(parsed);
    SqlSelect select = (SqlSelect) rewritten;
    assertEquals(2, select.getSelectList().size());
  }

  @Test
  public void testRewriteSelectStarReplace() throws SqlParseException {
    SqlNode parsed = parse("SELECT * REPLACE(age + 1 AS age) FROM employees");
    SqlNode rewritten = createRewriter().rewrite(parsed);
    SqlSelect select = (SqlSelect) rewritten;
    assertEquals(4, select.getSelectList().size());
    // age is at index 2; should be a SqlBasicCall (AS), not a plain SqlIdentifier
    SqlNode ageItem = select.getSelectList().get(2);
    assertTrue(ageItem instanceof SqlBasicCall);
  }

  @Test
  public void testRewriteSelectStarExceptAndReplace() throws SqlParseException {
    SqlNode parsed = parse("SELECT * EXCEPT(department) REPLACE(age + 1 AS age) FROM employees");
    SqlNode rewritten = createRewriter().rewrite(parsed);
    SqlSelect select = (SqlSelect) rewritten;
    assertEquals(3, select.getSelectList().size());
  }

  @Test
  public void testRewritePreservesNonStarItems() throws SqlParseException {
    SqlNode parsed = parse("SELECT id, * EXCEPT(id) FROM employees");
    SqlNode rewritten = createRewriter().rewrite(parsed);
    SqlSelect select = (SqlSelect) rewritten;
    assertEquals(4, select.getSelectList().size());
  }

  @Test
  public void testRewriteQualifiedStarExcept() throws SqlParseException {
    SqlNode parsed = parse("SELECT employees.* EXCEPT(age) FROM employees");
    SqlNode rewritten = createRewriter().rewrite(parsed);
    SqlSelect select = (SqlSelect) rewritten;
    assertEquals(3, select.getSelectList().size());
  }

  // --- End-to-end planner tests (Task 5) ---

  @Test
  public void testPlanSelectStarExcept() {
    RelNode plan = planner.plan("SELECT * EXCEPT(age) FROM employees");
    assertNotNull(plan);
    // Should have 3 columns: id, name, department (age excluded)
    assertEquals(3, plan.getRowType().getFieldCount());
    assertEquals("id", plan.getRowType().getFieldNames().get(0));
    assertEquals("name", plan.getRowType().getFieldNames().get(1));
    assertEquals("department", plan.getRowType().getFieldNames().get(2));
  }

  @Test
  public void testPlanSelectStarExceptMultipleColumns() {
    RelNode plan = planner.plan("SELECT * EXCEPT(age, department) FROM employees");
    assertNotNull(plan);
    assertEquals(2, plan.getRowType().getFieldCount());
    assertEquals("id", plan.getRowType().getFieldNames().get(0));
    assertEquals("name", plan.getRowType().getFieldNames().get(1));
  }

  @Test
  public void testPlanSelectQualifiedStarExcept() {
    RelNode plan = planner.plan("SELECT employees.* EXCEPT(age) FROM employees");
    assertNotNull(plan);
    assertEquals(3, plan.getRowType().getFieldCount());
  }
}
