/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.junit.Before;
import org.junit.Test;
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
}
