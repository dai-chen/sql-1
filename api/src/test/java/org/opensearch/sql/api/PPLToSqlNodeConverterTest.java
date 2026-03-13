/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api;

import static org.junit.Assert.assertEquals;

import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.junit.Test;
import org.opensearch.sql.ast.tree.UnresolvedPlan;

/**
 * PPL-to-SQL conversion tests.
 *
 * <p>Each test documents the exact SQL equivalent of a PPL query. The SQL is produced by
 * converting PPL AST → Calcite SqlNode → SQL string via CalciteSqlDialect.DEFAULT.
 *
 * <p>PPL pipe semantics map to nested subqueries: each pipe stage wraps the previous
 * result as {@code (SELECT ... FROM previous) AS _tN}.
 */
public class PPLToSqlNodeConverterTest {

  private static String toSql(SqlNode node) {
    return node.toSqlString(CalciteSqlDialect.DEFAULT).getSql();
  }

  /** Fluent assertion: {@code ppl("source=t | where a > 1").shouldTranslateTo("SELECT ...")} */
  private PplAssertion ppl(String ppl) {
    return new PplAssertion(ppl);
  }

  private class PplAssertion {
    private final String ppl;
    private final String sql;

    PplAssertion(String ppl) {
      this.ppl = ppl;
      UnresolvedPlan plan = PPLToSqlNodeConverter.parse(ppl);
      PPLToSqlNodeConverter converter = new PPLToSqlNodeConverter();
      this.sql = toSql(converter.convert(plan));
    }

    void shouldTranslateTo(String expectedSql) {
      assertEquals("PPL: " + ppl, expectedSql, sql);
    }
  }

  // ===== Core commands (US-003) =====

  @Test
  public void testSource() {
    ppl("source=t").shouldTranslateTo("""
        SELECT *
        FROM "t\"""");
  }

  @Test
  public void testWhereFilter() {
    ppl("source=t | where a > 1").shouldTranslateTo("""
        SELECT *
        FROM (SELECT *
        FROM "t") AS "_t1"
        WHERE "a" > 1""");
  }

  @Test
  public void testFieldsProject() {
    ppl("source=t | fields b, c").shouldTranslateTo("""
        SELECT "b", "c"
        FROM (SELECT *
        FROM "t") AS "_t1\"""");
  }

  @Test
  public void testSortOrderBy() {
    ppl("source=t | sort b").shouldTranslateTo("""
        SELECT *
        FROM "t"
        ORDER BY "b" NULLS FIRST""");
  }

  @Test
  public void testHeadLimit() {
    ppl("source=t | head 10").shouldTranslateTo("""
        SELECT *
        FROM "t"
        FETCH NEXT 10 ROWS ONLY""");
  }

  @Test
  public void testFullPipeline() {
    // Each pipe stage wraps the previous as a subquery (sort/head optimized to not wrap)
    ppl("source=t | where a > 1 | fields b, c | sort b | head 10").shouldTranslateTo("""
        SELECT "b", "c"
        FROM (SELECT *
        FROM (SELECT *
        FROM "t") AS "_t1"
        WHERE "a" > 1) AS "_t2"
        ORDER BY "b" NULLS FIRST
        FETCH NEXT 10 ROWS ONLY""");
  }

  // ===== Expression visitors (US-004) =====

  @Test
  public void testFunctionCall() {
    ppl("source=t | where abs(a) > 1").shouldTranslateTo("""
        SELECT *
        FROM (SELECT *
        FROM "t") AS "_t1"
        WHERE ABS("a") > 1""");
  }

  @Test
  public void testArithmeticOperators() {
    ppl("source=t | where a + b > 1").shouldTranslateTo("""
        SELECT *
        FROM (SELECT *
        FROM "t") AS "_t1"
        WHERE "a" + "b" > 1""");
  }

  @Test
  public void testIfFunction() {
    // PPL if(cond, then, else) → SQL CASE WHEN
    ppl("source=t | where if(a > 1, b, c) > 0").shouldTranslateTo("""
        SELECT *
        FROM (SELECT *
        FROM "t") AS "_t1"
        WHERE CASE WHEN "a" > 1 THEN "b" ELSE "c" END > 0""");
  }

  @Test
  public void testInExpression() {
    ppl("source=t | where a in (1, 2, 3)").shouldTranslateTo("""
        SELECT *
        FROM (SELECT *
        FROM "t") AS "_t1"
        WHERE "a" IN (1, 2, 3)""");
  }

  @Test
  public void testBetweenExpression() {
    ppl("source=t | where a between 1 and 10").shouldTranslateTo("""
        SELECT *
        FROM (SELECT *
        FROM "t") AS "_t1"
        WHERE "a" BETWEEN ASYMMETRIC 1 AND 10""");
  }

  @Test
  public void testIsNull() {
    // PPL isnull(a) → SQL IS NULL
    ppl("source=t | where isnull(a)").shouldTranslateTo("""
        SELECT *
        FROM (SELECT *
        FROM "t") AS "_t1"
        WHERE "a" IS NULL""");
  }

  @Test
  public void testCastExpression() {
    ppl("source=t | where cast(a as integer) > 0").shouldTranslateTo("""
        SELECT *
        FROM (SELECT *
        FROM "t") AS "_t1"
        WHERE CAST("a" AS INTEGER) > 0""");
  }

  @Test
  public void testXorExpression() {
    // PPL xor → SQL (A OR B) AND NOT (A AND B)
    ppl("source=t | where a xor b").shouldTranslateTo("""
        SELECT *
        FROM (SELECT *
        FROM "t") AS "_t1"
        WHERE ("a" OR "b") AND NOT ("a" AND "b")""");
  }

  @Test
  public void testRegexpCompare() {
    ppl("source=t | where a REGEXP 'pattern'").shouldTranslateTo("""
        SELECT *
        FROM (SELECT *
        FROM "t") AS "_t1"
        WHERE REGEXP_CONTAINS("a", 'pattern')""");
  }

  @Test
  public void testLiteralTypes() {
    ppl("source=t | where a = '2024-01-01'").shouldTranslateTo("""
        SELECT *
        FROM (SELECT *
        FROM "t") AS "_t1"
        WHERE "a" = '2024-01-01'""");
  }

  @Test
  public void testLogFunction() {
    // PPL log(a) → SQL LN(a)
    ppl("source=t | where log(a) > 1").shouldTranslateTo("""
        SELECT *
        FROM (SELECT *
        FROM "t") AS "_t1"
        WHERE LN("a") > 1""");
  }

  // ===== Stats / Aggregation (US-005) =====

  @Test
  public void testStatsCount() {
    ppl("source=t | stats count() as cnt").shouldTranslateTo("""
        SELECT COUNT(*) AS "cnt"
        FROM (SELECT *
        FROM "t") AS "_t1\"""");
  }

  @Test
  public void testStatsGroupBy() {
    ppl("source=t | stats avg(a) as avg_a by b").shouldTranslateTo("""
        SELECT CAST(AVG(CAST("a" AS DOUBLE)) AS DOUBLE) AS "avg_a", "b" AS "b"
        FROM (SELECT *
        FROM "t") AS "_t1"
        GROUP BY "b\"""");
  }

  @Test
  public void testStatsMultipleAggs() {
    ppl("source=t | stats count() as cnt, sum(a) as total by b").shouldTranslateTo("""
        SELECT COUNT(*) AS "cnt", SUM("a") AS "total", "b" AS "b"
        FROM (SELECT *
        FROM "t") AS "_t1"
        GROUP BY "b\"""");
  }

  @Test
  public void testStatsWithSpan() {
    // PPL span(a, 10) → SQL FLOOR(a / 10) * 10
    ppl("source=t | stats count() as cnt by span(a, 10)").shouldTranslateTo("""
        SELECT COUNT(*) AS "cnt", FLOOR("a" / 10) * 10 AS "span(a,10)"
        FROM (SELECT *
        FROM "t") AS "_t1"
        GROUP BY FLOOR("a" / 10) * 10""");
  }

  // ===== Eval (US-006) =====

  @Test
  public void testEvalSimple() {
    ppl("source=t | eval x = a + 1").shouldTranslateTo("""
        SELECT *, "a" + 1 AS "x"
        FROM (SELECT *
        FROM "t") AS "_t1\"""");
  }

  @Test
  public void testEvalForwardReference() {
    // y = x * 2 inlines x's definition: (a + 1) * 2
    ppl("source=t | eval x = a + 1, y = x * 2").shouldTranslateTo("""
        SELECT *, "a" + 1 AS "x", ("a" + 1) * 2 AS "y"
        FROM (SELECT *
        FROM "t") AS "_t1\"""");
  }

  @Test
  public void testEvalSelfReference() {
    // eval a = a + 1 shadows the original column
    ppl("source=t | eval a = a + 1").shouldTranslateTo("""
        SELECT *, "a" + 1 AS "a"
        FROM (SELECT *
        FROM "t") AS "_t1\"""");
  }

  // ===== Dedup (US-007) =====

  @Test
  public void testDedupSimple() {
    // Standard dedup: filter nulls → ROW_NUMBER() PARTITION BY field → keep rn <= 1
    ppl("source=t | dedup a").shouldTranslateTo("""
        SELECT *
        FROM (SELECT *
        FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY "a" ORDER BY "a") AS "_dedup_rn"
        FROM (SELECT *
        FROM (SELECT *
        FROM "t") AS "_t1"
        WHERE "a" IS NOT NULL) AS "_t2") AS "_t3"
        WHERE "_dedup_rn" <= 1) AS "_t4\"""");
  }

  @Test
  public void testDedupKeepEmpty() {
    // keepempty=true: keep rows where fields are null OR rn <= allowedDuplicates
    ppl("source=t | dedup 2 a, b keepempty=true").shouldTranslateTo("""
        SELECT *
        FROM (SELECT *
        FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY "a", "b" ORDER BY "a", "b") AS "_dedup_rn"
        FROM (SELECT *
        FROM "t") AS "_t1") AS "_t2"
        WHERE "a" IS NULL OR "b" IS NULL OR "_dedup_rn" <= 2) AS "_t3\"""");
  }

  @Test
  public void testDedupConsecutive() {
    // Consecutive dedup: gaps-and-islands algorithm using global_rn - group_rn
    ppl("source=t | dedup a consecutive=true").shouldTranslateTo("""
        SELECT *
        FROM (SELECT *
        FROM (SELECT *, ROW_NUMBER() OVER (PARTITION BY "a", "_global_rn" - "_group_rn" ORDER BY CAST("_id" AS INTEGER)) AS "_dedup_rn"
        FROM (SELECT *, ROW_NUMBER() OVER (ORDER BY CAST("_id" AS INTEGER)) AS "_global_rn", ROW_NUMBER() OVER (PARTITION BY "a" ORDER BY CAST("_id" AS INTEGER)) AS "_group_rn"
        FROM (SELECT *
        FROM (SELECT *
        FROM "t") AS "_t1"
        WHERE "a" IS NOT NULL) AS "_t2") AS "_t3") AS "_t4"
        WHERE "_dedup_rn" <= 1) AS "_t5\"""");
  }

  // ===== Join (US-008) =====

  @Test
  public void testJoinInner() {
    ppl("source=t1 | JOIN left=l, right=r ON l.id = r.id t2").shouldTranslateTo("""
        SELECT *
        FROM (SELECT *
        FROM "t1") AS "l"
        INNER JOIN (SELECT *
        FROM "t2") AS "r" ON "l"."id" = "r"."id\"""");
  }

  @Test
  public void testJoinLeft() {
    ppl("source=t1 | LEFT JOIN left=l, right=r ON l.id = r.id t2").shouldTranslateTo("""
        SELECT *
        FROM (SELECT *
        FROM "t1") AS "l"
        LEFT JOIN (SELECT *
        FROM "t2") AS "r" ON "l"."id" = "r"."id\"""");
  }

  @Test
  public void testJoinSemi() {
    // SEMI JOIN → EXISTS subquery
    ppl("source=t1 | SEMI JOIN left=l, right=r ON l.id = r.id t2").shouldTranslateTo("""
        SELECT *
        FROM (SELECT *
        FROM "t1") AS "l"
        WHERE EXISTS (SELECT 1
        FROM (SELECT *
        FROM "t2") AS "r"
        WHERE "l"."id" = "r"."id")""");
  }

  @Test
  public void testJoinAnti() {
    // ANTI JOIN → NOT EXISTS subquery
    ppl("source=t1 | ANTI JOIN left=l, right=r ON l.id = r.id t2").shouldTranslateTo("""
        SELECT *
        FROM (SELECT *
        FROM "t1") AS "l"
        WHERE NOT EXISTS (SELECT 1
        FROM (SELECT *
        FROM "t2") AS "r"
        WHERE "l"."id" = "r"."id")""");
  }

  // ===== Lookup (US-008) =====

  @Test
  public void testLookup() {
    // lookup → LEFT JOIN on matching field
    ppl("source=t1 | lookup t2 id").shouldTranslateTo("""
        SELECT *
        FROM (SELECT *
        FROM "t1") AS "_l"
        LEFT JOIN (SELECT *
        FROM "t2") AS "_r" ON "_l"."id" = "_r"."id\"""");
  }

  @Test
  public void testLookupWithOutput() {
    // lookup with replace → LEFT JOIN projecting specific columns from right
    ppl("source=t1 | lookup t2 id AS id replace salary").shouldTranslateTo("""
        SELECT "_l".*, "_r"."salary" AS "salary"
        FROM (SELECT *
        FROM "t1") AS "_l"
        LEFT JOIN (SELECT *
        FROM "t2") AS "_r" ON "_l"."id" = "_r"."id\"""");
  }

  // ===== Remaining core commands (US-009) =====

  @Test
  public void testRareTopN() {
    // top 3 a → COUNT + GROUP BY + ROW_NUMBER ordered DESC + filter rn <= 3
    ppl("source=t | top 3 a").shouldTranslateTo("""
        SELECT "a", "count"
        FROM (SELECT *
        FROM (SELECT *, ROW_NUMBER() OVER (ORDER BY "count" DESC) AS "_rn"
        FROM (SELECT "a", COUNT(*) AS "count"
        FROM (SELECT *
        FROM "t") AS "_t1" AS "_t2"
        GROUP BY "a") AS "_t3") AS "_t4"
        WHERE "_rn" <= 3) AS "_t5\"""");
  }

  @Test
  public void testRare() {
    // rare a → COUNT + GROUP BY + ROW_NUMBER ordered ASC + default limit 10
    ppl("source=t | rare a").shouldTranslateTo("""
        SELECT "a", "count"
        FROM (SELECT *
        FROM (SELECT *, ROW_NUMBER() OVER (ORDER BY "count") AS "_rn"
        FROM (SELECT "a", COUNT(*) AS "count"
        FROM (SELECT *
        FROM "t") AS "_t1" AS "_t2"
        GROUP BY "a") AS "_t3") AS "_t4"
        WHERE "_rn" <= 10) AS "_t5\"""");
  }

  @Test
  public void testWindow() {
    // eventstats → window function with PARTITION BY, full frame
    ppl("source=t | eventstats avg(a) as avg_a by b").shouldTranslateTo("""
        SELECT *, AVG(CAST("a" AS DOUBLE)) OVER (PARTITION BY "b" RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING) AS "avg_a"
        FROM (SELECT *
        FROM "t") AS "_t1\"""");
  }

  @Test
  public void testAppend() {
    // append → UNION ALL
    ppl("source=t1 | append [source=t2]").shouldTranslateTo("""
        SELECT *
        FROM (SELECT *
        FROM "t1"
        UNION ALL
        SELECT *
        FROM "t2") AS "_t1\"""");
  }

  @Test
  public void testRename() {
    ppl("source=t | rename a as b").shouldTranslateTo("""
        SELECT *, "a" AS "b"
        FROM (SELECT *
        FROM "t") AS "_t1\"""");
  }

  @Test
  public void testFillNull() {
    // fillnull → COALESCE(field, replacement)
    ppl("source=t | fillnull value=0 a, b").shouldTranslateTo("""
        SELECT *, COALESCE("a", 0) AS "a", COALESCE("b", 0) AS "b"
        FROM (SELECT *
        FROM "t") AS "_t1\"""");
  }

  @Test
  public void testParse() {
    // parse with named groups → REGEXP_EXTRACT per group
    ppl("source=t | parse msg '(?<year>\\d{4})-(?<month>\\d{2})'").shouldTranslateTo("""
        SELECT *, COALESCE(REGEXP_EXTRACT("msg", '(\\d{4})-(?:\\d{2})'), '') AS "year", \
        COALESCE(REGEXP_EXTRACT("msg", '(?:\\d{4})-(\\d{2})'), '') AS "month"
        FROM (SELECT *
        FROM "t") AS "_t1\"""");
  }

  @Test
  public void testReplace() {
    ppl("source=t | replace 'foo' with 'bar' in a").shouldTranslateTo("""
        SELECT *, REPLACE("a", 'foo', 'bar') AS "a"
        FROM (SELECT *
        FROM "t") AS "_t1\"""");
  }


}
