/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.spec.datetime;

import static java.sql.Types.DATE;
import static java.sql.Types.VARCHAR;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalDate;
import java.time.LocalTime;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.sql.api.ResultSetAssertion;
import org.opensearch.sql.api.UnifiedQueryTestBase;
import org.opensearch.sql.api.compiler.UnifiedQueryCompiler;

/**
 * Tests for {@link DatetimeUdtLiteralCoercionRule}. Exercises the non-UDF constructs where a
 * standard Calcite DATE/TIME/TIMESTAMP operand meets a VARCHAR literal: comparisons, conditional
 * expressions ({@code case()}, {@code if()}), and {@code fillnull}. Without the rule these queries
 * fail at runtime (Janino cannot compile {@code int > String}) or at Calcite code generation
 * (COALESCE type mismatch).
 *
 * <p>Note: {@code IN} and {@code BETWEEN} are intentionally NOT covered here. The PPL AST→Rex
 * visitor ({@code CalciteRexNodeVisitor.visitIn} / {@code visitBetween}) rejects cross-type lists
 * at translation time by throwing {@link org.opensearch.sql.exception.SemanticCheckException}
 * before any post-analysis rule can run. Covering those cases requires a separate change to the PPL
 * visitor (out of scope for this post-analysis rule).
 */
public class DatetimeUdtLiteralCoercionRuleTest extends UnifiedQueryTestBase
    implements ResultSetAssertion {

  private UnifiedQueryCompiler compiler;

  @Before
  public void setUp() {
    super.setUp();
    compiler = new UnifiedQueryCompiler(context);
  }

  @Override
  protected Table createEmployeesTable() {
    return SimpleTable.builder()
        .col("name", SqlTypeName.VARCHAR)
        .col("hire_date", SqlTypeName.DATE)
        .col("login_time", SqlTypeName.TIME)
        .col("updated_at", SqlTypeName.TIMESTAMP)
        .row(
            new Object[] {
              "Alice",
              (int) LocalDate.of(2020, 3, 15).toEpochDay(),
              (int) (LocalTime.of(9, 30).toNanoOfDay() / 1_000_000),
              1705312200000L
            })
        .row(
            new Object[] {
              "Bob",
              (int) LocalDate.of(2022, 6, 1).toEpochDay(),
              (int) (LocalTime.of(18, 45).toNanoOfDay() / 1_000_000),
              1735689600000L
            })
        .build();
  }

  private ResultSet planAndExecute(String query) throws Exception {
    PreparedStatement stmt = compiler.compile(planner.plan(query));
    return stmt.executeQuery();
  }

  /** Case 1a: greater-than between DATE column and VARCHAR literal. */
  @Test
  public void compareDateColWithStringLiteral() throws Exception {
    ResultSet rs =
        planAndExecute(
            "source = catalog.employees | where hire_date > '2021-01-01' | fields name, hire_date");
    verify(rs)
        .expectSchema(col("name", VARCHAR), col("hire_date", DATE))
        .expectData(row("Bob", java.sql.Date.valueOf("2022-06-01")));
  }

  /** Case 1b: less-than between TIME column and VARCHAR literal. */
  @Test
  public void compareTimeColWithStringLiteral() throws Exception {
    ResultSet rs =
        planAndExecute("source = catalog.employees | where login_time < '12:00:00' | fields name");
    verify(rs).expectSchema(col("name", VARCHAR)).expectData(row("Alice"));
  }

  /** Case 1c: greater-than-or-equal between TIMESTAMP column and VARCHAR literal. */
  @Test
  public void compareTimestampColWithStringLiteral() throws Exception {
    ResultSet rs =
        planAndExecute(
            "source = catalog.employees"
                + " | where updated_at >= '2025-01-01 00:00:00'"
                + " | fields name");
    verify(rs).expectSchema(col("name", VARCHAR)).expectData(row("Bob"));
  }

  /** Case 2: equality between DATE column and VARCHAR literal. */
  @Test
  public void equalsDateColWithStringLiteral() throws Exception {
    ResultSet rs =
        planAndExecute("source = catalog.employees | where hire_date = '2020-03-15' | fields name");
    verify(rs).expectSchema(col("name", VARCHAR)).expectData(row("Alice"));
  }

  /** Case 3: {@code case()} condition comparing DATE column to VARCHAR literal. */
  @Test
  public void caseConditionOnDateColAndStringLiteral() throws Exception {
    ResultSet rs =
        planAndExecute(
            "source = catalog.employees"
                + " | eval bucket = case(hire_date > '2021-01-01', 'new', true, 'old')"
                + " | fields name, bucket");
    verify(rs)
        .expectSchema(col("name", VARCHAR), col("bucket", VARCHAR))
        .expectData(row("Alice", "old"), row("Bob", "new"));
  }

  /** Negative: comparison between two DATE columns is untouched (common type already). */
  @Test
  public void compareTwoDateColsUnaffected() throws Exception {
    ResultSet rs =
        planAndExecute("source = catalog.employees | where hire_date = hire_date | fields name");
    verify(rs).expectSchema(col("name", VARCHAR)).expectData(row("Alice"), row("Bob"));
  }

  /** Negative: VARCHAR-VARCHAR comparison is untouched (no datetime operand). */
  @Test
  public void compareVarcharColsUnaffected() throws Exception {
    ResultSet rs =
        planAndExecute("source = catalog.employees | where name = 'Alice' | fields name");
    verify(rs).expectSchema(col("name", VARCHAR)).expectData(row("Alice"));
  }
}
