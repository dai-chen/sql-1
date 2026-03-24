/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api;

import static java.sql.Types.BIGINT;
import static java.sql.Types.DATE;
import static java.sql.Types.INTEGER;
import static java.sql.Types.VARCHAR;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Map;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.sql.api.compiler.UnifiedQueryCompiler;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.executor.QueryType;

/**
 * Acceptance tests for datetime UDT rewriting. Verifies that: 1) RelNode from
 * UnifiedQueryPlanner.plan() contains no datetime UDTs 2) UnifiedQueryCompiler can compile and
 * execute the clean RelNode
 */
public class DatetimeUdtRewriterTest extends UnifiedQueryTestBase implements ResultSetAssertion {

  private UnifiedQueryCompiler compiler;

  @Before
  public void setUp() {
    // Override with datetime-aware schema using standard Calcite types
    testSchema =
        new AbstractSchema() {
          @Override
          protected Map<String, Table> getTableMap() {
            return Map.of("employees", createDatetimeEmployeesTable());
          }
        };

    context =
        UnifiedQueryContext.builder()
            .language(QueryType.PPL)
            .catalog(DEFAULT_CATALOG, testSchema)
            .build();
    planner = new UnifiedQueryPlanner(context);
    compiler = new UnifiedQueryCompiler(context);
  }

  /** Employees table with standard Calcite DATE/TIME/TIMESTAMP columns. */
  private Table createDatetimeEmployeesTable() {
    return SimpleTable.builder()
        .col("id", SqlTypeName.INTEGER)
        .col("name", SqlTypeName.VARCHAR)
        .col("date_hired", SqlTypeName.DATE)
        .col("login_time", SqlTypeName.TIME)
        .col("last_updated", SqlTypeName.TIMESTAMP)
        .row(
            new Object[] {
              1,
              "Alice",
              (int) LocalDate.of(2020, 3, 15).toEpochDay(),
              (int) (LocalTime.of(9, 0).toNanoOfDay() / 1_000_000),
              1705312200000L // 2024-01-15 10:30:00 UTC
            })
        .row(
            new Object[] {
              2,
              "Bob",
              (int) LocalDate.of(2021, 6, 20).toEpochDay(),
              (int) (LocalTime.of(14, 30).toNanoOfDay() / 1_000_000),
              1718886600000L // 2024-06-20 14:30:00 UTC
            })
        .build();
  }

  /** Assert no datetime UDT types exist anywhere in the plan. */
  private void assertNoDatetimeUDTs(RelNode plan) {
    plan.accept(
        new RelHomogeneousShuttle() {
          @Override
          public RelNode visit(RelNode other) {
            for (RelDataTypeField field : other.getRowType().getFieldList()) {
              assertFalse(
                  "Field " + field.getName() + " has UDT type: " + field.getType(),
                  OpenSearchTypeFactory.isUserDefinedType(field.getType()));
            }
            return super.visit(other);
          }
        });
    // Also check explain output for UDT type names
    String explained = plan.explain();
    assertFalse("Plan contains EXPR_DATE UDT", explained.contains("EXPR_DATE"));
    assertFalse("Plan contains EXPR_TIME UDT", explained.contains("EXPR_TIME"));
    assertFalse("Plan contains EXPR_TIMESTAMP UDT", explained.contains("EXPR_TIMESTAMP"));
  }

  // ==================== T1: DATE column comparison with DATE() function ====================

  @Test
  public void testDateColumnComparisonWithDateFunction() throws Exception {
    RelNode plan =
        planner.plan(
            "source = catalog.employees | where date_hired > DATE('2020-06-01') | fields id, name");
    assertNoDatetimeUDTs(plan);

    try (PreparedStatement stmt = compiler.compile(plan)) {
      ResultSet rs = stmt.executeQuery();
      verify(rs).expectSchema(col("id", INTEGER), col("name", VARCHAR)).expectData(row(2, "Bob"));
    }
  }

  // ==================== T2: TIMESTAMP column comparison ====================

  @Test
  public void testTimestampColumnComparisonWithTimestampFunction() throws Exception {
    RelNode plan =
        planner.plan(
            "source = catalog.employees"
                + " | where last_updated > TIMESTAMP('2024-03-01 00:00:00')"
                + " | fields id, name");
    assertNoDatetimeUDTs(plan);

    try (PreparedStatement stmt = compiler.compile(plan)) {
      ResultSet rs = stmt.executeQuery();
      verify(rs).expectSchema(col("id", INTEGER), col("name", VARCHAR)).expectData(row(2, "Bob"));
    }
  }

  // ==================== T3: TIME column comparison ====================

  @Test
  public void testTimeColumnComparisonWithTimeFunction() throws Exception {
    RelNode plan =
        planner.plan(
            "source = catalog.employees"
                + " | where login_time > TIME('12:00:00')"
                + " | fields id, name");
    assertNoDatetimeUDTs(plan);

    try (PreparedStatement stmt = compiler.compile(plan)) {
      ResultSet rs = stmt.executeQuery();
      verify(rs).expectSchema(col("id", INTEGER), col("name", VARCHAR)).expectData(row(2, "Bob"));
    }
  }

  // ==================== T4: CURRENT_DATE() zero-arg function ====================

  @Test
  public void testCurrentDateFunction() throws Exception {
    RelNode plan =
        planner.plan("source = catalog.employees | eval today = CURRENT_DATE() | fields id, today");
    assertNoDatetimeUDTs(plan);

    try (PreparedStatement stmt = compiler.compile(plan)) {
      ResultSet rs = stmt.executeQuery();
      // Just verify it compiles and executes — value is dynamic
      int rowCount = 0;
      while (rs.next()) {
        rowCount++;
        // today should be a java.sql.Date (standard DATE type)
        Object today = rs.getObject("today");
        assertTrue(
            "CURRENT_DATE should return java.sql.Date but got " + today.getClass(),
            today instanceof java.sql.Date);
      }
      assertTrue("Should have rows", rowCount > 0);
    }
  }

  // ==================== T5: Nested datetime functions ====================

  @Test
  public void testNestedDatetimeFunctions() throws Exception {
    // Test nested rewrite: inner DATE() is rewritten (returns standard DATE),
    // then DATEDIFF receives the standard DATE operand (adapted to VARCHAR for the UDF).
    RelNode plan =
        planner.plan(
            "source = catalog.employees"
                + " | eval days_since = DATEDIFF(DATE('2025-01-01'), DATE(date_hired))"
                + " | fields id, days_since");
    assertNoDatetimeUDTs(plan);

    try (PreparedStatement stmt = compiler.compile(plan)) {
      ResultSet rs = stmt.executeQuery();
      verify(rs)
          .expectSchema(col("id", INTEGER), col("days_since", BIGINT))
          .expectData(
              row(1, 1753L), // 2025-01-01 - 2020-03-15
              row(2, 1291L)); // 2025-01-01 - 2021-06-20
    }
  }

  // ==================== T6: Date extraction functions ====================

  @Test
  public void testYearExtractionFromDateColumn() throws Exception {
    RelNode plan =
        planner.plan(
            "source = catalog.employees | eval hire_year = YEAR(date_hired) | fields id,"
                + " hire_year");
    assertNoDatetimeUDTs(plan);

    try (PreparedStatement stmt = compiler.compile(plan)) {
      ResultSet rs = stmt.executeQuery();
      verify(rs)
          .expectSchema(col("id", INTEGER), col("hire_year", INTEGER))
          .expectData(row(1, 2020), row(2, 2021));
    }
  }

  // ==================== T7: CAST to datetime type ====================

  @Test
  public void testCastToDateType() throws Exception {
    RelNode plan =
        planner.plan(
            "source = catalog.employees"
                + " | eval d = CAST('2024-01-01' AS DATE)"
                + " | fields id, d");
    assertNoDatetimeUDTs(plan);

    try (PreparedStatement stmt = compiler.compile(plan)) {
      ResultSet rs = stmt.executeQuery();
      verify(rs)
          .expectSchema(col("id", INTEGER), col("d", DATE))
          .expectData(
              row(1, java.sql.Date.valueOf("2024-01-01")),
              row(2, java.sql.Date.valueOf("2024-01-01")));
    }
  }

  // ==================== T8: DATEDIFF with standard date columns ====================

  @Test
  public void testDatediffWithStandardDateColumns() throws Exception {
    RelNode plan =
        planner.plan(
            "source = catalog.employees"
                + " | eval days = DATEDIFF(DATE('2025-01-01'), date_hired)"
                + " | fields id, days");
    assertNoDatetimeUDTs(plan);

    try (PreparedStatement stmt = compiler.compile(plan)) {
      ResultSet rs = stmt.executeQuery();
      verify(rs)
          .expectSchema(col("id", INTEGER), col("days", BIGINT))
          .expectData(
              row(1, 1753L), // 2025-01-01 - 2020-03-15
              row(2, 1291L)); // 2025-01-01 - 2021-06-20
    }
  }

  // ==================== T9: DATE_FORMAT with standard timestamp column ====================

  @Test
  public void testDateFormatWithStandardTimestampColumn() throws Exception {
    RelNode plan =
        planner.plan(
            "source = catalog.employees"
                + " | eval formatted = DATE_FORMAT(last_updated, '%Y-%m')"
                + " | fields id, formatted");
    assertNoDatetimeUDTs(plan);

    try (PreparedStatement stmt = compiler.compile(plan)) {
      ResultSet rs = stmt.executeQuery();
      verify(rs)
          .expectSchema(col("id", INTEGER), col("formatted", VARCHAR))
          .expectData(row(1, "2024-01"), row(2, "2024-06"));
    }
  }

  // ==================== T10: Plan inspection — no UDTs in explain ====================

  @Test
  public void testPlanHasNoDatetimeUDTs() {
    RelNode plan =
        planner.plan(
            "source = catalog.employees | where date_hired > DATE('2020-06-01') | fields id, name");
    assertNoDatetimeUDTs(plan);
  }

  // ==================== T11: Plan inspection — Category B UDF present with standard type ========

  @Test
  public void testCategoryBUdfPresentWithStandardReturnType() {
    RelNode plan =
        planner.plan(
            "source = catalog.employees"
                + " | eval d = ADDDATE(date_hired, 1)"
                + " | fields id, d");
    assertNoDatetimeUDTs(plan);
    String explained = plan.explain();
    // ADDDATE should still appear as a UDF (no standard equivalent)
    assertTrue("ADDDATE should be in plan", explained.contains("ADDDATE"));
  }
}
