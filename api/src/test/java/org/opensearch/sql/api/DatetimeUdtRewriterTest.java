/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api;

import static java.sql.Types.INTEGER;
import static java.sql.Types.VARCHAR;
import static org.junit.Assert.assertFalse;

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

  // ==================== T10: Plan inspection — no UDTs in explain ====================

  @Test
  public void testPlanHasNoDatetimeUDTs() {
    RelNode plan =
        planner.plan(
            "source = catalog.employees | where date_hired > DATE('2020-06-01') | fields id, name");
    assertNoDatetimeUDTs(plan);
  }
}
