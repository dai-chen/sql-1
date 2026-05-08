/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.spec.datetime;

import static org.apache.calcite.sql.type.SqlTypeName.BIGINT;
import static org.apache.calcite.sql.type.SqlTypeName.DATE;
import static org.apache.calcite.sql.type.SqlTypeName.INTEGER;
import static org.apache.calcite.sql.type.SqlTypeName.TIME;
import static org.apache.calcite.sql.type.SqlTypeName.TIMESTAMP;
import static org.apache.calcite.sql.type.SqlTypeName.VARCHAR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.sql.api.ResultSetAssertion;
import org.opensearch.sql.api.UnifiedQueryContext;
import org.opensearch.sql.api.UnifiedQueryTestBase;
import org.opensearch.sql.api.compiler.UnifiedQueryCompiler;
import org.opensearch.sql.executor.QueryType;

public class DatetimeExtensionTest extends UnifiedQueryTestBase implements ResultSetAssertion {

  private UnifiedQueryCompiler compiler;

  @Override
  protected UnifiedQueryContext.Builder contextBuilder() {
    return UnifiedQueryContext.builder()
        .language(QueryType.PPL)
        .catalog(
            DEFAULT_CATALOG,
            new AbstractSchema() {
              @Override
              protected Map<String, Table> getTableMap() {
                return Map.of("events", createEventsTable());
              }
            });
  }

  @Before
  public void setUp() {
    super.setUp();
    compiler = new UnifiedQueryCompiler(context);
  }

  private Table createEventsTable() {
    return SimpleTable.builder()
        .col("id", INTEGER)
        .col("name", VARCHAR)
        .col("hire_date", DATE)
        .col("start_time", TIME)
        .col("created_at", TIMESTAMP)
        .row(new Object[] {1, "Alice", 19738, 43200000, 1705305600000L})
        .row(new Object[] {2, "Bob", 19894, 50400000, 1718841600000L})
        .build();
  }

  @Test
  public void testUdfOnLiteralsNormalizedAndExecutable() throws Exception {
    var plan =
        givenQuery(
                """
                source = catalog.events \
                | eval d = DATE('2024-01-01'), t = TIME('12:30:00'), ts = TIMESTAMP('2024-01-01 12:30:00') \
                | fields d, t, ts\
                """)
            .assertPlan(
                """
                LogicalProject(d=[CAST($0):VARCHAR], t=[CAST($1):VARCHAR], ts=[CAST($2):VARCHAR])
                  LogicalProject(d=[DATE('2024-01-01':VARCHAR)], t=[TIME('12:30:00':VARCHAR)], ts=[TIMESTAMP('2024-01-01 12:30:00':VARCHAR)])
                    LogicalTableScan(table=[[catalog, events]])
                """)
            .plan();
    assertCallType(plan, "DATE", DATE);
    assertCallType(plan, "TIME", TIME, 9);
    assertCallType(plan, "TIMESTAMP", TIMESTAMP, 9);
    try (PreparedStatement stmt = compiler.compile(plan)) {
      ResultSet rs = stmt.executeQuery();
      verify(rs)
          .expectSchema(
              col("d", java.sql.Types.VARCHAR),
              col("t", java.sql.Types.VARCHAR),
              col("ts", java.sql.Types.VARCHAR))
          .expectData(
              row("2024-01-01", "12:30:00", "2024-01-01 12:30:00"),
              row("2024-01-01", "12:30:00", "2024-01-01 12:30:00"));
    }
  }

  @Test
  public void testNestedUdfCallsExecutable() throws Exception {
    var plan =
        givenQuery(
                """
                source = catalog.events \
                | eval d = DATEDIFF(DATE('2025-01-01'), DATE('2024-01-01')) \
                | fields d\
                """)
            .assertPlan(
                """
                LogicalProject(d=[DATEDIFF(DATE('2025-01-01':VARCHAR), DATE('2024-01-01':VARCHAR))])
                  LogicalTableScan(table=[[catalog, events]])
                """)
            .plan();
    assertCallType(plan, "DATE", DATE);
    assertCallType(plan, "DATEDIFF", BIGINT);
    try (PreparedStatement stmt = compiler.compile(plan)) {
      ResultSet rs = stmt.executeQuery();
      verify(rs).expectSchema(col("d", java.sql.Types.BIGINT)).expectData(row(366L), row(366L));
    }
  }

  @Test
  public void testFilterWithTimestampUdf() throws Exception {
    var plan =
        givenQuery(
                """
                source = catalog.events \
                | where created_at < TIMESTAMP('2024-06-01 00:00:00') \
                | fields id\
                """)
            .assertPlan(
                """
                LogicalProject(id=[$0])
                  LogicalFilter(condition=[<($4, TIMESTAMP('2024-06-01 00:00:00':VARCHAR))])
                    LogicalTableScan(table=[[catalog, events]])
                """)
            .plan();
    assertCallType(plan, "TIMESTAMP", TIMESTAMP, 9);
    try (PreparedStatement stmt = compiler.compile(plan)) {
      ResultSet rs = stmt.executeQuery();
      verify(rs).expectSchema(col("id", java.sql.Types.INTEGER)).expectData(row(1));
    }
  }

  @Test
  public void testStandardDatetimeFieldsCastToVarchar() throws Exception {
    var plan =
        givenQuery("source = catalog.events | fields hire_date, start_time, created_at")
            .assertPlan(
                """
                LogicalProject(hire_date=[CAST($0):VARCHAR NOT NULL], start_time=[CAST($1):VARCHAR NOT NULL], created_at=[CAST($2):VARCHAR NOT NULL])
                  LogicalProject(hire_date=[$2], start_time=[$3], created_at=[$4])
                    LogicalTableScan(table=[[catalog, events]])
                """)
            .plan();
    try (PreparedStatement stmt = compiler.compile(plan)) {
      ResultSet rs = stmt.executeQuery();
      verify(rs)
          .expectSchema(
              col("hire_date", java.sql.Types.VARCHAR),
              col("start_time", java.sql.Types.VARCHAR),
              col("created_at", java.sql.Types.VARCHAR))
          .expectData(
              row("2024-01-16", "12:00:00", "2024-01-15 08:00:00"),
              row("2024-06-20", "14:00:00", "2024-06-20 00:00:00"));
    }
  }

  @Test
  public void testNonDatetimeFieldsNotWrapped() throws Exception {
    var plan =
        givenQuery("source = catalog.events | fields id, name")
            .assertPlan(
                """
                LogicalProject(id=[$0], name=[$1])
                  LogicalTableScan(table=[[catalog, events]])
                """)
            .plan();
    try (PreparedStatement stmt = compiler.compile(plan)) {
      ResultSet rs = stmt.executeQuery();
      verify(rs)
          .expectSchema(col("id", java.sql.Types.INTEGER), col("name", java.sql.Types.VARCHAR))
          .expectData(row(1, "Alice"), row(2, "Bob"));
    }
  }

  @Test
  public void testNonDatetimeUdfUnaffected() throws Exception {
    var plan =
        givenQuery("source = catalog.events | eval s = CONCAT(name, ' test') | fields s")
            .assertPlan(
                """
                LogicalProject(s=[CONCAT($1, ' test':VARCHAR)])
                  LogicalTableScan(table=[[catalog, events]])
                """)
            .plan();
    try (PreparedStatement stmt = compiler.compile(plan)) {
      ResultSet rs = stmt.executeQuery();
      verify(rs)
          .expectSchema(col("s", java.sql.Types.VARCHAR))
          .expectData(row("Alice test"), row("Bob test"));
    }
  }

  private static void assertCallType(RelNode plan, String operatorName, SqlTypeName expectedType) {
    assertCallType(plan, operatorName, expectedType, -1);
  }

  private static void assertCallType(
      RelNode plan, String operatorName, SqlTypeName expectedType, int expectedPrecision) {
    AtomicReference<RexCall> ref = new AtomicReference<>();
    plan.accept(
        new RelHomogeneousShuttle() {
          @Override
          public RelNode visit(RelNode other) {
            RelNode visited = super.visit(other);
            visited.accept(
                new RexShuttle() {
                  @Override
                  public RexNode visitCall(RexCall call) {
                    if (ref.get() == null
                        && call.getOperator().getName().equalsIgnoreCase(operatorName)) {
                      ref.set(call);
                    }
                    return super.visitCall(call);
                  }
                });
            return visited;
          }
        });
    assertNotNull("No RexCall found for: " + operatorName, ref.get());
    assertEquals(operatorName + " type", expectedType, ref.get().getType().getSqlTypeName());
    if (expectedPrecision >= 0) {
      assertEquals(
          operatorName + " precision", expectedPrecision, ref.get().getType().getPrecision());
    }
  }

  @Test
  public void testAggMaxOnDatetimeUdf() throws Exception {
    var plan = planner.plan("source = catalog.events | stats max(DATE('2024-01-01')) as m");
    try (PreparedStatement stmt = compiler.compile(plan)) {
      ResultSet rs = stmt.executeQuery();
      verify(rs).expectSchema(col("m", java.sql.Types.VARCHAR)).expectData(row("2024-01-01"));
    }
  }

  @Test
  public void testAggGroupByDatetimeUdf() throws Exception {
    var plan =
        planner.plan(
            "source = catalog.events | eval d = DATE('2024-01-01') | stats count() as c by d");
    try (PreparedStatement stmt = compiler.compile(plan)) {
      ResultSet rs = stmt.executeQuery();
      verify(rs)
          .expectSchema(col("c", java.sql.Types.BIGINT), col("d", java.sql.Types.VARCHAR))
          .expectData(row(2L, "2024-01-01"));
    }
  }

  @Test
  public void testAggOnDatetimeColumnWorks() throws Exception {
    var plan = planner.plan("source = catalog.events | stats max(hire_date) as m");
    try (PreparedStatement stmt = compiler.compile(plan)) {
      ResultSet rs = stmt.executeQuery();
      verify(rs).expectSchema(col("m", java.sql.Types.VARCHAR)).expectData(row("2024-06-20"));
    }
  }
}
