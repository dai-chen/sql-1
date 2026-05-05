/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.spec.datetime;

import static java.sql.Types.VARCHAR;
import static org.apache.calcite.sql.type.SqlTypeName.DATE;
import static org.apache.calcite.sql.type.SqlTypeName.INTEGER;
import static org.apache.calcite.sql.type.SqlTypeName.TIME;
import static org.apache.calcite.sql.type.SqlTypeName.TIMESTAMP;
import static org.junit.Assert.assertFalse;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Map;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.sql.api.ResultSetAssertion;
import org.opensearch.sql.api.UnifiedQueryContext;
import org.opensearch.sql.api.UnifiedQueryTestBase;
import org.opensearch.sql.api.compiler.UnifiedQueryCompiler;
import org.opensearch.sql.calcite.type.AbstractExprRelDataType;
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
        .col("name", org.apache.calcite.sql.type.SqlTypeName.VARCHAR)
        .col("hire_date", DATE)
        .col("start_time", TIME)
        .col("created_at", TIMESTAMP)
        .row(new Object[] {1, "Alice", 19738, 43200000, 1705305600000L})
        .row(new Object[] {2, "Bob", 19894, 50400000, 1718841600000L})
        .build();
  }

  @Test
  public void testUdfResultNormalizedAndCastToVarchar() {
    var result =
        givenQuery("source = catalog.events | eval d = DATE(name) | fields d")
            .assertPlan(
                """
                LogicalProject(d=[CAST($0):VARCHAR])
                  LogicalProject(d=[DATE($1)])
                    LogicalTableScan(table=[[catalog, events]])
                """);
    assertNoUdt(result.plan());
  }

  @Test
  public void testNestedUdfCallsNormalized() {
    var result =
        givenQuery("source = catalog.events | eval d = DATEDIFF(DATE(name), DATE(name)) | fields d")
            .assertPlan(
                """
                LogicalProject(d=[DATEDIFF(DATE($1), DATE($1))])
                  LogicalTableScan(table=[[catalog, events]])
                """);
    assertNoUdt(result.plan());
  }

  @Test
  public void testDateLiteralCastToVarchar() {
    var result =
        givenQuery("source = catalog.events | eval d = DATE('2024-01-01') | fields d")
            .assertPlan(
                """
                LogicalProject(d=[CAST($0):VARCHAR])
                  LogicalProject(d=[DATE('2024-01-01':VARCHAR)])
                    LogicalTableScan(table=[[catalog, events]])
                """);
    assertNoUdt(result.plan());
  }

  @Test
  public void testFilterWithTimestampLiteral() {
    var result =
        givenQuery(
                """
                source = catalog.events | where created_at > "2024-01-01T00:00:00Z" | fields id\
                """)
            .assertPlan(
                """
                LogicalProject(id=[$0])
                  LogicalFilter(condition=[>($4, TIMESTAMP('2024-01-01T00:00:00Z':VARCHAR))])
                    LogicalTableScan(table=[[catalog, events]])
                """);
    assertNoUdt(result.plan());
  }

  @Test
  public void testComparisonWithDatetimeUdf() {
    var result =
        givenQuery("source = catalog.events | where created_at < DATE(name) | fields id")
            .assertPlan(
                """
                LogicalProject(id=[$0])
                  LogicalFilter(condition=[<($4, TIMESTAMP(DATE($1)))])
                    LogicalTableScan(table=[[catalog, events]])
                """);
    assertNoUdt(result.plan());
  }

  @Test
  public void testAllStandardDatetimeTypesCastToVarchar() {
    var result =
        givenQuery("source = catalog.events | fields hire_date, start_time, created_at")
            .assertPlan(
                """
                LogicalProject(hire_date=[CAST($0):VARCHAR NOT NULL], start_time=[CAST($1):VARCHAR NOT NULL], created_at=[CAST($2):VARCHAR NOT NULL])
                  LogicalProject(hire_date=[$2], start_time=[$3], created_at=[$4])
                    LogicalTableScan(table=[[catalog, events]])
                """);
    assertNoUdt(result.plan());
  }

  @Test
  public void testNonDatetimeFieldsNotWrapped() {
    var result =
        givenQuery("source = catalog.events | fields id, name")
            .assertPlan(
                """
                LogicalProject(id=[$0], name=[$1])
                  LogicalTableScan(table=[[catalog, events]])
                """);
    assertNoUdt(result.plan());
  }

  @Test
  public void testOutputCastCanCompileAndExecute() throws Exception {
    RelNode plan =
        planner.plan("source = catalog.events | fields hire_date, start_time, created_at");
    try (PreparedStatement statement = compiler.compile(plan)) {
      ResultSet resultSet = statement.executeQuery();
      verify(resultSet)
          .expectSchema(
              col("hire_date", VARCHAR), col("start_time", VARCHAR), col("created_at", VARCHAR))
          .expectData(
              row("2024-01-16", "12:00:00", "2024-01-15 08:00:00"),
              row("2024-06-20", "14:00:00", "2024-06-20 00:00:00"));
    }
  }

  private static void assertNoUdt(RelNode plan) {
    plan.accept(
        new RelHomogeneousShuttle() {
          @Override
          public RelNode visit(RelNode other) {
            RelNode visited = super.visit(other);
            visited.accept(
                new RexShuttle() {
                  @Override
                  public RexNode visitCall(RexCall call) {
                    assertFalse(
                        "Found UDT type in RexCall: " + call,
                        call.getType() instanceof AbstractExprRelDataType<?>);
                    return super.visitCall(call);
                  }
                });
            return visited;
          }
        });
  }
}
