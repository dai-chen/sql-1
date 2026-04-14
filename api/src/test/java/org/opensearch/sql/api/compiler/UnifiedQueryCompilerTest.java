/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.compiler;

import static java.sql.Types.BIGINT;
import static java.sql.Types.INTEGER;
import static java.sql.Types.VARCHAR;

import java.sql.Date;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.RelShuttle;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.opensearch.sql.api.ResultSetAssertion;
import org.opensearch.sql.api.UnifiedQueryContext;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.ExprUDT;
import org.opensearch.sql.api.UnifiedQueryPlanner;
import org.opensearch.sql.api.UnifiedQueryTestBase;

public class UnifiedQueryCompilerTest extends UnifiedQueryTestBase implements ResultSetAssertion {

  private UnifiedQueryCompiler compiler;

  @Before
  public void setUp() {
    super.setUp();
    compiler = new UnifiedQueryCompiler(context);
  }

  @Test
  public void testSimpleQuery() throws Exception {
    RelNode plan = planner.plan("source = catalog.employees | where age > 30");
    try (PreparedStatement statement = compiler.compile(plan)) {
      ResultSet resultSet = statement.executeQuery();

      verify(resultSet)
          .expectSchema(
              col("id", INTEGER),
              col("name", VARCHAR),
              col("age", INTEGER),
              col("department", VARCHAR))
          .expectData(row(2, "Bob", 35, "Sales"), row(3, "Charlie", 45, "Engineering"));
    }
  }

  @Test
  public void testComplexQuery() throws Exception {
    RelNode plan = planner.plan("source = catalog.employees | stats count() by department");
    try (PreparedStatement statement = compiler.compile(plan)) {
      ResultSet resultSet = statement.executeQuery();

      verify(resultSet)
          .expectSchema(col("count()", BIGINT), col("department", VARCHAR))
          .expectData(row(2L, "Engineering"), row(1L, "Sales"), row(1L, "Marketing"));
    }
  }

  @Test
  public void testDateComparisonInWhereClause() throws Exception {
    AbstractSchema schema =
        new AbstractSchema() {
          @Override
          protected Map<String, Table> getTableMap() {
            return Map.of(
                "employees",
                SimpleTable.builder()
                    .col("id", SqlTypeName.INTEGER)
                    .col("date_hired", SqlTypeName.DATE)
                    .row(new Object[] {1, Date.valueOf("2020-03-15")})
                    .row(new Object[] {2, Date.valueOf("2020-06-15")})
                    .build());
          }
        };

    try (UnifiedQueryContext dateContext =
            UnifiedQueryContext.builder()
                .language(queryType())
                .catalog(DEFAULT_CATALOG, schema)
                .build()) {
      UnifiedQueryPlanner datePlanner = new UnifiedQueryPlanner(dateContext);
      UnifiedQueryCompiler dateCompiler = new UnifiedQueryCompiler(dateContext);

      RelNode plan =
          datePlanner.plan(
              "source = catalog.employees | where date_hired > DATE('2020-06-01')");
      try (PreparedStatement statement = dateCompiler.compile(plan)) {
        ResultSet resultSet = statement.executeQuery();

        verify(resultSet)
            .expectSchema(col("id", INTEGER), col("date_hired", java.sql.Types.DATE))
            .expectData(row(2, Date.valueOf("2020-06-15")));
      }
    }
  }


  @Test
  public void testDateTimeComparisonsInWhereClause() throws Exception {
    AbstractSchema schema =
        new AbstractSchema() {
          @Override
          protected Map<String, Table> getTableMap() {
            return Map.of(
                "events",
                SimpleTable.builder()
                    .col("id", SqlTypeName.INTEGER)
                    .col("d", SqlTypeName.DATE)
                    .col("t", SqlTypeName.TIME)
                    .col("ts", SqlTypeName.TIMESTAMP)
                    .row(
                        new Object[] {
                          1,
                          Date.valueOf("2020-06-01"),
                          Time.valueOf("09:00:00"),
                          Timestamp.valueOf("2020-06-01 09:00:00")
                        })
                    .row(
                        new Object[] {
                          2,
                          Date.valueOf("2020-06-15"),
                          Time.valueOf("10:30:00"),
                          Timestamp.valueOf("2020-06-15 10:30:00")
                        })
                    .build());
          }
        };

    try (UnifiedQueryContext dateTimeContext =
            UnifiedQueryContext.builder()
                .language(queryType())
                .catalog(DEFAULT_CATALOG, schema)
                .build()) {
      UnifiedQueryPlanner dateTimePlanner = new UnifiedQueryPlanner(dateTimeContext);
      UnifiedQueryCompiler dateTimeCompiler = new UnifiedQueryCompiler(dateTimeContext);

      RelNode plan =
          dateTimePlanner.plan(
              "source = catalog.events"
                  + " | where d > DATE('2020-06-01')"
                  + " and t > TIME('09:30:00')"
                  + " and ts > TIMESTAMP('2020-06-01 09:30:00')"
                  + " | fields id");
      try (PreparedStatement statement = dateTimeCompiler.compile(plan)) {
        ResultSet resultSet = statement.executeQuery();

        verify(resultSet).expectSchema(col("id", INTEGER)).expectData(row(2));
      }
    }
  }


  @Test
  public void testUdtDateColumnCanBePlannedAndExecutedWithoutTypeException() throws Exception {
    AbstractSchema schema =
        new AbstractSchema() {
          @Override
          protected Map<String, Table> getTableMap() {
            ScannableTable udtDateTable =
                new ScannableTable() {
                  @Override
                  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
                    return typeFactory
                        .builder()
                        .add("id", typeFactory.createSqlType(SqlTypeName.INTEGER))
                        .add("d", OpenSearchTypeFactory.TYPE_FACTORY.createUDT(ExprUDT.EXPR_DATE, true))
                        .build();
                  }

                  @Override
                  public org.apache.calcite.linq4j.Enumerable<Object[]> scan(DataContext root) {
                    return Linq4j.asEnumerable(
                        List.of(
                            new Object[] {1, "2020-06-01"},
                            new Object[] {2, "2020-06-15"}));
                  }

                  @Override
                  public Statistic getStatistic() {
                    return Statistics.UNKNOWN;
                  }

                  @Override
                  public Schema.TableType getJdbcTableType() {
                    return Schema.TableType.TABLE;
                  }

                  @Override
                  public boolean isRolledUp(String column) {
                    return false;
                  }

                  @Override
                  public boolean rolledUpColumnValidInsideAgg(
                      String column,
                      SqlCall call,
                      SqlNode parent,
                      org.apache.calcite.config.CalciteConnectionConfig config) {
                    return false;
                  }
                };

            return Map.of("events", udtDateTable);
          }
        };

    try (UnifiedQueryContext udtContext =
            UnifiedQueryContext.builder()
                .language(queryType())
                .catalog(DEFAULT_CATALOG, schema)
                .build()) {
      UnifiedQueryPlanner udtPlanner = new UnifiedQueryPlanner(udtContext);
      UnifiedQueryCompiler udtCompiler = new UnifiedQueryCompiler(udtContext);

      RelNode plan =
          udtPlanner.plan(
              "source = catalog.events | eval d2 = DATE(d) | where d2 > DATE('2020-06-01') | fields id");
      try (PreparedStatement statement = udtCompiler.compile(plan)) {
        ResultSet resultSet = statement.executeQuery();
        verify(resultSet).expectSchema(col("id", INTEGER)).expectData(row(2));
      }
    }
  }

  @Test(expected = IllegalStateException.class)
  public void testCompileFailure() {
    RelNode mockPlan = Mockito.mock(RelNode.class);
    Mockito.when(mockPlan.accept(Mockito.any(RelShuttle.class)))
        .thenThrow(new RuntimeException("Intentional compilation failure"));

    compiler.compile(mockPlan);
  }
}
