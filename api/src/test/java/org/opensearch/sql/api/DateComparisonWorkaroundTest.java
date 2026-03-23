/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api;

import static org.apache.calcite.sql.type.SqlTypeName.DATE;
import static org.apache.calcite.sql.type.SqlTypeName.INTEGER;
import static org.apache.calcite.sql.type.SqlTypeName.VARCHAR;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.List;
import java.util.Map;
import org.apache.calcite.DataContext;
import org.apache.calcite.config.CalciteConnectionConfig;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Assert;
import org.junit.Test;
import org.opensearch.sql.api.compiler.UnifiedQueryCompiler;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.executor.QueryType;

/**
 * Reproduction and workaround verification for
 * https://github.com/opensearch-project/sql/issues/5250
 */
public class DateComparisonWorkaroundTest {

  /** Original code from the issue — fails with "Cannot compare types int and java.lang.String" */
  @Test
  public void testOriginalBugReproduction() throws Exception {
    Table table =
        simpleTable(Map.of("id", INTEGER, "date_hired", DATE), "2020-03-15", "2020-06-15");

    try (UnifiedQueryContext ctx = buildContext(table)) {
      UnifiedQueryPlanner p = new UnifiedQueryPlanner(ctx);
      UnifiedQueryCompiler c = new UnifiedQueryCompiler(ctx);

      Assert.assertThrows(
          IllegalStateException.class,
          () ->
          {
              RelNode plan = p.plan("source=employees | where date_hired > DATE('2020-06-01')");
              c.compile(plan)
                  .executeQuery();
          });
    }
  }

  /** Workaround 1: Use EXPR_DATE UDT instead of SqlTypeName.DATE */
  @Test
  public void testWorkaround_useExprDateUDT() throws Exception {
    Table table =
        new ScannableTable() {
          @Override
          public RelDataType getRowType(RelDataTypeFactory typeFactory) {
            return typeFactory
                .builder()
                .add("id", typeFactory.createSqlType(INTEGER))
                .add(
                    "date_hired",
                    OpenSearchTypeFactory.TYPE_FACTORY.createUDT(
                        OpenSearchTypeFactory.ExprUDT.EXPR_DATE, true))
                .build();
          }

          @Override
          public Enumerable<Object[]> scan(DataContext root) {
            return Linq4j.asEnumerable(
                List.of(new Object[] {1, "2020-03-15"}, new Object[] {2, "2020-06-15"}));
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
              String column, SqlCall call, SqlNode parent, CalciteConnectionConfig config) {
            return false;
          }
        };

    try (UnifiedQueryContext ctx = buildContext(table)) {
      UnifiedQueryPlanner p = new UnifiedQueryPlanner(ctx);
      UnifiedQueryCompiler c = new UnifiedQueryCompiler(ctx);

      try (PreparedStatement stmt =
          c.compile(p.plan("source=employees | where date_hired > DATE('2020-06-01')"))) {
        ResultSet rs = stmt.executeQuery();
        Assert.assertTrue(rs.next());
        Assert.assertEquals(2, rs.getInt(1));
        Assert.assertEquals("2020-06-15", rs.getString(2));
        Assert.assertFalse(rs.next());
      }
    }
  }

  /** Workaround 2: Use plain VARCHAR for the date column */
  @Test
  public void testWorkaround_useVarchar() throws Exception {
    Table table =
        simpleTable(Map.of("id", INTEGER, "date_hired", VARCHAR), "2020-03-15", "2020-06-15");

    try (UnifiedQueryContext ctx = buildContext(table)) {
      UnifiedQueryPlanner p = new UnifiedQueryPlanner(ctx);
      UnifiedQueryCompiler c = new UnifiedQueryCompiler(ctx);

      try (PreparedStatement stmt =
          c.compile(p.plan("source=employees | where date_hired > DATE('2020-06-01')"))) {
        ResultSet rs = stmt.executeQuery();
        Assert.assertTrue(rs.next());
        Assert.assertEquals(2, rs.getInt(1));
        Assert.assertEquals("2020-06-15", rs.getString(2));
        Assert.assertFalse(rs.next());
      }
    }
  }

  private UnifiedQueryContext buildContext(Table table) {
    AbstractSchema schema =
        new AbstractSchema() {
          @Override
          protected Map<String, Table> getTableMap() {
            return Map.of("employees", table);
          }
        };
    return UnifiedQueryContext.builder()
        .language(QueryType.PPL)
        .catalog("test", schema)
        .defaultNamespace("test")
        .build();
  }

  private static Table simpleTable(Map<String, SqlTypeName> cols, String date1, String date2) {
    return new ScannableTable() {
      @Override
      public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        RelDataTypeFactory.Builder builder = typeFactory.builder();
        cols.forEach((name, type) -> builder.add(name, typeFactory.createSqlType(type)));
        return builder.build();
      }

      @Override
      public Enumerable<Object[]> scan(DataContext root) {
        return Linq4j.asEnumerable(
            List.of(new Object[] {1, date1}, new Object[] {2, date2}));
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
          String column, SqlCall call, SqlNode parent, CalciteConnectionConfig config) {
        return false;
      }
    };
  }
}
