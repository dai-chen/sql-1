/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.cli;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.sql.api.UnifiedQueryContext;
import org.opensearch.sql.api.UnifiedQueryPlanner;
import org.opensearch.sql.api.compiler.UnifiedQueryCompiler;
import org.opensearch.sql.executor.QueryType;

/**
 * End-to-end validation that PPL grok command works through the unified query API (plan → compile →
 * executeQuery) using opensearch.log sample data.
 */
public class GrokValidationTest {

  private UnifiedQueryContext context;
  private UnifiedQueryPlanner planner;
  private UnifiedQueryCompiler compiler;

  // Use DATA instead of TIMESTAMP_ISO8601 to avoid duplicate named group keys
  // (TIMESTAMP_ISO8601 expands to sub-patterns like HOUR, MINUTE that cause
  // Collectors.toMap duplicate key errors in ParseFunction).
  private static final String GROK_PATTERN =
      "\\[%{DATA:ts}\\]\\[%{LOGLEVEL:level}%{SPACE}\\]\\[%{DATA:component}\\]"
          + " \\[%{DATA:node}\\] %{GREEDYDATA:msg}";

  @Before
  public void setUp() throws Exception {
    Map<String, Table> tables = SampleDataLoader.loadFile("cli/examples/opensearch.log");
    AbstractSchema schema =
        new AbstractSchema() {
          @Override
          protected Map<String, Table> getTableMap() {
            return tables;
          }
        };
    context =
        UnifiedQueryContext.builder()
            .language(QueryType.PPL)
            .catalog("catalog", schema)
            .defaultNamespace("catalog")
            .build();
    planner = new UnifiedQueryPlanner(context);
    compiler = new UnifiedQueryCompiler(context);
  }

  @After
  public void tearDown() throws Exception {
    if (context != null) {
      context.close();
    }
  }

  @Test
  public void testGrokExtractsFieldsFromLogLines() throws Exception {
    String query =
        "source = catalog.opensearch | grok line '"
            + GROK_PATTERN
            + "' | fields ts, level, component, node, msg";
    RelNode plan = planner.plan(query);
    try (PreparedStatement stmt = compiler.compile(plan)) {
      ResultSet rs = stmt.executeQuery();
      ResultSetMetaData meta = rs.getMetaData();

      List<String> columns = new ArrayList<>();
      for (int i = 1; i <= meta.getColumnCount(); i++) {
        columns.add(meta.getColumnName(i));
      }
      assertEquals(List.of("ts", "level", "component", "node", "msg"), columns);

      int rowCount = 0;
      while (rs.next()) {
        rowCount++;
        for (int i = 1; i <= meta.getColumnCount(); i++) {
          assertTrue(
              "Column " + meta.getColumnName(i) + " should not be null in row " + rowCount,
              rs.getObject(i) != null);
        }
      }
      // opensearch.log has 12 raw lines; multi-line joining produces 9 logical entries
      assertEquals(9, rowCount);
    }
  }

  @Test
  public void testGrokWithWhereFilter() throws Exception {
    String query =
        "source = catalog.opensearch | grok line '"
            + GROK_PATTERN
            + "' | where level = 'ERROR' | fields ts, level, msg";
    RelNode plan = planner.plan(query);
    try (PreparedStatement stmt = compiler.compile(plan)) {
      ResultSet rs = stmt.executeQuery();

      List<String> levels = new ArrayList<>();
      List<String> messages = new ArrayList<>();
      while (rs.next()) {
        levels.add(rs.getString("level"));
        messages.add(rs.getString("msg"));
      }
      assertEquals(1, levels.size());
      assertEquals("ERROR", levels.get(0));
      assertTrue(messages.get(0).contains("failed to execute bulk item"));
    }
  }

  @Test
  public void testGrokWithStatsAggregation() throws Exception {
    String query =
        "source = catalog.opensearch | grok line '" + GROK_PATTERN + "' | stats count() by level";
    RelNode plan = planner.plan(query);
    try (PreparedStatement stmt = compiler.compile(plan)) {
      ResultSet rs = stmt.executeQuery();

      int totalRows = 0;
      boolean foundInfo = false;
      while (rs.next()) {
        totalRows++;
        String level = rs.getString("level");
        long count = rs.getLong("count()");
        if ("INFO".equals(level)) {
          assertEquals(5, count);
          foundInfo = true;
        }
      }
      assertTrue("Should have INFO level aggregation", foundInfo);
      assertTrue("Should have multiple log levels", totalRows > 1);
    }
  }
}
