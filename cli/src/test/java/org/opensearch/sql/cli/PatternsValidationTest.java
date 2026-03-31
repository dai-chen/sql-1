/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.cli;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
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
 * End-to-end validation that PPL patterns command works through the unified query API (plan →
 * compile → executeQuery) using opensearch.log sample data.
 */
public class PatternsValidationTest {

  private UnifiedQueryContext context;
  private UnifiedQueryPlanner planner;
  private UnifiedQueryCompiler compiler;

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
            .setting("plugins.ppl.pattern.method", "simple_pattern")
            .setting("plugins.ppl.pattern.mode", "label")
            .setting("plugins.ppl.pattern.max.sample.count", 10)
            .setting("plugins.ppl.pattern.buffer.limit", 100000)
            .setting("plugins.ppl.pattern.show.numbered.token", false)
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
  public void testPatternsExtractsPatternFromLogLines() throws Exception {
    String query = "source = catalog.opensearch | patterns line | fields line, patterns_field";
    RelNode plan = planner.plan(query);
    try (PreparedStatement stmt = compiler.compile(plan)) {
      ResultSet rs = stmt.executeQuery();
      ResultSetMetaData meta = rs.getMetaData();

      List<String> columns = new ArrayList<>();
      for (int i = 1; i <= meta.getColumnCount(); i++) {
        columns.add(meta.getColumnName(i));
      }
      assertEquals(List.of("line", "patterns_field"), columns);

      int rowCount = 0;
      while (rs.next()) {
        rowCount++;
        String patternsField = rs.getString("patterns_field");
        assertNotNull("patterns_field should not be null in row " + rowCount, patternsField);
        // The pattern replaces [a-zA-Z0-9]+ with <*>, so result should contain <*>
        assertTrue(
            "patterns_field should contain '<*>' in row " + rowCount,
            patternsField.contains("<*>"));
      }
      assertEquals(9, rowCount);
    }
  }

  @Test
  public void testPatternsWithCustomNewField() throws Exception {
    String query =
        "source = catalog.opensearch | patterns line new_field='log_pattern'"
            + " | fields line, log_pattern";
    RelNode plan = planner.plan(query);
    try (PreparedStatement stmt = compiler.compile(plan)) {
      ResultSet rs = stmt.executeQuery();
      ResultSetMetaData meta = rs.getMetaData();

      List<String> columns = new ArrayList<>();
      for (int i = 1; i <= meta.getColumnCount(); i++) {
        columns.add(meta.getColumnName(i));
      }
      assertEquals(List.of("line", "log_pattern"), columns);

      int rowCount = 0;
      while (rs.next()) {
        rowCount++;
        assertNotNull(
            "log_pattern should not be null in row " + rowCount, rs.getString("log_pattern"));
      }
      assertTrue("Should have rows", rowCount > 0);
    }
  }

  @Test
  public void testPatternsAfterGrok() throws Exception {
    String query =
        "source = catalog.opensearch | grok line '"
            + GROK_PATTERN
            + "' | patterns msg | fields msg, patterns_field";
    RelNode plan = planner.plan(query);
    try (PreparedStatement stmt = compiler.compile(plan)) {
      ResultSet rs = stmt.executeQuery();
      ResultSetMetaData meta = rs.getMetaData();

      List<String> columns = new ArrayList<>();
      for (int i = 1; i <= meta.getColumnCount(); i++) {
        columns.add(meta.getColumnName(i));
      }
      assertEquals(List.of("msg", "patterns_field"), columns);

      int rowCount = 0;
      while (rs.next()) {
        rowCount++;
        assertNotNull(
            "patterns_field should not be null in row " + rowCount, rs.getString("patterns_field"));
      }
      assertEquals(9, rowCount);
    }
  }
}
