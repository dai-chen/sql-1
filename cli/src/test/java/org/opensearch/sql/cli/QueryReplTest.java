/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.cli;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.junit.Assert.assertEquals;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.Map;
import org.apache.calcite.schema.Table;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.sql.executor.QueryType;

public class QueryReplTest {

  private ByteArrayOutputStream baos;
  private PrintStream out;
  private Map<String, Table> tables;
  private QueryRepl repl;

  @Before
  public void setUp() throws Exception {
    baos = new ByteArrayOutputStream();
    out = new PrintStream(baos);
    tables = SampleDataLoader.loadFromClasspath("data/hr.json");
    repl = new QueryRepl(tables, QueryType.PPL, out);
  }

  @After
  public void tearDown() {
    // repl context is cleaned up internally
  }

  @Test
  public void testPromptShowsLanguage() {
    assertEquals("ppl> ", repl.prompt());
  }

  @Test
  public void testHelpCommand() {
    repl.dispatch(".help");
    String output = baos.toString();
    assertThat(output, containsString(".help"));
    assertThat(output, containsString(".quit"));
    assertThat(output, containsString(".language"));
    assertThat(output, containsString(".tables"));
    assertThat(output, containsString(".schema"));
    assertThat(output, containsString(".load"));
  }

  @Test
  public void testTablesCommand() {
    repl.dispatch(".tables");
    String output = baos.toString();
    assertThat(output, containsString("employees"));
    assertThat(output, containsString("departments"));
  }

  @Test
  public void testSchemaCommand() {
    repl.dispatch(".schema employees");
    String output = baos.toString();
    assertThat(output, containsString("id"));
    assertThat(output, containsString("name"));
    assertThat(output, containsString("INTEGER"));
    assertThat(output, containsString("VARCHAR"));
  }

  @Test
  public void testSchemaUnknownTable() {
    repl.dispatch(".schema nonexistent");
    assertThat(baos.toString(), containsString("Table not found"));
  }

  @Test
  public void testLanguageSwitch() {
    repl.dispatch(".language sql");
    assertEquals("sql> ", repl.prompt());
    assertThat(baos.toString(), containsString("Switched to SQL mode"));
  }

  @Test
  public void testLanguageInvalid() {
    repl.dispatch(".language foo");
    assertThat(baos.toString(), containsString("Usage: .language sql|ppl"));
  }

  @Test
  public void testUnknownMetaCommand() {
    repl.dispatch(".foo");
    assertThat(baos.toString(), containsString("Unknown command"));
  }

  @Test
  public void testExecutePplQuery() {
    repl.dispatch("source = catalog.employees | where age > 30");
    String output = baos.toString();
    assertThat(output, containsString("row(s) returned"));
  }

  @Test
  public void testExecuteSqlQuery() {
    repl.dispatch(".language sql");
    baos.reset();
    repl.dispatch("SELECT * FROM catalog.employees WHERE age > 30");
    String output = baos.toString();
    assertThat(output, containsString("row(s) returned"));
  }

  @Test
  public void testSyntaxError() {
    repl.dispatch("INVALID QUERY BLAH BLAH");
    String output = baos.toString();
    // Should show error, not stack trace
    assertThat(output, containsString("rror"));
  }

  @Test
  public void testLoadInvalidPath() {
    repl.dispatch(".load /nonexistent/path.json");
    assertThat(baos.toString(), containsString("Error loading file"));
  }

  @Test
  public void testLoadMissingArg() {
    repl.dispatch(".load");
    assertThat(baos.toString(), containsString("Usage: .load"));
  }

  @Test
  public void testLoadTextFile() {
    repl.dispatch(".load cli/examples/opensearch.log");
    String output = baos.toString();
    assertThat(output, containsString("Loaded tables:"));
    assertThat(output, containsString("opensearch"));
  }

  @Test
  public void testLoadTextFileWithAlias() {
    repl.dispatch(".load cli/examples/opensearch.log as mylog");
    String output = baos.toString();
    assertThat(output, containsString("Loaded tables:"));
    assertThat(output, containsString("mylog"));
  }

  @Test
  public void testSchemaMissingArg() {
    repl.dispatch(".schema");
    assertThat(baos.toString(), containsString("Usage: .schema"));
  }

  @Test
  public void testLoadWithFormat() {
    repl.dispatch(".load cli/examples/opensearch.log as logs --format opensearch-log");
    String output = baos.toString();
    assertThat(output, containsString("Loaded tables: logs"));
    baos.reset();
    repl.dispatch(".schema logs");
    output = baos.toString();
    assertThat(output, containsString("timestamp"));
    assertThat(output, containsString("level"));
    assertThat(output, containsString("component"));
    assertThat(output, containsString("node"));
    assertThat(output, containsString("message"));
  }

  @Test
  public void testChartMetaCommand() {
    repl.dispatch(".chart off");
    assertThat(baos.toString(), containsString("Chart display disabled"));
    baos.reset();
    repl.dispatch(".chart on");
    assertThat(baos.toString(), containsString("Chart display enabled"));
    baos.reset();
    repl.dispatch(".chart");
    assertThat(baos.toString(), containsString("Usage: .chart on|off"));
  }
}
