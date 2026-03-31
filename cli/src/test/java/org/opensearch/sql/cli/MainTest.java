/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.cli;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import picocli.CommandLine;

public class MainTest {

  private final PrintStream originalOut = System.out;
  private final PrintStream originalErr = System.err;
  private final InputStream originalIn = System.in;
  private ByteArrayOutputStream outContent;
  private ByteArrayOutputStream errContent;

  @Before
  public void setUp() {
    outContent = new ByteArrayOutputStream();
    errContent = new ByteArrayOutputStream();
    System.setOut(new PrintStream(outContent));
    System.setErr(new PrintStream(errContent));
  }

  @After
  public void tearDown() {
    System.setOut(originalOut);
    System.setErr(originalErr);
    System.setIn(originalIn);
  }

  @Test
  public void testNonInteractivePpl() {
    int exitCode =
        new CommandLine(new Main()).execute("-e", "source = catalog.employees | where age > 30");
    assertEquals(0, exitCode);
    assertThat(outContent.toString(), containsString("row(s) returned"));
  }

  @Test
  public void testNonInteractiveSql() {
    int exitCode =
        new CommandLine(new Main())
            .execute("-l", "sql", "-e", "SELECT * FROM catalog.employees WHERE age > 30");
    assertEquals(0, exitCode);
    assertThat(outContent.toString(), containsString("row(s) returned"));
  }

  @Test
  public void testNonInteractiveBadQuery() {
    int exitCode = new CommandLine(new Main()).execute("-e", "INVALID QUERY BLAH BLAH");
    String combined = outContent.toString() + errContent.toString();
    assertThat(combined, containsString("rror"));
  }

  @Test
  public void testHelpFlag() {
    int exitCode = new CommandLine(new Main()).execute("--help");
    assertEquals(0, exitCode);
    assertThat(outContent.toString(), containsString("opensearch-query"));
  }

  @Test
  public void testJsonOutputPpl() {
    int exitCode =
        new CommandLine(new Main())
            .execute("--output", "json", "-e", "source = catalog.employees | where age > 30");
    assertEquals(0, exitCode);
    String stdout = outContent.toString();
    assertThat(stdout, containsString("Bob"));
    assertThat(stdout, not(containsString("row(s) returned")));
    assertThat(stdout, not(containsString("Loaded")));
  }

  @Test
  public void testJsonOutputError() {
    int exitCode = new CommandLine(new Main()).execute("--output", "json", "-e", "INVALID QUERY");
    assertEquals(1, exitCode);
    assertThat(outContent.toString(), containsString("\"error\":true"));
  }

  @Test
  public void testJsonOutputFileError() {
    int exitCode =
        new CommandLine(new Main())
            .execute("--output", "json", "-d", "nonexistent.json", "-e", "query");
    assertEquals(2, exitCode);
    assertThat(outContent.toString(), containsString("\"error\":true"));
  }

  @Test
  public void testScriptMode() {
    String input = ".tables\nsource = catalog.employees | where age > 30\n";
    System.setIn(new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8)));
    int exitCode = new CommandLine(new Main()).execute("--script");
    assertEquals(0, exitCode);
    String stdout = outContent.toString();
    assertThat(stdout, containsString("employees"));
    assertThat(stdout, containsString("Bob"));
  }

  @Test
  public void testScriptModeNdjson() {
    String input = ".tables\nsource = catalog.employees | where age > 30\n";
    System.setIn(new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8)));
    int exitCode = new CommandLine(new Main()).execute("--script", "--output", "json");
    assertEquals(0, exitCode);
    String stdout = outContent.toString();
    String[] lines = stdout.strip().split("\n");
    assertEquals(2, lines.length);
    assertThat(lines[0], containsString("employees"));
    assertThat(lines[1], containsString("Bob"));
  }

  @Test
  public void testScriptModeErrorResilience() {
    String input =
        "source = catalog.employees | where age > 30\n"
            + "INVALID QUERY\n"
            + "source = catalog.employees | where age > 30\n";
    System.setIn(new ByteArrayInputStream(input.getBytes(StandardCharsets.UTF_8)));
    int exitCode = new CommandLine(new Main()).execute("--script");
    assertEquals(0, exitCode);
    String stdout = outContent.toString();
    // Should have output from all three commands (two good + one error)
    assertThat(stdout, containsString("rror"));
    assertThat(stdout, containsString("Bob"));
  }
}
