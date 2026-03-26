/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.cli;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Paths;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.Map;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.jline.reader.EndOfFileException;
import org.jline.reader.LineReader;
import org.jline.reader.LineReaderBuilder;
import org.jline.reader.UserInterruptException;
import org.jline.terminal.Terminal;
import org.jline.terminal.TerminalBuilder;
import org.opensearch.sql.api.UnifiedQueryContext;
import org.opensearch.sql.api.UnifiedQueryPlanner;
import org.opensearch.sql.api.compiler.UnifiedQueryCompiler;
import org.opensearch.sql.common.antlr.SyntaxCheckException;
import org.opensearch.sql.executor.QueryType;

/** Interactive REPL for executing SQL/PPL queries against in-memory tables. */
public class QueryRepl {

  private static final String DEFAULT_CATALOG = "catalog";
  private static final String HISTORY_FILE = ".opensearch-query-history";

  private final PrintStream out;
  private Map<String, Table> tables;
  private QueryType language;
  private UnifiedQueryContext context;
  private UnifiedQueryPlanner planner;
  private UnifiedQueryCompiler compiler;

  public QueryRepl(Map<String, Table> tables, QueryType language, PrintStream out) {
    this.tables = tables;
    this.language = language;
    this.out = out;
    rebuildContext();
  }

  /** Start the interactive REPL loop. */
  public void run() throws IOException {
    try (Terminal terminal = TerminalBuilder.builder().system(true).build()) {
      LineReader reader =
          LineReaderBuilder.builder()
              .terminal(terminal)
              .variable(
                  LineReader.HISTORY_FILE, Paths.get(System.getProperty("user.home"), HISTORY_FILE))
              .build();
      loop(reader);
    } finally {
      closeContext();
    }
  }

  /** Main loop, package-private for testing. */
  void loop(LineReader reader) {
    while (true) {
      String line;
      try {
        line = reader.readLine(prompt());
      } catch (UserInterruptException e) {
        continue;
      } catch (EndOfFileException e) {
        break;
      }
      if (line == null || line.isBlank()) {
        continue;
      }
      dispatch(line.strip());
    }
  }

  /** Dispatch a single input line. Package-private for testing. */
  void dispatch(String input) {
    if (input.startsWith(".")) {
      handleMeta(input);
    } else {
      executeQuery(input);
    }
  }

  private void handleMeta(String input) {
    String[] parts = input.split("\\s+", 2);
    String cmd = parts[0].toLowerCase();
    String arg = parts.length > 1 ? parts[1].strip() : "";

    switch (cmd) {
      case ".quit":
      case ".exit":
        throw new EndOfFileException();
      case ".help":
        printHelp();
        break;
      case ".language":
        switchLanguage(arg);
        break;
      case ".tables":
        listTables();
        break;
      case ".schema":
        showSchema(arg);
        break;
      case ".load":
        loadData(arg);
        break;
      default:
        out.println("Unknown command: " + cmd + ". Type .help for available commands.");
    }
  }

  private void printHelp() {
    out.println("Meta-commands:");
    out.println("  .help                Show this help message");
    out.println("  .quit / .exit        Exit the shell");
    out.println("  .language sql|ppl    Switch query language");
    out.println("  .tables              List loaded tables and columns");
    out.println("  .schema <table>      Show column details for a table");
    out.println("  .load <path>         Load a JSON data file");
    out.println();
    out.println("Enter any other input to execute as a query.");
  }

  private void switchLanguage(String arg) {
    if (arg.equalsIgnoreCase("sql")) {
      language = QueryType.SQL;
    } else if (arg.equalsIgnoreCase("ppl")) {
      language = QueryType.PPL;
    } else {
      out.println("Usage: .language sql|ppl");
      return;
    }
    rebuildContext();
    out.println("Switched to " + language.name() + " mode.");
  }

  private void listTables() {
    if (tables.isEmpty()) {
      out.println("No tables loaded.");
      return;
    }
    for (Map.Entry<String, Table> entry : tables.entrySet()) {
      String cols =
          String.join(
              ", ",
              entry
                  .getValue()
                  .getRowType(new org.apache.calcite.jdbc.JavaTypeFactoryImpl())
                  .getFieldNames());
      out.println("  " + entry.getKey() + " (" + cols + ")");
    }
  }

  private void showSchema(String tableName) {
    if (tableName.isEmpty()) {
      out.println("Usage: .schema <table>");
      return;
    }
    Table table = tables.get(tableName);
    if (table == null) {
      out.println("Table not found: " + tableName);
      return;
    }
    var rowType = table.getRowType(new org.apache.calcite.jdbc.JavaTypeFactoryImpl());
    for (var field : rowType.getFieldList()) {
      out.println("  " + field.getName() + " " + field.getType().getSqlTypeName());
    }
  }

  private void loadData(String path) {
    if (path.isEmpty()) {
      out.println("Usage: .load <path>");
      return;
    }
    try (FileInputStream fis = new FileInputStream(path)) {
      tables = SampleDataLoader.load(fis);
      rebuildContext();
      out.println("Loaded tables: " + String.join(", ", tables.keySet()));
    } catch (IOException e) {
      out.println("Error loading file: " + e.getMessage());
    }
  }

  private void executeQuery(String query) {
    try {
      RelNode plan = planner.plan(query);
      PreparedStatement stmt = compiler.compile(plan);
      try (ResultSet rs = stmt.executeQuery()) {
        ResultSetFormatter.format(rs, out);
      }
    } catch (SyntaxCheckException e) {
      out.println("Syntax error: " + e.getMessage());
    } catch (Exception e) {
      String msg = e.getCause() != null ? e.getCause().getMessage() : e.getMessage();
      out.println("Error: " + msg);
    }
  }

  private void rebuildContext() {
    closeContext();
    context =
        UnifiedQueryContext.builder()
            .language(language)
            .catalog(
                DEFAULT_CATALOG,
                new AbstractSchema() {
                  @Override
                  protected Map<String, Table> getTableMap() {
                    return tables;
                  }
                })
            .defaultNamespace(DEFAULT_CATALOG)
            .build();
    planner = new UnifiedQueryPlanner(context);
    compiler = new UnifiedQueryCompiler(context);
  }

  private void closeContext() {
    if (context != null) {
      try {
        context.close();
      } catch (Exception e) {
        // ignore
      }
    }
  }

  String prompt() {
    return language.name().toLowerCase() + "> ";
  }
}
