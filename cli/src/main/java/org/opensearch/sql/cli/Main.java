/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.cli;

import java.util.Map;
import java.util.concurrent.Callable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Table;
import org.opensearch.sql.executor.QueryType;
import picocli.CommandLine;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

/** Entry point for the OpenSearch Unified Query Shell. */
@Command(
    name = "opensearch-query",
    description = "OpenSearch Unified Query Shell",
    mixinStandardHelpOptions = true)
public class Main implements Callable<Integer> {

  @Option(
      names = {"-d", "--data"},
      description = "Path to data file (JSON or text)")
  String dataFile;

  @Option(
      names = {"-l", "--language"},
      description = "Query language (sql or ppl)",
      defaultValue = "ppl")
  String language;

  @Option(
      names = {"-e", "--execute"},
      description = "Execute a single query and exit")
  String query;

  @Option(
      names = {"-f", "--format"},
      description = "Log format for structured parsing (e.g., opensearch-log)")
  String format;

  public static void main(String[] args) {
    System.exit(new CommandLine(new Main()).execute(args));
  }

  @Override
  public Integer call() throws Exception {
    Map<String, Table> tables =
        dataFile != null
            ? SampleDataLoader.loadFile(dataFile, format)
            : SampleDataLoader.loadFromClasspath("data/hr.json");

    QueryType queryType = "sql".equalsIgnoreCase(language) ? QueryType.SQL : QueryType.PPL;

    if (query != null) {
      try {
        QueryRepl repl = new QueryRepl(tables, queryType, System.out);
        repl.dispatch(query);
        return 0;
      } catch (Exception e) {
        System.err.println("Error: " + e.getMessage());
        return 1;
      }
    }

    System.out.println("OpenSearch Unified Query Shell");
    System.out.println("Loaded tables:");
    for (Map.Entry<String, Table> entry : tables.entrySet()) {
      System.out.println("  " + entry.getKey() + " (" + countRows(entry.getValue()) + " rows)");
    }
    System.out.println();
    System.out.println("Type .help for available commands, .quit to exit.");

    QueryRepl repl = new QueryRepl(tables, queryType, System.out);
    repl.run();
    return 0;
  }

  private static int countRows(Table table) {
    Enumerator<Object[]> enumerator = ((ScannableTable) table).scan(null).enumerator();
    int count = 0;
    while (enumerator.moveNext()) {
      count++;
    }
    enumerator.close();
    return count;
  }
}
