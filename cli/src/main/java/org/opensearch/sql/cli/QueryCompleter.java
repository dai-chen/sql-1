/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.cli;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.schema.Table;
import org.jline.reader.Candidate;
import org.jline.reader.Completer;
import org.jline.reader.LineReader;
import org.jline.reader.ParsedLine;
import org.opensearch.sql.executor.QueryType;

/** Tab-completion for SQL/PPL keywords, table names, and column names. */
public class QueryCompleter implements Completer {

  private static final List<String> SQL_KEYWORDS =
      Arrays.asList(
          "SELECT",
          "FROM",
          "WHERE",
          "GROUP",
          "BY",
          "ORDER",
          "JOIN",
          "HAVING",
          "LIMIT",
          "AS",
          "ON",
          "AND",
          "OR",
          "NOT",
          "IN",
          "BETWEEN",
          "LIKE",
          "IS",
          "NULL",
          "INSERT",
          "UPDATE",
          "DELETE",
          "CREATE",
          "DROP",
          "ALTER",
          "SET",
          "INTO",
          "VALUES",
          "DISTINCT",
          "COUNT",
          "SUM",
          "AVG",
          "MIN",
          "MAX",
          "INNER",
          "LEFT",
          "RIGHT",
          "OUTER",
          "CROSS",
          "UNION",
          "ALL",
          "EXISTS",
          "CASE",
          "WHEN",
          "THEN",
          "ELSE",
          "END",
          "ASC",
          "DESC",
          "OFFSET",
          "TRUE",
          "FALSE");

  private static final List<String> PPL_KEYWORDS =
      Arrays.asList(
          "source",
          "where",
          "fields",
          "stats",
          "sort",
          "eval",
          "head",
          "tail",
          "top",
          "rare",
          "dedup",
          "rename",
          "parse",
          "grok",
          "patterns",
          "lookup",
          "join",
          "by",
          "as",
          "and",
          "or",
          "not",
          "in",
          "like",
          "between",
          "is",
          "null",
          "true",
          "false",
          "count",
          "sum",
          "avg",
          "min",
          "max",
          "if",
          "case",
          "else");

  private static final Pattern SQL_FROM_TABLE =
      Pattern.compile("(?i)\\bFROM\\s+(?:catalog\\.)?([a-zA-Z_]\\w*)");
  private static final Pattern PPL_SOURCE_TABLE =
      Pattern.compile("(?i)\\bsource\\s*=\\s*(?:catalog\\.)?([a-zA-Z_]\\w*)");
  private static final Pattern SQL_TABLE_CONTEXT = Pattern.compile("(?i)\\bFROM\\s+$");
  private static final Pattern SQL_COLUMN_CONTEXT = Pattern.compile("(?i)\\bWHERE\\s+$");
  private static final Pattern PPL_TABLE_CONTEXT = Pattern.compile("(?i)\\bsource\\s*=\\s*$");
  private static final Pattern PPL_COLUMN_CONTEXT =
      Pattern.compile("(?i)\\|\\s*(?:where|eval|fields)\\s+$");
  private static final Pattern PPL_STATS_BY_CONTEXT =
      Pattern.compile("(?i)\\|\\s*stats\\s+.*\\bby\\s+$");

  private QueryType language;
  private Map<String, Table> tables;

  public QueryCompleter(Map<String, Table> tables, QueryType language) {
    this.tables = tables;
    this.language = language;
  }

  public void updateLanguage(QueryType language) {
    this.language = language;
  }

  public void updateTables(Map<String, Table> tables) {
    this.tables = tables;
  }

  @Override
  public void complete(LineReader reader, ParsedLine line, List<Candidate> candidates) {
    String prefix = line.word().substring(0, line.wordCursor());
    String lower = prefix.toLowerCase();
    String fullLine = line.line();
    String beforeCursor = fullLine.substring(0, line.cursor());

    // Check for context-specific completion
    if (isTableContext(beforeCursor)) {
      addTableCandidates(lower, candidates);
      return;
    }

    if (isColumnContext(beforeCursor)) {
      String tableName = extractTableName(beforeCursor);
      if (tableName != null) {
        addColumnCandidates(tableName, lower, candidates);
        return;
      }
    }

    // Fallback: keywords + all table/column names
    List<String> keywords = language == QueryType.SQL ? SQL_KEYWORDS : PPL_KEYWORDS;
    for (String kw : keywords) {
      if (kw.toLowerCase().startsWith(lower)) {
        candidates.add(new Candidate(kw));
      }
    }

    List<String> names = tableAndColumnNames();
    for (String name : names) {
      if (name.toLowerCase().startsWith(lower)) {
        candidates.add(new Candidate(name));
      }
    }
  }

  private boolean isTableContext(String beforeCursor) {
    if (language == QueryType.SQL) {
      return SQL_TABLE_CONTEXT.matcher(beforeCursor).find();
    }
    return PPL_TABLE_CONTEXT.matcher(beforeCursor).find();
  }

  private boolean isColumnContext(String beforeCursor) {
    if (language == QueryType.SQL) {
      return SQL_COLUMN_CONTEXT.matcher(beforeCursor).find();
    }
    return PPL_COLUMN_CONTEXT.matcher(beforeCursor).find()
        || PPL_STATS_BY_CONTEXT.matcher(beforeCursor).find();
  }

  private String extractTableName(String beforeCursor) {
    Matcher m;
    if (language == QueryType.SQL) {
      m = SQL_FROM_TABLE.matcher(beforeCursor);
    } else {
      m = PPL_SOURCE_TABLE.matcher(beforeCursor);
    }
    String tableName = null;
    while (m.find()) {
      tableName = m.group(1);
    }
    return tableName;
  }

  private void addTableCandidates(String lower, List<Candidate> candidates) {
    if (tables == null) {
      return;
    }
    boolean isPpl = language == QueryType.PPL;
    for (String name : tables.keySet()) {
      String display = isPpl ? "catalog." + name : name;
      if (display.toLowerCase().startsWith(lower)) {
        candidates.add(new Candidate(display));
      }
    }
  }

  private void addColumnCandidates(String tableName, String lower, List<Candidate> candidates) {
    if (tables == null) {
      return;
    }
    Table table = tables.get(tableName);
    if (table == null) {
      return;
    }
    for (String col : table.getRowType(new JavaTypeFactoryImpl()).getFieldNames()) {
      if (col.toLowerCase().startsWith(lower)) {
        candidates.add(new Candidate(col));
      }
    }
  }

  private List<String> tableAndColumnNames() {
    List<String> names = new ArrayList<>();
    if (tables == null) {
      return names;
    }
    for (Map.Entry<String, Table> entry : tables.entrySet()) {
      names.add(entry.getKey());
      names.addAll(entry.getValue().getRowType(new JavaTypeFactoryImpl()).getFieldNames());
    }
    return names;
  }
}
