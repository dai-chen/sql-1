/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.cli;

import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Pattern;
import org.apache.calcite.schema.Table;
import org.jline.reader.Highlighter;
import org.jline.reader.LineReader;
import org.jline.utils.AttributedString;
import org.jline.utils.AttributedStringBuilder;
import org.jline.utils.AttributedStyle;
import org.opensearch.sql.executor.QueryType;

/** Syntax highlighter for SQL/PPL queries in the interactive REPL. */
public class QueryHighlighter implements Highlighter {

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

  private QueryType language;
  private Map<String, Table> tables;

  public QueryHighlighter(Map<String, Table> tables, QueryType language) {
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
  public AttributedString highlight(LineReader reader, String buffer) {
    AttributedStringBuilder builder = new AttributedStringBuilder();
    Set<String> keywords = new HashSet<>();
    for (String kw : (language == QueryType.SQL ? SQL_KEYWORDS : PPL_KEYWORDS)) {
      keywords.add(kw.toLowerCase());
    }

    int i = 0;
    while (i < buffer.length()) {
      char c = buffer.charAt(i);

      if (c == '\'') {
        int start = i;
        i++;
        while (i < buffer.length() && buffer.charAt(i) != '\'') {
          i++;
        }
        if (i < buffer.length()) {
          i++;
        }
        builder.styled(
            new AttributedStyle().foreground(AttributedStyle.GREEN), buffer.substring(start, i));
        continue;
      }

      if (Character.isWhitespace(c)) {
        builder.append(c);
        i++;
        continue;
      }

      int start = i;
      while (i < buffer.length()
          && !Character.isWhitespace(buffer.charAt(i))
          && buffer.charAt(i) != '\'') {
        i++;
      }
      String token = buffer.substring(start, i);

      if (token.startsWith(".")) {
        builder.styled(new AttributedStyle().foreground(AttributedStyle.CYAN), token);
      } else if (keywords.contains(token.toLowerCase())) {
        builder.styled(new AttributedStyle().bold().foreground(AttributedStyle.BLUE), token);
      } else if (token.matches("-?\\d+(\\.\\d+)?")) {
        builder.styled(new AttributedStyle().foreground(AttributedStyle.MAGENTA), token);
      } else {
        builder.append(token);
      }
    }
    return builder.toAttributedString();
  }

  @Override
  public void setErrorPattern(Pattern errorPattern) {}

  @Override
  public void setErrorIndex(int errorIndex) {}
}
