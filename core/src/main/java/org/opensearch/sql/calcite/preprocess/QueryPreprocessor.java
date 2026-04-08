package org.opensearch.sql.calcite.preprocess;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Pre-processes SQL before Calcite parsing.
 * Handles OpenSearch-specific identifier conventions that Calcite's lexer cannot accept directly.
 */
public class QueryPreprocessor {

  // Match unquoted identifiers after FROM/JOIN that contain hyphens or start with dot
  // Captures: word chars, hyphens, dots (but not already backtick-quoted)
  private static final Pattern TABLE_IDENTIFIER =
      Pattern.compile(
          "(?i)((?:FROM|JOIN)\\s+)" +           // group 1: FROM/JOIN keyword + space
          "(?!`)" +                               // not already backtick-quoted
          "([a-zA-Z_][a-zA-Z0-9_]*" +            // start of identifier
          "(?:[-.]" +                             // followed by hyphen or dot
          "[a-zA-Z0-9_]+)+)",                    // and more word chars (repeating)
          Pattern.CASE_INSENSITIVE);

  private QueryPreprocessor() {}

  /** Pre-process SQL: auto-quote special identifiers and strip trailing semicolons. */
  public static String process(String sql) {
    if (sql == null || sql.isEmpty()) {
      return sql;
    }
    String result = quoteSpecialIdentifiers(sql);
    result = stripTrailingSemicolon(result);
    return result;
  }

  private static String quoteSpecialIdentifiers(String sql) {
    Matcher matcher = TABLE_IDENTIFIER.matcher(sql);
    StringBuilder sb = new StringBuilder();
    while (matcher.find()) {
      String prefix = matcher.group(1);
      String identifier = matcher.group(2);
      matcher.appendReplacement(sb, Matcher.quoteReplacement(prefix + "`" + identifier + "`"));
    }
    matcher.appendTail(sb);
    return sb.toString();
  }

  private static String stripTrailingSemicolon(String sql) {
    String trimmed = sql.stripTrailing();
    if (trimmed.endsWith(";")) {
      return trimmed.substring(0, trimmed.length() - 1).stripTrailing();
    }
    return trimmed;
  }
}
