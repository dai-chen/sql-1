/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.parser;

import java.util.List;
import lombok.RequiredArgsConstructor;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.util.SqlVisitor;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.common.antlr.SyntaxCheckException;

/**
 * Calcite SQL query parser that produces {@link SqlNode} as the native parse result. Applies
 * post-parse rewrite rules (e.g., named argument normalization) before returning.
 */
@RequiredArgsConstructor
public class CalciteSqlQueryParser implements UnifiedQueryParser<SqlNode> {

  /** Calcite plan context providing parser configuration (e.g., case sensitivity, conformance). */
  private final CalcitePlanContext planContext;

  /** Post-parse rewrite rules applied after parsing and before validation. */
  private final List<SqlVisitor<SqlNode>> postParseRules;

  @Override
  public SqlNode parse(String query) {
    try {
      SqlParser parser = SqlParser.create(query, planContext.config.getParserConfig());
      SqlNode parsed = parser.parseQuery();

      SqlNode result = parsed;
      for (SqlVisitor<SqlNode> visitor : postParseRules) {
        result = result.accept(visitor);
      }
      return result;
    } catch (SqlParseException e) {
      throw new SyntaxCheckException("Failed to parse SQL query: " + e.getMessage());
    }
  }
}
