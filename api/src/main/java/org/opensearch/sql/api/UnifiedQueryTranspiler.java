/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.rel2sql.RelToSqlConverter;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.pretty.SqlPrettyWriter;

/**
 * Transpiles Calcite logical plans ({@link RelNode}) into SQL strings for various target databases.
 * Uses Calcite's {@link RelToSqlConverter} to perform the conversion, respecting the specified SQL
 * dialect and formatting options.
 */
public class UnifiedQueryTranspiler {

  /**
   * Converts a Calcite logical plan to a SQL string using the specified transpile options.
   *
   * @param plan the logical plan to convert (must not be null)
   * @param options the transpilation options including target dialect and formatting preferences
   * @return the generated SQL string
   */
  public String toSql(RelNode plan, TranspileOptions options) {
    try {
      SqlDialect dialect = options.getSqlDialect();
      RelToSqlConverter converter = new RelToSqlConverter(dialect);
      SqlNode sqlNode = converter.visitRoot(plan).asStatement();
      return formatSql(sqlNode, dialect, options.isPrettyPrint());
    } catch (Exception e) {
      throw new IllegalStateException("Failed to transpile logical plan to SQL", e);
    }
  }

  private String formatSql(SqlNode sqlNode, SqlDialect dialect, boolean prettyPrint) {
    if (prettyPrint) {
      SqlPrettyWriter writer = new SqlPrettyWriter(dialect);
      writer.setAlwaysUseParentheses(false);
      writer.setSelectListItemsOnSeparateLines(false);
      writer.setIndentation(2);
      return writer.format(sqlNode);
    } else {
      return sqlNode.toSqlString(dialect).getSql();
    }
  }
}
