/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.parser.flint;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

/** AST node for REFRESH SKIPPING INDEX / REFRESH INDEX / REFRESH MATERIALIZED VIEW. */
public class SqlRefreshFlintIndex extends SqlCall {

  private static final SqlOperator OPERATOR =
      new SqlSpecialOperator("REFRESH FLINT INDEX", SqlKind.OTHER);

  private final String indexType;
  private final SqlIdentifier indexName; // null for SKIPPING
  private final SqlIdentifier tableName; // null for MATERIALIZED_VIEW

  public SqlRefreshFlintIndex(
      SqlParserPos pos, String indexType, SqlIdentifier indexName, SqlIdentifier tableName) {
    super(pos);
    this.indexType = indexType;
    this.indexName = indexName;
    this.tableName = tableName;
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    List<SqlNode> list = new ArrayList<>();
    if (indexName != null) {
      list.add(indexName);
    }
    if (tableName != null) {
      list.add(tableName);
    }
    return list;
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("REFRESH");
    switch (indexType) {
      case "SKIPPING":
        writer.keyword("SKIPPING INDEX ON");
        break;
      case "COVERING":
        writer.keyword("INDEX");
        break;
      case "MATERIALIZED_VIEW":
        writer.keyword("MATERIALIZED VIEW");
        break;
    }
    if (indexName != null) {
      indexName.unparse(writer, 0, 0);
    }
    if (tableName != null && !"SKIPPING".equals(indexType)) {
      writer.keyword("ON");
    }
    if (tableName != null) {
      tableName.unparse(writer, 0, 0);
    }
  }

  public String getIndexType() {
    return indexType;
  }

  public SqlIdentifier getIndexName() {
    return indexName;
  }

  public SqlIdentifier getTableName() {
    return tableName;
  }
}
