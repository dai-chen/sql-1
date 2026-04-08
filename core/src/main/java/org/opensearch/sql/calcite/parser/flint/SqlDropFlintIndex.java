/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.parser.flint;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

/** AST node for DROP SKIPPING INDEX / DROP INDEX / DROP MATERIALIZED VIEW. */
public class SqlDropFlintIndex extends SqlDrop {

  private static final SqlOperator OPERATOR =
      new SqlSpecialOperator("DROP FLINT INDEX", SqlKind.OTHER_DDL);

  private final String indexType; // "SKIPPING", "COVERING", "MATERIALIZED_VIEW"
  private final SqlIdentifier indexName; // null for SKIPPING
  private final SqlIdentifier tableName; // null for MATERIALIZED_VIEW

  public SqlDropFlintIndex(
      SqlParserPos pos,
      boolean ifExists,
      String indexType,
      SqlIdentifier indexName,
      SqlIdentifier tableName) {
    super(OPERATOR, pos, ifExists);
    this.indexType = indexType;
    this.indexName = indexName;
    this.tableName = tableName;
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
    writer.keyword("DROP");
    switch (indexType) {
      case "SKIPPING":
        writer.keyword("SKIPPING INDEX");
        break;
      case "COVERING":
        writer.keyword("INDEX");
        break;
      case "MATERIALIZED_VIEW":
        writer.keyword("MATERIALIZED VIEW");
        break;
    }
    if (ifExists) {
      writer.keyword("IF EXISTS");
    }
    if (indexName != null) {
      indexName.unparse(writer, 0, 0);
    }
    if (tableName != null) {
      writer.keyword("ON");
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
