/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.parser.flint;

import java.util.List;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

/** AST node for {@code CREATE INDEX name ON table (col, ...)}. */
public class SqlCreateCoveringIndex extends SqlCreate {

  private static final SqlOperator OPERATOR =
      new SqlSpecialOperator("CREATE INDEX", SqlKind.OTHER_DDL);

  private final SqlIdentifier indexName;
  private final SqlIdentifier tableName;
  private final SqlNodeList columns;

  public SqlCreateCoveringIndex(
      SqlParserPos pos,
      boolean ifNotExists,
      SqlIdentifier indexName,
      SqlIdentifier tableName,
      SqlNodeList columns) {
    super(OPERATOR, pos, false, ifNotExists);
    this.indexName = indexName;
    this.tableName = tableName;
    this.columns = columns;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return List.of(indexName, tableName, columns);
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("CREATE INDEX");
    if (ifNotExists) {
      writer.keyword("IF NOT EXISTS");
    }
    indexName.unparse(writer, 0, 0);
    writer.keyword("ON");
    tableName.unparse(writer, 0, 0);
  }

  public SqlIdentifier getIndexName() {
    return indexName;
  }

  public SqlIdentifier getTableName() {
    return tableName;
  }

  public SqlNodeList getColumns() {
    return columns;
  }
}
