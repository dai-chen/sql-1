/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.parser.flint;

import java.util.List;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

/** AST node for {@code CREATE SKIPPING INDEX ON table (col type, ...)}. */
public class SqlCreateSkippingIndex extends SqlCreate {

  private static final SqlOperator OPERATOR =
      new SqlSpecialOperator("CREATE SKIPPING INDEX", SqlKind.OTHER_DDL);

  private final SqlIdentifier tableName;
  private final SqlNodeList columns;

  public SqlCreateSkippingIndex(
      SqlParserPos pos, boolean ifNotExists, SqlIdentifier tableName, SqlNodeList columns) {
    super(OPERATOR, pos, false, ifNotExists);
    this.tableName = tableName;
    this.columns = columns;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return List.of(tableName, columns);
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("CREATE SKIPPING INDEX");
    if (ifNotExists) {
      writer.keyword("IF NOT EXISTS");
    }
    writer.keyword("ON");
    tableName.unparse(writer, 0, 0);
  }

  public SqlIdentifier getTableName() {
    return tableName;
  }

  public SqlNodeList getColumns() {
    return columns;
  }
}
