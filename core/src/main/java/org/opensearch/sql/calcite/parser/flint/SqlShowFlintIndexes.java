/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.parser.flint;

import java.util.List;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

/** AST node for {@code SHOW FLINT INDEX[ES] IN catalog.database}. */
public class SqlShowFlintIndexes extends SqlCall {

  private static final SqlOperator OPERATOR =
      new SqlSpecialOperator("SHOW FLINT INDEXES", SqlKind.OTHER);

  private final SqlIdentifier catalogDb;

  public SqlShowFlintIndexes(SqlParserPos pos, SqlIdentifier catalogDb) {
    super(pos);
    this.catalogDb = catalogDb;
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return List.of(catalogDb);
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("SHOW FLINT INDEXES IN");
    catalogDb.unparse(writer, 0, 0);
  }

  public SqlIdentifier getCatalogDb() {
    return catalogDb;
  }
}
