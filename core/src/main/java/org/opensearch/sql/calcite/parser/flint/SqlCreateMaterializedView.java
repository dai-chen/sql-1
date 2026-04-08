/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.parser.flint;

import java.util.List;
import org.apache.calcite.sql.*;
import org.apache.calcite.sql.parser.SqlParserPos;

/** AST node for {@code CREATE MATERIALIZED VIEW name AS query}. */
public class SqlCreateMaterializedView extends SqlCreate {

  private static final SqlOperator OPERATOR =
      new SqlSpecialOperator("CREATE MATERIALIZED VIEW", SqlKind.OTHER_DDL);

  private final SqlIdentifier viewName;
  private final SqlNode query;

  public SqlCreateMaterializedView(
      SqlParserPos pos, boolean ifNotExists, SqlIdentifier viewName, SqlNode query) {
    super(OPERATOR, pos, false, ifNotExists);
    this.viewName = viewName;
    this.query = query;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return List.of(viewName, query);
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.keyword("CREATE MATERIALIZED VIEW");
    if (ifNotExists) {
      writer.keyword("IF NOT EXISTS");
    }
    viewName.unparse(writer, 0, 0);
    writer.keyword("AS");
    query.unparse(writer, 0, 0);
  }

  public SqlIdentifier getViewName() {
    return viewName;
  }

  public SqlNode getQuery() {
    return query;
  }
}
