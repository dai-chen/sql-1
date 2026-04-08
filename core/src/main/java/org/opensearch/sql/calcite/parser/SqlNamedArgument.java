/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.parser;

import java.util.List;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

/** AST node for named argument syntax: {@code key=value}. */
public class SqlNamedArgument extends SqlCall {

  private static final SqlOperator OPERATOR =
      new SqlSpecialOperator("NAMED_ARGUMENT", SqlKind.ARGUMENT_ASSIGNMENT);

  private final SqlNode name;
  private final SqlNode value;

  public SqlNamedArgument(SqlParserPos pos, SqlNode name, SqlNode value) {
    super(pos);
    this.name = name;
    this.value = value;
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return List.of(name, value);
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    name.unparse(writer, 0, 0);
    writer.print("=");
    value.unparse(writer, 0, 0);
  }

  public SqlNode getName() {
    return name;
  }

  public SqlNode getValue() {
    return value;
  }
}
