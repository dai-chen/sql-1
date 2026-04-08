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

/** AST node for array literal syntax: {@code [field1, field2]}. */
public class SqlArrayLiteral extends SqlCall {

  private static final SqlOperator OPERATOR =
      new SqlSpecialOperator("ARRAY_LITERAL", SqlKind.ARRAY_VALUE_CONSTRUCTOR);

  private final List<SqlNode> elements;

  public SqlArrayLiteral(SqlParserPos pos, List<SqlNode> elements) {
    super(pos);
    this.elements = List.copyOf(elements);
  }

  @Override
  public SqlOperator getOperator() {
    return OPERATOR;
  }

  @Override
  public List<SqlNode> getOperandList() {
    return elements;
  }

  @Override
  public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
    writer.print("[");
    for (int i = 0; i < elements.size(); i++) {
      if (i > 0) {
        writer.print(", ");
      }
      elements.get(i).unparse(writer, 0, 0);
    }
    writer.print("]");
  }
}
