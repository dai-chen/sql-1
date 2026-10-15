/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.dialect.clickhouse;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlShuttle;

/** Clause/idiom adapter: ClickHouse {@code count()} (no argument) means {@code COUNT(*)}. */
public class ClickHouseCountRule extends SqlShuttle {
  @Override
  public SqlNode visit(SqlCall call) {
    boolean isCount =
        call.getKind() == SqlKind.COUNT
            || (call.getKind() == SqlKind.OTHER_FUNCTION
                && "COUNT".equalsIgnoreCase(call.getOperator().getName()));
    if (isCount && call.operandCount() == 0) {
      return SqlStdOperatorTable.COUNT.createCall(
          call.getFunctionQuantifier(),
          call.getParserPosition(),
          SqlIdentifier.star(SqlParserPos.ZERO));
    }
    return super.visit(call);
  }
}
