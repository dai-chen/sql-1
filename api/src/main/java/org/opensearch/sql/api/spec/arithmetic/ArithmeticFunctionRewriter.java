/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.spec.arithmetic;

import java.util.Map;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.util.SqlShuttle;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Post-parse rewriter that converts OpenSearch SQL's function-style arithmetic calls into standard
 * SQL binary operators: {@code add(a,b) → a+b}, {@code subtract(a,b) → a-b}, {@code multiply(a,b) →
 * a*b}, {@code divide(a,b) → a/b}, {@code modulus(a,b) → MOD(a,b)}.
 *
 * <p>Addresses compatibility report buckets #17-21 (multiply/add/modulus/divide/subtract, 5
 * failures total).
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class ArithmeticFunctionRewriter extends SqlShuttle {

  public static final ArithmeticFunctionRewriter INSTANCE = new ArithmeticFunctionRewriter();

  /** Maps lowercase function names to their Calcite operator equivalents. */
  private static final Map<String, SqlOperator> OPERATOR_MAP =
      Map.of(
          "add", SqlStdOperatorTable.PLUS,
          "subtract", SqlStdOperatorTable.MINUS,
          "multiply", SqlStdOperatorTable.MULTIPLY,
          "divide", SqlStdOperatorTable.DIVIDE,
          "modulus", SqlStdOperatorTable.MOD);

  @Override
  public @Nullable SqlNode visit(SqlCall call) {
    SqlCall visited = (SqlCall) super.visit(call);
    if (visited == null) {
      return null;
    }
    // Gate on operator name FIRST to avoid touching operands of non-matching calls.
    SqlOperator replacement = OPERATOR_MAP.get(visited.getOperator().getName().toLowerCase());
    if (replacement == null) {
      return visited;
    }
    if (visited.getOperandList().size() != 2) {
      return visited;
    }
    return replacement.createCall(
        visited.getParserPosition(), visited.operand(0), visited.operand(1));
  }
}
