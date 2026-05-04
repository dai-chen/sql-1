/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.spec.aggregate;

import java.util.Set;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlShuttle;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Post-parse rewriter that converts {@code AVG(CAST(x AS <temporal>))} to {@code AVG(CAST(x AS
 * BIGINT))} and {@code AVG(timestamp(expr))} / {@code AVG(date(expr))} / etc. to {@code
 * AVG(CAST(expr AS BIGINT))}.
 *
 * <p>Calcite's validator rejects {@code AVG} over temporal types with errors like "Cannot apply
 * 'AVG' to arguments of type 'AVG(&lt;TIMESTAMP(0)&gt;)'". This rewriter resolves those validation
 * errors by replacing the temporal target type with {@code BIGINT} before validation.
 *
 * <p>Only rewrites when the AVG operand is:
 *
 * <ul>
 *   <li>A {@code CAST} expression with an explicit temporal target type (DATE, TIME, TIMESTAMP,
 *       TIMESTAMP_WITH_LOCAL_TIME_ZONE), or
 *   <li>A function call whose name matches a temporal constructor ({@code timestamp}, {@code date},
 *       {@code time}, {@code datetime}) with a single non-literal operand.
 * </ul>
 *
 * <p>Addresses compatibility report buckets #8, #12, #13 (~8 failures total).
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class AvgTemporalCastRewriter extends SqlShuttle {

  public static final AvgTemporalCastRewriter INSTANCE = new AvgTemporalCastRewriter();

  private static final Set<SqlTypeName> TEMPORAL_TYPES =
      Set.of(
          SqlTypeName.DATE,
          SqlTypeName.TIME,
          SqlTypeName.TIMESTAMP,
          SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE);

  private static final Set<String> TEMPORAL_FUNCTION_NAMES =
      Set.of("timestamp", "date", "time", "datetime");

  @Override
  public @Nullable SqlNode visit(SqlCall call) {
    SqlCall visited = (SqlCall) super.visit(call);
    if (visited == null) {
      return null;
    }
    // Gate on AVG operator name FIRST.
    if (!visited.getOperator().getName().equalsIgnoreCase("AVG")) {
      return visited;
    }
    if (visited.getOperandList().size() != 1) {
      return visited;
    }

    SqlNode operand = visited.operand(0);
    SqlNode rewritten = tryRewriteOperand(operand);
    if (rewritten == null) {
      return visited;
    }
    return visited.getOperator().createCall(visited.getParserPosition(), rewritten);
  }

  /**
   * Attempts to rewrite the AVG operand to a CAST-to-BIGINT. Returns null if the operand is not a
   * targetable pattern.
   */
  private static @Nullable SqlNode tryRewriteOperand(SqlNode operand) {
    if (!(operand instanceof SqlCall operandCall)) {
      return null;
    }

    // Case (a): CAST(x AS <temporal>)
    if (operandCall.getKind() == SqlKind.CAST && operandCall.getOperandList().size() == 2) {
      SqlNode typeOperand = operandCall.operand(1);
      if (typeOperand instanceof SqlDataTypeSpec spec && isTemporalType(spec)) {
        return castToBigint(operandCall.operand(0));
      }
      return null;
    }

    // Case (b): timestamp(expr), date(expr), time(expr), datetime(expr)
    String fnName = operandCall.getOperator().getName().toLowerCase();
    if (TEMPORAL_FUNCTION_NAMES.contains(fnName) && operandCall.getOperandList().size() == 1) {
      SqlNode inner = operandCall.operand(0);
      // Only rewrite when the inner operand is NOT a string literal (DateTimeLiteralRewriter
      // handles those).
      if (inner instanceof SqlLiteral) {
        return null;
      }
      return castToBigint(inner);
    }

    return null;
  }

  private static boolean isTemporalType(SqlDataTypeSpec spec) {
    String typeName = spec.getTypeName().getSimple().toUpperCase();
    return TEMPORAL_TYPES.stream().anyMatch(t -> t.getName().equals(typeName));
  }

  private static SqlNode castToBigint(SqlNode operand) {
    SqlParserPos pos = operand.getParserPosition();
    SqlDataTypeSpec bigintSpec =
        new SqlDataTypeSpec(new SqlBasicTypeNameSpec(SqlTypeName.BIGINT, pos), pos);
    return SqlStdOperatorTable.CAST.createCall(pos, operand, bigintSpec);
  }
}
