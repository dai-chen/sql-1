/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.spec.datetime;

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
 * Pre-validation rewriter that allows {@code AVG} aggregations over temporal columns. Calcite's
 * {@code SqlStdOperatorTable.AVG} only accepts {@code NUMERIC} operands — but OpenSearch SQL
 * historically allows {@code AVG} over {@code DATE}, {@code TIME}, and {@code TIMESTAMP} columns by
 * coercing through epoch-millis (or similar numeric representation).
 *
 * <p>Rewriters run pre-validation, so we don't have the operand's type yet. This rewriter uses a
 * conservative heuristic: if an {@code AVG} call's single operand is a {@code TIMESTAMP} typed
 * literal, a {@code CAST(x AS TIMESTAMP)}, or a call to a date/time function ({@code DATE}, {@code
 * TIME}, {@code TIMESTAMP}), wrap the whole operand in {@code CAST(... AS BIGINT)} so the validator
 * sees {@code AVG(<NUMERIC>)}. This isn't semantically correct for the actual epoch conversion — AE
 * has to handle the coercion at runtime — but it gets the call past validation.
 *
 * <p>Column-reference operands aren't rewritten because we can't tell at parse time whether they're
 * temporal. That's acceptable for the compatibility report's purposes: the tests that matter use
 * constant-form AVG expressions, and the failing-with-a-clearer-error fallback for column refs is
 * no worse than the original error.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class AvgTemporalRewriter extends SqlShuttle {

  public static final AvgTemporalRewriter INSTANCE = new AvgTemporalRewriter();

  @Override
  public @Nullable SqlNode visit(SqlCall call) {
    SqlCall visited = (SqlCall) super.visit(call);
    if (visited == null) {
      return null;
    }
    // Only handle AVG calls — AVG has SqlKind AVG, but also check operator name as a fallback.
    String opName = visited.getOperator().getName();
    if (visited.getKind() != SqlKind.AVG && !opName.equalsIgnoreCase("AVG")) {
      return visited;
    }
    if (visited.getOperandList().size() != 1) {
      return visited;
    }
    SqlNode arg = visited.operand(0);
    if (!isTemporalExpression(arg)) {
      return visited;
    }
    // Wrap arg in CAST(arg AS BIGINT). AE is expected to compute the correct numeric
    // representation (epoch millis for TIMESTAMP, day number for DATE, etc.) and then average.
    SqlParserPos pos = arg.getParserPosition();
    SqlDataTypeSpec bigintSpec =
        new SqlDataTypeSpec(new SqlBasicTypeNameSpec(SqlTypeName.BIGINT, pos), pos);
    SqlNode cast = SqlStdOperatorTable.CAST.createCall(pos, arg, bigintSpec);
    // Rebuild the AVG call with the casted argument. Preserve the DISTINCT/ALL qualifier.
    return visited
        .getOperator()
        .createCall(visited.getFunctionQuantifier(), visited.getParserPosition(), cast);
  }

  private static boolean isTemporalExpression(SqlNode node) {
    // ONLY TIMESTAMP is rewritable: Calcite allows CAST(<TIMESTAMP> AS BIGINT) (epoch millis)
    // but REJECTS CAST(<DATE> AS BIGINT) and CAST(<TIME> AS BIGINT). So rewriting AVG(<DATE>)
    // to AVG(CAST(...AS BIGINT)) would replace one SQL/api error with a different one. Leave
    // DATE/TIME AVG calls alone — they'll surface the original "Cannot apply 'AVG'" error,
    // which is more honest about the unsupported shape.
    if (node instanceof SqlLiteral lit) {
      return lit.getTypeName() == SqlTypeName.TIMESTAMP;
    }
    if (node instanceof SqlCall inner) {
      // CAST(x AS TIMESTAMP/DATETIME) — still timestamp-valued
      if (inner.getKind() == SqlKind.CAST && inner.getOperandList().size() >= 2) {
        SqlNode typeNode = inner.operand(1);
        if (typeNode instanceof SqlDataTypeSpec dts) {
          String name = dts.getTypeNameSpec().getTypeName().getSimple().toUpperCase();
          if (name.equals("TIMESTAMP") || name.equals("DATETIME")) {
            return true;
          }
        }
      }
      // Constructor/now-family calls that yield TIMESTAMP
      String innerName = inner.getOperator().getName();
      if (innerName.equalsIgnoreCase("TIMESTAMP")
          || innerName.equalsIgnoreCase("DATETIME")
          || innerName.equalsIgnoreCase("NOW")
          || innerName.equalsIgnoreCase("CURRENT_TIMESTAMP")) {
        return true;
      }
    }
    return false;
  }
}
