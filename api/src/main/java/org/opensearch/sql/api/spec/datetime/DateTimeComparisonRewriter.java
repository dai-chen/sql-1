/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.spec.datetime;

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
 * Post-parse rewriter that inserts explicit {@code CAST(... AS TIMESTAMP)} around temporal literal
 * operands in comparison expressions where the two sides have mismatched date/time types.
 *
 * <p>Calcite's validator rejects comparisons like {@code DATE '2020-09-16' = TIMESTAMP '2020-09-16
 * 00:00:00'} with "Cannot apply '=' to arguments of type ...". This rewriter resolves those
 * validation errors by casting the narrower operand(s) to {@code TIMESTAMP} before validation.
 *
 * <p>Type promotion rules:
 *
 * <ul>
 *   <li>{@code DATE} vs {@code TIMESTAMP} → cast DATE to TIMESTAMP
 *   <li>{@code TIME} vs {@code TIMESTAMP} → cast TIME to TIMESTAMP
 *   <li>{@code DATE} vs {@code TIME} → cast both to TIMESTAMP
 * </ul>
 *
 * <p>Only rewrites when <em>both</em> operands are typed temporal {@link SqlLiteral}s with
 * conflicting types. Non-literal operands (columns, expressions) are left untouched.
 *
 * <p>This rewriter must run <em>after</em> {@link DateTimeLiteralRewriter} so that function-call
 * style literals ({@code DATE('...')}) have already been converted to ANSI typed literals ({@code
 * DATE '...'}).
 *
 * @see DateTimeLiteralRewriter
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class DateTimeComparisonRewriter extends SqlShuttle {

  public static final DateTimeComparisonRewriter INSTANCE = new DateTimeComparisonRewriter();

  private static final Set<SqlKind> COMPARISON_KINDS =
      Set.of(
          SqlKind.EQUALS,
          SqlKind.NOT_EQUALS,
          SqlKind.LESS_THAN,
          SqlKind.LESS_THAN_OR_EQUAL,
          SqlKind.GREATER_THAN,
          SqlKind.GREATER_THAN_OR_EQUAL);

  private static final Set<SqlTypeName> TEMPORAL_TYPES =
      Set.of(SqlTypeName.DATE, SqlTypeName.TIME, SqlTypeName.TIMESTAMP);

  @Override
  public @Nullable SqlNode visit(SqlCall call) {
    SqlCall visited = (SqlCall) super.visit(call);
    if (visited == null || visited.getOperandList().size() != 2) {
      return visited;
    }
    if (!COMPARISON_KINDS.contains(visited.getKind())) {
      return visited;
    }

    SqlNode left = visited.operand(0);
    SqlNode right = visited.operand(1);
    if (!(left instanceof SqlLiteral leftLit) || !(right instanceof SqlLiteral rightLit)) {
      return visited;
    }

    SqlTypeName leftType = leftLit.getTypeName();
    SqlTypeName rightType = rightLit.getTypeName();
    if (!TEMPORAL_TYPES.contains(leftType) || !TEMPORAL_TYPES.contains(rightType)) {
      return visited;
    }
    if (leftType == rightType) {
      return visited;
    }

    // Cast the narrower type(s) to TIMESTAMP.
    SqlNode newLeft = leftType == SqlTypeName.TIMESTAMP ? left : castToTimestamp(left);
    SqlNode newRight = rightType == SqlTypeName.TIMESTAMP ? right : castToTimestamp(right);
    return visited.getOperator().createCall(visited.getParserPosition(), newLeft, newRight);
  }

  private static SqlNode castToTimestamp(SqlNode operand) {
    SqlParserPos pos = operand.getParserPosition();
    SqlDataTypeSpec timestampSpec =
        new SqlDataTypeSpec(new SqlBasicTypeNameSpec(SqlTypeName.TIMESTAMP, pos), pos);
    return SqlStdOperatorTable.CAST.createCall(pos, operand, timestampSpec);
  }
}
