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
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlUnresolvedFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.InferTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlShuttle;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Post-parse rewriter that converts MySQL-style {@code convert_tz(datetime, from_tz, to_tz)} into
 * Calcite's {@code CONVERT_TIMEZONE(from_tz, to_tz, datetime)} by reordering arguments.
 *
 * <p>Calcite 1.41's {@code SqlLibraryOperators.CONVERT_TIMEZONE} expects {@code (sourceTz,
 * targetTz, value)}. The blanket library registration in {@code LibraryOperatorExtension} makes
 * {@code CONVERT_TIMEZONE} resolvable by name.
 *
 * <p>Addresses compatibility report bucket #4: {@code No match found for function signature
 * convert_tz(<CHARACTER>, <CHARACTER>, <CHARACTER>)} (14 failures).
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class ConvertTzRewriter extends SqlShuttle {

  public static final ConvertTzRewriter INSTANCE = new ConvertTzRewriter();

  @Override
  public @Nullable SqlNode visit(SqlCall call) {
    SqlCall visited = (SqlCall) super.visit(call);
    if (visited == null) {
      return null;
    }
    // Gate on operator name FIRST to avoid touching operands of non-matching calls.
    if (!visited.getOperator().getName().equalsIgnoreCase("convert_tz")) {
      return visited;
    }
    if (visited.getOperandList().size() != 3) {
      return visited;
    }
    SqlNode dt = visited.operand(0);
    SqlNode fromTz = visited.operand(1);
    SqlNode toTz = visited.operand(2);
    SqlParserPos pos = visited.getParserPosition();
    SqlUnresolvedFunction convertTimezone =
        new SqlUnresolvedFunction(
            new SqlIdentifier("CONVERT_TIMEZONE", pos),
            ReturnTypes.TIMESTAMP_NULLABLE,
            InferTypes.RETURN_TYPE,
            null,
            null,
            SqlFunctionCategory.TIMEDATE);
    // Wrap dt in CAST(dt AS TIMESTAMP) when it is a string literal, so Calcite's
    // CONVERT_TIMEZONE sees a TIMESTAMP third argument instead of CHARACTER.
    if (dt instanceof SqlLiteral lit) {
      SqlTypeName litType = lit.getTypeName();
      if (litType == SqlTypeName.CHAR || litType == SqlTypeName.VARCHAR) {
        SqlDataTypeSpec tsSpec =
            new SqlDataTypeSpec(new SqlBasicTypeNameSpec(SqlTypeName.TIMESTAMP, pos), pos);
        dt = SqlStdOperatorTable.CAST.createCall(pos, dt, tsSpec);
      }
    }
    return convertTimezone.createCall(pos, fromTz, toTz, dt);
  }
}
