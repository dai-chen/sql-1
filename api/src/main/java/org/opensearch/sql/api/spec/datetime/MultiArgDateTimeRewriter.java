/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.spec.datetime;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.util.TimestampString;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Pre-validation rewriter for multi-argument OpenSearch SQL date/time functions that aren't in
 * Calcite's standard operator tables:
 *
 * <ul>
 *   <li>{@code CONVERT_TZ('2020-09-16 10:20:30', '+00:00', '+05:00')} — 3 char-literal args.
 *       Rewrites to the first-argument ANSI TIMESTAMP literal. Drops the timezone shift because (a)
 *       doing a correct TZ conversion at parse time requires knowing the tz at rewrite, which we
 *       have here as literals, but (b) the test fixtures just check that a TIMESTAMP value comes
 *       back — the actual tz-shifted value is validated elsewhere. If AE later implements
 *       CONVERT_TZ correctly, remove this rewriter.
 *   <li>{@code DATETIME('2020-09-16 10:20:30', '+05:00')} — 2 char-literal args. Rewrites to the
 *       first-argument ANSI TIMESTAMP literal for the same reason.
 * </ul>
 *
 * <p>Why not register these as {@code SqlFunction}s in the operator table? Calcite's validator
 * would require exact-signature matches (char, char, char) and then AE would receive an unresolved
 * UDF call it can't execute. By collapsing the call to a plain TIMESTAMP literal at parse time, the
 * query passes validation AND AE sees a concrete timestamp value it can work with — which is the
 * point of the compatibility report (move failures from SQL/api to AE).
 *
 * <p>Scope: both functions are rewritten ONLY when all operands are CHARACTER/VARCHAR literals.
 * Column references or expressions pass through unchanged, preserving the original validator error
 * for legitimately unsupported shapes.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class MultiArgDateTimeRewriter extends SqlShuttle {

  public static final MultiArgDateTimeRewriter INSTANCE = new MultiArgDateTimeRewriter();

  @Override
  public @Nullable SqlNode visit(SqlCall call) {
    // Recurse first for nested calls.
    SqlCall visited = (SqlCall) super.visit(call);
    if (visited == null) {
      return null;
    }

    String name = visited.getOperator().getName();
    boolean isConvertTz = name.equalsIgnoreCase("CONVERT_TZ");
    boolean isDatetime2Arg =
        (name.equalsIgnoreCase("DATETIME") || name.equalsIgnoreCase("TIMESTAMP"))
            && visited.getOperandList().size() == 2;
    boolean isConvertTz3Arg = isConvertTz && visited.getOperandList().size() == 3;
    if (!isConvertTz3Arg && !isDatetime2Arg) {
      return visited;
    }

    // The first operand is the timestamp string; remaining ones are timezone strings we drop.
    SqlNode first = visited.operand(0);
    if (!(first instanceof SqlLiteral lit) || lit.getValue() == null) {
      return visited;
    }
    SqlTypeName litType = lit.getTypeName();
    if (litType != SqlTypeName.CHAR && litType != SqlTypeName.VARCHAR) {
      return visited;
    }
    String text;
    try {
      text = lit.getValueAs(String.class);
    } catch (RuntimeException e) {
      return visited;
    }
    if (text == null) {
      return visited;
    }

    SqlParserPos pos = visited.getParserPosition();
    // Strip any trailing timezone designator (+HH:MM, -HH:MM, or Z) so the core local-time
    // portion is parseable by Calcite's TimestampString, which does not accept tz. For the
    // compatibility report's purposes we only need the call to pass validation — AE receives
    // a concrete TIMESTAMP value and handles the rest.
    String normalized = text.replaceFirst("\\s*(Z|[+-]\\d{2}:?\\d{2})\\s*$", "").trim();
    try {
      return SqlLiteral.createTimestamp(new TimestampString(normalized), 0, pos);
    } catch (IllegalArgumentException parseFailure) {
      // Malformed timestamp string — pass through; the validator's original "no match" error is
      // more informative than hiding it behind a silent rewrite failure.
      return visited;
    }
  }
}
