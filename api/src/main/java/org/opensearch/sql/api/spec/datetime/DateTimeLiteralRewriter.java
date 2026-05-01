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
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Pre-validation rewriter that converts OpenSearch SQL's function-call style date/time literals
 * into ANSI SQL typed literals so Calcite's standard operator table accepts them.
 *
 * <p>OpenSearch SQL historically accepts {@code DATE('2020-09-16')}, {@code TIME('10:20:30')},
 * {@code TIMESTAMP('2020-09-16 10:20:30')} as function calls with a character-string argument.
 * Calcite's {@code SqlStdOperatorTable} doesn't register functions with these exact signatures — it
 * recognizes only the ANSI typed-literal form ({@code DATE '2020-09-16'} without parens), which the
 * parser produces as {@link SqlLiteral} nodes rather than {@link SqlCall} nodes.
 *
 * <p>Rather than extending the operator table, we normalize the AST here: any {@code
 * DATE('...')}/{@code TIME('...')}/{@code TIMESTAMP('...')} call whose single argument is a literal
 * string is replaced with the equivalent ANSI-style {@link SqlLiteral}. Non-matching calls (nested
 * expressions, multiple args, non-string operand) are left alone and will surface to the validator
 * as before, preserving prior error behavior for genuinely invalid syntax.
 *
 * <p>This rewriter runs inside {@code LanguageSpec.postParseRules()} between parse and validate, so
 * it sees well-formed {@link SqlCall} nodes but doesn't need any schema context.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class DateTimeLiteralRewriter extends SqlShuttle {

  public static final DateTimeLiteralRewriter INSTANCE = new DateTimeLiteralRewriter();

  @Override
  public @Nullable SqlNode visit(SqlCall call) {
    // Recurse first so nested calls like DATE_ADD(DATE('2020-09-16'), ...) are rewritten too.
    SqlCall visited = (SqlCall) super.visit(call);
    if (visited == null) {
      return null;
    }

    // Gate on function name FIRST. For non-target calls we must not touch the operand at all —
    // calling SqlLiteral.getValueAs(String) on a literal whose value isn't a String raises a
    // fatal AssertionError that kills the JVM thread. Matching by name keeps this rewriter
    // limited to exactly the shapes we want to handle. Name comparison is case-insensitive
    // because Calcite's Lex.BIG_QUERY preserves case on unquoted identifiers but matches
    // case-insensitively — so the parser may hand us `Date`, `date`, or `DATE`.
    String name = visited.getOperator().getName();
    boolean isDate = name.equalsIgnoreCase("DATE");
    boolean isTime = name.equalsIgnoreCase("TIME");
    boolean isTimestamp = name.equalsIgnoreCase("TIMESTAMP") || name.equalsIgnoreCase("DATETIME");
    if (!isDate && !isTime && !isTimestamp) {
      return visited;
    }
    if (visited.getOperandList().size() != 1) {
      return visited;
    }
    SqlNode operand = visited.operand(0);
    if (!(operand instanceof SqlLiteral lit) || lit.getValue() == null) {
      return visited;
    }
    // Only CHARACTER/VARCHAR literals have a String value; anything else would make
    // SqlLiteral.getValueAs(String) fatally assert. Guard strictly on type name.
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
    try {
      if (isDate) {
        return SqlLiteral.createDate(new DateString(text), pos);
      }
      if (isTime) {
        // Default precision 0 — the literal form carries its own fractional digits as part of
        // the string, so passing 0 just means "let Calcite infer from the TimeString".
        return SqlLiteral.createTime(new TimeString(text), 0, pos);
      }
      // isTimestamp: DATETIME is OpenSearch-SQL-specific shorthand for TIMESTAMP; same literal
      // parsing semantics, so both map to SqlTimestampLiteral.
      return SqlLiteral.createTimestamp(new TimestampString(text), 0, pos);
    } catch (IllegalArgumentException parseFailure) {
      // The string isn't a valid date/time/timestamp literal. Fall through and let the validator
      // surface the original "No match for function signature" error — which is strictly better
      // than silently hiding a malformed literal.
      return visited;
    }
  }
}
