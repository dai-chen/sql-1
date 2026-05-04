/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.spec.cast;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlShuttle;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Post-parse rewriter that converts {@code CAST(x AS STRING)} to {@code CAST(x AS VARCHAR)}.
 *
 * <p>{@code STRING} is a non-standard type name that Calcite's Babel parser accepts (BigQuery
 * compatibility) but the validator rejects with "Unknown identifier 'STRING'". This rewriter
 * normalizes it to the standard {@code VARCHAR} type before validation.
 *
 * <p>Addresses compatibility report bucket #8: {@code Unknown identifier 'STRING'} (6 failures,
 * AggregationIT tests with {@code CAST(time0 AS STRING)}).
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class StringTypeNameRewriter extends SqlShuttle {

  public static final StringTypeNameRewriter INSTANCE = new StringTypeNameRewriter();

  @Override
  public @Nullable SqlNode visit(SqlCall call) {
    SqlCall visited = (SqlCall) super.visit(call);
    if (visited == null) {
      return null;
    }
    // Gate on CAST kind FIRST.
    if (visited.getKind() != SqlKind.CAST) {
      return visited;
    }
    if (visited.getOperandList().size() != 2) {
      return visited;
    }
    SqlNode typeOperand = visited.operand(1);
    if (!isStringType(typeOperand)) {
      return visited;
    }
    SqlParserPos pos = visited.getParserPosition();
    SqlDataTypeSpec varcharSpec =
        new SqlDataTypeSpec(new SqlBasicTypeNameSpec(SqlTypeName.VARCHAR, pos), pos);
    return SqlStdOperatorTable.CAST.createCall(pos, visited.operand(0), varcharSpec);
  }

  /**
   * Checks whether the type operand represents the non-standard STRING type. The Babel parser with
   * BIG_QUERY lex produces a {@link SqlDataTypeSpec} whose type-name simple name is "STRING".
   */
  private static boolean isStringType(SqlNode typeOperand) {
    if (typeOperand instanceof SqlDataTypeSpec spec) {
      return spec.getTypeName().getSimple().equalsIgnoreCase("STRING");
    }
    return false;
  }
}
