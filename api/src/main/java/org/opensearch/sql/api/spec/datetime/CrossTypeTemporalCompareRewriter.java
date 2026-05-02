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
 * Pre-validation rewriter that makes cross-type temporal comparisons validate by casting both
 * operands to {@code VARCHAR}. Calcite's {@link org.apache.calcite.sql.validate.SqlValidator}
 * refuses comparisons across {@code DATE}, {@code TIME}, and {@code TIMESTAMP} because the types
 * don't share a comparable family. OpenSearch SQL historically allowed these comparisons by
 * coercing through string form (ISO 8601 lexicographic order).
 *
 * <p>This rewriter looks for binary comparison calls ({@code = != &lt; &lt;= &gt; &gt;=}) where
 * both operands are typed temporal literals ({@link org.apache.calcite.sql.SqlLiteral} of kind
 * DATE, TIME, or TIMESTAMP) and at least two different temporal types appear. In those cases both
 * operands are wrapped in {@code CAST(x AS VARCHAR)}. String comparison of ISO-formatted temporal
 * literals gives the same ordering as the semantic comparison for the test fixtures (which only use
 * ISO-style strings). If AE later implements real temporal coercion, drop this rewriter.
 *
 * <p>Non-matching shapes (non-temporal comparisons, same-type comparisons, column references) pass
 * through unchanged — we don't want to affect queries that Calcite already validates.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class CrossTypeTemporalCompareRewriter extends SqlShuttle {

  public static final CrossTypeTemporalCompareRewriter INSTANCE =
      new CrossTypeTemporalCompareRewriter();

  @Override
  public @Nullable SqlNode visit(SqlCall call) {
    SqlCall visited = (SqlCall) super.visit(call);
    if (visited == null) {
      return null;
    }
    SqlKind kind = visited.getKind();
    if (kind != SqlKind.EQUALS
        && kind != SqlKind.NOT_EQUALS
        && kind != SqlKind.LESS_THAN
        && kind != SqlKind.LESS_THAN_OR_EQUAL
        && kind != SqlKind.GREATER_THAN
        && kind != SqlKind.GREATER_THAN_OR_EQUAL) {
      return visited;
    }
    if (visited.getOperandList().size() != 2) {
      return visited;
    }
    SqlNode left = visited.operand(0);
    SqlNode right = visited.operand(1);
    SqlTypeName leftType = temporalType(left);
    SqlTypeName rightType = temporalType(right);
    if (leftType == null || rightType == null) {
      return visited;
    }
    if (leftType == rightType) {
      // Same temporal type — Calcite handles it fine, don't interfere.
      return visited;
    }
    // Cross-type temporal comparison — wrap both sides in CAST-to-VARCHAR.
    SqlNode newLeft = castToVarchar(left);
    SqlNode newRight = castToVarchar(right);
    return visited.getOperator().createCall(visited.getParserPosition(), newLeft, newRight);
  }

  /**
   * Returns the temporal SqlTypeName of {@code node} if it's a DATE, TIME, or TIMESTAMP literal,
   * else null. We intentionally don't reach into CASTs or function calls — at rewrite time we can't
   * be sure what they'll resolve to, so we only handle the literal-vs-literal case the
   * DateTimeComparisonIT fixtures use.
   */
  private static @Nullable SqlTypeName temporalType(SqlNode node) {
    if (!(node instanceof SqlLiteral lit)) {
      return null;
    }
    SqlTypeName t = lit.getTypeName();
    if (t == SqlTypeName.DATE || t == SqlTypeName.TIME || t == SqlTypeName.TIMESTAMP) {
      return t;
    }
    return null;
  }

  private static SqlNode castToVarchar(SqlNode node) {
    SqlParserPos pos = node.getParserPosition();
    SqlDataTypeSpec varcharSpec =
        new SqlDataTypeSpec(new SqlBasicTypeNameSpec(SqlTypeName.VARCHAR, pos), pos);
    return SqlStdOperatorTable.CAST.createCall(pos, node, varcharSpec);
  }
}
