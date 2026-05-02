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
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlTypeNameSpec;
import org.apache.calcite.sql.SqlUserDefinedTypeNameSpec;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlShuttle;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Pre-validation rewriter that normalizes OpenSearch SQL's non-standard type names in {@code CAST}
 * expressions to their ANSI SQL equivalents. Legacy OpenSearch SQL (and many MySQL-style dialects)
 * accept {@code CAST(x AS STRING)}, but Calcite's standard validator only recognizes ANSI type
 * names like {@code VARCHAR} — so {@code STRING} ends up as a {@link SqlUserDefinedTypeNameSpec}
 * and the validator raises {@code "Unknown identifier 'STRING'"}.
 *
 * <p>Current mappings:
 *
 * <ul>
 *   <li>{@code STRING} → {@code VARCHAR}
 * </ul>
 *
 * <p>Runs as a {@link SqlShuttle} post-parse rewrite so the validator sees only ANSI type names.
 * Non-CAST calls and CAST calls with standard types pass through unchanged.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class CastTypeNameRewriter extends SqlShuttle {

  public static final CastTypeNameRewriter INSTANCE = new CastTypeNameRewriter();

  @Override
  public @Nullable SqlNode visit(SqlCall call) {
    SqlCall visited = (SqlCall) super.visit(call);
    if (visited == null) {
      return null;
    }
    if (visited.getKind() != SqlKind.CAST) {
      return visited;
    }
    if (visited.getOperandList().size() < 2) {
      return visited;
    }
    SqlNode typeOperand = visited.operand(1);
    if (!(typeOperand instanceof SqlDataTypeSpec dts)) {
      return visited;
    }
    SqlTypeNameSpec spec = dts.getTypeNameSpec();
    if (!(spec instanceof SqlUserDefinedTypeNameSpec)) {
      return visited;
    }
    SqlIdentifier typeName = spec.getTypeName();
    if (typeName == null || typeName.names.size() != 1) {
      return visited;
    }
    SqlTypeName replacement = mapType(typeName.getSimple());
    if (replacement == null) {
      return visited;
    }
    SqlParserPos pos = dts.getParserPosition();
    SqlDataTypeSpec newSpec = new SqlDataTypeSpec(new SqlBasicTypeNameSpec(replacement, pos), pos);
    SqlNode[] newOperands = new SqlNode[] {visited.operand(0), newSpec};
    for (int i = 2; i < visited.getOperandList().size(); i++) {
      newOperands = append(newOperands, visited.operand(i));
    }
    return visited.getOperator().createCall(visited.getParserPosition(), newOperands);
  }

  private static SqlNode[] append(SqlNode[] arr, SqlNode extra) {
    SqlNode[] out = new SqlNode[arr.length + 1];
    System.arraycopy(arr, 0, out, 0, arr.length);
    out[arr.length] = extra;
    return out;
  }

  /**
   * Maps OpenSearch-specific type names to the nearest ANSI equivalent. Returns null when the name
   * isn't one we rewrite, so the validator's original "Unknown identifier" error still fires for
   * truly unsupported types.
   */
  private static @Nullable SqlTypeName mapType(String name) {
    return switch (name.toUpperCase()) {
      case "STRING" -> SqlTypeName.VARCHAR;
      default -> null;
    };
  }
}
