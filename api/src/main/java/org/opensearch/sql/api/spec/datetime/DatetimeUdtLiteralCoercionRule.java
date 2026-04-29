/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.spec.datetime;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.api.spec.LanguageSpec.PostAnalysisRule;
import org.opensearch.sql.api.spec.datetime.DatetimeUdtExtension.UdtMapping;

/**
 * Coerces VARCHAR literals and references that appear alongside standard datetime operands inside
 * non-UDF operators (comparisons, IN, BETWEEN/SEARCH, COALESCE) by wrapping the VARCHAR side in
 * {@code CAST(... AS DATE|TIME|TIMESTAMP)}. This closes the gap left by {@link
 * DatetimeUdtNormalizeRule}, which only rewrites operators backed by {@code
 * ImplementableUDFunction}.
 *
 * <p>Only operand sub-trees inside {@code RexCall} nodes are modified; no {@code RelNode} row type
 * is changed and no {@code RexInputRef} slot identity is altered. This keeps the rewrite safe
 * against Calcite's cached {@code RexInputRef} types (unlike an in-place ref rewrite that would
 * invalidate parent nodes).
 */
public class DatetimeUdtLiteralCoercionRule implements PostAnalysisRule {

  @Override
  public RelNode apply(RelNode plan) {
    RexBuilder rexBuilder = plan.getCluster().getRexBuilder();
    return plan.accept(
        new RelHomogeneousShuttle() {
          @Override
          public RelNode visit(RelNode other) {
            return super.visit(other).accept(new LiteralCoercionShuttle(rexBuilder));
          }
        });
  }

  private static class LiteralCoercionShuttle extends RexShuttle {

    private final RexBuilder rexBuilder;

    LiteralCoercionShuttle(RexBuilder rexBuilder) {
      this.rexBuilder = rexBuilder;
    }

    @Override
    public RexNode visitCall(RexCall call) {
      RexCall visited = (RexCall) super.visitCall(call);
      if (!isTargetOperator(visited)) {
        return visited;
      }
      Optional<UdtMapping> datetime = findDatetimeOperand(visited);
      if (datetime.isEmpty()) {
        return visited;
      }
      List<RexNode> coerced = coerceVarcharOperands(visited.getOperands(), datetime.get());
      if (coerced.equals(visited.getOperands())) {
        return visited;
      }
      return visited.clone(visited.getType(), coerced);
    }

    /** Operators where we perform VARCHAR ↔ datetime operand coercion. */
    private static boolean isTargetOperator(RexCall call) {
      SqlKind kind = call.getKind();
      return kind == SqlKind.EQUALS
          || kind == SqlKind.NOT_EQUALS
          || kind == SqlKind.GREATER_THAN
          || kind == SqlKind.GREATER_THAN_OR_EQUAL
          || kind == SqlKind.LESS_THAN
          || kind == SqlKind.LESS_THAN_OR_EQUAL
          || kind == SqlKind.IN
          || kind == SqlKind.SEARCH
          || kind == SqlKind.BETWEEN
          || kind == SqlKind.COALESCE;
    }

    /** Returns the first operand whose type is a standard Calcite datetime. */
    private static Optional<UdtMapping> findDatetimeOperand(RexCall call) {
      for (RexNode op : call.getOperands()) {
        Optional<UdtMapping> m = UdtMapping.fromStdType(op.getType());
        if (m.isPresent()) {
          return m;
        }
      }
      return Optional.empty();
    }

    /** Wraps every VARCHAR/CHAR operand in {@code CAST(... AS <datetime>)}. */
    private List<RexNode> coerceVarcharOperands(List<RexNode> operands, UdtMapping datetime) {
      List<RexNode> coerced = new ArrayList<>(operands.size());
      boolean changed = false;
      for (RexNode op : operands) {
        if (isCharType(op.getType())) {
          RelDataType target = datetime.toStdType(rexBuilder, op.getType().isNullable());
          coerced.add(rexBuilder.makeCast(target, op));
          changed = true;
        } else {
          coerced.add(op);
        }
      }
      return changed ? coerced : operands;
    }

    private static boolean isCharType(RelDataType type) {
      SqlTypeName name = type.getSqlTypeName();
      return name == SqlTypeName.VARCHAR || name == SqlTypeName.CHAR;
    }
  }
}
