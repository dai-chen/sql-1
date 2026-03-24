/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.rewriter;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;

/**
 * Pre-compilation adapter that inserts CAST(operand AS VARCHAR) for PPL UDF operands that have
 * standard Calcite DATE/TIME/TIMESTAMP types.
 *
 * <p>PPL UDF implementors call {@code ExprValueUtils.fromObjectValue(Object, ExprType)} which casts
 * the object to String for datetime types. When the operand is a standard Calcite datetime type
 * (int/long at runtime), this cast fails. The adapter inserts explicit VARCHAR CASTs so the
 * implementor receives String values.
 *
 * <p>This is an <b>execution</b> concern, not a logical plan concern. It should be applied only
 * before compilation (e.g., in {@link org.opensearch.sql.api.compiler.UnifiedQueryCompiler}), not
 * in the planner output. This keeps the logical plan clean for downstream consumers like pushdown
 * rules and transpilers.
 */
public class UdfOperandAdapter {

  private UdfOperandAdapter() {}

  /**
   * Adapts UDF operands for compilation by inserting CAST to VARCHAR where needed.
   *
   * @param plan the RelNode tree to adapt
   * @param rexBuilder the RexBuilder for creating CAST expressions
   * @return the adapted RelNode tree ready for Enumerable compilation
   */
  public static RelNode adapt(RelNode plan, RexBuilder rexBuilder) {
    return plan.accept(new AdapterRelShuttle(rexBuilder));
  }

  private static class AdapterRelShuttle extends RelHomogeneousShuttle {
    private final RexBuilder rexBuilder;

    AdapterRelShuttle(RexBuilder rexBuilder) {
      this.rexBuilder = rexBuilder;
    }

    @Override
    public RelNode visit(RelNode other) {
      RelNode visited = super.visit(other);
      return visited.accept(new AdapterRexShuttle(rexBuilder));
    }
  }

  private static class AdapterRexShuttle extends RexShuttle {
    private final RexBuilder rexBuilder;

    AdapterRexShuttle(RexBuilder rexBuilder) {
      this.rexBuilder = rexBuilder;
    }

    @Override
    public RexNode visitCall(RexCall call) {
      call = (RexCall) super.visitCall(call);

      if (!(call.getOperator() instanceof SqlUserDefinedFunction)) {
        return call;
      }

      List<RexNode> adapted = new ArrayList<>(call.getOperands().size());
      boolean changed = false;
      for (RexNode operand : call.getOperands()) {
        SqlTypeName typeName = operand.getType().getSqlTypeName();
        if (typeName == SqlTypeName.DATE
            || typeName == SqlTypeName.TIME
            || typeName == SqlTypeName.TIMESTAMP) {
          RelDataType varcharType =
              rexBuilder
                  .getTypeFactory()
                  .createTypeWithNullability(
                      rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR),
                      operand.getType().isNullable());
          adapted.add(rexBuilder.makeCast(varcharType, operand));
          changed = true;
        } else {
          adapted.add(operand);
        }
      }
      return changed ? rexBuilder.makeCall(call.getType(), call.getOperator(), adapted) : call;
    }
  }
}
