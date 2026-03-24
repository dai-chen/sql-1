/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.rewriter;

import java.util.Map;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;
import org.opensearch.sql.calcite.type.AbstractExprRelDataType;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.ExprUDT;

/**
 * Rewrites a RelNode tree to replace datetime UDT types (EXPR_DATE, EXPR_TIME, EXPR_TIMESTAMP) with
 * standard Calcite DATE/TIME/TIMESTAMP types.
 *
 * <p>This is a <b>logical</b> rewrite — it only changes types, not execution semantics. The
 * resulting RelNode is clean for downstream consumers (pushdown rules, transpilers, etc.).
 *
 * <p>Category A functions (NOW, CURRENT_DATE, CURRENT_TIME, etc.) are replaced with their
 * SqlStdOperatorTable equivalents. Category B functions (PPL-specific datetime UDFs) keep the
 * original UDF call and wrap it with a CAST to the standard Calcite type.
 *
 * <p>Operand adaptation (CAST datetime → VARCHAR for PPL UDF implementors) is NOT done here — that
 * is an execution concern handled by {@link UdfOperandAdapter} in the compilation phase.
 */
public class DatetimeUdtRewriter {

  private static final Map<String, SqlOperator> CATEGORY_A =
      Map.of(
          "NOW", SqlStdOperatorTable.CURRENT_TIMESTAMP,
          "CURRENT_TIMESTAMP", SqlStdOperatorTable.CURRENT_TIMESTAMP,
          "CURRENT_DATE", SqlStdOperatorTable.CURRENT_DATE,
          "CURRENT_TIME", SqlStdOperatorTable.CURRENT_TIME,
          "LOCALTIMESTAMP", SqlStdOperatorTable.LOCALTIMESTAMP,
          "LOCALTIME", SqlStdOperatorTable.LOCALTIME,
          "LAST_DAY", SqlStdOperatorTable.LAST_DAY);

  private DatetimeUdtRewriter() {}

  /**
   * Rewrites a RelNode to replace datetime UDT types with standard Calcite types.
   *
   * @param plan the RelNode tree to rewrite
   * @param rexBuilder the RexBuilder for creating new expressions
   * @return the rewritten RelNode tree with no datetime UDTs
   */
  public static RelNode rewrite(RelNode plan, RexBuilder rexBuilder) {
    return plan.accept(new UdtRelShuttle(rexBuilder));
  }

  private static class UdtRelShuttle extends RelHomogeneousShuttle {
    private final RexBuilder rexBuilder;

    UdtRelShuttle(RexBuilder rexBuilder) {
      this.rexBuilder = rexBuilder;
    }

    @Override
    public RelNode visit(RelNode other) {
      RelNode visited = super.visit(other);
      return visited.accept(new UdtRexShuttle(rexBuilder));
    }
  }

  private static class UdtRexShuttle extends RexShuttle {
    private final RexBuilder rexBuilder;

    UdtRexShuttle(RexBuilder rexBuilder) {
      this.rexBuilder = rexBuilder;
    }

    @Override
    public RexNode visitCall(RexCall call) {
      call = (RexCall) super.visitCall(call);

      if (!(call.getType() instanceof AbstractExprRelDataType<?> udtType)) {
        return call;
      }
      ExprUDT udt = udtType.getUdt();
      SqlTypeName targetType = mapUdtToSqlType(udt);
      if (targetType == null) {
        return call;
      }

      RelDataType stdReturnType =
          rexBuilder
              .getTypeFactory()
              .createTypeWithNullability(
                  rexBuilder.getTypeFactory().createSqlType(targetType),
                  call.getType().isNullable());

      // Category A: replace with SqlStdOperatorTable equivalent
      SqlOperator stdOp = CATEGORY_A.get(call.getOperator().getName());
      if (stdOp != null) {
        return rexBuilder.makeCall(stdReturnType, stdOp, call.getOperands());
      }

      // Category B: keep original UDF call, wrap with CAST to standard type
      if (call.getOperator() instanceof SqlUserDefinedFunction) {
        return rexBuilder.makeCast(stdReturnType, call);
      }

      return call;
    }
  }

  /**
   * Maps datetime UDTs to standard Calcite types. Only datetime UDTs are rewritten because they
   * have standard Calcite equivalents with different runtime representations (int/long vs String).
   * EXPR_BINARY and EXPR_IP have no standard Calcite equivalent and are String-compatible as-is.
   */
  private static SqlTypeName mapUdtToSqlType(ExprUDT udt) {
    return switch (udt) {
      case EXPR_DATE -> SqlTypeName.DATE;
      case EXPR_TIME -> SqlTypeName.TIME;
      case EXPR_TIMESTAMP -> SqlTypeName.TIMESTAMP;
      default -> null;
    };
  }
}
