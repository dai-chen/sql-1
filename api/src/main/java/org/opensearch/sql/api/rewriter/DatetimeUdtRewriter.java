/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.rewriter;

import java.util.ArrayList;
import java.util.List;
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
 * <p>Category A functions (NOW, CURRENT_DATE, CURRENT_TIME, etc.) are replaced with their
 * SqlStdOperatorTable equivalents.
 *
 * <p>Category B functions (PPL-specific datetime UDFs) keep the original UDF call (which returns
 * String via UDT) and wrap it with a CAST to the standard Calcite type. Calcite's built-in CAST
 * from VARCHAR to DATE/TIME/TIMESTAMP calls DateTimeUtils conversion methods internally.
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

  /** Visits every RelNode and applies the RexShuttle to rewrite expressions. */
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

  /** Visits every RexCall bottom-up and rewrites datetime UDT calls to standard Calcite types. */
  private static class UdtRexShuttle extends RexShuttle {
    private final RexBuilder rexBuilder;

    UdtRexShuttle(RexBuilder rexBuilder) {
      this.rexBuilder = rexBuilder;
    }

    @Override
    public RexNode visitCall(RexCall call) {
      // Bottom-up: rewrite operands first
      call = (RexCall) super.visitCall(call);

      // For ANY UDF with standard datetime operands, adapt them to VARCHAR
      // so the original implementor (which expects String) works correctly.
      if (call.getOperator() instanceof SqlUserDefinedFunction) {
        List<RexNode> adaptedOperands = adaptOperands(call.getOperands());
        if (!adaptedOperands.equals(call.getOperands())) {
          call = (RexCall) rexBuilder.makeCall(call.getType(), call.getOperator(), adaptedOperands);
        }
      }

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

      // Category B: keep original UDF call, wrap with CAST to standard type.
      // The original UDF returns UDT (VARCHAR-backed String). Calcite's built-in CAST
      // from VARCHAR to DATE/TIME/TIMESTAMP calls DateTimeUtils.dateStringToUnixDate etc.
      if (call.getOperator() instanceof SqlUserDefinedFunction) {
        return rexBuilder.makeCast(stdReturnType, call);
      }

      return call;
    }

    /**
     * For operands that have standard DATE/TIME/TIMESTAMP types (from rewritten inner calls),
     * insert a CAST to VARCHAR so the original implementor receives String values.
     */
    private List<RexNode> adaptOperands(List<RexNode> operands) {
      List<RexNode> adapted = new ArrayList<>(operands.size());
      for (RexNode operand : operands) {
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
        } else {
          adapted.add(operand);
        }
      }
      return adapted;
    }
  }

  /** Maps a datetime ExprUDT to the corresponding standard Calcite SqlTypeName. */
  private static SqlTypeName mapUdtToSqlType(ExprUDT udt) {
    return switch (udt) {
      case EXPR_DATE -> SqlTypeName.DATE;
      case EXPR_TIME -> SqlTypeName.TIME;
      case EXPR_TIMESTAMP -> SqlTypeName.TIMESTAMP;
      default -> null;
    };
  }
}
