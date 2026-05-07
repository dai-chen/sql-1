/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.spec.datetime;

import static org.opensearch.sql.api.spec.datetime.DatetimeExtension.UdtMapping.isDatetimeType;

import java.util.ArrayList;
import java.util.List;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlUserDefinedFunction;

/**
 * Adapts datetime UDF calls for Enumerable compilation. PPL UDF implementors expect String
 * input/output, but after normalization the plan uses standard DATE/TIME/TIMESTAMP types
 * (int/long). This rule inserts CASTs to bridge the mismatch:
 *
 * <pre>
 *   Before: LAST_DAY($2:DATE) : DATE
 *   After:  CAST(LAST_DAY(CAST($2 AS VARCHAR)):VARCHAR AS DATE)
 * </pre>
 */
@NoArgsConstructor(access = AccessLevel.PACKAGE)
class DatetimeUdfCompilationAdapterRule extends RelHomogeneousShuttle {

  static final DatetimeUdfCompilationAdapterRule INSTANCE = new DatetimeUdfCompilationAdapterRule();

  @Override
  public RelNode visit(RelNode other) {
    RelNode visited = super.visit(other);
    RexBuilder rexBuilder = visited.getCluster().getRexBuilder();
    RelDataTypeFactory typeFactory = rexBuilder.getTypeFactory();
    return visited.accept(
        new RexShuttle() {
          @Override
          public RexNode visitCall(RexCall call) {
            call = (RexCall) super.visitCall(call);
            if (!(call.getOperator() instanceof SqlUserDefinedFunction)) {
              return call;
            }

            // Adapt operands: CAST(datetime_operand AS VARCHAR) for UDF implementor
            List<RexNode> adapted = new ArrayList<>(call.getOperands().size());
            boolean operandsChanged = false;
            for (RexNode operand : call.getOperands()) {
              if (isDatetimeType(operand.getType().getSqlTypeName())) {
                RelDataType varcharType =
                    typeFactory.createTypeWithNullability(
                        typeFactory.createSqlType(SqlTypeName.VARCHAR),
                        operand.getType().isNullable());
                adapted.add(rexBuilder.makeCast(varcharType, operand));
                operandsChanged = true;
              } else {
                adapted.add(operand);
              }
            }

            // Adapt result: if return type is datetime, wrap call with VARCHAR return + CAST back
            if (isDatetimeType(call.getType().getSqlTypeName())) {
              RelDataType declaredType = call.getType();
              RelDataType varcharType =
                  typeFactory.createTypeWithNullability(
                      typeFactory.createSqlType(SqlTypeName.VARCHAR), declaredType.isNullable());
              RexCall varcharCall =
                  call.clone(varcharType, operandsChanged ? adapted : call.getOperands());
              return rexBuilder.makeCast(declaredType, varcharCall);
            }

            return operandsChanged ? call.clone(call.getType(), adapted) : call;
          }
        });
  }
}
