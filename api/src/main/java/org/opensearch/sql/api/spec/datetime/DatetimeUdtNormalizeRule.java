/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.spec.datetime;

import java.util.List;
import java.util.Optional;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.api.spec.datetime.DatetimeExtension.UdtMapping;

/**
 * Temporary patch that rewrites datetime UDT return types on RexCall nodes to standard Calcite
 * types.
 *
 * <p>Not a singleton: {@link RelHomogeneousShuttle} inherits a stateful {@code stack} field from
 * {@link org.apache.calcite.rel.RelShuttleImpl}, so a fresh instance must be used per plan().
 */
class DatetimeUdtNormalizeRule extends RelHomogeneousShuttle {

  @Override
  public RelNode visit(RelNode other) {
    // For Aggregate nodes, normalize children first, then rebuild with normalized AggregateCall
    // types. Must be done before super.visit() to avoid assertion during Aggregate reconstruction.
    if (other instanceof Aggregate agg) {
      RelNode newInput = agg.getInput().accept(this);
      RelDataTypeFactory typeFactory = agg.getCluster().getTypeFactory();
      List<AggregateCall> newCalls =
          agg.getAggCallList().stream()
              .map(
                  call -> {
                    Optional<UdtMapping> mapping = UdtMapping.fromUdtType(call.getType());
                    if (mapping.isEmpty()) return call;
                    RelDataType stdType =
                        toStdType(mapping.get(), typeFactory, call.getType().isNullable());
                    return AggregateCall.create(
                        call.getAggregation(),
                        call.isDistinct(),
                        call.isApproximate(),
                        call.getArgList(),
                        call.filterArg,
                        call.getCollation(),
                        stdType,
                        call.getName());
                  })
              .toList();
      return agg.copy(
          agg.getTraitSet(),
          newInput,
          agg.getGroupSet(),
          agg.getGroupSets(),
          newCalls);
    }

    RelNode visited = super.visit(other);
    RexBuilder rexBuilder = visited.getCluster().getRexBuilder();
    RelDataTypeFactory typeFactory = rexBuilder.getTypeFactory();

    return visited.accept(
        new RexShuttle() {
          @Override
          public RexNode visitCall(RexCall call) {
            call = (RexCall) super.visitCall(call);
            Optional<UdtMapping> mapping = UdtMapping.fromUdtType(call.getType());
            if (mapping.isEmpty()) {
              return call;
            }

            // Normalize UDT return type to standard Calcite DATE/TIME/TIMESTAMP
            RelDataType stdType = toStdType(mapping.get(), typeFactory, call.getType().isNullable());
            return call.clone(stdType, call.getOperands());
          }
        });
  }

  private static RelDataType toStdType(
      UdtMapping mapping, RelDataTypeFactory typeFactory, boolean nullable) {
    SqlTypeName stdTypeName = mapping.getStdType();
    RelDataType baseType =
        stdTypeName.allowsPrec()
            ? typeFactory.createSqlType(
                stdTypeName, typeFactory.getTypeSystem().getMaxPrecision(stdTypeName))
            : typeFactory.createSqlType(stdTypeName);
    return typeFactory.createTypeWithNullability(baseType, nullable);
  }
}
