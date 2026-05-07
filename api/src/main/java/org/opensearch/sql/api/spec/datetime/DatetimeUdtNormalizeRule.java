/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.spec.datetime;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.api.spec.datetime.DatetimeExtension.UdtMapping;

/**
 * Temporary patch that rewrites datetime UDT return types on RexCall nodes to standard Calcite
 * types.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
class DatetimeUdtNormalizeRule extends RelHomogeneousShuttle {

  static final DatetimeUdtNormalizeRule INSTANCE = new DatetimeUdtNormalizeRule();

  @Override
  public RelNode visit(RelNode other) {
    // Visit children first
    List<RelNode> newInputs = new ArrayList<>();
    boolean childChanged = false;
    for (RelNode input : other.getInputs()) {
      RelNode newInput = input.accept(this);
      newInputs.add(newInput);
      if (newInput != input) {
        childChanged = true;
      }
    }

    // Rebuild current node if children changed
    RelNode current = other;
    if (childChanged) {
      if (current instanceof LogicalAggregate agg) {
        // Aggregate needs AggregateCall types rebuilt
        RelNode newInput = newInputs.get(0);
        List<AggregateCall> newAggCalls =
            agg.getAggCallList().stream()
                .map(
                    call ->
                        AggregateCall.create(
                            call.getAggregation(),
                            call.isDistinct(),
                            call.isApproximate(),
                            call.ignoreNulls(),
                            call.rexList,
                            call.getArgList(),
                            call.filterArg,
                            call.distinctKeys,
                            call.collation,
                            agg.getGroupCount(),
                            newInput,
                            null,
                            call.getName()))
                .toList();
        current =
            agg.copy(
                agg.getTraitSet(), newInput, agg.getGroupSet(), agg.getGroupSets(), newAggCalls);
      } else if (current instanceof LogicalProject proj) {
        // Project needs RexInputRef types refreshed from new child
        RelNode newInput = newInputs.get(0);
        RexBuilder rexBuilder = proj.getCluster().getRexBuilder();
        List<RexNode> newProjects =
            proj.getProjects().stream()
                .map(
                    expr ->
                        expr.accept(
                            new RexShuttle() {
                              @Override
                              public RexNode visitInputRef(RexInputRef ref) {
                                RelDataType newType =
                                    newInput
                                        .getRowType()
                                        .getFieldList()
                                        .get(ref.getIndex())
                                        .getType();
                                if (!newType.equals(ref.getType())) {
                                  return rexBuilder.makeInputRef(newType, ref.getIndex());
                                }
                                return ref;
                              }
                            }))
                .toList();
        current =
            LogicalProject.create(
                newInput, proj.getHints(), newProjects, proj.getRowType().getFieldNames());
      } else {
        current = current.copy(current.getTraitSet(), newInputs);
      }
    }

    // Apply RexShuttle to normalize UDT types in this node's expressions
    RexBuilder rexBuilder = current.getCluster().getRexBuilder();
    RelDataTypeFactory typeFactory = rexBuilder.getTypeFactory();
    return current.accept(
        new RexShuttle() {
          @Override
          public RexNode visitCall(RexCall call) {
            call = (RexCall) super.visitCall(call);
            Optional<UdtMapping> mapping = UdtMapping.fromUdtType(call.getType());
            if (mapping.isEmpty()) {
              return call;
            }

            UdtMapping m = mapping.get();
            SqlTypeName stdTypeName = m.getStdType();
            RelDataType baseType =
                stdTypeName.allowsPrec()
                    ? typeFactory.createSqlType(
                        stdTypeName, typeFactory.getTypeSystem().getMaxPrecision(stdTypeName))
                    : typeFactory.createSqlType(stdTypeName);
            RelDataType stdType =
                typeFactory.createTypeWithNullability(baseType, call.getType().isNullable());
            return call.clone(stdType, call.getOperands());
          }
        });
  }
}
