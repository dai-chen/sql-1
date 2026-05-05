/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.spec.datetime;

import static org.opensearch.sql.api.spec.datetime.DatetimeExtension.UdtMapping.isDatetimeType;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.type.SqlTypeName;

/** Wraps the root output with CAST(datetime → VARCHAR) for PPL wire-format compatibility. */
class DatetimeOutputCastRule extends RelHomogeneousShuttle {

  static final DatetimeOutputCastRule INSTANCE = new DatetimeOutputCastRule();

  @Override
  public RelNode visit(RelNode other) {
    List<RelDataTypeField> fields = other.getRowType().getFieldList();
    if (fields.stream().noneMatch(f -> isDatetimeType(f.getType().getSqlTypeName()))) {
      return other;
    }

    RexBuilder rexBuilder = other.getCluster().getRexBuilder();
    List<RexNode> projects = new ArrayList<>(fields.size());
    List<String> names = new ArrayList<>(fields.size());

    for (RelDataTypeField field : fields) {
      RexNode ref = rexBuilder.makeInputRef(other, field.getIndex());
      if (isDatetimeType(field.getType().getSqlTypeName())) {
        projects.add(castToVarchar(rexBuilder, ref, field.getType()));
      } else {
        projects.add(ref);
      }
      names.add(field.getName());
    }
    return LogicalProject.create(other, List.of(), projects, names);
  }

  private static RexNode castToVarchar(RexBuilder rexBuilder, RexNode expr, RelDataType fieldType) {
    RelDataTypeFactory typeFactory = rexBuilder.getTypeFactory();
    RelDataType varcharType =
        typeFactory.createTypeWithNullability(
            typeFactory.createSqlType(SqlTypeName.VARCHAR), fieldType.isNullable());
    return rexBuilder.makeCast(varcharType, expr);
  }
}
