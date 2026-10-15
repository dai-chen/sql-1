/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.dialect.clickhouse;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.tools.RelBuilder;

/**
 * LEFT SEMI / LEFT ANTI JOIN. Calcite 1.41 parses and validates them (BABEL conformance) but
 * SqlToRel cannot convert them, so: (1) post-parse, the join becomes a LEFT JOIN whose ON condition
 * carries a marker; (2) post-analysis, that join becomes a SEMI/ANTI join (executed natively by the
 * Analytics Engine), projected back to the original row type with NULL right-side columns.
 */
public final class ClickHouseSemiJoinRule {

  private ClickHouseSemiJoinRule() {}

  /** Parse-tree half. */
  public static class Rewrite extends SqlShuttle {
    @Override
    public SqlNode visit(SqlCall call) {
      SqlNode visited = super.visit(call);
      if (visited instanceof SqlJoin join
          && (join.getJoinType() == JoinType.LEFT_SEMI_JOIN
              || join.getJoinType() == JoinType.LEFT_ANTI_JOIN)) {
        SqlParserPos pos = join.getParserPosition();
        SqlNode marker =
            (join.getJoinType() == JoinType.LEFT_SEMI_JOIN
                    ? ClickHouseOperators.SEMI_JOIN_MARKER
                    : ClickHouseOperators.ANTI_JOIN_MARKER)
                .createCall(pos);
        SqlNode cond =
            join.getCondition() == null
                ? marker
                : SqlStdOperatorTable.AND.createCall(pos, join.getCondition(), marker);
        join.setOperand(2, JoinType.LEFT.symbol(pos));
        join.setOperand(5, cond);
      }
      return visited;
    }
  }

  /** Logical-plan half. */
  public static class Convert extends RelHomogeneousShuttle {
    @Override
    public RelNode visit(RelNode other) {
      RelNode visited = super.visit(other);
      if (!(visited instanceof LogicalJoin join) || join.getJoinType() != JoinRelType.LEFT) {
        return visited;
      }
      List<RexNode> conjuncts = new ArrayList<>(RelOptUtilBridge.conjunctions(join.getCondition()));
      JoinRelType type = null;
      for (RexNode c : List.copyOf(conjuncts)) {
        if (c instanceof RexCall call) {
          if (call.getOperator() == ClickHouseOperators.SEMI_JOIN_MARKER) {
            type = JoinRelType.SEMI;
            conjuncts.remove(c);
          } else if (call.getOperator() == ClickHouseOperators.ANTI_JOIN_MARKER) {
            type = JoinRelType.ANTI;
            conjuncts.remove(c);
          }
        }
      }
      if (type == null) {
        return visited;
      }
      RelBuilder b = RelFactories.LOGICAL_BUILDER.create(join.getCluster(), null);
      b.push(join.getLeft()).push(join.getRight());
      b.join(type, RexUtil.composeConjunction(b.getRexBuilder(), conjuncts));
      // Restore the LEFT JOIN row type: left columns, then NULLs for the right side.
      List<RexNode> projects = new ArrayList<>(b.fields());
      for (RelDataTypeField f : join.getRight().getRowType().getFieldList()) {
        projects.add(
            b.getRexBuilder()
                .makeNullLiteral(b.getTypeFactory().createTypeWithNullability(f.getType(), true)));
      }
      return b.project(projects, join.getRowType().getFieldNames(), true).build();
    }
  }

  /** Small indirection to keep Calcite's RelOptUtil import local. */
  private static final class RelOptUtilBridge {
    static List<RexNode> conjunctions(RexNode cond) {
      return org.apache.calcite.plan.RelOptUtil.conjunctions(cond);
    }
  }
}
