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
import org.apache.calcite.rel.core.SetOp;
import org.apache.calcite.rel.logical.LogicalIntersect;
import org.apache.calcite.rel.logical.LogicalMinus;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.tools.RelBuilder;

/**
 * ClickHouse INTERSECT / EXCEPT (and MINUS) semantics expressed with operators the Analytics Engine
 * already executes: {@code A INTERSECT ALL B} keeps every row of A whose value occurs in B (a semi
 * join, duplicates of A preserved); {@code EXCEPT ALL} keeps rows of A whose value does not occur
 * in B (an anti join). {@code DISTINCT} additionally de-duplicates. NULLs compare equal, as in
 * ClickHouse set operations.
 */
public class ClickHouseSetOpRule extends RelHomogeneousShuttle {

  @Override
  public RelNode visit(RelNode other) {
    RelNode visited = super.visit(other);
    if (visited instanceof LogicalIntersect || visited instanceof LogicalMinus) {
      SetOp setOp = (SetOp) visited;
      JoinRelType type = visited instanceof LogicalIntersect ? JoinRelType.SEMI : JoinRelType.ANTI;
      RelBuilder b = RelFactories.LOGICAL_BUILDER.create(visited.getCluster(), null);
      RelNode acc = setOp.getInputs().get(0);
      for (int i = 1; i < setOp.getInputs().size(); i++) {
        RelNode right = setOp.getInputs().get(i);
        b.push(acc).push(right);
        b.join(type, nullSafeEquals(b, acc.getRowType().getFieldCount()));
        acc = b.build();
      }
      if (!setOp.all) {
        acc = b.push(acc).distinct().build();
      }
      return acc;
    }
    return visited;
  }

  /** AND over columns of (l = r OR (l IS NULL AND r IS NULL)). */
  private static RexNode nullSafeEquals(RelBuilder b, int columns) {
    RexBuilder rex = b.getRexBuilder();
    List<RexNode> conds = new ArrayList<>();
    for (int c = 0; c < columns; c++) {
      RexNode l = b.field(2, 0, c);
      RexNode r = b.field(2, 1, c);
      conds.add(
          rex.makeCall(
              SqlStdOperatorTable.OR,
              rex.makeCall(SqlStdOperatorTable.EQUALS, l, r),
              rex.makeCall(
                  SqlStdOperatorTable.AND,
                  rex.makeCall(SqlStdOperatorTable.IS_NULL, l),
                  rex.makeCall(SqlStdOperatorTable.IS_NULL, r))));
    }
    return conds.size() == 1 ? conds.get(0) : rex.makeCall(SqlStdOperatorTable.AND, conds);
  }
}
