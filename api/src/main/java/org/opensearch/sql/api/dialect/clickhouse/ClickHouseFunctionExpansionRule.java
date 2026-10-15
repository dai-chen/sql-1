/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.dialect.clickhouse;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;

/**
 * Lowers ClickHouse functions into standard operators the Analytics Engine already supports, so
 * they run in the normal distributed plan (pushed to shards when the planner chooses to).
 */
public class ClickHouseFunctionExpansionRule extends RelHomogeneousShuttle {

  @Override
  public RelNode visit(RelNode other) {
    RelNode visited = super.visit(other);
    return visited.accept(new Expander(visited.getCluster().getRexBuilder()));
  }

  private final class Expander extends RexShuttle {
    private final RexBuilder rex;

    Expander(RexBuilder rex) {
      this.rex = rex;
    }

    @Override
    public RexNode visitSubQuery(RexSubQuery subQuery) {
      RexSubQuery visited = (RexSubQuery) super.visitSubQuery(subQuery);
      return visited.clone(visited.rel.accept(ClickHouseFunctionExpansionRule.this));
    }

    @Override
    public RexNode visitCall(RexCall call) {
      RexCall visited = (RexCall) super.visitCall(call);
      List<RexNode> args = visited.getOperands();
      if (visited.getOperator() == ClickHouseOperators.LENGTH_UTF8) {
        // (B) alias: same semantics as CHAR_LENGTH (code points).
        return rex.makeCall(visited.getType(), SqlStdOperatorTable.CHAR_LENGTH, args);
      }
      if (visited.getOperator() == ClickHouseOperators.TO_YYYYMM) {
        // (C) new function: EXTRACT(YEAR) * 100 + EXTRACT(MONTH).
        RexNode ts = args.get(0);
        RexNode year =
            rex.makeCall(SqlStdOperatorTable.EXTRACT, rex.makeFlag(TimeUnitRange.YEAR), ts);
        RexNode month =
            rex.makeCall(SqlStdOperatorTable.EXTRACT, rex.makeFlag(TimeUnitRange.MONTH), ts);
        RexNode value =
            rex.makeCall(
                SqlStdOperatorTable.PLUS,
                rex.makeCall(
                    SqlStdOperatorTable.MULTIPLY,
                    year,
                    rex.makeExactLiteral(java.math.BigDecimal.valueOf(100))),
                month);
        return rex.makeCast(
            rex.getTypeFactory()
                .createTypeWithNullability(
                    rex.getTypeFactory().createSqlType(SqlTypeName.INTEGER),
                    visited.getType().isNullable()),
            value);
      }
      if (visited.getOperator() == ClickHouseOperators.CONCAT_NULL_PROPAGATING) {
        // (C) dialect-local override: NULL if any argument is NULL, otherwise a || b || ...
        List<RexNode> nullChecks = new ArrayList<>();
        for (RexNode arg : args) {
          nullChecks.add(rex.makeCall(SqlStdOperatorTable.IS_NULL, arg));
        }
        RexNode anyNull =
            nullChecks.size() == 1
                ? nullChecks.get(0)
                : rex.makeCall(SqlStdOperatorTable.OR, nullChecks);
        RexNode concat = args.get(0);
        for (int i = 1; i < args.size(); i++) {
          concat = rex.makeCall(SqlStdOperatorTable.CONCAT, concat, args.get(i));
        }
        return rex.makeCall(
            visited.getType(),
            SqlStdOperatorTable.CASE,
            List.of(
                anyNull,
                rex.makeNullLiteral(visited.getType()),
                rex.makeCast(visited.getType(), concat, true, false)));
      }
      return visited;
    }
  }
}
