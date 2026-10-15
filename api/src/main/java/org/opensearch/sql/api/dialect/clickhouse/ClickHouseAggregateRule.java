/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.dialect.clickhouse;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSelectKeyword;
import org.apache.calcite.sql.SqlUnresolvedFunction;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlShuttle;

/**
 * Generic adapter for ClickHouse aggregate names and combinators over the base aggregates listed in
 * the inventory: {@code <base>}, {@code <base>If}, {@code <base>Distinct}, {@code <base>OrNull},
 * {@code <base>OrDefault}. {@code -If} is lowered to a CASE inside the aggregate (not FILTER, which
 * the Analytics Engine does not execute correctly today).
 */
public class ClickHouseAggregateRule extends SqlShuttle {

  /** ClickHouse base aggregate name → standard Calcite aggregate (null = special composition). */
  private static final Map<String, SqlOperator> BASES =
      Map.ofEntries(
          Map.entry("count", SqlStdOperatorTable.COUNT),
          Map.entry("sum", SqlStdOperatorTable.SUM),
          Map.entry("avg", SqlStdOperatorTable.AVG),
          Map.entry("min", SqlStdOperatorTable.MIN),
          Map.entry("max", SqlStdOperatorTable.MAX),
          Map.entry("stddevPop", SqlStdOperatorTable.STDDEV_POP),
          Map.entry("stddevSamp", SqlStdOperatorTable.STDDEV_SAMP),
          Map.entry("varPop", SqlStdOperatorTable.VAR_POP),
          Map.entry("varSamp", SqlStdOperatorTable.VAR_SAMP),
          Map.entry("uniqExact", SqlStdOperatorTable.COUNT),
          Map.entry("argMin", SqlStdOperatorTable.ARG_MIN),
          Map.entry("argMax", SqlStdOperatorTable.ARG_MAX),
          Map.entry("covarPop", SqlStdOperatorTable.COVAR_POP),
          Map.entry("covarSamp", SqlStdOperatorTable.COVAR_SAMP),
          Map.entry("corr", SqlStdOperatorTable.COVAR_POP));

  private static final List<String> SUFFIXES = List.of("OrDefault", "OrNull", "Distinct", "If");

  @Override
  public SqlNode visit(SqlCall call) {
    SqlCall visited = (SqlCall) super.visit(call);
    if (visited == null || !(visited.getOperator() instanceof SqlUnresolvedFunction fn)) {
      return visited;
    }
    String name = fn.getName();
    String suffix = "";
    String base = name;
    for (String s : SUFFIXES) {
      if (name.endsWith(s) && BASES.containsKey(name.substring(0, name.length() - s.length()))) {
        suffix = s;
        base = name.substring(0, name.length() - s.length());
        break;
      }
    }
    if (!BASES.containsKey(base) || (suffix.isEmpty() && isStandardSpelling(base))) {
      return visited;
    }
    SqlParserPos pos = visited.getParserPosition();
    List<SqlNode> args = new ArrayList<>(visited.getOperandList());
    if (suffix.equals("If")) {
      SqlNode cond = args.remove(args.size() - 1);
      if (args.isEmpty()) { // countIf(cond)
        args.add(caseWhen(cond, SqlLiteral.createExactNumeric("1", pos), pos));
      } else {
        args.replaceAll(a -> caseWhen(cond, a, pos));
      }
    }
    boolean distinct = suffix.equals("Distinct") || base.equals("uniqExact");
    SqlNode agg = aggregate(base, args, distinct, pos);
    if (suffix.equals("OrNull") || suffix.equals("OrDefault")) {
      SqlNode empty =
          SqlStdOperatorTable.EQUALS.createCall(
              pos,
              SqlStdOperatorTable.COUNT.createCall(pos, SqlIdentifier.star(pos)),
              SqlLiteral.createExactNumeric("0", pos));
      SqlNode whenEmpty =
          suffix.equals("OrNull")
              ? SqlLiteral.createNull(pos)
              : SqlLiteral.createExactNumeric("0", pos);
      agg = new SqlCase(pos, null, SqlNodeList.of(empty), SqlNodeList.of(whenEmpty), agg);
    }
    return agg;
  }

  /** Lower-case standard names (count, sum, ...) are already standard SQL aggregates. */
  private static boolean isStandardSpelling(String base) {
    return List.of("count", "sum", "avg", "min", "max").contains(base);
  }

  private static SqlNode aggregate(
      String base, List<SqlNode> args, boolean distinct, SqlParserPos pos) {
    SqlLiteral quantifier = distinct ? SqlSelectKeyword.DISTINCT.symbol(pos) : null;
    if (base.equals("corr")) {
      // corr(x, y) = covar_pop(x, y) / (stddev_pop(x) * stddev_pop(y)) over pairs with both
      // non-null.
      SqlNode x = args.get(0);
      SqlNode y = args.get(1);
      SqlNode both =
          SqlStdOperatorTable.AND.createCall(
              pos,
              SqlStdOperatorTable.IS_NOT_NULL.createCall(pos, x),
              SqlStdOperatorTable.IS_NOT_NULL.createCall(pos, y));
      SqlNode px = caseWhen(both, x, pos);
      SqlNode py = caseWhen(both, y, pos);
      return SqlStdOperatorTable.DIVIDE.createCall(
          pos,
          SqlStdOperatorTable.COVAR_POP.createCall(quantifier, pos, px, py),
          SqlStdOperatorTable.MULTIPLY.createCall(
              pos,
              SqlStdOperatorTable.STDDEV_POP.createCall(quantifier, pos, px),
              SqlStdOperatorTable.STDDEV_POP.createCall(quantifier, pos, py)));
    }
    if (args.isEmpty() && base.equals("count")) {
      args = List.of(SqlIdentifier.star(pos));
    }
    return BASES.get(base).createCall(quantifier, pos, args.toArray(new SqlNode[0]));
  }

  private static SqlNode caseWhen(SqlNode cond, SqlNode value, SqlParserPos pos) {
    return new SqlCase(
        pos, null, SqlNodeList.of(cond), SqlNodeList.of(value), SqlLiteral.createNull(pos));
  }

  @SuppressWarnings("unused")
  private static boolean isCall(SqlNode n) {
    return n instanceof SqlBasicCall;
  }
}
