/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.dialect.clickhouse;

import java.util.EnumSet;
import java.util.Locale;
import java.util.Set;
import org.apache.calcite.rel.RelHomogeneousShuttle;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Aggregate;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.TableFunctionScan;
import org.apache.calcite.rel.core.TableModify;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;

/**
 * Validates the final logical plan of a ClickHouse dialect query: only OpenSearch index scans, no
 * table functions or modifications, and only operators on an explicit approved list. Index-level
 * authorization is still enforced by the Analytics Engine's transport action (SecurityFilter) for
 * the indices extracted from this plan.
 */
public class ClickHousePlanValidator extends RelHomogeneousShuttle {

  private static final Set<SqlKind> APPROVED_KINDS =
      EnumSet.of(
          // comparison, logic, null tests, search
          SqlKind.EQUALS,
          SqlKind.NOT_EQUALS,
          SqlKind.LESS_THAN,
          SqlKind.LESS_THAN_OR_EQUAL,
          SqlKind.GREATER_THAN,
          SqlKind.GREATER_THAN_OR_EQUAL,
          SqlKind.IS_DISTINCT_FROM,
          SqlKind.IS_NOT_DISTINCT_FROM,
          SqlKind.AND,
          SqlKind.OR,
          SqlKind.NOT,
          SqlKind.IS_NULL,
          SqlKind.IS_NOT_NULL,
          SqlKind.IS_TRUE,
          SqlKind.IS_FALSE,
          SqlKind.IS_NOT_TRUE,
          SqlKind.IS_NOT_FALSE,
          SqlKind.SEARCH,
          SqlKind.IN,
          SqlKind.NOT_IN,
          SqlKind.BETWEEN,
          SqlKind.LIKE,
          SqlKind.EXISTS,
          SqlKind.SCALAR_QUERY,
          SqlKind.SOME,
          SqlKind.ALL,
          // arithmetic, conditional, conversion
          SqlKind.PLUS,
          SqlKind.MINUS,
          SqlKind.TIMES,
          SqlKind.DIVIDE,
          SqlKind.MOD,
          SqlKind.MINUS_PREFIX,
          SqlKind.PLUS_PREFIX,
          SqlKind.CASE,
          SqlKind.CAST,
          SqlKind.SAFE_CAST,
          SqlKind.COALESCE,
          SqlKind.NULLIF,
          SqlKind.GREATEST,
          SqlKind.LEAST,
          // datetime, string, structure
          SqlKind.EXTRACT,
          SqlKind.FLOOR,
          SqlKind.CEIL,
          SqlKind.TRIM,
          SqlKind.POSITION,
          SqlKind.ITEM,
          SqlKind.ROW,
          SqlKind.ARRAY_VALUE_CONSTRUCTOR,
          SqlKind.REINTERPRET,
          // aggregates
          SqlKind.COUNT,
          SqlKind.SUM,
          SqlKind.SUM0,
          SqlKind.AVG,
          SqlKind.MIN,
          SqlKind.MAX,
          SqlKind.STDDEV_POP,
          SqlKind.STDDEV_SAMP,
          SqlKind.VAR_POP,
          SqlKind.VAR_SAMP,
          SqlKind.COVAR_POP,
          SqlKind.COVAR_SAMP,
          SqlKind.ANY_VALUE,
          SqlKind.SINGLE_VALUE,
          SqlKind.GROUPING,
          SqlKind.GROUP_ID,
          SqlKind.ARG_MAX,
          SqlKind.ARG_MIN,
          // window functions
          SqlKind.ROW_NUMBER,
          SqlKind.RANK,
          SqlKind.DENSE_RANK,
          SqlKind.LAG,
          SqlKind.LEAD,
          SqlKind.FIRST_VALUE,
          SqlKind.LAST_VALUE,
          SqlKind.NTH_VALUE,
          SqlKind.NTILE,
          SqlKind.PERCENT_RANK,
          SqlKind.CUME_DIST);

  /** Approved {@code OTHER_FUNCTION}/{@code OTHER} operators by (upper-case) name. */
  private static final Set<String> APPROVED_NAMES =
      Set.of(
          "||",
          "UPPER",
          "LOWER",
          "SUBSTRING",
          "CHAR_LENGTH",
          "CHARACTER_LENGTH",
          "OVERLAY",
          "ABS",
          "POWER",
          "SQRT",
          "LN",
          "LOG10",
          "EXP",
          "ROUND",
          "TRUNCATE",
          "SIGN",
          "MOD",
          "CURRENT_TIMESTAMP",
          "CURRENT_DATE",
          "LOCALTIMESTAMP",
          "INITCAP",
          "REPLACE",
          "ASCII",
          // pure JSON functions
          "JSON_VALUE",
          "JSON_QUERY",
          "JSON_EXISTS",
          "JSON_ARRAY",
          "JSON_OBJECT",
          "JSON_SET",
          "JSON_TYPE",
          "JSON_DEPTH",
          "JSON_KEYS",
          "JSON_LENGTH",
          "JSON_REMOVE",
          "JSON_INSERT",
          "JSON_REPLACE",
          "JSON_PRETTY");

  /** Operators reachable through the ClickHouse alias table are approved by construction. */
  private static final Set<String> ALIAS_TARGETS =
      ClickHouseFunctionAliasRule.ALIASES.values().stream()
          .map(ClickHouseFunctionAliasRule.Alias::target)
          .collect(java.util.stream.Collectors.toUnmodifiableSet());

  @Override
  public RelNode visit(RelNode other) {
    if (other instanceof TableScan scan
        && scan.getTable().unwrap(org.apache.calcite.schema.TransientTable.class) != null) {
      // Working table of a recursive CTE (RepeatUnion), not an index scan.
    } else if (other instanceof TableScan scan) {
      var names = scan.getTable().getQualifiedName();
      if (names.size() != 2 || !ClickHouseQueryGuard.CATALOG.equals(names.get(0))) {
        throw ClickHouseQueryGuard.reject("scan of non-OpenSearch table " + names);
      }
      ClickHouseQueryGuard.checkTableName(names);
    } else if (other instanceof TableFunctionScan || other instanceof TableModify) {
      throw ClickHouseQueryGuard.reject("relational operator " + other.getRelTypeName());
    }
    if (other instanceof Aggregate agg) {
      for (AggregateCall call : agg.getAggCallList()) {
        check(call.getAggregation());
      }
    }
    other.accept(
        new RexShuttle() {
          @Override
          public RexNode visitCall(RexCall call) {
            check(call.getOperator());
            return super.visitCall(call);
          }

          @Override
          public RexNode visitSubQuery(RexSubQuery subQuery) {
            subQuery.rel.accept(ClickHousePlanValidator.this);
            return super.visitSubQuery(subQuery);
          }
        });
    return super.visit(other);
  }

  private static void check(SqlOperator op) {
    if (APPROVED_KINDS.contains(op.getKind())
        || APPROVED_NAMES.contains(op.getName().toUpperCase(Locale.ROOT))
        || ALIAS_TARGETS.contains(op.getName())) {
      return;
    }
    throw ClickHouseQueryGuard.reject("function [" + op.getName() + "] is not approved");
  }
}
