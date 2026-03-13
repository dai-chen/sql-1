/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlBasicFunction;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.JoinConditionType;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlNumericLiteral;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSelectKeyword;
import org.apache.calcite.sql.SqlWindow;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;

/** Fluent DSL for constructing Calcite {@link SqlNode} trees programmatically. */
public final class SqlNodeDSL {

  private static final SqlParserPos POS = SqlParserPos.ZERO;

  private SqlNodeDSL() {}

  // ---- SelectBuilder ----

  public static SelectBuilder select(SqlNode... nodes) {
    return new SelectBuilder(new SqlNodeList(Arrays.asList(nodes), POS));
  }

  /** Fluent builder that produces a {@link SqlSelect} (or {@link SqlOrderBy} when needed). */
  public static final class SelectBuilder {
    private final SqlNodeList selectList;
    private SqlNode from;
    private SqlNode where;
    private SqlNodeList groupBy;
    private SqlNode having;
    private SqlNodeList orderBy;
    private SqlNode offset;
    private SqlNode fetch;

    private SelectBuilder(SqlNodeList selectList) {
      this.selectList = selectList;
    }

    public SelectBuilder from(SqlNode from) {
      this.from = from;
      return this;
    }

    public SelectBuilder where(SqlNode where) {
      this.where = where;
      return this;
    }

    public SelectBuilder groupBy(SqlNode... nodes) {
      this.groupBy = new SqlNodeList(Arrays.asList(nodes), POS);
      return this;
    }

    public SelectBuilder having(SqlNode having) {
      this.having = having;
      return this;
    }

    public SelectBuilder orderBy(SqlNode... nodes) {
      this.orderBy = new SqlNodeList(Arrays.asList(nodes), POS);
      return this;
    }

    public SelectBuilder limit(SqlNode fetch) {
      this.fetch = fetch;
      return this;
    }

    public SelectBuilder offset(SqlNode offset) {
      this.offset = offset;
      return this;
    }

    public SqlNode build() {
      SqlSelect sel =
          new SqlSelect(
              POS,
              null, // keywords
              selectList,
              from,
              where,
              groupBy,
              having,
              null, // windowDecls
              null, // orderBy handled via SqlOrderBy wrapper
              null, // offset handled via SqlOrderBy wrapper
              null, // fetch handled via SqlOrderBy wrapper
              null); // hints
      if (orderBy != null || offset != null || fetch != null) {
        return new SqlOrderBy(
            POS,
            sel,
            orderBy != null ? orderBy : SqlNodeList.EMPTY,
            offset,
            fetch);
      }
      return sel;
    }
  }

  // ---- Identifiers & Literals ----

  public static SqlIdentifier star() {
    return SqlIdentifier.star(POS);
  }

  public static SqlIdentifier identifier(String... names) {
    if (names.length == 1) {
      return new SqlIdentifier(names[0], SqlParserPos.QUOTED_ZERO);
    }
    return new SqlIdentifier(Arrays.asList(names),
        null, SqlParserPos.QUOTED_ZERO,
        Collections.nCopies(names.length, SqlParserPos.QUOTED_ZERO));
  }

  public static SqlNode[] identifiers(String... names) {
    SqlNode[] result = new SqlNode[names.length];
    for (int i = 0; i < names.length; i++) {
      result[i] = new SqlIdentifier(names[i], SqlParserPos.QUOTED_ZERO);
    }
    return result;
  }

  public static SqlNode literal(Object value) {
    if (value == null) {
      return SqlLiteral.createNull(POS);
    } else if (value instanceof Integer || value instanceof Long) {
      return SqlLiteral.createExactNumeric(value.toString(), POS);
    } else if (value instanceof Float || value instanceof Double) {
      return SqlLiteral.createApproxNumeric(value.toString(), POS);
    } else if (value instanceof java.math.BigDecimal) {
      return SqlLiteral.createExactNumeric(value.toString(), POS);
    } else if (value instanceof String) {
      return SqlLiteral.createCharString((String) value, POS);
    } else if (value instanceof Boolean) {
      return SqlLiteral.createBoolean((Boolean) value, POS);
    } else if (value instanceof Short) {
      return SqlLiteral.createExactNumeric(value.toString(), POS);
    } else if (value instanceof Byte) {
      return SqlLiteral.createExactNumeric(value.toString(), POS);
    }
    throw new IllegalArgumentException("Unsupported literal type: " + value.getClass());
  }

  public static SqlNumericLiteral intLiteral(int value) {
    return (SqlNumericLiteral) SqlLiteral.createExactNumeric(Integer.toString(value), POS);
  }

  public static SqlIdentifier table(String name) {
    return new SqlIdentifier(name, POS);
  }

  public static SqlNode subquery(SqlNode inner, String alias) {
    return new SqlBasicCall(
        SqlStdOperatorTable.AS,
        new SqlNode[] {inner, identifier(alias)},
        POS);
  }

  public static SqlNodeList asList(SqlNode... nodes) {
    return new SqlNodeList(Arrays.asList(nodes), POS);
  }

  // ---- Comparison operators ----

  public static SqlNode gt(SqlNode left, SqlNode right) {
    return new SqlBasicCall(SqlStdOperatorTable.GREATER_THAN, new SqlNode[] {left, right}, POS);
  }

  public static SqlNode lt(SqlNode left, SqlNode right) {
    return new SqlBasicCall(SqlStdOperatorTable.LESS_THAN, new SqlNode[] {left, right}, POS);
  }

  public static SqlNode gte(SqlNode left, SqlNode right) {
    return new SqlBasicCall(
        SqlStdOperatorTable.GREATER_THAN_OR_EQUAL, new SqlNode[] {left, right}, POS);
  }

  public static SqlNode lte(SqlNode left, SqlNode right) {
    return new SqlBasicCall(
        SqlStdOperatorTable.LESS_THAN_OR_EQUAL, new SqlNode[] {left, right}, POS);
  }

  public static SqlNode eq(SqlNode left, SqlNode right) {
    return new SqlBasicCall(SqlStdOperatorTable.EQUALS, new SqlNode[] {left, right}, POS);
  }

  public static SqlNode neq(SqlNode left, SqlNode right) {
    return new SqlBasicCall(SqlStdOperatorTable.NOT_EQUALS, new SqlNode[] {left, right}, POS);
  }

  // ---- Arithmetic operators ----

  public static SqlNode plus(SqlNode left, SqlNode right) {
    return new SqlBasicCall(SqlStdOperatorTable.PLUS, new SqlNode[] {left, right}, POS);
  }

  public static SqlNode minus(SqlNode left, SqlNode right) {
    return new SqlBasicCall(SqlStdOperatorTable.MINUS, new SqlNode[] {left, right}, POS);
  }

  public static SqlNode times(SqlNode left, SqlNode right) {
    return new SqlBasicCall(SqlStdOperatorTable.MULTIPLY, new SqlNode[] {left, right}, POS);
  }

  public static SqlNode divide(SqlNode left, SqlNode right) {
    return new SqlBasicCall(SqlStdOperatorTable.DIVIDE, new SqlNode[] {left, right}, POS);
  }

  public static SqlNode mod(SqlNode left, SqlNode right) {
    return new SqlBasicCall(SqlStdOperatorTable.MOD, new SqlNode[] {left, right}, POS);
  }

  // ---- Logical operators ----

  public static SqlNode and(SqlNode... operands) {
    SqlNode result = operands[0];
    for (int i = 1; i < operands.length; i++) {
      result =
          new SqlBasicCall(SqlStdOperatorTable.AND, new SqlNode[] {result, operands[i]}, POS);
    }
    return result;
  }

  public static SqlNode or(SqlNode... operands) {
    SqlNode result = operands[0];
    for (int i = 1; i < operands.length; i++) {
      result =
          new SqlBasicCall(SqlStdOperatorTable.OR, new SqlNode[] {result, operands[i]}, POS);
    }
    return result;
  }

  public static SqlNode not(SqlNode operand) {
    return new SqlBasicCall(SqlStdOperatorTable.NOT, new SqlNode[] {operand}, POS);
  }

  // ---- Function helpers ----

  public static SqlNode call(String funcName, SqlNode... args) {
    SqlBasicFunction func =
        SqlBasicFunction.create(funcName, ReturnTypes.ARG0_NULLABLE, OperandTypes.VARIADIC);
    return new SqlBasicCall(func, args, POS);
  }

  public static SqlNode agg(String funcName, SqlNode... args) {
    SqlBasicFunction func =
        SqlBasicFunction.create(
            funcName, ReturnTypes.ARG0_NULLABLE, OperandTypes.VARIADIC,
            SqlFunctionCategory.USER_DEFINED_FUNCTION);
    return new SqlBasicCall(func, args, POS);
  }

  public static SqlNode cast(SqlNode expr, SqlDataTypeSpec type) {
    return new SqlBasicCall(SqlStdOperatorTable.CAST, new SqlNode[] {expr, type}, POS);
  }

  public static SqlNode as(SqlNode expr, String alias) {
    return new SqlBasicCall(
        SqlStdOperatorTable.AS, new SqlNode[] {expr, identifier(alias)}, POS);
  }

  public static SqlNode between(SqlNode expr, SqlNode low, SqlNode high) {
    return new SqlBasicCall(
        SqlStdOperatorTable.BETWEEN,
        new SqlNode[] {expr, low, high},
        POS);
  }

  public static SqlNode in(SqlNode expr, SqlNodeList values) {
    return new SqlBasicCall(SqlStdOperatorTable.IN, new SqlNode[] {expr, values}, POS);
  }

  public static SqlNode caseWhen(List<SqlNode> whens, List<SqlNode> thens, SqlNode elseExpr) {
    return new SqlCase(
        POS,
        null,
        new SqlNodeList(whens, POS),
        new SqlNodeList(thens, POS),
        elseExpr);
  }

  public static SqlNode window(SqlNode agg, SqlNodeList partitionBy, SqlNodeList orderBy) {
    SqlWindow win =
        SqlWindow.create(
            null,
            null,
            partitionBy,
            orderBy,
            SqlLiteral.createBoolean(false, POS),
            null,
            null,
            null,
            POS);
    return new SqlBasicCall(SqlStdOperatorTable.OVER, new SqlNode[] {agg, win}, POS);
  }

  /** Window with explicit frame bounds (ROWS/RANGE BETWEEN lower AND upper). */
  public static SqlNode windowWithFrame(
      SqlNode agg, SqlNodeList partitionBy, SqlNodeList orderBy,
      boolean isRows, SqlNode lowerBound, SqlNode upperBound) {
    SqlWindow win =
        SqlWindow.create(
            null, null, partitionBy, orderBy,
            SqlLiteral.createBoolean(isRows, POS),
            lowerBound, upperBound, null, POS);
    return new SqlBasicCall(SqlStdOperatorTable.OVER, new SqlNode[] {agg, win}, POS);
  }

  /** UNION ALL of two queries. */
  public static SqlNode unionAll(SqlNode left, SqlNode right) {
    return new SqlBasicCall(
        SqlStdOperatorTable.UNION_ALL, new SqlNode[] {left, right}, POS);
  }

  // ---- Join helper ----

  public static SqlNode join(SqlNode left, JoinType joinType, SqlNode right, SqlNode condition) {
    return new SqlJoin(POS, left,
        SqlLiteral.createBoolean(false, POS),
        joinType.symbol(POS),
        right,
        condition != null ? JoinConditionType.ON.symbol(POS) : JoinConditionType.NONE.symbol(POS),
        condition);
  }

  // ---- Null checks ----

  public static SqlNode isNull(SqlNode expr) {
    return new SqlBasicCall(SqlStdOperatorTable.IS_NULL, new SqlNode[] {expr}, POS);
  }

  public static SqlNode isNotNull(SqlNode expr) {
    return new SqlBasicCall(SqlStdOperatorTable.IS_NOT_NULL, new SqlNode[] {expr}, POS);
  }

  // ---- Order helpers ----

  public static SqlNode desc(SqlNode expr) {
    return new SqlBasicCall(SqlStdOperatorTable.DESC, new SqlNode[] {expr}, POS);
  }

  public static SqlNode nullsFirst(SqlNode expr) {
    return new SqlBasicCall(SqlStdOperatorTable.NULLS_FIRST, new SqlNode[] {expr}, POS);
  }

  public static SqlNode nullsLast(SqlNode expr) {
    return new SqlBasicCall(SqlStdOperatorTable.NULLS_LAST, new SqlNode[] {expr}, POS);
  }

  // ---- Pattern matching ----

  public static SqlNode like(SqlNode expr, SqlNode pattern) {
    return new SqlBasicCall(SqlStdOperatorTable.LIKE, new SqlNode[] {expr, pattern}, POS);
  }

  public static SqlNode notLike(SqlNode expr, SqlNode pattern) {
    return new SqlBasicCall(SqlStdOperatorTable.NOT_LIKE, new SqlNode[] {expr, pattern}, POS);
  }

  // ---- EXISTS / IN subquery ----

  public static SqlNode exists(SqlNode subquery) {
    return new SqlBasicCall(SqlStdOperatorTable.EXISTS, new SqlNode[] {subquery}, POS);
  }

  public static SqlNode inSub(SqlNode expr, SqlNode subquery) {
    return new SqlBasicCall(SqlStdOperatorTable.IN, new SqlNode[] {expr, subquery}, POS);
  }

  // ---- DISTINCT aggregate wrapper ----

  public static SqlNode distinct(SqlNode arg) {
    return new SqlBasicCall(
        SqlStdOperatorTable.AS,
        new SqlNode[] {
          SqlLiteral.createSymbol(SqlSelectKeyword.DISTINCT, POS), arg
        },
        POS);
  }

  // ---- Aggregate shortcuts ----

  public static SqlNode count(SqlNode... args) {
    return new SqlBasicCall(SqlStdOperatorTable.COUNT, args, POS);
  }

  public static SqlNode countStar() {
    return new SqlBasicCall(SqlStdOperatorTable.COUNT, new SqlNode[] {star()}, POS);
  }

  public static SqlNode sum(SqlNode arg) {
    return new SqlBasicCall(SqlStdOperatorTable.SUM, new SqlNode[] {arg}, POS);
  }

  public static SqlNode avg(SqlNode arg) {
    return new SqlBasicCall(SqlStdOperatorTable.AVG, new SqlNode[] {arg}, POS);
  }

  public static SqlNode min(SqlNode arg) {
    return new SqlBasicCall(SqlStdOperatorTable.MIN, new SqlNode[] {arg}, POS);
  }

  public static SqlNode max(SqlNode arg) {
    return new SqlBasicCall(SqlStdOperatorTable.MAX, new SqlNode[] {arg}, POS);
  }
}
