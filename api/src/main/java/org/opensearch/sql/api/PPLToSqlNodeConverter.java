/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api;

import static org.opensearch.sql.calcite.utils.SqlNodeDSL.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelectKeyword;
import org.apache.calcite.sql.SqlWindow;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.Node;
import org.opensearch.sql.ast.expression.AggregateFunction;
import org.opensearch.sql.ast.expression.Alias;
import org.opensearch.sql.ast.expression.AllFields;
import org.opensearch.sql.ast.expression.AllFieldsExcludeMeta;
import org.opensearch.sql.ast.expression.Argument;
import org.opensearch.sql.ast.expression.Between;
import org.opensearch.sql.ast.expression.Case;
import org.opensearch.sql.ast.expression.Cast;
import org.opensearch.sql.ast.expression.DataType;
import org.opensearch.sql.ast.expression.EqualTo;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.Function;
import org.opensearch.sql.ast.expression.In;
import org.opensearch.sql.ast.expression.Interval;
import org.opensearch.sql.ast.expression.Let;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.expression.Span;
import org.opensearch.sql.ast.expression.SpanUnit;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.ast.expression.When;
import org.opensearch.sql.ast.expression.WindowFunction;
import org.opensearch.sql.ast.expression.Xor;
import org.opensearch.sql.ast.expression.subquery.ExistsSubquery;
import org.opensearch.sql.ast.expression.subquery.InSubquery;
import org.opensearch.sql.ast.expression.subquery.ScalarSubquery;
import org.opensearch.sql.ast.statement.Query;
import org.opensearch.sql.ast.statement.Statement;
import org.opensearch.sql.ast.tree.Filter;
import org.opensearch.sql.ast.tree.Head;
import org.opensearch.sql.ast.tree.Project;
import org.opensearch.sql.ast.tree.Relation;
import org.opensearch.sql.ast.tree.Sort;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.calcite.utils.SqlNodeDSL;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.ppl.antlr.PPLSyntaxParser;
import org.opensearch.sql.ppl.parser.AstBuilder;
import org.opensearch.sql.ppl.parser.AstStatementBuilder;

/**
 * Converts PPL AST to Calcite SqlNode tree. Base converter handles static commands (no schema
 * dependency): source, where, fields (include-only), sort, head.
 */
public class PPLToSqlNodeConverter extends AbstractNodeVisitor<SqlNode, Void> {

  private static final SqlParserPos POS = SqlParserPos.ZERO;

  private static final Map<String, String> FUNC_MAP = new HashMap<>();

  static {
    // Math
    FUNC_MAP.put("abs", "ABS");
    FUNC_MAP.put("ceil", "CEIL");
    FUNC_MAP.put("ceiling", "CEIL");
    FUNC_MAP.put("floor", "FLOOR");
    FUNC_MAP.put("round", "ROUND");
    FUNC_MAP.put("sqrt", "SQRT");
    FUNC_MAP.put("pow", "POWER");
    FUNC_MAP.put("power", "POWER");
    FUNC_MAP.put("ln", "LN");
    FUNC_MAP.put("log10", "LOG10");
    FUNC_MAP.put("exp", "EXP");
    FUNC_MAP.put("sign", "SIGN");
    FUNC_MAP.put("signum", "SIGN");
    FUNC_MAP.put("sin", "SIN");
    FUNC_MAP.put("cos", "COS");
    FUNC_MAP.put("tan", "TAN");
    FUNC_MAP.put("asin", "ASIN");
    FUNC_MAP.put("acos", "ACOS");
    FUNC_MAP.put("atan", "ATAN");
    FUNC_MAP.put("atan2", "ATAN2");
    FUNC_MAP.put("radians", "RADIANS");
    FUNC_MAP.put("degrees", "DEGREES");
    FUNC_MAP.put("mod", "MOD");
    FUNC_MAP.put("truncate", "TRUNCATE");
    FUNC_MAP.put("rand", "RAND");
    // String
    FUNC_MAP.put("upper", "UPPER");
    FUNC_MAP.put("lower", "LOWER");
    FUNC_MAP.put("length", "CHAR_LENGTH");
    FUNC_MAP.put("char_length", "CHAR_LENGTH");
    FUNC_MAP.put("character_length", "CHAR_LENGTH");
    FUNC_MAP.put("octet_length", "OCTET_LENGTH");
    FUNC_MAP.put("bit_length", "BIT_LENGTH");
    FUNC_MAP.put("substring", "SUBSTRING");
    FUNC_MAP.put("trim", "TRIM");
    FUNC_MAP.put("replace", "REGEXP_REPLACE");
    FUNC_MAP.put("regexp_match", "REGEXP_CONTAINS");
    FUNC_MAP.put("regex_match", "REGEXP_CONTAINS");
    FUNC_MAP.put("concat", "CONCAT");
    FUNC_MAP.put("reverse", "REVERSE");
    FUNC_MAP.put("ascii", "ASCII");
    // Condition/null
    FUNC_MAP.put("coalesce", "COALESCE");
    FUNC_MAP.put("nullif", "NULLIF");
    FUNC_MAP.put("typeof", "TYPEOF");
    // Date/time
    FUNC_MAP.put("now", "CURRENT_TIMESTAMP");
    FUNC_MAP.put("curdate", "CURRENT_DATE");
    FUNC_MAP.put("current_date", "CURRENT_DATE");
    FUNC_MAP.put("curtime", "CURRENT_TIME");
    FUNC_MAP.put("current_time", "CURRENT_TIME");
    FUNC_MAP.put("current_timestamp", "CURRENT_TIMESTAMP");
    FUNC_MAP.put("localtime", "LOCALTIMESTAMP");
    FUNC_MAP.put("localtimestamp", "LOCALTIMESTAMP");
    FUNC_MAP.put("sysdate", "CURRENT_TIMESTAMP");
    FUNC_MAP.put("utc_date", "CURRENT_DATE");
    FUNC_MAP.put("utc_time", "CURRENT_TIME");
    FUNC_MAP.put("utc_timestamp", "CURRENT_TIMESTAMP");
    FUNC_MAP.put("left", "LEFT");
    FUNC_MAP.put("right", "RIGHT");
    FUNC_MAP.put("ltrim", "LTRIM");
    FUNC_MAP.put("rtrim", "RTRIM");
    FUNC_MAP.put("position", "POSITION");
    FUNC_MAP.put("locate", "LOCATE");
    // Aggregates (used by visitFunction for window context)
    FUNC_MAP.put("count", "COUNT");
    FUNC_MAP.put("sum", "SUM");
    FUNC_MAP.put("avg", "AVG");
    FUNC_MAP.put("min", "MIN");
    FUNC_MAP.put("max", "MAX");
  }

  private static final Set<String> SQL_DATETIME_KEYWORDS =
      Set.of(
          "CURRENT_TIMESTAMP",
          "CURRENT_DATE",
          "CURRENT_TIME",
          "LOCALTIMESTAMP",
          "LOCALTIME");

  private int aliasCounter = 0;

  private static final Settings DEFAULT_SETTINGS =
      new Settings() {
        @SuppressWarnings("unchecked")
        @Override
        public <T> T getSettingValue(Key key) {
          switch (key) {
            case CALCITE_ENGINE_ENABLED:
              return (T) Boolean.TRUE;
            case CALCITE_SUPPORT_ALL_JOIN_TYPES:
              return (T) Boolean.TRUE;
            case PPL_SUBSEARCH_MAXOUT:
            case PPL_JOIN_SUBSEARCH_MAXOUT:
              return (T) Integer.valueOf(10000);
            case PPL_REX_MAX_MATCH_LIMIT:
              return (T) Integer.valueOf(10);
            case PATTERN_METHOD:
              return (T) "simple_pattern";
            case PATTERN_MODE:
              return (T) "regex";
            case PATTERN_MAX_SAMPLE_COUNT:
              return (T) Integer.valueOf(10);
            case PATTERN_BUFFER_LIMIT:
              return (T) Integer.valueOf(1000);
            case PATTERN_SHOW_NUMBERED_TOKEN:
              return (T) Boolean.FALSE;
            default:
              return null;
          }
        }

        @Override
        public java.util.List<?> getSettings() {
          return java.util.Collections.emptyList();
        }
      };

  /** Parse PPL string to AST. */
  public static UnresolvedPlan parse(String ppl) {
    PPLSyntaxParser parser = new PPLSyntaxParser();
    ParseTree cst = parser.parse(ppl);
    AstBuilder astBuilder = new AstBuilder(ppl, DEFAULT_SETTINGS);
    AstStatementBuilder stmtBuilder =
        new AstStatementBuilder(
            astBuilder, AstStatementBuilder.StatementBuilderContext.builder().build());
    Statement stmt = stmtBuilder.visit(cst);
    return ((Query) stmt).getPlan();
  }

  /** Convert a PPL AST to SqlNode. */
  public SqlNode convert(UnresolvedPlan plan) {
    List<UnresolvedPlan> nodes = new ArrayList<>();
    flatten(plan, nodes);

    SqlNode current = null;
    for (UnresolvedPlan node : nodes) {
      current = node.accept(this, null);
    }
    return current;
  }

  // -- Pipe state: the current SqlNode being built up --
  protected SqlNode pipe;

  private String nextAlias() {
    return "_t" + (++aliasCounter);
  }

  /** Flatten AST linked list from outermost to leaf, then reverse so leaf (Relation) is first. */
  private static void flatten(UnresolvedPlan node, List<UnresolvedPlan> out) {
    List<? extends Node> children = node.getChild();
    if (!children.isEmpty()) {
      flatten((UnresolvedPlan) children.get(0), out);
    }
    out.add(node);
  }

  /** Wrap current pipe as a subquery with a generated alias. */
  protected SqlNode wrapAsSubquery() {
    return subquery(pipe, nextAlias());
  }

  // -- Visitor methods --

  @Override
  public SqlNode visitRelation(Relation node, Void ctx) {
    String tableName = node.getTableQualifiedName().toString();
    pipe = select(star()).from(table(tableName)).build();
    return pipe;
  }

  @Override
  public SqlNode visitFilter(Filter node, Void ctx) {
    SqlNode condition = node.getCondition().accept(this, null);
    pipe = select(star()).from(wrapAsSubquery()).where(condition).build();
    return pipe;
  }

  @Override
  public SqlNode visitProject(Project node, Void ctx) {
    if (node.getProjectList().size() == 1 && node.getProjectList().get(0) instanceof AllFields) {
      return pipe;
    }
    SqlNode[] cols =
        node.getProjectList().stream()
            .map(expr -> expr.accept(this, null))
            .toArray(SqlNode[]::new);
    pipe = select(cols).from(wrapAsSubquery()).build();
    return pipe;
  }

  @Override
  public SqlNode visitSort(Sort node, Void ctx) {
    List<SqlNode> orderItems = new ArrayList<>();
    for (Field f : node.getSortList()) {
      SqlNode col = f.getField().accept(this, null);
      boolean asc = true;
      boolean nullFirst = true;
      for (Argument arg : f.getFieldArgs()) {
        if ("asc".equals(arg.getArgName())) {
          asc = Boolean.TRUE.equals(arg.getValue().getValue());
        } else if ("nullFirst".equals(arg.getArgName())) {
          nullFirst = Boolean.TRUE.equals(arg.getValue().getValue());
        }
      }
      if (!asc) col = desc(col);
      col = nullFirst ? nullsFirst(col) : nullsLast(col);
      orderItems.add(col);
    }
    pipe =
        select(star())
            .from(wrapAsSubquery())
            .orderBy(orderItems.toArray(new SqlNode[0]))
            .build();
    return pipe;
  }

  @Override
  public SqlNode visitHead(Head node, Void ctx) {
    SqlNodeDSL.SelectBuilder builder =
        select(star()).from(wrapAsSubquery()).limit(intLiteral(node.getSize()));
    if (node.getFrom() != null && node.getFrom() > 0) {
      builder = builder.offset(intLiteral(node.getFrom()));
    }
    pipe = builder.build();
    return pipe;
  }

  // -- Expression visitors --

  @Override
  public SqlNode visitQualifiedName(QualifiedName node, Void ctx) {
    List<String> parts = node.getParts();
    return identifier(parts.toArray(new String[0]));
  }

  @Override
  public SqlNode visitField(Field node, Void ctx) {
    return node.getField().accept(this, null);
  }

  @Override
  public SqlNode visitLiteral(Literal node, Void ctx) {
    DataType type = node.getType();
    if (type == DataType.NULL) {
      return literal(null);
    }
    switch (type) {
      case DATE:
        return cast(literal(node.getValue().toString()),
            new SqlDataTypeSpec(new SqlBasicTypeNameSpec(SqlTypeName.DATE, POS), POS));
      case TIME:
        return cast(literal(node.getValue().toString()),
            new SqlDataTypeSpec(new SqlBasicTypeNameSpec(SqlTypeName.TIME, POS), POS));
      case TIMESTAMP:
        return cast(literal(node.getValue().toString()),
            new SqlDataTypeSpec(new SqlBasicTypeNameSpec(SqlTypeName.TIMESTAMP, POS), POS));
      case DOUBLE:
        return cast(literal(node.getValue()),
            new SqlDataTypeSpec(new SqlBasicTypeNameSpec(SqlTypeName.DOUBLE, POS), POS));
      case FLOAT:
        return cast(literal(node.getValue()),
            new SqlDataTypeSpec(new SqlBasicTypeNameSpec(SqlTypeName.FLOAT, POS), POS));
      case LONG:
        return cast(literal(node.getValue()),
            new SqlDataTypeSpec(new SqlBasicTypeNameSpec(SqlTypeName.BIGINT, POS), POS));
      default:
        return literal(node.getValue());
    }
  }

  @Override
  public SqlNode visitCompare(org.opensearch.sql.ast.expression.Compare node, Void ctx) {
    SqlNode left = node.getLeft().accept(this, null);
    SqlNode right = node.getRight().accept(this, null);
    switch (node.getOperator()) {
      case ">":
        return gt(left, right);
      case "<":
        return lt(left, right);
      case ">=":
        return gte(left, right);
      case "<=":
        return lte(left, right);
      case "=":
        return eq(left, right);
      case "!=":
      case "<>":
        return neq(left, right);
      case "REGEXP":
      case "regexp":
        return call("REGEXP_CONTAINS", left, right);
      default:
        throw new UnsupportedOperationException("Unsupported operator: " + node.getOperator());
    }
  }

  @Override
  public SqlNode visitAnd(org.opensearch.sql.ast.expression.And node, Void ctx) {
    return and(node.getLeft().accept(this, null), node.getRight().accept(this, null));
  }

  @Override
  public SqlNode visitOr(org.opensearch.sql.ast.expression.Or node, Void ctx) {
    return or(node.getLeft().accept(this, null), node.getRight().accept(this, null));
  }

  @Override
  public SqlNode visitNot(org.opensearch.sql.ast.expression.Not node, Void ctx) {
    return not(node.getExpression().accept(this, null));
  }

  @Override
  public SqlNode visitAllFields(AllFields node, Void ctx) {
    return star();
  }

  @Override
  public SqlNode visitAlias(Alias node, Void ctx) {
    SqlNode expr = node.getDelegated().accept(this, null);
    return node.getName() != null ? as(expr, node.getName()) : expr;
  }

  @Override
  public SqlNode visitEqualTo(EqualTo node, Void ctx) {
    return eq(node.getLeft().accept(this, null), node.getRight().accept(this, null));
  }

  @Override
  public SqlNode visitXor(Xor node, Void ctx) {
    SqlNode l = node.getLeft().accept(this, null);
    SqlNode r = node.getRight().accept(this, null);
    return and(or(l, r), not(and(l, r)));
  }

  @Override
  public SqlNode visitFunction(Function node, Void ctx) {
    String name = node.getFuncName().toLowerCase();
    List<SqlNode> args =
        node.getFuncArgs().stream()
            .map(a -> a.accept(this, null))
            .collect(Collectors.toList());
    // Arithmetic operators
    if ("+".equals(name) && args.size() == 2) return plus(args.get(0), args.get(1));
    if ("-".equals(name) && args.size() == 2) return minus(args.get(0), args.get(1));
    if ("*".equals(name) && args.size() == 2) return times(args.get(0), args.get(1));
    if ("/".equals(name) && args.size() == 2) {
      return caseWhen(
          List.of(eq(args.get(1), literal(0))),
          List.of(literal(null)),
          divide(args.get(0), args.get(1)));
    }
    if ("%".equals(name) && args.size() == 2) return mod(args.get(0), args.get(1));
    // Special-case rewrites
    if ("if".equals(name))
      return caseWhen(List.of(args.get(0)), List.of(args.get(1)), args.get(2));
    if ("ifnull".equals(name)) return call("COALESCE", args.get(0), args.get(1));
    if ("isnull".equals(name) || "is null".equals(name)) return isNull(args.get(0));
    if ("isnotnull".equals(name) || "is not null".equals(name)) return isNotNull(args.get(0));
    if ("log".equals(name)) {
      if (args.size() == 2) return divide(call("LN", args.get(1)), call("LN", args.get(0)));
      return call("LN", args.get(0));
    }
    if ("log2".equals(name)) return divide(call("LN", args.get(0)), call("LN", literal(2)));
    if ("like".equals(name)) return like(args.get(0), args.get(1));
    if ("not like".equals(name)) return notLike(args.get(0), args.get(1));
    // Default: FUNC_MAP lookup
    String sqlName = FUNC_MAP.getOrDefault(name, name.toUpperCase());
    if (args.isEmpty() && SQL_DATETIME_KEYWORDS.contains(sqlName)) return call(sqlName);
    return call(sqlName, args.toArray(new SqlNode[0]));
  }

  @Override
  public SqlNode visitAggregateFunction(AggregateFunction node, Void ctx) {
    String name = node.getFuncName().toLowerCase();
    if ("distinct_count".equals(name) || "dc".equals(name)) {
      return new SqlBasicCall(
          SqlStdOperatorTable.COUNT,
          new SqlNode[] {
            SqlLiteral.createSymbol(SqlSelectKeyword.DISTINCT, POS),
            node.getField().accept(this, null)
          },
          POS);
    }
    if ("count".equals(name)) {
      UnresolvedExpression field = node.getField();
      if (field instanceof AllFields
          || (field instanceof Literal && ((Literal) field).getValue().equals(1))) {
        return countStar();
      }
      if (Boolean.TRUE.equals(node.getDistinct())) {
        return new SqlBasicCall(
            SqlStdOperatorTable.COUNT,
            new SqlNode[] {
              SqlLiteral.createSymbol(SqlSelectKeyword.DISTINCT, POS),
              field.accept(this, null)
            },
            POS);
      }
      return count(field.accept(this, null));
    }
    String sqlName = FUNC_MAP.getOrDefault(name, name.toUpperCase());
    SqlNode field = node.getField().accept(this, null);
    if (Boolean.TRUE.equals(node.getDistinct())) {
      return new SqlBasicCall(
          SqlStdOperatorTable.COUNT,
          new SqlNode[] {SqlLiteral.createSymbol(SqlSelectKeyword.DISTINCT, POS), field},
          POS);
    }
    return call(sqlName, field);
  }

  @Override
  public SqlNode visitIn(In node, Void ctx) {
    SqlNode field = node.getField().accept(this, null);
    SqlNodeList vals =
        new SqlNodeList(
            node.getValueList().stream()
                .map(v -> v.accept(this, null))
                .collect(Collectors.toList()),
            POS);
    return in(field, vals);
  }

  @Override
  public SqlNode visitBetween(Between node, Void ctx) {
    return between(
        node.getValue().accept(this, null),
        node.getLowerBound().accept(this, null),
        node.getUpperBound().accept(this, null));
  }

  @Override
  public SqlNode visitCase(Case node, Void ctx) {
    List<SqlNode> whens = new ArrayList<>();
    List<SqlNode> thens = new ArrayList<>();
    for (When w : node.getWhenClauses()) {
      whens.add(w.getCondition().accept(this, null));
      thens.add(w.getResult().accept(this, null));
    }
    SqlNode elseExpr =
        node.getElseClause().map(e -> e.accept(this, null)).orElse(literal(null));
    return caseWhen(whens, thens, elseExpr);
  }

  @Override
  public SqlNode visitCast(Cast node, Void ctx) {
    SqlNode expr = node.getExpression().accept(this, null);
    String typeName = node.getConvertedType().toString().toUpperCase();
    SqlTypeName sqlType;
    switch (typeName) {
      case "STRING":
        sqlType = SqlTypeName.VARCHAR;
        break;
      case "INT":
      case "INTEGER":
        sqlType = SqlTypeName.INTEGER;
        break;
      case "LONG":
        sqlType = SqlTypeName.BIGINT;
        break;
      case "FLOAT":
        sqlType = SqlTypeName.FLOAT;
        break;
      case "DOUBLE":
        sqlType = SqlTypeName.DOUBLE;
        break;
      case "BOOLEAN":
        sqlType = SqlTypeName.BOOLEAN;
        break;
      case "DATE":
        sqlType = SqlTypeName.DATE;
        break;
      case "TIME":
        sqlType = SqlTypeName.TIME;
        break;
      case "TIMESTAMP":
      case "DATETIME":
        sqlType = SqlTypeName.TIMESTAMP;
        break;
      default:
        sqlType = SqlTypeName.VARCHAR;
        break;
    }
    return cast(expr, typeSpec(sqlType));
  }

  private static SqlDataTypeSpec typeSpec(SqlTypeName typeName) {
    return new SqlDataTypeSpec(new SqlBasicTypeNameSpec(typeName, POS), POS);
  }

  @Override
  public SqlNode visitSpan(Span node, Void ctx) {
    SqlNode field = node.getField().accept(this, null);
    SqlNode value = node.getValue().accept(this, null);
    SpanUnit unit = node.getUnit();
    if (unit == SpanUnit.NONE || !SpanUnit.isTimeUnit(unit)) {
      return times(call("FLOOR", divide(field, value)), value);
    }
    return call("DATE_TRUNC", field, literal(spanUnitToSql(unit)));
  }

  private static String spanUnitToSql(SpanUnit u) {
    switch (SpanUnit.getName(u)) {
      case "s": return "SECOND";
      case "m": return "MINUTE";
      case "h": return "HOUR";
      case "d": return "DAY";
      case "w": return "WEEK";
      case "M": return "MONTH";
      case "q": return "QUARTER";
      case "y": return "YEAR";
      default: return "DAY";
    }
  }

  @Override
  public SqlNode visitInterval(Interval node, Void ctx) {
    SqlNode value = node.getValue().accept(this, null);
    String unit = node.getUnit().name();
    return call("INTERVAL_" + unit, value);
  }

  @Override
  public SqlNode visitWindowFunction(WindowFunction node, Void ctx) {
    SqlNode func = node.getFunction().accept(this, null);
    SqlNodeList partBy =
        new SqlNodeList(
            node.getPartitionByList().stream()
                .map(e -> e.accept(this, null))
                .collect(Collectors.toList()),
            POS);
    List<SqlNode> ordItems = new ArrayList<>();
    for (org.apache.commons.lang3.tuple.Pair<
            org.opensearch.sql.ast.tree.Sort.SortOption,
            UnresolvedExpression>
        p : node.getSortList()) {
      SqlNode col = p.getRight().accept(this, null);
      if (p.getLeft() != null
          && p.getLeft() == org.opensearch.sql.ast.tree.Sort.SortOption.DEFAULT_DESC) {
        col = desc(col);
      }
      ordItems.add(col);
    }
    SqlNodeList ordBy = new SqlNodeList(ordItems, POS);
    return window(func, partBy, ordBy);
  }

  @Override
  public SqlNode visitLet(Let node, Void ctx) {
    return as(node.getExpression().accept(this, null), node.getVar().getField().toString());
  }

  @Override
  public SqlNode visitInSubquery(InSubquery node, Void ctx) {
    SqlNode sub = convertSubPlan(node.getQuery());
    SqlNode field = node.getValue().get(0).accept(this, null);
    return inSub(field, sub);
  }

  @Override
  public SqlNode visitScalarSubquery(ScalarSubquery node, Void ctx) {
    return convertSubPlan(node.getQuery());
  }

  @Override
  public SqlNode visitExistsSubquery(ExistsSubquery node, Void ctx) {
    return exists(convertSubPlan(node.getQuery()));
  }

  protected SqlNode convertSubPlan(UnresolvedPlan plan) {
    PPLToSqlNodeConverter sub = new PPLToSqlNodeConverter();
    return sub.convert(plan);
  }
}
