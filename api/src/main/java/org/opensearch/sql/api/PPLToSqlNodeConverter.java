/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api;

import static org.opensearch.sql.calcite.utils.SqlNodeDSL.*;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.JoinType;
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
import org.opensearch.sql.ast.expression.ParseMethod;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.expression.Span;
import org.opensearch.sql.ast.expression.SpanUnit;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.ast.expression.When;
import org.opensearch.sql.ast.expression.WindowBound;
import org.opensearch.sql.ast.expression.WindowFrame;
import org.opensearch.sql.ast.expression.WindowFunction;
import org.opensearch.sql.ast.expression.Xor;
import org.opensearch.sql.ast.expression.subquery.ExistsSubquery;
import org.opensearch.sql.ast.expression.subquery.InSubquery;
import org.opensearch.sql.ast.expression.subquery.ScalarSubquery;
import org.opensearch.sql.ast.statement.Query;
import org.opensearch.sql.ast.statement.Statement;
import org.opensearch.sql.ast.tree.Aggregation;
import org.opensearch.sql.ast.tree.Append;
import org.opensearch.sql.ast.tree.Bin;
import org.opensearch.sql.ast.tree.Dedupe;
import org.opensearch.sql.ast.tree.Eval;
import org.opensearch.sql.ast.tree.FillNull;
import org.opensearch.sql.ast.tree.Filter;
import org.opensearch.sql.ast.tree.Head;
import org.opensearch.sql.ast.tree.Join;
import org.opensearch.sql.ast.tree.Lookup;
import org.opensearch.sql.ast.tree.Parse;
import org.opensearch.sql.ast.tree.Project;
import org.opensearch.sql.ast.tree.RareTopN;
import org.opensearch.sql.ast.tree.Relation;
import org.opensearch.sql.ast.tree.Rename;
import org.opensearch.sql.ast.tree.Replace;
import org.opensearch.sql.ast.tree.ReplacePair;
import org.opensearch.sql.ast.tree.Sort;
import org.opensearch.sql.ast.tree.SpanBin;
import org.opensearch.sql.ast.tree.StreamWindow;
import org.opensearch.sql.ast.tree.SubqueryAlias;
import org.opensearch.sql.ast.tree.Trendline;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.ast.tree.Window;
import org.opensearch.sql.calcite.utils.SqlNodeDSL;
import org.opensearch.sql.calcite.utils.WildcardUtils;
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

  @Override
  public SqlNode visitAggregation(Aggregation node, Void ctx) {
    // Build SELECT items: aggregates first, then span (if any), then group-by fields
    List<SqlNode> selectItems = new ArrayList<>();
    for (UnresolvedExpression expr : node.getAggExprList()) {
      selectItems.add(expr.accept(this, null));
    }

    List<SqlNode> groupByItems = new ArrayList<>();
    // Handle span — stored separately from groupExprList
    if (node.getSpan() != null) {
      selectItems.add(node.getSpan().accept(this, null));
      Alias spanAlias = (Alias) node.getSpan();
      groupByItems.add(spanAlias.getDelegated().accept(this, null));
    }
    // Handle group-by fields
    for (UnresolvedExpression expr : node.getGroupExprList()) {
      selectItems.add(expr.accept(this, null));
      if (expr instanceof Alias) {
        groupByItems.add(((Alias) expr).getDelegated().accept(this, null));
      } else {
        groupByItems.add(expr.accept(this, null));
      }
    }

    SqlNodeDSL.SelectBuilder builder =
        select(selectItems.toArray(new SqlNode[0])).from(wrapAsSubquery());
    if (!groupByItems.isEmpty()) {
      builder = builder.groupBy(groupByItems.toArray(new SqlNode[0]));
    }

    // bucket_nullable=false: filter out null group-by keys
    boolean bucketNullable =
        node.getArgExprList().stream()
            .noneMatch(
                a ->
                    "bucket_nullable".equals(a.getArgName())
                        && Boolean.FALSE.equals(a.getValue().getValue()));
    if (!bucketNullable && !groupByItems.isEmpty()) {
      SqlNode nullFilter = groupByItems.stream()
          .map(col -> isNotNull(col))
          .reduce((a, b) -> and(a, b))
          .get();
      builder = builder.where(nullFilter);
    }

    pipe = builder.build();
    return pipe;
  }

  @Override
  public SqlNode visitEval(Eval node, Void ctx) {
    Map<String, SqlNode> evalAliases = new java.util.LinkedHashMap<>();
    List<SqlNode> items = new ArrayList<>();
    items.add(star());
    for (Let let : node.getExpressionList()) {
      String varName = let.getVar().getField().toString();
      SqlNode expr = let.getExpression().accept(this, null);
      if (!evalAliases.isEmpty()) {
        expr = expr.accept(new org.apache.calcite.sql.util.SqlShuttle() {
          @Override
          public SqlNode visit(org.apache.calcite.sql.SqlIdentifier id) {
            if (id.isSimple() && evalAliases.containsKey(id.getSimple())) {
              return evalAliases.get(id.getSimple());
            }
            return id;
          }
        });
      }
      evalAliases.put(varName, expr);
      items.add(as(expr, varName));
    }
    pipe = select(items.toArray(new SqlNode[0])).from(wrapAsSubquery()).build();
    return pipe;
  }

  @Override
  public SqlNode visitDedupe(Dedupe node, Void ctx) {
    int allowedDuplication =
        (Integer) ((Literal) node.getOptions().get(0).getValue()).getValue();
    boolean keepEmpty =
        (Boolean) ((Literal) node.getOptions().get(1).getValue()).getValue();
    boolean consecutive =
        (Boolean) ((Literal) node.getOptions().get(2).getValue()).getValue();

    List<SqlNode> fieldNodes =
        node.getFields().stream()
            .map(f -> f.getField().accept(this, null))
            .collect(Collectors.toList());

    SqlNodeList partBy = new SqlNodeList(fieldNodes, POS);

    if (!consecutive) {
      // Standard dedup
      if (!keepEmpty) {
        SqlNode notNullFilter =
            fieldNodes.stream().map(f -> isNotNull(f)).reduce((a, b) -> and(a, b)).get();
        pipe = select(star()).from(wrapAsSubquery()).where(notNullFilter).build();
      }

      SqlNode rowNum = as(
          window(call("ROW_NUMBER"), partBy, new SqlNodeList(fieldNodes, POS)),
          "_dedup_rn");
      pipe = select(star(), rowNum).from(wrapAsSubquery()).build();

      SqlNode rnCond = lte(identifier("_dedup_rn"), intLiteral(allowedDuplication));
      if (keepEmpty) {
        SqlNode nullCheck =
            fieldNodes.stream().map(f -> isNull(f)).reduce((a, b) -> or(a, b)).get();
        rnCond = or(nullCheck, rnCond);
      }
      pipe = select(star()).from(wrapAsSubquery()).where(rnCond).build();
      pipe = select(star()).from(wrapAsSubquery()).build();
    } else {
      // Consecutive dedup — gaps-and-islands
      if (!keepEmpty) {
        SqlNode notNullFilter =
            fieldNodes.stream().map(f -> isNotNull(f)).reduce((a, b) -> and(a, b)).get();
        pipe = select(star()).from(wrapAsSubquery()).where(notNullFilter).build();
      }

      SqlNode idOrder = cast(identifier("_id"), typeSpec(SqlTypeName.INTEGER));
      SqlNodeList globalOrd = new SqlNodeList(List.of(idOrder), POS);

      SqlNode globalRn = as(
          window(call("ROW_NUMBER"), SqlNodeList.EMPTY, globalOrd), "_global_rn");
      SqlNode groupRn = as(
          window(call("ROW_NUMBER"), partBy, new SqlNodeList(List.of(idOrder), POS)),
          "_group_rn");
      pipe = select(star(), globalRn, groupRn).from(wrapAsSubquery()).build();

      List<SqlNode> islandParts = new ArrayList<>(fieldNodes);
      islandParts.add(minus(identifier("_global_rn"), identifier("_group_rn")));
      SqlNode dedupRn = as(
          window(call("ROW_NUMBER"),
              new SqlNodeList(islandParts, POS),
              new SqlNodeList(List.of(cast(identifier("_id"), typeSpec(SqlTypeName.INTEGER))), POS)),
          "_dedup_rn");
      pipe = select(star(), dedupRn).from(wrapAsSubquery()).build();

      SqlNode rnCond = lte(identifier("_dedup_rn"), intLiteral(allowedDuplication));
      if (keepEmpty) {
        SqlNode nullCheck =
            fieldNodes.stream().map(f -> isNull(f)).reduce((a, b) -> or(a, b)).get();
        rnCond = or(nullCheck, rnCond);
      }
      pipe = select(star()).from(wrapAsSubquery()).where(rnCond).build();
      pipe = select(star()).from(wrapAsSubquery()).build();
    }
    return pipe;
  }

  // -- Join & Lookup visitors --

  @Override
  public SqlNode visitJoin(Join node, Void ctx) {
    String leftAlias = node.getLeftAlias().orElse(null);
    SqlNode leftSide = leftAlias != null ? subquery(pipe, leftAlias) : wrapAsSubquery();

    SqlNode rightSide = resolveJoinRight(node.getRight(), node.getRightAlias().orElse(null));

    SqlNode condition = null;
    if (node.getJoinCondition().isPresent()) {
      condition = node.getJoinCondition().get().accept(this, null);
    } else if (node.getJoinFields().isPresent() && !node.getJoinFields().get().isEmpty()) {
      condition = buildFieldListCondition(node.getJoinFields().get(),
          node.getLeftAlias().orElse(null), node.getRightAlias().orElse(null));
    }

    Join.JoinType jt = node.getJoinType();

    if (jt == Join.JoinType.SEMI || jt == Join.JoinType.ANTI) {
      SqlNode subSelect = condition != null
          ? select(literal(1)).from(rightSide).where(condition).build()
          : select(literal(1)).from(rightSide).build();
      SqlNode existsExpr = jt == Join.JoinType.SEMI ? exists(subSelect) : not(exists(subSelect));
      pipe = select(star()).from(leftSide).where(existsExpr).build();
      return pipe;
    }

    JoinType calciteJoinType;
    switch (jt) {
      case LEFT: calciteJoinType = JoinType.LEFT; break;
      case RIGHT: calciteJoinType = JoinType.RIGHT; break;
      case FULL: calciteJoinType = JoinType.FULL; break;
      case CROSS: calciteJoinType = condition != null ? JoinType.INNER : JoinType.CROSS; break;
      default: calciteJoinType = condition != null ? JoinType.INNER : JoinType.CROSS; break;
    }

    pipe = select(star()).from(SqlNodeDSL.join(leftSide, calciteJoinType, rightSide, condition)).build();
    return pipe;
  }

  @Override
  public SqlNode visitLookup(Lookup node, Void ctx) {
    String leftAlias = "_l";
    String rightAlias = "_r";
    SqlNode leftSide = subquery(pipe, leftAlias);

    Relation lookupRel = extractRelation(node.getLookupRelation());
    String lookupTable = lookupRel != null
        ? lookupRel.getTableQualifiedName().toString()
        : node.getLookupRelation().toString();
    SqlNode rightSide = subquery(
        select(star()).from(table(lookupTable)).build(), rightAlias);

    SqlNode onCondition = null;
    for (Map.Entry<String, String> e : node.getMappingAliasMap().entrySet()) {
      SqlNode cond = eq(
          identifier(leftAlias, e.getValue()),
          identifier(rightAlias, e.getKey()));
      onCondition = onCondition == null ? cond : and(onCondition, cond);
    }

    SqlNode joinNode = SqlNodeDSL.join(leftSide, JoinType.LEFT, rightSide, onCondition);

    Map<String, String> outputMap = node.getOutputAliasMap();
    boolean isReplace = node.getOutputStrategy() == Lookup.OutputStrategy.REPLACE;

    if (outputMap.isEmpty()) {
      pipe = select(star()).from(joinNode).build();
    } else {
      List<SqlNode> selectItems = new ArrayList<>();
      selectItems.add(new SqlIdentifier(java.util.Arrays.asList(leftAlias, ""), POS));
      for (Map.Entry<String, String> e : outputMap.entrySet()) {
        SqlNode rRef = identifier(rightAlias, e.getKey());
        if (isReplace) {
          selectItems.add(as(rRef, e.getValue()));
        } else {
          selectItems.add(as(call("COALESCE", identifier(leftAlias, e.getValue()), rRef), e.getValue()));
        }
      }
      pipe = select(selectItems.toArray(new SqlNode[0])).from(joinNode).build();
    }
    return pipe;
  }

  // -- US-009: remaining core commands --

  @Override
  public SqlNode visitRareTopN(RareTopN node, Void ctx) {
    Argument.ArgumentMap args = Argument.ArgumentMap.of(node.getArguments());
    String countFieldName = (String) args.get(RareTopN.Option.countField.name()).getValue();
    boolean showCount = (Boolean) args.get(RareTopN.Option.showCount.name()).getValue();
    boolean useNull = (Boolean) args.get(RareTopN.Option.useNull.name()).getValue();
    int k = node.getNoOfResults();
    boolean isTop = node.getCommandType() == RareTopN.CommandType.TOP;

    List<SqlNode> fieldNodes = node.getFields().stream()
        .map(f -> f.accept(this, null)).collect(Collectors.toList());
    List<SqlNode> groupNodes = node.getGroupExprList().stream()
        .map(e -> e.accept(this, null)).collect(Collectors.toList());

    // Step 0: wrap + optional null filter
    pipe = wrapAsSubquery();
    if (!useNull) {
      List<SqlNode> allCols = new ArrayList<>(groupNodes);
      allCols.addAll(fieldNodes);
      SqlNode nullCheck = allCols.stream().map(c -> isNotNull(c))
          .reduce((a, b) -> and(a, b)).orElse(null);
      if (nullCheck != null) {
        pipe = select(star()).from(pipe).where(nullCheck).build();
      }
    }

    // Step 1: GROUP BY with COUNT(*)
    pipe = wrapAsSubquery();
    List<SqlNode> allGroupBy = new ArrayList<>(groupNodes);
    allGroupBy.addAll(fieldNodes);
    List<SqlNode> sel1 = new ArrayList<>(allGroupBy);
    sel1.add(as(countStar(), countFieldName));
    pipe = select(sel1.toArray(new SqlNode[0]))
        .from(pipe).groupBy(allGroupBy.toArray(new SqlNode[0])).build();

    // Step 2: ROW_NUMBER window
    pipe = wrapAsSubquery();
    SqlNode orderDir = isTop ? desc(identifier(countFieldName)) : identifier(countFieldName);
    SqlNodeList partBy = groupNodes.isEmpty() ? SqlNodeList.EMPTY
        : new SqlNodeList(groupNodes, POS);
    SqlNode rn = as(
        window(call("ROW_NUMBER"), partBy, new SqlNodeList(List.of(orderDir), POS)),
        "_rn");
    pipe = select(star(), rn).from(pipe).build();

    // Step 3: filter _rn <= k
    pipe = wrapAsSubquery();
    pipe = select(star()).from(pipe).where(lte(identifier("_rn"), intLiteral(k))).build();

    // Step 4: project final columns
    pipe = wrapAsSubquery();
    List<SqlNode> finalCols = new ArrayList<>(groupNodes);
    finalCols.addAll(fieldNodes);
    if (showCount) finalCols.add(identifier(countFieldName));
    pipe = select(finalCols.toArray(new SqlNode[0])).from(pipe).build();
    return pipe;
  }

  @Override
  public SqlNode visitTrendline(Trendline node, Void ctx) {
    // Build ORDER BY for window frame
    SqlNodeList overOrderBy = SqlNodeList.EMPTY;
    if (node.getSortByField().isPresent()) {
      Field sortField = node.getSortByField().get();
      SqlNode col = sortField.accept(this, null);
      boolean asc = true;
      for (Argument arg : sortField.getFieldArgs()) {
        if ("asc".equals(arg.getArgName())) {
          asc = Boolean.TRUE.equals(((Literal) arg.getValue()).getValue());
        }
      }
      if (!asc) col = desc(col);
      overOrderBy = new SqlNodeList(List.of(col), POS);
    }

    // Build trendline computations as window expressions
    List<SqlNode> windowExprs = new ArrayList<>();
    for (Trendline.TrendlineComputation comp : node.getComputations()) {
      int n = comp.getNumberOfDataPoints();
      SqlNode field = comp.getDataField().accept(this, null);
      SqlNode preceding = SqlWindow.createPreceding(
          SqlLiteral.createExactNumeric(String.valueOf(n - 1), POS), POS);
      SqlNode currentRow = SqlWindow.createCurrentRow(POS);

      SqlNode countCheck = gt(
          windowWithFrame(call("COUNT", star()), SqlNodeList.EMPTY, overOrderBy,
              true, preceding, currentRow),
          intLiteral(n - 1));

      SqlNode expr;
      if (comp.getComputationType() == Trendline.TrendlineType.SMA) {
        SqlNode castField = cast(field, typeSpec(SqlTypeName.DOUBLE));
        SqlNode sumWin = windowWithFrame(call("SUM", castField), SqlNodeList.EMPTY, overOrderBy,
            true, preceding, currentRow);
        SqlNode countWin = cast(
            windowWithFrame(call("COUNT", field), SqlNodeList.EMPTY, overOrderBy,
                true, preceding, currentRow),
            typeSpec(SqlTypeName.DOUBLE));
        expr = caseWhen(List.of(countCheck), List.of(divide(sumWin, countWin)), literal(null));
      } else {
        // WMA: sum(value_i * i) / (n*(n+1)/2)
        List<SqlNode> weightedTerms = new ArrayList<>();
        for (int i = 1; i <= n; i++) {
          SqlNode nthVal = windowWithFrame(
              call("NTH_VALUE", field, intLiteral(i)), SqlNodeList.EMPTY, overOrderBy,
              true, preceding, currentRow);
          weightedTerms.add(times(cast(nthVal, typeSpec(SqlTypeName.DOUBLE)), intLiteral(i)));
        }
        SqlNode wmaSum = weightedTerms.stream().reduce((a, b) -> plus(a, b)).get();
        int denom = n * (n + 1) / 2;
        expr = caseWhen(List.of(countCheck),
            List.of(divide(wmaSum, literal(denom + ".0"))), literal(null));
      }
      windowExprs.add(as(expr, comp.getAlias()));
    }

    // Add null filter for data fields
    List<SqlNode> nullChecks = node.getComputations().stream()
        .map(c -> isNotNull(c.getDataField().accept(this, null)))
        .collect(Collectors.toList());
    SqlNode nullFilter = nullChecks.stream().reduce((a, b) -> and(a, b)).orElse(null);
    if (nullFilter != null) {
      pipe = select(star()).from(wrapAsSubquery()).where(nullFilter).build();
    }

    // SELECT *, trendline_exprs
    List<SqlNode> selectItems = new ArrayList<>();
    selectItems.add(star());
    selectItems.addAll(windowExprs);
    pipe = select(selectItems.toArray(new SqlNode[0])).from(wrapAsSubquery()).build();
    return pipe;
  }

  @Override
  public SqlNode visitWindow(Window node, Void ctx) {
    pipe = wrapAsSubquery();

    // Build PARTITION BY
    List<SqlNode> partCols = new ArrayList<>();
    for (UnresolvedExpression expr : node.getGroupList()) {
      if (expr instanceof Alias && ((Alias) expr).getDelegated() instanceof Span) {
        partCols.add(((Alias) expr).getDelegated().accept(this, null));
      } else if (expr instanceof Alias) {
        partCols.add(((Alias) expr).getDelegated().accept(this, null));
      } else {
        partCols.add(expr.accept(this, null));
      }
    }
    SqlNodeList partBy = partCols.isEmpty() ? SqlNodeList.EMPTY
        : new SqlNodeList(partCols, POS);

    // Null check for bucketNullable=false
    SqlNode nullCheck = null;
    if (!node.isBucketNullable() && !partCols.isEmpty()) {
      nullCheck = partCols.stream().map(c -> isNotNull(c))
          .reduce((a, b) -> and(a, b)).orElse(null);
    }

    // RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    SqlNode unboundedPreceding = SqlWindow.createUnboundedPreceding(POS);
    SqlNode unboundedFollowing = SqlWindow.createUnboundedFollowing(POS);

    List<SqlNode> windowExprs = new ArrayList<>();
    for (UnresolvedExpression item : node.getWindowFunctionList()) {
      Alias alias = (Alias) item;
      WindowFunction wf = (WindowFunction) alias.getDelegated();
      SqlNode aggSql = buildWindowAggSql(wf.getFunction());
      SqlNode winExpr = windowWithFrame(aggSql, partBy, SqlNodeList.EMPTY,
          false, unboundedPreceding, unboundedFollowing);
      if (nullCheck != null) {
        winExpr = caseWhen(List.of(nullCheck), List.of(winExpr), literal(null));
      }
      windowExprs.add(as(winExpr, alias.getName()));
    }

    List<SqlNode> selectItems = new ArrayList<>();
    selectItems.add(star());
    selectItems.addAll(windowExprs);
    pipe = select(selectItems.toArray(new SqlNode[0])).from(pipe).build();
    return pipe;
  }

  @Override
  public SqlNode visitStreamWindow(StreamWindow node, Void ctx) {
    pipe = wrapAsSubquery();

    // Build PARTITION BY
    List<SqlNode> partCols = new ArrayList<>();
    for (UnresolvedExpression expr : node.getGroupList()) {
      if (expr instanceof Alias && ((Alias) expr).getDelegated() instanceof Span) {
        partCols.add(((Alias) expr).getDelegated().accept(this, null));
      } else if (expr instanceof Alias) {
        partCols.add(((Alias) expr).getDelegated().accept(this, null));
      } else {
        partCols.add(expr.accept(this, null));
      }
    }
    SqlNodeList partBy = partCols.isEmpty() ? SqlNodeList.EMPTY
        : new SqlNodeList(partCols, POS);

    // Null check for bucketNullable=false
    SqlNode nullCheck = null;
    if (!node.isBucketNullable() && !partCols.isEmpty()) {
      nullCheck = partCols.stream().map(c -> isNotNull(c))
          .reduce((a, b) -> and(a, b)).orElse(null);
    }

    List<SqlNode> windowExprs = new ArrayList<>();
    for (UnresolvedExpression item : node.getWindowFunctionList()) {
      Alias alias = (Alias) item;
      WindowFunction wf = (WindowFunction) alias.getDelegated();
      SqlNode aggSql = buildWindowAggSql(wf.getFunction());

      // Build frame from WindowFunction's frame
      WindowFrame frame = wf.getWindowFrame();
      SqlNode lower = windowBoundToSqlNode(frame.getLower());
      SqlNode upper = windowBoundToSqlNode(frame.getUpper());
      boolean isRows = frame.getType() == WindowFrame.FrameType.ROWS;

      // Build ORDER BY from sort list if present
      List<SqlNode> ordItems = new ArrayList<>();
      for (org.apache.commons.lang3.tuple.Pair<Sort.SortOption, UnresolvedExpression> p
          : wf.getSortList()) {
        SqlNode col = p.getRight().accept(this, null);
        if (p.getLeft() == Sort.SortOption.DEFAULT_DESC) col = desc(col);
        ordItems.add(col);
      }
      SqlNodeList ordBy = ordItems.isEmpty() ? SqlNodeList.EMPTY
          : new SqlNodeList(ordItems, POS);

      SqlNode winExpr = windowWithFrame(aggSql, partBy, ordBy, isRows, lower, upper);
      if (nullCheck != null) {
        winExpr = caseWhen(List.of(nullCheck), List.of(winExpr), literal(null));
      }
      windowExprs.add(as(winExpr, alias.getName()));
    }

    List<SqlNode> selectItems = new ArrayList<>();
    selectItems.add(star());
    selectItems.addAll(windowExprs);
    pipe = select(selectItems.toArray(new SqlNode[0])).from(pipe).build();
    return pipe;
  }

  private SqlNode windowBoundToSqlNode(WindowBound bound) {
    if (bound instanceof WindowBound.CurrentRowWindowBound) {
      return SqlWindow.createCurrentRow(POS);
    } else if (bound instanceof WindowBound.UnboundedWindowBound) {
      return ((WindowBound.UnboundedWindowBound) bound).isPreceding()
          ? SqlWindow.createUnboundedPreceding(POS) : SqlWindow.createUnboundedFollowing(POS);
    } else if (bound instanceof WindowBound.OffSetWindowBound) {
      WindowBound.OffSetWindowBound ob = (WindowBound.OffSetWindowBound) bound;
      SqlNode offset = SqlLiteral.createExactNumeric(String.valueOf(ob.getOffset()), POS);
      return ob.isPreceding()
          ? SqlWindow.createPreceding(offset, POS) : SqlWindow.createFollowing(offset, POS);
    }
    throw new UnsupportedOperationException("Unknown window bound: " + bound);
  }

  @Override
  public SqlNode visitAppend(Append node, Void ctx) {
    SqlNode mainSql = pipe;
    SqlNode subSql = convertSubPlan(node.getSubSearch());
    pipe = select(star()).from(subquery(unionAll(mainSql, subSql), nextAlias())).build();
    return pipe;
  }

  @Override
  public SqlNode visitBin(Bin node, Void ctx) {
    if (!(node instanceof SpanBin)) {
      throw new UnsupportedOperationException(
          "Unsupported bin type in V4: " + node.getClass().getSimpleName());
    }
    SpanBin spanBin = (SpanBin) node;
    SqlNode field = spanBin.getField().accept(this, null);
    String alias = spanBin.getAlias() != null ? spanBin.getAlias()
        : getFieldName(spanBin.getField());
    UnresolvedExpression spanExpr = spanBin.getSpan();

    SqlNode binSql;
    if (spanExpr instanceof Literal) {
      Literal lit = (Literal) spanExpr;
      if (lit.getType() == DataType.INTEGER || lit.getType() == DataType.DECIMAL) {
        SqlNode span = literal(lit.getValue());
        boolean isInt = lit.getType() == DataType.INTEGER;
        SqlNode start = times(call("FLOOR", divide(field, span)), span);
        SqlNode end = plus(start, span);
        SqlDataTypeSpec intType = typeSpec(SqlTypeName.INTEGER);
        SqlDataTypeSpec varcharType = typeSpec(SqlTypeName.VARCHAR);
        if (isInt) {
          binSql = call("CONCAT",
              call("CONCAT", cast(cast(start, intType), varcharType), literal("-")),
              cast(cast(end, intType), varcharType));
        } else {
          binSql = call("CONCAT",
              call("CONCAT", cast(start, varcharType), literal("-")),
              cast(end, varcharType));
        }
      } else {
        // String literal: time-based or log-based span — delegate to string transpiler approach
        String spanStr = lit.getValue().toString();
        String fieldStr = field.toSqlString(
            org.apache.calcite.sql.dialect.CalciteSqlDialect.DEFAULT).getSql();
        binSql = SqlLiteral.createCharString(
            "UNSUPPORTED_SPAN:" + spanStr, POS); // fallback
        // Use transpileSpanToSqlNode for time/log spans
        binSql = transpileSpanToSqlNode(spanStr, field);
      }
    } else {
      binSql = cast(field, typeSpec(SqlTypeName.VARCHAR));
    }

    // Emit as SELECT *, binSql AS alias
    pipe = select(star(), as(binSql, alias)).from(wrapAsSubquery()).build();
    return pipe;
  }

  private SqlNode transpileSpanToSqlNode(String spanStr, SqlNode field) {
    // Log-based span
    java.util.regex.Matcher logMatcher =
        java.util.regex.Pattern.compile("^(\\d+\\.?\\d*)?log(\\d+)$").matcher(spanStr);
    if (logMatcher.matches()) {
      String coeffStr = logMatcher.group(1);
      double coeff = (coeffStr == null || coeffStr.isEmpty()) ? 1.0 : Double.parseDouble(coeffStr);
      int base = Integer.parseInt(logMatcher.group(2));
      SqlNode logExpr = call("FLOOR",
          divide(call("LN", divide(field, literal(coeff))), call("LN", literal(base))));
      SqlNode start = times(literal(coeff), call("POWER", literal(base), logExpr));
      SqlNode end = times(literal(coeff), call("POWER", literal(base), plus(logExpr, intLiteral(1))));
      SqlDataTypeSpec vc = typeSpec(SqlTypeName.VARCHAR);
      return call("CONCAT", call("CONCAT", cast(start, vc), literal("-")), cast(end, vc));
    }

    // Time-based span
    java.util.regex.Matcher timeMatcher =
        java.util.regex.Pattern.compile("^(\\d+)(\\w+)$").matcher(spanStr);
    if (!timeMatcher.matches()) {
      throw new UnsupportedOperationException("Unsupported span format: " + spanStr);
    }
    int value = Integer.parseInt(timeMatcher.group(1));
    String unit = timeMatcher.group(2);
    String unitLower = unit.equals("M") ? "M" : unit.toLowerCase();

    boolean isMonth = false;
    int secondsPerUnit = 0;
    String truncUnit = null;
    switch (unitLower) {
      case "s": case "sec": case "secs": case "second": case "seconds":
        secondsPerUnit = 1; truncUnit = "SECOND"; break;
      case "m": case "min": case "mins": case "minute": case "minutes":
        secondsPerUnit = 60; truncUnit = "MINUTE"; break;
      case "h": case "hr": case "hrs": case "hour": case "hours":
        secondsPerUnit = 3600; truncUnit = "HOUR"; break;
      case "d": case "day": case "days":
        secondsPerUnit = 86400; truncUnit = "DAY"; break;
      case "w": case "week": case "weeks":
        secondsPerUnit = 604800; truncUnit = "WEEK"; break;
      case "mon": case "month": case "months": case "M":
        isMonth = true; truncUnit = "MONTH"; break;
      case "q": case "qtr": case "qtrs": case "quarter": case "quarters":
        isMonth = true; truncUnit = "QUARTER"; break;
      case "y": case "year": case "years":
        isMonth = true; truncUnit = "YEAR"; break;
      default:
        truncUnit = "SECOND"; break;
    }

    if (isMonth && value == 1) {
      return call("SUBSTRING",
          cast(call("FLOOR", field, identifier(truncUnit)), typeSpec(SqlTypeName.VARCHAR)),
          intLiteral(1), intLiteral(7));
    }
    if (value == 1 && secondsPerUnit > 0) {
      return call("FLOOR", field, identifier(truncUnit));
    }
    if (secondsPerUnit > 0) {
      long totalSeconds = (long) value * secondsPerUnit;
      SqlNode epoch = call("TIMESTAMPDIFF",
          identifier("SECOND"), literal("1970-01-01 00:00:00"), field);
      SqlNode floored = times(call("FLOOR",
          divide(epoch, literal(totalSeconds))), literal(totalSeconds));
      return call("TIMESTAMPADD",
          identifier("SECOND"), cast(floored, typeSpec(SqlTypeName.INTEGER)),
          literal("1970-01-01 00:00:00"));
    }
    // Fallback for sub-second or multi-month
    return call("FLOOR", field, identifier("SECOND"));
  }

  @Override
  public SqlNode visitReplace(Replace node, Void ctx) {
    pipe = wrapAsSubquery();
    List<SqlNode> selectItems = new ArrayList<>();
    selectItems.add(star());
    for (Field field : node.getFieldList()) {
      String fieldName = field.getField().toString();
      SqlNode expr = identifier(fieldName);
      for (ReplacePair pair : node.getReplacePairs()) {
        String pattern = pair.getPattern().getValue().toString();
        String replacement = pair.getReplacement().getValue().toString();
        if (WildcardUtils.containsWildcard(pattern) || WildcardUtils.containsWildcard(replacement)) {
          WildcardUtils.validateWildcardSymmetry(pattern, replacement);
          String regexPattern = WildcardUtils.convertWildcardPatternToRegex(pattern);
          String regexReplacement = WildcardUtils.convertWildcardReplacementToRegex(replacement);
          expr = call("REGEXP_REPLACE", expr, literal(regexPattern), literal(regexReplacement));
        } else {
          expr = call("REPLACE", expr, literal(pattern), literal(replacement));
        }
      }
      selectItems.add(as(expr, fieldName));
    }
    pipe = select(selectItems.toArray(new SqlNode[0])).from(pipe).build();
    return pipe;
  }

  @Override
  public SqlNode visitRename(Rename node, Void ctx) {
    pipe = wrapAsSubquery();
    LinkedHashMap<String, String> mappings = new LinkedHashMap<>();
    for (org.opensearch.sql.ast.expression.Map mapping : node.getRenameList()) {
      String origin = getFieldName(mapping.getOrigin());
      String target = getFieldName(mapping.getTarget());
      // Handle chained renames
      String realOrigin = null;
      for (Map.Entry<String, String> prev : mappings.entrySet()) {
        if (prev.getValue().equals(origin)) { realOrigin = prev.getKey(); break; }
      }
      if (realOrigin != null) mappings.put(realOrigin, target);
      else mappings.put(origin, target);
    }
    mappings.entrySet().removeIf(e -> e.getKey().equals(e.getValue()));
    if (mappings.isEmpty()) return pipe;

    // Wildcard case: SELECT *, old AS new
    List<SqlNode> selectItems = new ArrayList<>();
    selectItems.add(star());
    for (Map.Entry<String, String> e : mappings.entrySet()) {
      selectItems.add(as(identifier(e.getKey()), e.getValue()));
    }
    pipe = select(selectItems.toArray(new SqlNode[0])).from(pipe).build();
    return pipe;
  }

  @Override
  public SqlNode visitFillNull(FillNull node, Void ctx) {
    pipe = wrapAsSubquery();
    List<org.apache.commons.lang3.tuple.Pair<Field, UnresolvedExpression>> pairs =
        node.getReplacementPairs();
    if (!pairs.isEmpty()) {
      List<SqlNode> selectItems = new ArrayList<>();
      selectItems.add(star());
      for (org.apache.commons.lang3.tuple.Pair<Field, UnresolvedExpression> pair : pairs) {
        String fieldName = pair.getLeft().getField().toString();
        SqlNode value = pair.getRight().accept(this, null);
        selectItems.add(as(call("COALESCE", identifier(fieldName), value), fieldName));
      }
      pipe = select(selectItems.toArray(new SqlNode[0])).from(pipe).build();
    }
    // replacementForAll case is handled by DynamicPPLToSqlNodeConverter (needs schema)
    return pipe;
  }

  @Override
  public SqlNode visitParse(Parse node, Void ctx) {
    if (node.getParseMethod() != ParseMethod.REGEX) {
      throw new UnsupportedOperationException(
          "Unsupported PPL command: Parse (" + node.getParseMethod() + ")");
    }
    SqlNode sourceField = node.getSourceField().accept(this, null);
    String pattern = ((Literal) node.getPattern()).getValue().toString();

    // Extract named group names
    java.util.regex.Pattern namedGroupPattern =
        java.util.regex.Pattern.compile("\\(\\?<([a-zA-Z][a-zA-Z0-9]*)>");
    java.util.regex.Matcher matcher = namedGroupPattern.matcher(pattern);
    List<String> groupNames = new ArrayList<>();
    while (matcher.find()) groupNames.add(matcher.group(1));

    pipe = wrapAsSubquery();
    List<SqlNode> selectItems = new ArrayList<>();
    selectItems.add(star());
    for (int i = 0; i < groupNames.size(); i++) {
      // Build pattern with only target group as capturing, others as non-capturing
      String singleGroupPattern = pattern;
      for (int j = 0; j < groupNames.size(); j++) {
        String gn = groupNames.get(j);
        if (j == i) {
          singleGroupPattern = singleGroupPattern.replace("(?<" + gn + ">", "(");
        } else {
          singleGroupPattern = singleGroupPattern.replace("(?<" + gn + ">", "(?:");
        }
      }
      SqlNode regexExpr = call("COALESCE",
          call("REGEXP_EXTRACT", sourceField, literal(singleGroupPattern)),
          literal(""));
      selectItems.add(as(regexExpr, groupNames.get(i)));
    }
    pipe = select(selectItems.toArray(new SqlNode[0])).from(pipe).build();
    return pipe;
  }

  /** Build SQL for an aggregate function inside a window expression (eventstats/streamstats). */
  private SqlNode buildWindowAggSql(UnresolvedExpression func) {
    if (func instanceof AggregateFunction) return func.accept(this, null);
    if (!(func instanceof Function)) return func.accept(this, null);
    Function f = (Function) func;
    String name = f.getFuncName().toLowerCase();
    List<UnresolvedExpression> args = f.getFuncArgs();

    if ("count".equals(name)) {
      if (args.isEmpty() || (args.size() == 1 && args.get(0) instanceof AllFields))
        return countStar();
      return count(args.get(0).accept(this, null));
    }
    if ("dc".equals(name) || "distinct_count".equals(name))
      return count(distinct(args.get(0).accept(this, null)));
    if ("avg".equals(name))
      return agg("AVG", cast(args.get(0).accept(this, null), typeSpec(SqlTypeName.DOUBLE)));
    if ("var_samp".equals(name))
      return agg("VAR_SAMP", cast(args.get(0).accept(this, null), typeSpec(SqlTypeName.DOUBLE)));
    if ("var_pop".equals(name))
      return agg("VAR_POP", cast(args.get(0).accept(this, null), typeSpec(SqlTypeName.DOUBLE)));
    if ("stddev_samp".equals(name))
      return agg("STDDEV_SAMP", cast(args.get(0).accept(this, null), typeSpec(SqlTypeName.DOUBLE)));
    if ("stddev_pop".equals(name))
      return agg("STDDEV_POP", cast(args.get(0).accept(this, null), typeSpec(SqlTypeName.DOUBLE)));
    if ("first".equals(name)) return call("PPL_FIRST", args.get(0).accept(this, null));
    if ("last".equals(name)) return call("PPL_LAST", args.get(0).accept(this, null));

    String sqlName = FUNC_MAP.getOrDefault(name, name.toUpperCase());
    if (args.isEmpty()) {
      return SQL_DATETIME_KEYWORDS.contains(sqlName) ? identifier(sqlName) : call(sqlName);
    }
    SqlNode[] sqlArgs = args.stream().map(a -> a.accept(this, null)).toArray(SqlNode[]::new);
    return call(sqlName, sqlArgs);
  }

  private static String getFieldName(UnresolvedExpression expr) {
    if (expr instanceof Field) return ((Field) expr).getField().toString();
    if (expr instanceof QualifiedName) return ((QualifiedName) expr).toString();
    return expr.toString();
  }

  private SqlNode resolveJoinRight(UnresolvedPlan plan, String alias) {
    if (plan instanceof SubqueryAlias) {
      SubqueryAlias sa = (SubqueryAlias) plan;
      String saAlias = alias != null ? alias : sa.getAlias();
      UnresolvedPlan child = (UnresolvedPlan) sa.getChild().get(0);
      Relation rel = extractRelation(child);
      if (rel != null) {
        return subquery(select(star()).from(table(rel.getTableQualifiedName().toString())).build(), saAlias);
      }
      return subquery(convertSubPlan(child), saAlias);
    }
    Relation rel = extractRelation(plan);
    if (rel != null) {
      String tableName = rel.getTableQualifiedName().toString();
      return alias != null
          ? subquery(select(star()).from(table(tableName)).build(), alias)
          : table(tableName);
    }
    SqlNode subSql = convertSubPlan(plan);
    return alias != null ? subquery(subSql, alias) : subquery(subSql, nextAlias());
  }

  private static Relation extractRelation(UnresolvedPlan plan) {
    if (plan instanceof Relation) return (Relation) plan;
    if (plan instanceof Project) {
      Project proj = (Project) plan;
      if (proj.getProjectList().size() == 1
          && proj.getProjectList().get(0) instanceof AllFields
          && !proj.getChild().isEmpty()) {
        UnresolvedPlan child = (UnresolvedPlan) proj.getChild().get(0);
        if (child instanceof Relation) return (Relation) child;
      }
    }
    return null;
  }

  private SqlNode buildFieldListCondition(List<Field> fields, String leftAlias, String rightAlias) {
    SqlNode result = null;
    for (Field f : fields) {
      String fname = f.getField().toString();
      SqlNode lRef = leftAlias != null ? identifier(leftAlias, fname) : identifier(fname);
      SqlNode rRef = rightAlias != null ? identifier(rightAlias, fname) : identifier(fname);
      SqlNode cond = eq(lRef, rRef);
      result = result == null ? cond : and(result, cond);
    }
    return result;
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
