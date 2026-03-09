/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.antlr.v4.runtime.tree.ParseTree;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.AggregateFunction;
import org.opensearch.sql.ast.expression.Alias;
import org.opensearch.sql.ast.expression.AllFields;
import org.opensearch.sql.ast.expression.And;
import org.opensearch.sql.ast.expression.Argument;
import org.opensearch.sql.ast.expression.Between;
import org.opensearch.sql.ast.expression.Case;
import org.opensearch.sql.ast.expression.Cast;
import org.opensearch.sql.ast.expression.Compare;
import org.opensearch.sql.ast.expression.DataType;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.Function;
import org.opensearch.sql.ast.expression.In;
import org.opensearch.sql.ast.expression.Interval;
import org.opensearch.sql.ast.expression.Let;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.Not;
import org.opensearch.sql.ast.expression.ParseMethod;
import org.opensearch.sql.ast.expression.Or;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.expression.SearchAnd;
import org.opensearch.sql.ast.expression.SearchComparison;
import org.opensearch.sql.ast.expression.SearchExpression;
import org.opensearch.sql.ast.expression.SearchGroup;
import org.opensearch.sql.ast.expression.SearchLiteral;
import org.opensearch.sql.ast.expression.SearchNot;
import org.opensearch.sql.ast.expression.SearchOr;
import org.opensearch.sql.ast.expression.Span;
import org.opensearch.sql.ast.expression.SpanUnit;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.ast.expression.When;
import org.opensearch.sql.ast.expression.Xor;
import org.opensearch.sql.ast.expression.subquery.ExistsSubquery;
import org.opensearch.sql.ast.expression.subquery.InSubquery;
import org.opensearch.sql.ast.expression.subquery.ScalarSubquery;
import org.opensearch.sql.ast.Node;
import org.opensearch.sql.ast.statement.Query;
import org.opensearch.sql.ast.statement.Statement;
import org.opensearch.sql.ast.expression.WindowFunction;
import org.opensearch.sql.ast.tree.Aggregation;
import org.opensearch.sql.ast.tree.Append;
import org.opensearch.sql.ast.tree.AppendPipe;
import org.opensearch.sql.ast.tree.Dedupe;
import org.opensearch.sql.ast.tree.Eval;
import org.opensearch.sql.ast.tree.FillNull;
import org.opensearch.sql.ast.tree.Filter;
import org.opensearch.sql.ast.tree.Head;
import org.opensearch.sql.ast.tree.Project;
import org.opensearch.sql.ast.tree.Join;
import org.opensearch.sql.ast.tree.Lookup;
import org.opensearch.sql.ast.tree.Parse;
import org.opensearch.sql.ast.tree.RareTopN;
import org.opensearch.sql.ast.tree.Relation;
import org.opensearch.sql.ast.tree.Rename;
import org.opensearch.sql.ast.tree.Search;
import org.opensearch.sql.ast.tree.Sort;
import org.opensearch.sql.ast.tree.SubqueryAlias;
import org.opensearch.sql.ast.tree.Trendline;
import org.opensearch.sql.ast.tree.Window;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.ppl.antlr.PPLSyntaxParser;
import org.opensearch.sql.ppl.parser.AstBuilder;
import org.opensearch.sql.ppl.parser.AstStatementBuilder;

/**
 * Pure PPL-to-SQL transpiler. Parses a PPL query string, walks the AST, and produces an equivalent
 * SQL string.
 */
public class PPLToSqlTranspiler extends AbstractNodeVisitor<String, Void> {

  private static final Map<String, String> FUNC_MAP = new HashMap<>();
  private static int subqueryCounter = 0;
  private int evalCounter = 0;
  /** Current SelectBuilder — used by visitQualifiedName to check join context. */
  private SelectBuilder currentSb;

  /** Default settings for the PPL parser — provides safe defaults for all settings. */
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

  static {
    FUNC_MAP.put("abs", "ABS");
    FUNC_MAP.put("ceil", "CEIL");
    FUNC_MAP.put("floor", "FLOOR");
    FUNC_MAP.put("round", "ROUND");
    FUNC_MAP.put("sqrt", "SQRT");
    FUNC_MAP.put("pow", "POWER");
    FUNC_MAP.put("ln", "LN");
    FUNC_MAP.put("log10", "LOG10");
    FUNC_MAP.put("exp", "EXP");
    FUNC_MAP.put("upper", "UPPER");
    FUNC_MAP.put("lower", "LOWER");
    FUNC_MAP.put("length", "CHAR_LENGTH");
    FUNC_MAP.put("substring", "SUBSTRING");
    FUNC_MAP.put("trim", "TRIM");
    FUNC_MAP.put("replace", "REPLACE");
    FUNC_MAP.put("concat", "CONCAT");
    FUNC_MAP.put("coalesce", "COALESCE");
    FUNC_MAP.put("count", "COUNT");
    FUNC_MAP.put("sum", "SUM");
    FUNC_MAP.put("avg", "AVG");
    FUNC_MAP.put("min", "MIN");
    FUNC_MAP.put("max", "MAX");
    // Math functions
    FUNC_MAP.put("power", "POWER");
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
    FUNC_MAP.put("mod", "MOD"); // handled as special case with zero-guard
    FUNC_MAP.put("truncate", "TRUNCATE");
    FUNC_MAP.put("rand", "RAND");
    FUNC_MAP.put("ceiling", "CEIL");
    // String functions
    FUNC_MAP.put("left", "LEFT");
    FUNC_MAP.put("right", "RIGHT");
    FUNC_MAP.put("reverse", "REVERSE");
    FUNC_MAP.put("ltrim", "LTRIM");
    FUNC_MAP.put("rtrim", "RTRIM");
    FUNC_MAP.put("position", "POSITION");
    FUNC_MAP.put("locate", "LOCATE");
    FUNC_MAP.put("ascii", "ASCII");
    FUNC_MAP.put("char_length", "CHAR_LENGTH");
    FUNC_MAP.put("character_length", "CHAR_LENGTH");
    FUNC_MAP.put("octet_length", "OCTET_LENGTH");
    FUNC_MAP.put("bit_length", "BIT_LENGTH");
    // Condition/null functions
    FUNC_MAP.put("nullif", "NULLIF");
    FUNC_MAP.put("typeof", "TYPEOF");
    // Date/time functions
    FUNC_MAP.put("now", "NOW");
    FUNC_MAP.put("curdate", "CURRENT_DATE");
    FUNC_MAP.put("current_date", "CURRENT_DATE");
    FUNC_MAP.put("curtime", "CURRENT_TIME");
    FUNC_MAP.put("current_time", "CURRENT_TIME");
    FUNC_MAP.put("current_timestamp", "CURRENT_TIMESTAMP");
    FUNC_MAP.put("localtime", "LOCALTIME");
    FUNC_MAP.put("localtimestamp", "LOCALTIMESTAMP");
  }

  /** Transpile a PPL query string to SQL. */
  public static String transpile(String ppl) {
    PPLSyntaxParser parser = new PPLSyntaxParser();
    ParseTree cst = parser.parse(ppl);
    AstBuilder astBuilder = new AstBuilder(ppl, DEFAULT_SETTINGS);
    AstStatementBuilder stmtBuilder =
        new AstStatementBuilder(
            astBuilder, AstStatementBuilder.StatementBuilderContext.builder().build());
    Statement stmt = stmtBuilder.visit(cst);
    Query query = (Query) stmt;
    UnresolvedPlan plan = query.getPlan();

    // Flatten the AST chain from leaf (Relation) outward
    List<UnresolvedPlan> nodes = new ArrayList<>();
    flatten(plan, nodes);

    PPLToSqlTranspiler transpiler = new PPLToSqlTranspiler();
    SelectBuilder sb = new SelectBuilder();
    transpiler.currentSb = sb;

    for (UnresolvedPlan node : nodes) {
      if (node instanceof Relation) {
        Relation rel = (Relation) node;
        String tableName = rel.getTableQualifiedName().toString();
        sb.from = quoteId(tableName);
        sb.tableAliases.add(tableName);
      } else if (node instanceof Filter) {
        transpiler.processFilter((Filter) node, sb);
      } else if (node instanceof Aggregation) {
        transpiler.processAggregation((Aggregation) node, sb);
      } else if (node instanceof Project) {
        transpiler.processProject((Project) node, sb);
      } else if (node instanceof Sort) {
        transpiler.processSort((Sort) node, sb);
      } else if (node instanceof Head) {
        transpiler.processHead((Head) node, sb);
      } else if (node instanceof Eval) {
        transpiler.processEval((Eval) node, sb);
      } else if (node instanceof Rename) {
        transpiler.processRename((Rename) node, sb);
      } else if (node instanceof Dedupe) {
        transpiler.processDedupe((Dedupe) node, sb);
      } else if (node instanceof RareTopN) {
        transpiler.processRareTopN((RareTopN) node, sb);
      } else if (node instanceof SubqueryAlias) {
        SubqueryAlias alias = (SubqueryAlias) node;
        sb.from = sb.from + " " + quoteId(alias.getAlias());
        sb.tableAliases.add(alias.getAlias());
      } else if (node instanceof Join) {
        transpiler.processJoin((Join) node, sb);
      } else if (node instanceof Window) {
        transpiler.processWindow((Window) node, sb);
      } else if (node instanceof Trendline) {
        transpiler.processTrendline((Trendline) node, sb);
      } else if (node instanceof FillNull) {
        transpiler.processFillNull((FillNull) node, sb);
      } else if (node instanceof Parse) {
        transpiler.processParse((Parse) node, sb);
      } else if (node instanceof Lookup) {
        transpiler.processLookup((Lookup) node, sb);
      } else if (node instanceof Append) {
        transpiler.processAppend((Append) node, sb);
      } else if (node instanceof AppendPipe) {
        transpiler.processAppendPipe((AppendPipe) node, sb);
      } else if (node instanceof Search) {
        Search search = (Search) node;
        String cond = visitSearchExpr(search.getOriginalExpression());
        if (sb.where == null) sb.where = cond;
        else sb.where = "(" + sb.where + ") AND (" + cond + ")";
      } else {
        throw new UnsupportedOperationException(
            "Unsupported PPL command: " + node.getClass().getSimpleName());
      }
    }

    return sb.build();
  }

  /** Flatten AST linked list from outermost to leaf, then reverse so leaf (Relation) is first. */
  private static void flatten(UnresolvedPlan node, List<UnresolvedPlan> out) {
    List<? extends Node> children = node.getChild();
    if (!children.isEmpty()) {
      flatten((UnresolvedPlan) children.get(0), out);
    }
    out.add(node);
  }

  // --- Plan node processors ---

  private void processFilter(Filter node, SelectBuilder sb) {
    String cond = visitExpr(node.getCondition());
    boolean hasComputedColumns = sb.select.size() > 1
        || (sb.select.size() == 1 && !"*".equals(sb.select.get(0)));
    if (sb.hasGroupBy || hasComputedColumns) {
      Map<String, String> savedComputed = new HashMap<>(sb.computedColumns);
      sb.wrapAsSubquery();
      sb.computedColumns.putAll(savedComputed);
    }
    // Resolve computedColumn references in the condition
    for (Map.Entry<String, String> entry : sb.computedColumns.entrySet()) {
      cond = cond.replaceAll("(?i)\\b" + java.util.regex.Pattern.quote(entry.getKey()) + "\\b", entry.getValue());
    }
    if (sb.where == null) {
      sb.where = cond;
    } else {
      sb.where = sb.where + " AND " + cond;
    }
  }

  private void processAggregation(Aggregation node, SelectBuilder sb) {
    // If current select has computed columns (e.g. from rename/eval), or pending
    // ORDER BY/LIMIT, wrap first to snapshot the current state
    boolean hasComputedColumns = sb.select.size() > 1
        || (sb.select.size() == 1 && !"*".equals(sb.select.get(0)));
    if (hasComputedColumns || sb.orderBy != null || sb.limit != null) {
      sb.wrapAsSubquery();
    }
    List<String> selectItems = new ArrayList<>();
    for (UnresolvedExpression expr : node.getAggExprList()) {
      selectItems.add(visitExpr(expr));
    }
    List<String> groupSelect = new ArrayList<>();
    List<String> groupBy = new ArrayList<>();
    for (UnresolvedExpression expr : node.getGroupExprList()) {
      String selectSql = visitExpr(expr);
      String groupBySql;
      if (expr instanceof Alias) {
        groupBySql = visitExpr(((Alias) expr).getDelegated());
      } else {
        groupBySql = visitExpr(expr);
      }
      // Resolve computedColumns (e.g. from parse command) in group-by fields
      String fieldName = groupBySql;
      if (sb.computedColumns.containsKey(fieldName)) {
        String resolved = sb.computedColumns.remove(fieldName);
        groupBySql = resolved;
        selectSql = resolved + " AS " + quoteId(fieldName);
      }
      // Fix C: Preserve qualified names (e.g. b.country) in output schema
      // Only for 2-part names like "b.country" (table alias + column), not nested paths
      if (!selectSql.contains(" AS ") && !selectSql.contains("(") && !selectSql.startsWith("\"")) {
        int dotIdx = selectSql.indexOf('.');
        if (dotIdx > 0 && selectSql.indexOf('.', dotIdx + 1) < 0) {
          // Exactly one dot — likely a table.column reference
          selectSql = selectSql + " AS " + quoteId(selectSql);
        }
      }
      groupSelect.add(selectSql);
      groupBy.add(groupBySql);
    }
    // Handle span — stored separately from groupExprList
    String spanSelect = null;
    if (node.getSpan() != null) {
      spanSelect = visitExpr(node.getSpan());
      Alias spanAlias = (Alias) node.getSpan();
      groupBy.add(visitExpr(spanAlias.getDelegated()));
    }
    // PPL convention: aggregates first, then span (if any), then other group-by fields
    List<String> finalSelect = new ArrayList<>(selectItems);
    if (spanSelect != null) { finalSelect.add(spanSelect); }
    finalSelect.addAll(groupSelect);
    sb.select = finalSelect;
    if (!groupBy.isEmpty()) {
      sb.groupBy = groupBy;
      sb.hasGroupBy = true;
    }
  }

  private void processWindow(Window node, SelectBuilder sb) {
    // Wrap as subquery if there are pending computed columns, ORDER BY, or LIMIT
    boolean hasComputedColumns = sb.select.size() > 1
        || (sb.select.size() == 1 && !"*".equals(sb.select.get(0)));
    if (hasComputedColumns || sb.orderBy != null || sb.limit != null) {
      sb.wrapAsSubquery();
    }

    // Build PARTITION BY clause from groupList
    List<String> partitionCols = new ArrayList<>();
    String spanAlias = null;
    String spanExpr = null;
    for (UnresolvedExpression expr : node.getGroupList()) {
      if (expr instanceof Alias && ((Alias) expr).getDelegated() instanceof Span) {
        Alias alias = (Alias) expr;
        spanAlias = alias.getName();
        spanExpr = visitExpr(alias.getDelegated());
        partitionCols.add(spanExpr);
      } else if (expr instanceof Alias) {
        partitionCols.add(visitExpr(((Alias) expr).getDelegated()));
      } else {
        partitionCols.add(visitExpr(expr));
      }
    }

    String partitionClause = partitionCols.isEmpty() ? ""
        : "PARTITION BY " + String.join(", ", partitionCols) + " ";
    String overClause = " OVER (" + partitionClause
        + "RANGE BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING)";

    // Build null check for bucketNullable=false
    String nullCheck = null;
    if (!node.isBucketNullable() && !partitionCols.isEmpty()) {
      List<String> checks = new ArrayList<>();
      for (String col : partitionCols) {
        checks.add(col + " IS NOT NULL");
      }
      nullCheck = String.join(" AND ", checks);
    }

    // Build window function expressions
    List<String> windowExprs = new ArrayList<>();
    for (UnresolvedExpression item : node.getWindowFunctionList()) {
      Alias alias = (Alias) item;
      String aliasName = alias.getName();
      WindowFunction wf = (WindowFunction) alias.getDelegated();
      String aggSql = buildWindowAggSql(wf.getFunction());
      String windowExpr = aggSql + overClause;
      if (nullCheck != null) {
        windowExpr = "CASE WHEN " + nullCheck + " THEN " + windowExpr + " ELSE NULL END";
      }
      windowExprs.add(windowExpr + " AS " + quoteId(aliasName));
    }

    // Build final SELECT: keep *, add span column if present, add window expressions
    List<String> finalSelect = new ArrayList<>();
    finalSelect.add("*");
    if (spanAlias != null) {
      finalSelect.add(spanExpr + " AS " + quoteId(spanAlias));
    }
    finalSelect.addAll(windowExprs);
    sb.select = finalSelect;
  }

  private void processTrendline(Trendline node, SelectBuilder sb) {
    // Handle optional sort - build ORDER BY clause for both OVER and final ORDER BY
    String overOrderBy = "";
    if (node.getSortByField().isPresent()) {
      Field sortField = node.getSortByField().get();
      String name = quoteId(getFieldName(sortField));
      String dir = "";
      for (Argument arg : sortField.getFieldArgs()) {
        if ("asc".equals(arg.getArgName())) {
          dir = Boolean.TRUE.equals(((Literal) arg.getValue()).getValue()) ? " ASC" : " DESC";
        }
      }
      overOrderBy = "ORDER BY " + name + dir + " ";
      sb.orderBy = new ArrayList<>();
      sb.orderBy.add(name + dir);
    }

    // Build null pre-filter for each computation's data field
    for (Trendline.TrendlineComputation comp : node.getComputations()) {
      String nullCond = quoteId(getFieldName(comp.getDataField())) + " IS NOT NULL";
      if (sb.where == null) sb.where = nullCond;
      else sb.where = sb.where + " AND " + nullCond;
    }

    // Store window expressions as computed columns for later inlining by Project
    for (Trendline.TrendlineComputation comp : node.getComputations()) {
      int n = comp.getNumberOfDataPoints();
      String field = quoteId(getFieldName(comp.getDataField()));
      String alias = comp.getAlias();
      String frame = overOrderBy + "ROWS BETWEEN " + (n - 1) + " PRECEDING AND CURRENT ROW";
      String countCheck = "COUNT(*) OVER (" + frame + ") > " + (n - 1);

      String expr;
      if (comp.getComputationType() == Trendline.TrendlineType.SMA) {
        expr = "CASE WHEN " + countCheck
            + " THEN SUM(CAST(" + field + " AS DOUBLE)) OVER (" + frame + ")"
            + " / CAST(COUNT(" + field + ") OVER (" + frame + ") AS DOUBLE)"
            + " ELSE NULL END";
      } else {
        StringBuilder wmaSum = new StringBuilder();
        for (int i = 1; i <= n; i++) {
          if (i > 1) wmaSum.append(" + ");
          wmaSum.append("CAST(NTH_VALUE(" + field + ", " + i + ") OVER (" + frame + ") AS DOUBLE) * " + i);
        }
        int denominator = n * (n + 1) / 2;
        expr = "CASE WHEN " + countCheck
            + " THEN (" + wmaSum + ") / " + denominator + ".0"
            + " ELSE NULL END";
      }
      sb.computedColumns.put(alias, expr);
    }
  }

  private void processFillNull(FillNull node, SelectBuilder sb) {
    List<org.apache.commons.lang3.tuple.Pair<Field, UnresolvedExpression>> pairs = node.getReplacementPairs();
    if (!pairs.isEmpty()) {
      for (org.apache.commons.lang3.tuple.Pair<Field, UnresolvedExpression> pair : pairs) {
        String fieldName = getFieldName(pair.getLeft());
        String value = visitExpr(pair.getRight());
        sb.computedColumns.put(fieldName, "COALESCE(" + quoteId(fieldName) + ", " + value + ")");
      }
    } else if (node.getReplacementForAll().isPresent()) {
      String value = visitExpr(node.getReplacementForAll().get());
      if (sb.select.size() == 1 && "*".equals(sb.select.get(0))) {
        return;
      }
      List<String> newSelect = new ArrayList<>();
      for (String col : sb.select) {
        String unquoted = col.startsWith("\"") && col.endsWith("\"") ? col.substring(1, col.length() - 1) : col;
        newSelect.add("COALESCE(" + col + ", " + value + ") AS " + quoteId(unquoted));
      }
      sb.select = newSelect;
    }
  }

  private void processParse(Parse node, SelectBuilder sb) {
    if (node.getParseMethod() != ParseMethod.REGEX) {
      throw new UnsupportedOperationException(
          "Unsupported PPL command: Parse (" + node.getParseMethod() + ")");
    }
    String sourceField = visitExpr(node.getSourceField());
    String pattern = ((Literal) node.getPattern()).getValue().toString();

    // Extract named group names in order
    java.util.regex.Pattern namedGroupPattern =
        java.util.regex.Pattern.compile("\\(\\?<([a-zA-Z][a-zA-Z0-9]*)>");
    java.util.regex.Matcher matcher = namedGroupPattern.matcher(pattern);
    List<String> groupNames = new ArrayList<>();
    while (matcher.find()) {
      groupNames.add(matcher.group(1));
    }

    String sqlPattern = pattern.replace("'", "''");
    for (int i = 0; i < groupNames.size(); i++) {
      String groupName = groupNames.get(i);
      // Calcite REGEXP_EXTRACT doesn't support multiple capturing groups.
      // Build a pattern with only the target group as capturing, others as non-capturing.
      String singleGroupPattern = sqlPattern;
      for (int j = 0; j < groupNames.size(); j++) {
        String gn = groupNames.get(j);
        if (j == i) {
          // Keep this group as capturing but remove the name: (?<name>...) -> (...)
          singleGroupPattern = singleGroupPattern.replace("(?<" + gn + ">", "(");
        } else {
          // Convert to non-capturing: (?<name>...) -> (?:...)
          singleGroupPattern = singleGroupPattern.replace("(?<" + gn + ">", "(?:");
        }
      }
      String expr = "COALESCE(REGEXP_EXTRACT(" + sourceField + ", '" + singleGroupPattern + "'), '')";
      sb.computedColumns.put(groupName, expr);
      sb.deferredColumns.put(groupName, expr);
    }
  }

  /**
   * Build SQL for an aggregate function inside a window expression.
   * The PPL parser wraps eventstats aggregates as Function (not AggregateFunction),
   * so we must apply aggregate-specific mappings manually.
   */
  private String buildWindowAggSql(UnresolvedExpression func) {
    if (func instanceof AggregateFunction) {
      return visitExpr(func);
    }
    if (!(func instanceof Function)) {
      return visitExpr(func);
    }
    Function f = (Function) func;
    String name = f.getFuncName().toLowerCase();
    List<UnresolvedExpression> args = f.getFuncArgs();

    if ("count".equals(name)) {
      if (args.isEmpty() || (args.size() == 1 && args.get(0) instanceof AllFields)) {
        return "COUNT(*)";
      }
      return "COUNT(" + visitExpr(args.get(0)) + ")";
    }
    if ("dc".equals(name) || "distinct_count".equals(name)) {
      return "COUNT(DISTINCT " + visitExpr(args.get(0)) + ")";
    }
    if ("avg".equals(name)) {
      return "AVG(CAST(" + visitExpr(args.get(0)) + " AS DOUBLE))";
    }
    if ("var_samp".equals(name)) {
      return "VAR_SAMP(CAST(" + visitExpr(args.get(0)) + " AS DOUBLE))";
    }
    if ("var_pop".equals(name)) {
      return "VAR_POP(CAST(" + visitExpr(args.get(0)) + " AS DOUBLE))";
    }
    if ("stddev_samp".equals(name)) {
      return "STDDEV_SAMP(CAST(" + visitExpr(args.get(0)) + " AS DOUBLE))";
    }
    if ("stddev_pop".equals(name)) {
      return "STDDEV_POP(CAST(" + visitExpr(args.get(0)) + " AS DOUBLE))";
    }
    if ("earliest".equals(name)) {
      return "ARG_MIN(" + visitExpr(args.get(0)) + ", " + quoteId("@timestamp") + ")";
    }
    if ("latest".equals(name)) {
      return "ARG_MAX(" + visitExpr(args.get(0)) + ", " + quoteId("@timestamp") + ")";
    }
    if ("first".equals(name)) {
      return "PPL_FIRST(" + visitExpr(args.get(0)) + ")";
    }
    if ("last".equals(name)) {
      return "PPL_LAST(" + visitExpr(args.get(0)) + ")";
    }

    String sqlName = FUNC_MAP.getOrDefault(name, name.toUpperCase());
    if (args.isEmpty()) {
      return sqlName + "()";
    }
    String argsSql = args.stream().map(this::visitExpr).collect(Collectors.joining(", "));
    return sqlName + "(" + argsSql + ")";
  }

  private void processProject(Project node, SelectBuilder sb) {
    List<UnresolvedExpression> projectList = node.getProjectList();
    // Top-level Project(AllFields) is a passthrough
    if (projectList.size() == 1 && projectList.get(0) instanceof AllFields) {
      return;
    }

    // Handle fields - exclusion: remove excluded fields from current select list
    if (node.isExcluded()) {
      java.util.Set<String> excludedNames = new java.util.HashSet<>();
      for (UnresolvedExpression expr : projectList) {
        excludedNames.add(getFieldName(expr));
      }
      boolean hasExplicitColumns = sb.select.size() > 1
          || (sb.select.size() == 1 && !"*".equals(sb.select.get(0)));
      if (hasExplicitColumns) {
        sb.select.removeIf(col -> {
          String name = col;
          int asIdx = col.toUpperCase().lastIndexOf(" AS ");
          if (asIdx >= 0) {
            name = col.substring(asIdx + 4).trim();
          }
          if (name.startsWith("\"") && name.endsWith("\"")) {
            name = name.substring(1, name.length() - 1);
          }
          return excludedNames.contains(name);
        });
      } else {
        String exceptCols = excludedNames.stream()
            .map(n -> "\"" + n.replace("\"", "\"\"") + "\"")
            .collect(Collectors.joining(", "));
        sb.select.set(0, "* EXCEPT(" + exceptCols + ")");
      }
      return;
    }

    // If current select has computed columns (not just [*]), wrap as subquery first
    // so that eval/aggregation columns are preserved
    boolean hasComputedColumns = sb.select.size() > 1 || (sb.select.size() == 1 && !"*".equals(sb.select.get(0)));
    if (hasComputedColumns) {
      Map<String, String> savedComputed = new HashMap<>(sb.computedColumns);
      java.util.LinkedHashMap<String, String> savedRenames = new java.util.LinkedHashMap<>(sb.renames);
      sb.wrapAsSubquery();
      sb.computedColumns.putAll(savedComputed);
      sb.renames.putAll(savedRenames);
    }

    // Collect all simple column names to detect duplicates
    Map<String, Integer> nameCount = new HashMap<>();
    for (UnresolvedExpression expr : projectList) {
      if (expr instanceof Field) {
        UnresolvedExpression fieldExpr = ((Field) expr).getField();
        if (fieldExpr instanceof QualifiedName) {
          List<String> parts = ((QualifiedName) fieldExpr).getParts();
          String simpleName = parts.get(parts.size() - 1);
          nameCount.merge(simpleName, 1, Integer::sum);
        }
      }
    }

    // Build select list with PPL-style aliases for duplicate column names
    List<String> cols = new ArrayList<>();
    Map<String, Boolean> firstSeen = new HashMap<>();
    for (UnresolvedExpression expr : projectList) {
      String col = visitExpr(expr);
      // Resolve computed columns (e.g. trendline window expressions, rename mappings) by inlining
      String fieldName = (expr instanceof Field) ? getFieldName(expr) : null;
      if (fieldName != null && sb.computedColumns.containsKey(fieldName)) {
        col = sb.computedColumns.remove(fieldName) + " AS " + quoteId(fieldName);
        cols.add(col);
        continue;
      }
      if (expr instanceof Field) {
        UnresolvedExpression fieldExpr = ((Field) expr).getField();
        if (fieldExpr instanceof QualifiedName) {
          List<String> parts = ((QualifiedName) fieldExpr).getParts();
          String simpleName = parts.get(parts.size() - 1);
          if (nameCount.getOrDefault(simpleName, 0) > 1) {
            if (!firstSeen.containsKey(simpleName)) {
              // First occurrence: keep simple name
              firstSeen.put(simpleName, true);
              if (parts.size() >= 2) {
                col = col + " AS " + quoteId(simpleName);
              }
            } else {
              // Subsequent occurrences: alias as qualified name (e.g., b.country)
              if (parts.size() >= 2) {
                String qualifiedName = String.join(".", parts);
                col = col + " AS " + quoteId(qualifiedName);
              }
            }
          }
        }
      }
      cols.add(col);
    }
    sb.select = cols;
  }

  private void processSort(Sort node, SelectBuilder sb) {
    // Sort count > 0 means return only that many rows
    if (node.getCount() != null && node.getCount() > 0) {
      sb.limit = String.valueOf(node.getCount());
    }
    List<String> orderItems = new ArrayList<>();
    for (Field f : node.getSortList()) {
      String col = visitExpr(f.getField());
      String dir = "ASC";
      String nullOrd = "";
      for (Argument arg : f.getFieldArgs()) {
        if ("asc".equals(arg.getArgName())) {
          dir = Boolean.TRUE.equals(arg.getValue().getValue()) ? "ASC" : "DESC";
        } else if ("nullFirst".equals(arg.getArgName())) {
          nullOrd =
              Boolean.TRUE.equals(arg.getValue().getValue()) ? " NULLS FIRST" : " NULLS LAST";
        }
      }
      // PPL default: NULLS FIRST for ASC, NULLS LAST for DESC
      if (nullOrd.isEmpty()) {
        nullOrd = "ASC".equals(dir) ? " NULLS FIRST" : " NULLS LAST";
      }
      orderItems.add(col + " " + dir + nullOrd);
    }
    sb.orderBy = orderItems;
  }

  private void processHead(Head node, SelectBuilder sb) {
    sb.limit = String.valueOf(node.getSize());
    if (node.getFrom() != null && node.getFrom() > 0) {
      sb.offset = String.valueOf(node.getFrom());
    }
  }

  private void processEval(Eval node, SelectBuilder sb) {
    // If select is just ['*'] and there are pending deferredColumns (e.g. from parse),
    // defer eval expressions too to avoid SELECT * expansion (EXPR_TIMESTAMP issue)
    if (sb.select.size() == 1 && "*".equals(sb.select.get(0))
        && !sb.deferredColumns.isEmpty() && sb.where == null && !sb.hasGroupBy) {
      for (Let let : node.getExpressionList()) {
        String varName = let.getVar().getField().toString();
        String expr = visitExpr(let.getExpression());
        sb.computedColumns.put(varName, expr);
        sb.deferredColumns.put(varName, expr);
      }
      return;
    }
    // Eval adds computed columns — wrap current state as subquery, then add columns
    sb.wrapAsSubquery();
    List<String> items = new ArrayList<>();
    items.add("*");
    // Track eval aliases defined in this batch for forward-reference inlining
    java.util.LinkedHashMap<String, String> evalAliases = new java.util.LinkedHashMap<>();
    for (Let let : node.getExpressionList()) {
      String varName = let.getVar().getField().toString();
      String expr = visitExpr(let.getExpression());
      // Inline references to earlier eval aliases defined in this same batch
      for (Map.Entry<String, String> prev : evalAliases.entrySet()) {
        expr = expr.replaceAll(
            "(?i)\\b" + java.util.regex.Pattern.quote(prev.getKey()) + "\\b(?!\\s*\\()",
            "(" + prev.getValue() + ")");
      }
      // Detect self-referencing eval (column override) to avoid duplicate column ambiguity
      // Match varName as a bare column reference (not followed by '(' which would be a function call)
      boolean isSelfRef = java.util.regex.Pattern.compile(
          "(?i)\\b" + java.util.regex.Pattern.quote(varName) + "\\b(?!\\s*\\()")
          .matcher(expr.replaceAll("'[^']*'", "")).find();
      if (isSelfRef) {
        String tempAlias = "_e" + (evalCounter++);
        items.add(expr + " AS " + quoteId(tempAlias));
        sb.computedColumns.put(varName, quoteId(tempAlias));
        evalAliases.put(varName, expr);
      } else {
        items.add(expr + " AS " + quoteId(varName));
        evalAliases.put(varName, expr);
      }
    }
    sb.select = items;
  }

  private void processRename(Rename node, SelectBuilder sb) {
    java.util.LinkedHashMap<String, String> mappings = new java.util.LinkedHashMap<>();
    for (org.opensearch.sql.ast.expression.Map mapping : node.getRenameList()) {
      String origin = getFieldName(mapping.getOrigin());
      String target = getFieldName(mapping.getTarget());
      String realOrigin = null;
      for (Map.Entry<String, String> prev : mappings.entrySet()) {
        if (prev.getValue().equals(origin)) { realOrigin = prev.getKey(); break; }
      }
      if (realOrigin != null) { mappings.put(realOrigin, target); } else { mappings.put(origin, target); }
    }
    mappings.entrySet().removeIf(e -> e.getKey().equals(e.getValue()));
    if (mappings.isEmpty()) return;

    boolean isWildcard = sb.select.size() == 1 && "*".equals(sb.select.get(0));

    if (!isWildcard) {
      // Explicit columns: replace old column names with old AS new in the select list
      // Also handle chained renames from previous rename commands stored in computedColumns
      List<String> newSelect = new ArrayList<>();
      for (String col : sb.select) {
        // Extract the effective column name (handle "expr AS alias" patterns)
        String effectiveName = col;
        String asAlias = null;
        int asIdx = col.toUpperCase().lastIndexOf(" AS ");
        if (asIdx >= 0) {
          asAlias = col.substring(asIdx + 4).trim();
          // Unquote the alias for comparison
          String unquoted = asAlias.startsWith("\"") && asAlias.endsWith("\"")
              ? asAlias.substring(1, asAlias.length() - 1) : asAlias;
          effectiveName = unquoted;
        }

        boolean replaced = false;
        for (Map.Entry<String, String> e : mappings.entrySet()) {
          if (effectiveName.equals(e.getKey())) {
            // Replace this column: use the original expression but alias to new name
            if (asIdx >= 0) {
              // Already has an alias, replace the alias part
              newSelect.add(col.substring(0, asIdx) + " AS " + quoteId(e.getValue()));
            } else {
              newSelect.add(quoteId(e.getKey()) + " AS " + quoteId(e.getValue()));
            }
            replaced = true;
            break;
          }
        }
        if (!replaced) {
          // Check if this column is being replaced by a rename-to-existing-field
          boolean removedByRename = false;
          for (Map.Entry<String, String> e : mappings.entrySet()) {
            if (effectiveName.equals(e.getValue()) && !e.getKey().equals(e.getValue())) {
              // This column's name matches a rename target — it gets replaced
              removedByRename = true;
              break;
            }
          }
          if (!removedByRename) {
            newSelect.add(col);
          }
        }
      }
      sb.select = newSelect;
    } else {
      // Wildcard case: wrap as subquery, add aliases, track renames for exclusion
      sb.wrapAsSubquery();
      List<String> items = new ArrayList<>();
      items.add("*");
      for (Map.Entry<String, String> e : mappings.entrySet()) {
        items.add(quoteId(e.getKey()) + " AS " + quoteId(e.getValue()));
      }
      sb.select = items;
      // Track renames so build() can encode exclusion metadata and
      // subsequent processProject can detect renamed-away columns
      sb.renames.putAll(mappings);
      // Store in computedColumns so subsequent fields command can resolve new names
      for (Map.Entry<String, String> e : mappings.entrySet()) {
        sb.computedColumns.put(e.getValue(), quoteId(e.getKey()));
      }
    }
  }

  private void processDedupe(Dedupe node, SelectBuilder sb) {
    // Extract options
    int number = (Integer) ((Literal) node.getOptions().get(0).getValue()).getValue();
    boolean keepempty = (Boolean) ((Literal) node.getOptions().get(1).getValue()).getValue();
    boolean consecutive = (Boolean) ((Literal) node.getOptions().get(2).getValue()).getValue();

    // Extract field names
    List<String> fieldNames = new ArrayList<>();
    for (Field f : node.getFields()) {
      fieldNames.add(visitExpr(f));
    }

    // Save current select for later column restoration
    List<String> savedSelect = new ArrayList<>(sb.select);

    if (consecutive) {
      processConsecutiveDedupe(sb, fieldNames, number, keepempty, savedSelect);
    } else {
      processStandardDedupe(sb, fieldNames, number, keepempty, savedSelect);
    }
  }

  private void processStandardDedupe(SelectBuilder sb, List<String> fieldNames, int number,
      boolean keepempty, List<String> savedSelect) {
    // Step 1: Wrap current state as subquery
    sb.wrapAsSubquery();

    // For keepempty=false, filter nulls first
    if (!keepempty) {
      StringBuilder nullFilter = new StringBuilder();
      for (int i = 0; i < fieldNames.size(); i++) {
        if (i > 0) nullFilter.append(" AND ");
        nullFilter.append(fieldNames.get(i)).append(" IS NOT NULL");
      }
      sb.where = nullFilter.toString();
    }

    // Step 2: Add ROW_NUMBER window function
    sb.wrapAsSubquery();
    String partition = String.join(", ", fieldNames);
    sb.select = new ArrayList<>();
    sb.select.add("*");
    sb.select.add("ROW_NUMBER() OVER (PARTITION BY " + partition + " ORDER BY " + partition
        + ") AS _dedup_rn");

    // Step 3: Wrap and apply dedup filter
    sb.wrapAsSubquery();
    if (keepempty) {
      StringBuilder nullCheck = new StringBuilder("(");
      for (int i = 0; i < fieldNames.size(); i++) {
        if (i > 0) nullCheck.append(" OR ");
        nullCheck.append(fieldNames.get(i)).append(" IS NULL");
      }
      nullCheck.append(") OR _dedup_rn <= ").append(number);
      sb.where = nullCheck.toString();
    } else {
      sb.where = "_dedup_rn <= " + number;
    }

    // Step 4: Strip helper columns
    stripDedupColumns(sb, savedSelect);
  }

  private void processConsecutiveDedupe(SelectBuilder sb, List<String> fieldNames, int number,
      boolean keepempty, List<String> savedSelect) {
    // Gaps-and-islands technique for consecutive dedup
    String idOrder = "CAST(\"_id\" AS INTEGER)";

    // Step 1: Wrap and filter nulls if needed
    sb.wrapAsSubquery();
    if (!keepempty) {
      StringBuilder nullFilter = new StringBuilder();
      for (int i = 0; i < fieldNames.size(); i++) {
        if (i > 0) nullFilter.append(" AND ");
        nullFilter.append(fieldNames.get(i)).append(" IS NOT NULL");
      }
      sb.where = nullFilter.toString();
    }

    // Step 2: Add global_rn and per-partition group_rn
    sb.wrapAsSubquery();
    String partition = String.join(", ", fieldNames);
    sb.select = new ArrayList<>();
    sb.select.add("*");
    sb.select.add("ROW_NUMBER() OVER (ORDER BY " + idOrder + ") AS _global_rn");
    sb.select.add("ROW_NUMBER() OVER (PARTITION BY " + partition + " ORDER BY " + idOrder
        + ") AS _group_rn");

    // Step 3: ROW_NUMBER within each consecutive island
    sb.wrapAsSubquery();
    sb.select = new ArrayList<>();
    sb.select.add("*");
    sb.select.add("ROW_NUMBER() OVER (PARTITION BY " + partition
        + ", (_global_rn - _group_rn) ORDER BY " + idOrder + ") AS _dedup_rn");

    // Step 4: Filter by island rank
    sb.wrapAsSubquery();
    if (keepempty) {
      StringBuilder nullCheck = new StringBuilder("(");
      for (int i = 0; i < fieldNames.size(); i++) {
        if (i > 0) nullCheck.append(" OR ");
        nullCheck.append(fieldNames.get(i)).append(" IS NULL");
      }
      nullCheck.append(") OR _dedup_rn <= ").append(number);
      sb.where = nullCheck.toString();
    } else {
      sb.where = "_dedup_rn <= " + number;
    }

    // Step 5: Strip helper columns
    stripDedupColumns(sb, savedSelect);
  }

  private void stripDedupColumns(SelectBuilder sb, List<String> savedSelect) {
    sb.wrapAsSubquery();
    boolean canEnumerate = !savedSelect.contains("*");
    if (canEnumerate) {
      sb.select = new ArrayList<>(savedSelect);
    }
  }

  private void processRareTopN(RareTopN node, SelectBuilder sb) {
    // Extract options
    Argument.ArgumentMap args = Argument.ArgumentMap.of(node.getArguments());
    String countFieldName = (String) args.get(RareTopN.Option.countField.name()).getValue();
    boolean showCount = (Boolean) args.get(RareTopN.Option.showCount.name()).getValue();
    boolean useNull = (Boolean) args.get(RareTopN.Option.useNull.name()).getValue();
    int k = node.getNoOfResults();
    boolean isTop = node.getCommandType() == RareTopN.CommandType.TOP;

    List<String> fieldNames = node.getFields().stream()
        .map(f -> visitExpr(f)).collect(Collectors.toList());
    List<String> groupNames = node.getGroupExprList().stream()
        .map(this::visitExpr).collect(Collectors.toList());
    List<String> allGroupBy = new ArrayList<>(groupNames);
    allGroupBy.addAll(fieldNames);

    // Step 0: wrap current state
    sb.wrapAsSubquery();

    // If useNull=false, filter out nulls on the field columns
    if (!useNull) {
      List<String> nullChecks = new ArrayList<>();
      for (String f : allGroupBy) {
        nullChecks.add(f + " IS NOT NULL");
      }
      sb.where = String.join(" AND ", nullChecks);
    }

    // Step 1: GROUP BY allGroupBy with COUNT(*)
    sb.wrapAsSubquery();
    List<String> selectItems = new ArrayList<>(allGroupBy);
    selectItems.add("COUNT(*) AS " + quoteId(countFieldName));
    sb.select = selectItems;
    sb.groupBy = new ArrayList<>(allGroupBy);
    sb.hasGroupBy = true;

    // Step 2: ROW_NUMBER window
    sb.wrapAsSubquery();
    String orderDir = isTop ? "DESC" : "ASC";
    String partitionClause = groupNames.isEmpty() ? "" : "PARTITION BY " + String.join(", ", groupNames) + " ";
    sb.select = new ArrayList<>();
    sb.select.add("*");
    sb.select.add("ROW_NUMBER() OVER (" + partitionClause + "ORDER BY " + quoteId(countFieldName) + " " + orderDir + ") AS _rn");

    // Step 3: filter _rn <= k
    sb.wrapAsSubquery();
    sb.where = "_rn <= " + k;

    // Step 4: project final columns (strip _rn, optionally strip count)
    sb.wrapAsSubquery();
    List<String> finalCols = new ArrayList<>(fieldNames);
    finalCols.addAll(0, groupNames);
    if (showCount) {
      finalCols.add(quoteId(countFieldName));
    }
    sb.select = finalCols;
  }

  // --- Join support ---

  private void processJoin(Join node, SelectBuilder sb) {
    sb.inJoin = true;
    // Snapshot the left side. If there are pending clauses (where, groupBy, etc.),
    // wrap as subquery, preserving the left alias for ON clause references.
    String leftSql;
    String leftAlias = resolveAlias(node.getLeft());
    boolean leftWrapped = false;
    if (sb.where != null || sb.hasGroupBy || sb.orderBy != null || sb.limit != null
        || (sb.select.size() != 1 || !"*".equals(sb.select.get(0)))) {
      // Wrap as subquery but use the left alias so ON clause references still work
      leftWrapped = true;
      String sql = sb.build();
      if (leftAlias != null) {
        leftSql = "(" + sql + ") " + quoteId(leftAlias);
      } else {
        String gen = nextAlias();
        leftAlias = gen;
        leftSql = "(" + sql + ") " + gen;
      }
    } else {
      leftSql = sb.from;
    }

    // Resolve right side
    String rightSql = resolveJoinSide(node.getRight());
    String rightAlias = resolveAlias(node.getRight());

    // Fix D: Self-join — disambiguate when left and right reference the same table
    if (leftAlias != null && leftAlias.equals(rightAlias) && !leftWrapped) {
      leftSql = leftSql + " " + quoteId("_l");
      rightSql = rightSql + " " + quoteId("_r");
      leftAlias = "_l";
      rightAlias = "_r";
    }

    // Track join aliases so visitQualifiedName can distinguish alias.col from nested fields
    if (leftAlias != null) sb.tableAliases.add(leftAlias);
    if (rightAlias != null) sb.tableAliases.add(rightAlias);

    // Map join type to SQL keyword
    Join.JoinType jt = node.getJoinType();

    // Build ON clause
    String onClause = null;
    if (node.getJoinCondition().isPresent()) {
      onClause = visitExpr(node.getJoinCondition().get());
    } else if (node.getJoinFields().isPresent() && !node.getJoinFields().get().isEmpty()) {
      List<String> conditions = new ArrayList<>();
      for (Field f : node.getJoinFields().get()) {
        String fname = f.getField().toString();
        String lRef = leftAlias != null ? quoteId(leftAlias) + "." + quoteId(fname) : quoteId(fname);
        String rRef = rightAlias != null ? quoteId(rightAlias) + "." + quoteId(fname) : quoteId(fname);
        conditions.add(lRef + " = " + rRef);
      }
      onClause = String.join(" AND ", conditions);
    }

    // SEMI/ANTI joins → EXISTS/NOT EXISTS (Calcite SQL parser doesn't support SEMI/ANTI syntax)
    if (jt == Join.JoinType.SEMI || jt == Join.JoinType.ANTI) {
      String existsPrefix = (jt == Join.JoinType.SEMI) ? "EXISTS" : "NOT EXISTS";
      String subquery = "SELECT 1 FROM " + rightSql;
      if (onClause != null) {
        subquery += " WHERE " + onClause;
      }
      sb.from = leftSql;
      String existsCond = existsPrefix + " (" + subquery + ")";
      sb.where = (sb.where != null) ? sb.where + " AND " + existsCond : existsCond;
      sb.select = new ArrayList<>();
      sb.select.add("*");
      sb.groupBy = null;
      sb.orderBy = null;
      sb.limit = null;
      sb.offset = null;
      sb.hasGroupBy = false;
      return;
    }

    // Map join type to SQL keyword — Fix E: no-condition join → CROSS JOIN
    String joinKeyword;
    switch (jt) {
      case LEFT: joinKeyword = "LEFT JOIN"; break;
      case RIGHT: joinKeyword = "RIGHT JOIN"; break;
      case CROSS:
        if (onClause != null) {
          joinKeyword = "INNER JOIN"; // cross join with condition → inner join
        } else {
          joinKeyword = "CROSS JOIN";
        }
        break;
      case FULL: joinKeyword = "FULL OUTER JOIN"; break;
      default:
        joinKeyword = (onClause == null) ? "CROSS JOIN" : "INNER JOIN";
        break;
    }

    // Assemble FROM clause
    StringBuilder fromBuilder = new StringBuilder();
    fromBuilder.append(leftSql);
    fromBuilder.append(" ").append(joinKeyword).append(" ");
    fromBuilder.append(rightSql);
    if (onClause != null) {
      fromBuilder.append(" ON ").append(onClause);
    }

    sb.from = fromBuilder.toString();
    sb.select = new ArrayList<>();
    sb.select.add("*");
    sb.where = null;
    sb.groupBy = null;
    sb.orderBy = null;
    sb.limit = null;
    sb.offset = null;
    sb.hasGroupBy = false;
  }

  /** Resolve a join side (left or right) into a SQL FROM fragment, e.g. "tableName alias" */
  private String resolveJoinSide(UnresolvedPlan plan) {
    if (plan instanceof SubqueryAlias) {
      SubqueryAlias sa = (SubqueryAlias) plan;
      UnresolvedPlan child = (UnresolvedPlan) sa.getChild().get(0);
      Relation rel = extractRelation(child);
      if (rel != null) {
        String table = quoteId(rel.getTableQualifiedName().toString());
        return table + " " + quoteId(sa.getAlias());
      }
      // Subquery pipeline on right side — recursively transpile it
      String innerSql = transpileSubPlan(child);
      return "(" + innerSql + ") " + quoteId(sa.getAlias());
    } else if (plan instanceof Relation) {
      return quoteId(((Relation) plan).getTableQualifiedName().toString());
    }
    Relation rel = extractRelation(plan);
    if (rel != null) {
      return quoteId(rel.getTableQualifiedName().toString());
    }
    // Fallback: transpile as subquery
    String innerSql = transpileSubPlan(plan);
    return "(" + innerSql + ") " + nextAlias();
  }

  /**
   * Extract a Relation from a plan that is just Project(AllFields) wrapping a Relation,
   * or a Relation directly. Returns null if the plan has real pipeline logic.
   */
  private static Relation extractRelation(UnresolvedPlan plan) {
    if (plan instanceof Relation) return (Relation) plan;
    if (plan instanceof Project) {
      Project proj = (Project) plan;
      boolean isAllFields = proj.getProjectList().size() == 1
          && proj.getProjectList().get(0) instanceof AllFields;
      if (isAllFields && !proj.getChild().isEmpty()) {
        UnresolvedPlan child = (UnresolvedPlan) proj.getChild().get(0);
        if (child instanceof Relation) return (Relation) child;
      }
    }
    return null;
  }

  /** Extract alias from a plan node (SubqueryAlias or Relation name). */
  private static String resolveAlias(UnresolvedPlan plan) {
    if (plan instanceof SubqueryAlias) {
      return ((SubqueryAlias) plan).getAlias();
    }
    Relation rel = extractRelation(plan);
    if (rel != null) {
      return rel.getTableQualifiedName().toString();
    }
    return null;
  }

  /**
   * Process LOOKUP command: LEFT JOIN source to lookup table, with REPLACE or APPEND semantics.
   */
  private void processLookup(Lookup node, SelectBuilder sb) {
    sb.inJoin = true;
    String leftSql = sb.build();
    String leftAlias = "_l";
    String rightAlias = "_r";
    sb.tableAliases.add(leftAlias);
    sb.tableAliases.add(rightAlias);

    Relation lookupRel = extractRelation(node.getLookupRelation());
    String lookupTable = lookupRel != null
        ? quoteId(lookupRel.getTableQualifiedName().toString())
        : quoteId(node.getLookupRelation().toString());

    // Build ON clause
    List<String> onConditions = new ArrayList<>();
    for (Map.Entry<String, String> e : node.getMappingAliasMap().entrySet()) {
      onConditions.add(quoteId(leftAlias) + "." + quoteId(e.getValue())
          + " = " + quoteId(rightAlias) + "." + quoteId(e.getKey()));
    }

    String fromClause = "(" + leftSql + ") " + quoteId(leftAlias)
        + " LEFT JOIN " + lookupTable + " " + quoteId(rightAlias)
        + " ON " + String.join(" AND ", onConditions);

    Map<String, String> outputMap = node.getOutputAliasMap();
    boolean isReplace = node.getOutputStrategy() == Lookup.OutputStrategy.REPLACE;

    // Save original select list before resetting (needed for known-columns path)
    List<String> origSelect = new ArrayList<>(sb.select);

    sb.from = fromClause;
    sb.where = null;
    sb.groupBy = null;
    sb.orderBy = null;
    sb.limit = null;
    sb.offset = null;
    sb.hasGroupBy = false;
    sb.computedColumns = new HashMap<>();

    if (outputMap.isEmpty()) {
      // No output spec: all lookup fields with REPLACE semantics.
      // Use SELECT * — keeps select as ["*"] so processProject won't wrap as subquery.
      // Duplicate columns from join are resolved by Calcite (last occurrence wins).
      sb.select = new ArrayList<>();
      sb.select.add("*");
    } else {
      // Specific output fields: include source columns plus lookup columns.
      // When source columns are known (not *), enumerate them explicitly and
      // replace/append target fields inline. When *, use temp names + computedColumns.
      boolean sourceColumnsKnown = !origSelect.contains("*");
      sb.select = new ArrayList<>();

      if (sourceColumnsKnown) {
        // Enumerate source columns, replacing target fields with lookup expressions
        java.util.Set<String> targetFields = new java.util.HashSet<>();
        for (Map.Entry<String, String> e : outputMap.entrySet()) {
          targetFields.add(e.getValue());
        }
        // Map from targetField to lookup expression
        Map<String, String> lookupExprs = new HashMap<>();
        for (Map.Entry<String, String> e : outputMap.entrySet()) {
          String lookupField = e.getKey();
          String targetField = e.getValue();
          String rRef = quoteId(rightAlias) + "." + quoteId(lookupField);
          String lRef = quoteId(leftAlias) + "." + quoteId(targetField);
          if (isReplace) {
            lookupExprs.put(targetField, rRef + " AS " + quoteId(targetField));
          } else {
            // APPEND: COALESCE(source, lookup)
            lookupExprs.put(targetField,
                "COALESCE(" + lRef + ", " + rRef + ") AS " + quoteId(targetField));
          }
        }
        // Build SELECT: for each source column, skip target fields (they go at end for both REPLACE and APPEND)
        for (String col : origSelect) {
          String simpleName = col.replaceAll("\"", "").replaceAll(".*\\bAS\\s+", "").trim();
          if (lookupExprs.containsKey(simpleName)) {
            // Skip — will be added at end
            continue;
          } else {
            sb.select.add(quoteId(leftAlias) + "." + quoteId(simpleName));
          }
        }
        // Add lookup expressions at end
        for (String expr : lookupExprs.values()) {
          sb.select.add(expr);
        }
      } else {
        // Source columns unknown (*): use temp names + computedColumns for processProject
        sb.select.add(quoteId(leftAlias) + ".*");
        for (Map.Entry<String, String> e : outputMap.entrySet()) {
          String lookupField = e.getKey();
          String targetField = e.getValue();
          String tempName = "_lookup_" + lookupField;
          sb.select.add(quoteId(rightAlias) + "." + quoteId(lookupField) + " AS " + quoteId(tempName));
          if (isReplace) {
            sb.computedColumns.put(targetField, quoteId(tempName));
          } else {
            if (!targetField.equals(lookupField)) {
              sb.computedColumns.put(targetField,
                  "COALESCE(" + quoteId(targetField) + ", " + quoteId(tempName) + ")");
            } else {
              sb.computedColumns.put(targetField, quoteId(tempName));
            }
          }
        }
      }
    }
  }

  /** Transpile a sub-plan (e.g. right side of join with pipeline) into SQL. */
  private String transpileSubPlan(UnresolvedPlan plan) {
    List<UnresolvedPlan> nodes = new ArrayList<>();
    flatten(plan, nodes);
    PPLToSqlTranspiler transpiler = new PPLToSqlTranspiler();
    SelectBuilder sb = new SelectBuilder();
    // Propagate outer table aliases for correlated subquery references
    if (currentSb != null) {
      sb.tableAliases.addAll(currentSb.tableAliases);
    }
    transpiler.currentSb = sb;
    for (UnresolvedPlan node : nodes) {
      if (node instanceof Relation) {
        String tableName = ((Relation) node).getTableQualifiedName().toString();
        sb.from = quoteId(tableName);
        sb.tableAliases.add(tableName);
      } else if (node instanceof Filter) {
        transpiler.processFilter((Filter) node, sb);
      } else if (node instanceof Aggregation) {
        transpiler.processAggregation((Aggregation) node, sb);
      } else if (node instanceof Project) {
        transpiler.processProject((Project) node, sb);
      } else if (node instanceof Sort) {
        transpiler.processSort((Sort) node, sb);
      } else if (node instanceof Head) {
        transpiler.processHead((Head) node, sb);
      } else if (node instanceof Eval) {
        transpiler.processEval((Eval) node, sb);
      } else if (node instanceof Rename) {
        transpiler.processRename((Rename) node, sb);
      } else if (node instanceof Dedupe) {
        transpiler.processDedupe((Dedupe) node, sb);
      } else if (node instanceof RareTopN) {
        transpiler.processRareTopN((RareTopN) node, sb);
      } else if (node instanceof SubqueryAlias) {
        String subAlias = ((SubqueryAlias) node).getAlias();
        sb.from = sb.from + " " + quoteId(subAlias);
        sb.tableAliases.add(subAlias);
      } else if (node instanceof Join) {
        transpiler.processJoin((Join) node, sb);
      } else if (node instanceof Window) {
        transpiler.processWindow((Window) node, sb);
      } else if (node instanceof Trendline) {
        transpiler.processTrendline((Trendline) node, sb);
      } else if (node instanceof FillNull) {
        transpiler.processFillNull((FillNull) node, sb);
      } else if (node instanceof Parse) {
        transpiler.processParse((Parse) node, sb);
      } else if (node instanceof Lookup) {
        transpiler.processLookup((Lookup) node, sb);
      } else if (node instanceof Append) {
        transpiler.processAppend((Append) node, sb);
      } else if (node instanceof AppendPipe) {
        transpiler.processAppendPipe((AppendPipe) node, sb);
      } else if (node instanceof Search) {
        Search search = (Search) node;
        String cond = visitSearchExpr(search.getOriginalExpression());
        if (sb.where == null) sb.where = cond;
        else sb.where = "(" + sb.where + ") AND (" + cond + ")";
      }
      // Silently skip unsupported nodes in subplans
    }
    return sb.build();
  }

  private void processAppend(Append node, SelectBuilder sb) {
    String mainSql = sb.build();
    String subSql = transpileSubPlan(node.getSubSearch());
    String alias = nextAlias();
    sb.from = "(" + mainSql + " UNION ALL " + subSql + ") " + alias;
    sb.select = new ArrayList<>();
    sb.select.add("*");
    sb.where = null;
    sb.groupBy = null;
    sb.orderBy = null;
    sb.limit = null;
    sb.offset = null;
    sb.hasGroupBy = false;
    sb.computedColumns = new HashMap<>();
    sb.renames = new java.util.LinkedHashMap<>();
  }

  private void processAppendPipe(AppendPipe node, SelectBuilder sb) {
    String mainSql = sb.build();
    // Flatten the sub-query AST and apply commands with main query as source
    List<UnresolvedPlan> subNodes = new ArrayList<>();
    flatten(node.getSubQuery(), subNodes);
    PPLToSqlTranspiler subTranspiler = new PPLToSqlTranspiler();
    SelectBuilder subSb = new SelectBuilder();
    subTranspiler.currentSb = subSb;
    String subAlias = nextAlias();
    subSb.from = "(" + mainSql + ") " + subAlias;
    for (UnresolvedPlan subNode : subNodes) {
      if (subNode instanceof Relation) continue;
      if (subNode instanceof Filter) {
        subTranspiler.processFilter((Filter) subNode, subSb);
      } else if (subNode instanceof Aggregation) {
        subTranspiler.processAggregation((Aggregation) subNode, subSb);
      } else if (subNode instanceof Project) {
        subTranspiler.processProject((Project) subNode, subSb);
      } else if (subNode instanceof Sort) {
        subTranspiler.processSort((Sort) subNode, subSb);
      } else if (subNode instanceof Head) {
        subTranspiler.processHead((Head) subNode, subSb);
      } else if (subNode instanceof Eval) {
        subTranspiler.processEval((Eval) subNode, subSb);
      } else if (subNode instanceof Rename) {
        subTranspiler.processRename((Rename) subNode, subSb);
      } else if (subNode instanceof Dedupe) {
        subTranspiler.processDedupe((Dedupe) subNode, subSb);
      } else if (subNode instanceof Join) {
        subTranspiler.processJoin((Join) subNode, subSb);
      } else if (subNode instanceof Window) {
        subTranspiler.processWindow((Window) subNode, subSb);
      } else if (subNode instanceof Trendline) {
        subTranspiler.processTrendline((Trendline) subNode, subSb);
      } else if (subNode instanceof FillNull) {
        subTranspiler.processFillNull((FillNull) subNode, subSb);
      } else if (subNode instanceof Lookup) {
        subTranspiler.processLookup((Lookup) subNode, subSb);
      }
    }
    String subSql = subSb.build();
    String unionAlias = nextAlias();
    sb.from = "(" + mainSql + " UNION ALL " + subSql + ") " + unionAlias;
    sb.select = new ArrayList<>();
    sb.select.add("*");
    sb.where = null;
    sb.groupBy = null;
    sb.orderBy = null;
    sb.limit = null;
    sb.offset = null;
    sb.hasGroupBy = false;
    sb.computedColumns = new HashMap<>();
    sb.renames = new java.util.LinkedHashMap<>();
  }

  private static String getFieldName(UnresolvedExpression expr) {
    if (expr instanceof Field) return ((Field) expr).getField().toString();
    if (expr instanceof QualifiedName) return ((QualifiedName) expr).toString();
    return expr.toString();
  }

  /** Convert a SearchExpression tree to a SQL WHERE clause fragment. */
  private static String visitSearchExpr(SearchExpression expr) {
    if (expr instanceof SearchComparison) {
      SearchComparison cmp = (SearchComparison) expr;
      String field = quoteId(getFieldName(cmp.getField()));
      String value = visitSearchLiteralValue(cmp.getValue());
      String op;
      switch (cmp.getOperator()) {
        case EQUALS: op = "="; break;
        case NOT_EQUALS: op = "!="; break;
        case LESS_THAN: op = "<"; break;
        case LESS_OR_EQUAL: op = "<="; break;
        case GREATER_THAN: op = ">"; break;
        case GREATER_OR_EQUAL: op = ">="; break;
        default: op = "="; break;
      }
      return "(" + field + " " + op + " " + value + ")";
    } else if (expr instanceof SearchAnd) {
      SearchAnd and = (SearchAnd) expr;
      return "(" + visitSearchExpr(and.getLeft()) + " AND " + visitSearchExpr(and.getRight()) + ")";
    } else if (expr instanceof SearchOr) {
      SearchOr or = (SearchOr) expr;
      return "(" + visitSearchExpr(or.getLeft()) + " OR " + visitSearchExpr(or.getRight()) + ")";
    } else if (expr instanceof SearchGroup) {
      return visitSearchExpr(((SearchGroup) expr).getExpression());
    } else if (expr instanceof SearchNot) {
      return "(NOT " + visitSearchExpr(((SearchNot) expr).getExpression()) + ")";
    }
    throw new UnsupportedOperationException("Unsupported search expression: " + expr.getClass().getSimpleName());
  }

  private static String visitSearchLiteralValue(SearchLiteral lit) {
    UnresolvedExpression inner = lit.getLiteral();
    if (inner instanceof Literal) {
      Literal l = (Literal) inner;
      if (l.getType() == DataType.STRING) {
        return "'" + l.getValue().toString().replace("'", "''") + "'";
      }
      return l.getValue().toString();
    }
    return inner.toString();
  }

  // --- Expression visitor methods ---

  private String visitExpr(UnresolvedExpression expr) {
    return expr.accept(this, null);
  }

  @Override
  public String visitLiteral(Literal node, Void ctx) {
    if (node.getType() == DataType.NULL) {
      return "NULL";
    } else if (node.getType() == DataType.BOOLEAN) {
      return Boolean.TRUE.equals(node.getValue()) ? "TRUE" : "FALSE";
    } else if (node.getType() == DataType.STRING) {
      return "'" + node.getValue().toString().replace("'", "''") + "'";
    } else if (node.getType() == DataType.DOUBLE) {
      return "CAST(" + node.getValue().toString() + " AS DOUBLE)";
    } else if (node.getType() == DataType.FLOAT) {
      return "CAST(" + node.getValue().toString() + " AS FLOAT)";
    } else if (node.getType() == DataType.LONG) {
      return "CAST(" + node.getValue().toString() + " AS BIGINT)";
    }
    return node.getValue().toString();
  }

  @Override
  public String visitQualifiedName(QualifiedName node, Void ctx) {
    List<String> parts = node.getParts();
    // Resolve deferred parse columns for single-part names
    if (currentSb != null && parts.size() == 1) {
      String name = parts.get(0);
      String computed = currentSb.deferredColumns.get(name);
      if (computed != null) {
        return computed;
      }
    }
    // Multi-part names: check if first part is a known table alias (table.column reference)
    if (parts.size() >= 2 && currentSb != null && currentSb.tableAliases.contains(parts.get(0))) {
      return parts.stream()
          .map(PPLToSqlTranspiler::quoteId)
          .collect(Collectors.joining("."));
    }
    // Otherwise, treat as a single OpenSearch flattened field name
    return quoteId(String.join(".", parts));
  }

  @Override
  public String visitField(Field node, Void ctx) {
    return visitExpr(node.getField());
  }

  @Override
  public String visitAlias(Alias node, Void ctx) {
    String expr = visitExpr(node.getDelegated());
    String name = node.getName();
    // If the alias name matches the expression text, skip redundant alias
    if (name != null && !name.equals(expr) && !quoteId(name).equals(expr)) {
      return expr + " AS " + quoteId(name);
    }
    return expr;
  }

  private String castInt(String expr) { return "CAST(" + expr + " AS INTEGER)"; }

  public String visitFunction(Function node, Void ctx) {
    String name = node.getFuncName().toLowerCase();
    List<String> args =
        node.getFuncArgs().stream().map(this::visitExpr).collect(Collectors.toList());

    // Arithmetic operators: PPL parser creates +/-/*/÷ functions
    if ("+".equals(name) && args.size() == 2) {
      return "(" + args.get(0) + " + " + args.get(1) + ")";
    }
    if ("-".equals(name) && args.size() == 2) {
      return "(" + args.get(0) + " - " + args.get(1) + ")";
    }
    if ("*".equals(name) && args.size() == 2) {
      return "(" + args.get(0) + " * " + args.get(1) + ")";
    }
    if ("/".equals(name) && args.size() == 2) {
      return "CASE WHEN " + args.get(1) + " = 0 THEN NULL ELSE (" + args.get(0) + " / " + args.get(1) + ") END";
    }
    if ("%".equals(name) && args.size() == 2) {
      return "CASE WHEN " + args.get(1) + " = 0 THEN NULL ELSE MOD(" + args.get(0) + ", " + args.get(1) + ") END";
    }

    // Special-case rewrites
    if ("if".equals(name)) {
      return "CASE WHEN " + args.get(0) + " THEN " + castVarcharIfStringLiteral(args.get(1)) + " ELSE " + castVarcharIfStringLiteral(args.get(2))
          + " END";
    }
    if ("ifnull".equals(name)) {
      return "COALESCE(" + args.get(0) + ", " + args.get(1) + ")";
    }
    if ("isnull".equals(name) || "is null".equals(name)) {
      return "(" + args.get(0) + " IS NULL)";
    }
    if ("log".equals(name)) {
      if (args.size() == 2) {
        return "(LN(" + args.get(1) + ") / LN(" + args.get(0) + "))";
      }
      return "LN(" + args.get(0) + ")";
    }
    if ("log2".equals(name)) {
      return "LN(" + args.get(0) + ") / LN(2)";
    }
    if ("like".equals(name)) {
      return args.get(0) + " LIKE " + args.get(1);
    }
    if ("not like".equals(name)) {
      return args.get(0) + " NOT LIKE " + args.get(1);
    }
    if (("min".equals(name) || "scalar_min".equals(name)) && args.size() > 1) {
      return buildLeast(args);
    }
    if (("max".equals(name) || "scalar_max".equals(name)) && args.size() > 1) {
      return buildGreatest(args);
    }
    if ("scalar_min".equals(name) && args.size() == 1) {
      return args.get(0);
    }
    if ("scalar_max".equals(name) && args.size() == 1) {
      return args.get(0);
    }
    if ("pi".equals(name)) {
      return "PI()";
    }
    if ("e".equals(name)) {
      return "EXP(1)";
    }
    if ("cot".equals(name)) {
      return "(1.0 / TAN(" + args.get(0) + "))";
    }
    if ("cbrt".equals(name)) {
      return "POWER(" + args.get(0) + ", 1.0 / 3)";
    }
    if ("isnotnull".equals(name) || "is not null".equals(name)) {
      return "(" + args.get(0) + " IS NOT NULL)";
    }
    if ("ispresent".equals(name)) {
      return "(" + args.get(0) + " IS NOT NULL)";
    }
    if ("isempty".equals(name)) {
      return "(" + args.get(0) + " IS NULL OR " + args.get(0) + " = '')";
    }
    if ("isblank".equals(name)) {
      return "(" + args.get(0) + " IS NULL OR TRIM(" + args.get(0) + ") = '')";
    }
    // String functions — Calcite-compatible rewrites
    if ("concat".equals(name)) {
      return "(" + String.join(" || ", args) + ")";
    }
    if ("concat_ws".equals(name)) {
      String sep = args.get(0);
      List<String> parts = args.subList(1, args.size());
      return "(" + String.join(" || " + sep + " || ", parts) + ")";
    }
    if ("left".equals(name)) {
      return "SUBSTRING(" + args.get(0) + ", 1, " + args.get(1) + ")";
    }
    if ("right".equals(name)) {
      return "SUBSTRING(" + args.get(0) + " FROM CHAR_LENGTH(" + args.get(0) + ") - " + args.get(1) + " + 1)";
    }
    if ("reverse".equals(name)) {
      return "REVERSE(" + args.get(0) + ")";
    }
    if ("locate".equals(name)) {
      if (args.size() == 2) {
        return "POSITION(" + args.get(0) + " IN " + args.get(1) + ")";
      }
      return "CASE WHEN POSITION(" + args.get(0) + " IN SUBSTRING(" + args.get(1) + " FROM " + args.get(2) + ")) > 0 THEN POSITION(" + args.get(0) + " IN SUBSTRING(" + args.get(1) + " FROM " + args.get(2) + ")) + " + args.get(2) + " - 1 ELSE 0 END";
    }
    if ("position".equals(name)) {
      return "POSITION(" + args.get(0) + " IN " + args.get(1) + ")";
    }
    if ("strcmp".equals(name)) {
      return "CASE WHEN " + args.get(0) + " < " + args.get(1) + " THEN -1 WHEN " + args.get(0) + " > " + args.get(1) + " THEN 1 ELSE 0 END";
    }
    if ("ltrim".equals(name)) {
      return "TRIM(LEADING ' ' FROM " + args.get(0) + ")";
    }
    if ("rtrim".equals(name)) {
      return "TRIM(TRAILING ' ' FROM " + args.get(0) + ")";
    }
    if ("replace".equals(name)) {
      return "REPLACE(" + args.get(0) + ", " + args.get(1) + ", " + args.get(2) + ")";
    }
    if ("substr".equals(name)) {
      return "SUBSTRING(" + String.join(", ", args) + ")";
    }
    if ("mod".equals(name) || "%".equals(name)) {
      return "CASE WHEN " + args.get(1) + " = 0 THEN NULL ELSE MOD(" + args.get(0) + ", " + args.get(1) + ") END";
    }
    if ("EXTRACT".equals(name)) {
      String part = args.get(0).replace("'", "");
      return "EXTRACT(" + part + " FROM " + args.get(1) + ")";
    }
    if ("year".equals(name)) return castInt("EXTRACT(YEAR FROM " + args.get(0) + ")");
    if ("month".equals(name)) return castInt("EXTRACT(MONTH FROM " + args.get(0) + ")");
    if ("day".equals(name)) return castInt("EXTRACT(DAY FROM " + args.get(0) + ")");
    if ("hour".equals(name)) return castInt("EXTRACT(HOUR FROM " + args.get(0) + ")");
    if ("minute".equals(name)) return castInt("EXTRACT(MINUTE FROM " + args.get(0) + ")");
    if ("second".equals(name)) return castInt("EXTRACT(SECOND FROM " + args.get(0) + ")");
    if ("dayofweek".equals(name) || "day_of_week".equals(name)) return castInt("DAYOFWEEK(" + args.get(0) + ")");
    if ("dayofyear".equals(name) || "day_of_year".equals(name)) return castInt("DAYOFYEAR(" + args.get(0) + ")");
    if ("dayofmonth".equals(name) || "day_of_month".equals(name)) return castInt("EXTRACT(DAY FROM " + args.get(0) + ")");
    if ("weekofyear".equals(name) || "week_of_year".equals(name) || "week".equals(name)) return castInt("EXTRACT(WEEK FROM " + args.get(0) + ")");
    if ("hour_of_day".equals(name)) return castInt("EXTRACT(HOUR FROM " + args.get(0) + ")");
    if ("minute_of_hour".equals(name)) return castInt("EXTRACT(MINUTE FROM " + args.get(0) + ")");
    if ("second_of_minute".equals(name)) return castInt("EXTRACT(SECOND FROM " + args.get(0) + ")");
    if ("month_of_year".equals(name)) return castInt("EXTRACT(MONTH FROM " + args.get(0) + ")");
    if ("quarter".equals(name)) return castInt("EXTRACT(QUARTER FROM " + args.get(0) + ")");
    if ("microsecond".equals(name)) return castInt("EXTRACT(MICROSECOND FROM " + args.get(0) + ")");
    if ("minute_of_day".equals(name)) return castInt("(EXTRACT(HOUR FROM " + args.get(0) + ") * 60 + EXTRACT(MINUTE FROM " + args.get(0) + "))");
    if ("date_add".equals(name) || "adddate".equals(name)) {
      if (args.get(1).startsWith("INTERVAL")) {
        String[] parts = args.get(1).split("\\s+");
        return "CAST(TIMESTAMPADD(" + parts[parts.length - 1] + ", " + parts[1] + ", " + args.get(0) + ") AS TIMESTAMP)";
      }
      return "TIMESTAMPADD(DAY, " + args.get(1) + ", " + args.get(0) + ")";
    }
    if ("date_sub".equals(name) || "subdate".equals(name)) {
      if (args.get(1).startsWith("INTERVAL")) {
        String[] parts = args.get(1).split("\\s+");
        return "CAST(TIMESTAMPADD(" + parts[parts.length - 1] + ", -" + parts[1] + ", " + args.get(0) + ") AS TIMESTAMP)";
      }
      return "TIMESTAMPADD(DAY, -" + args.get(1) + ", " + args.get(0) + ")";
    }
    if ("datediff".equals(name)) {
      return "CAST(TIMESTAMPDIFF(DAY, " + args.get(1) + ", " + args.get(0) + ") AS BIGINT)";
    }
    if ("timestampdiff".equals(name)) {
      String unit = args.get(0).replace("'", "");
      return "CAST(TIMESTAMPDIFF(" + unit + ", " + args.get(1) + ", " + args.get(2) + ") AS BIGINT)";
    }
    if ("timestampadd".equals(name)) {
      String unit = args.get(0).replace("'", "");
      return "TIMESTAMPADD(" + unit + ", " + args.get(1) + ", " + args.get(2) + ")";
    }
    if ("date".equals(name)) return "CAST(" + args.get(0) + " AS DATE)";
    if ("time".equals(name)) return "CAST(" + args.get(0) + " AS TIME)";
    if ("timestamp".equals(name)) {
      if (args.size() == 1) return "CAST(" + args.get(0) + " AS TIMESTAMP)";
      return "CAST(CAST(" + args.get(0) + " AS VARCHAR) || ' ' || CAST(" + args.get(1) + " AS VARCHAR) AS TIMESTAMP)";
    }
    if ("datetime".equals(name)) {
      if (args.size() == 1) return "CAST(" + args.get(0) + " AS TIMESTAMP)";
      return "CAST(NULL AS TIMESTAMP)";
    }
    if ("dayname".equals(name)) return "DAYNAME(" + args.get(0) + ")";
    if ("monthname".equals(name)) return "MONTHNAME(" + args.get(0) + ")";
    if ("weekday".equals(name))
      return "CASE WHEN " + args.get(0) + " IS NULL THEN NULL ELSE " + castInt("MOD(DAYOFWEEK(" + args.get(0) + ") + 5, 7)") + " END";
    if ("yearweek".equals(name))
      return "CASE WHEN " + args.get(0) + " IS NULL THEN NULL ELSE " + castInt("(EXTRACT(YEAR FROM " + args.get(0) + ") * 100 + EXTRACT(WEEK FROM " + args.get(0) + "))") + " END";
    if ("unix_timestamp".equals(name)) {
      if (args.isEmpty()) return "CAST(TIMESTAMPDIFF(SECOND, TIMESTAMP '1970-01-01 00:00:00', CURRENT_TIMESTAMP) AS DOUBLE)";
      return "CAST(TIMESTAMPDIFF(SECOND, TIMESTAMP '1970-01-01 00:00:00', CAST(" + args.get(0) + " AS TIMESTAMP)) AS DOUBLE)";
    }
    if ("from_unixtime".equals(name))
      return "CAST(TIMESTAMPADD(SECOND, CAST(" + args.get(0) + " AS INTEGER), TIMESTAMP '1970-01-01 00:00:00') AS TIMESTAMP)";
    if ("to_days".equals(name))
      return "CASE WHEN " + args.get(0) + " IS NULL THEN NULL ELSE CAST(TIMESTAMPDIFF(DAY, DATE '0001-01-01', CAST(" + args.get(0) + " AS DATE)) + 366 AS BIGINT) END";
    if ("from_days".equals(name))
      return "CASE WHEN " + args.get(0) + " IS NULL THEN NULL ELSE CAST(TIMESTAMPADD(DAY, CAST(" + args.get(0) + " AS INTEGER) - 366, DATE '0001-01-01') AS DATE) END";
    if ("to_seconds".equals(name))
      return "CASE WHEN " + args.get(0) + " IS NULL THEN NULL ELSE CAST((TIMESTAMPDIFF(DAY, DATE '0001-01-01', CAST(" + args.get(0) + " AS DATE)) + 366) * 86400 AS BIGINT) END";
    if ("last_day".equals(name)) return "CAST(LAST_DAY(" + args.get(0) + ") AS DATE)";
    if ("time_to_sec".equals(name))
      return "CAST((EXTRACT(HOUR FROM " + args.get(0) + ") * 3600 + EXTRACT(MINUTE FROM " + args.get(0) + ") * 60 + EXTRACT(SECOND FROM " + args.get(0) + ")) AS BIGINT)";
    if ("sec_to_time".equals(name))
      return "CAST(TIMESTAMPADD(SECOND, CAST(" + args.get(0) + " AS INTEGER), TIME '00:00:00') AS TIME)";
    if ("timediff".equals(name))
      return "CASE WHEN (" + args.get(0) + ") IS NULL OR (" + args.get(1) + ") IS NULL THEN NULL ELSE CAST(TIMESTAMPADD(SECOND, TIMESTAMPDIFF(SECOND, CAST(" + args.get(1) + " AS TIMESTAMP), CAST(" + args.get(0) + " AS TIMESTAMP)), TIME '00:00:00') AS TIME) END";
    if ("makedate".equals(name))
      return "CAST(TIMESTAMPADD(DAY, CAST(" + args.get(1) + " AS INTEGER) - 1, CAST(CAST(" + args.get(0) + " AS VARCHAR) || '-01-01' AS DATE)) AS DATE)";
    if ("maketime".equals(name))
      return "CAST(TIMESTAMPADD(SECOND, CAST(" + args.get(2) + " AS INTEGER), TIMESTAMPADD(MINUTE, CAST(" + args.get(1) + " AS INTEGER), TIMESTAMPADD(HOUR, CAST(" + args.get(0) + " AS INTEGER), TIME '00:00:00'))) AS TIME)";
    if ("addtime".equals(name))
      return "TIMESTAMPADD(SECOND, CAST(EXTRACT(HOUR FROM " + args.get(1) + ") * 3600 + EXTRACT(MINUTE FROM " + args.get(1) + ") * 60 + EXTRACT(SECOND FROM " + args.get(1) + ") AS INTEGER), CAST(" + args.get(0) + " AS TIMESTAMP))";
    if ("subtime".equals(name))
      return "TIMESTAMPADD(SECOND, CAST(-(EXTRACT(HOUR FROM " + args.get(1) + ") * 3600 + EXTRACT(MINUTE FROM " + args.get(1) + ") * 60 + EXTRACT(SECOND FROM " + args.get(1) + ")) AS INTEGER), CAST(" + args.get(0) + " AS TIMESTAMP))";
    if ("convert_tz".equals(name))
      return "CASE WHEN (" + args.get(0) + ") IS NULL THEN NULL ELSE CAST(" + args.get(0) + " AS TIMESTAMP) END";
    if ("date_format".equals(name) || "time_format".equals(name))
      return "CASE WHEN (" + args.get(0) + ") IS NULL THEN NULL ELSE CAST(" + args.get(0) + " AS VARCHAR) END";
    if ("str_to_date".equals(name))
      return "CASE WHEN (" + args.get(0) + ") IS NULL THEN NULL ELSE CAST(" + args.get(0) + " AS TIMESTAMP) END";
    if ("period_add".equals(name))
      return "CASE WHEN (" + args.get(0) + ") IS NULL OR (" + args.get(1) + ") IS NULL THEN NULL ELSE (" + args.get(0) + " + " + args.get(1) + ") END";
    if ("period_diff".equals(name))
      return "CASE WHEN (" + args.get(0) + ") IS NULL OR (" + args.get(1) + ") IS NULL THEN NULL ELSE (" + args.get(0) + " - " + args.get(1) + ") END";

    String sqlName = FUNC_MAP.getOrDefault(name, name.toUpperCase());
    return sqlName + "(" + String.join(", ", args) + ")";
  }

  @Override
  public String visitAggregateFunction(AggregateFunction node, Void ctx) {
    String name = node.getFuncName().toLowerCase();

    // distinct_count / dc → COUNT(DISTINCT ...)
    if ("distinct_count".equals(name) || "dc".equals(name)) {
      return "COUNT(DISTINCT " + visitExpr(node.getField()) + ")";
    }
    if ("avg".equals(name)) { String f = visitExpr(node.getField()); return "AVG(CAST(" + f + " AS DOUBLE))"; }
    if ("var_samp".equals(name)) { String f = visitExpr(node.getField()); return "VAR_SAMP(CAST(" + f + " AS DOUBLE))"; }
    if ("var_pop".equals(name)) { String f = visitExpr(node.getField()); return "VAR_POP(CAST(" + f + " AS DOUBLE))"; }
    if ("stddev_samp".equals(name)) { String f = visitExpr(node.getField()); return "STDDEV_SAMP(CAST(" + f + " AS DOUBLE))"; }
    if ("stddev_pop".equals(name)) { String f = visitExpr(node.getField()); return "STDDEV_POP(CAST(" + f + " AS DOUBLE))"; }
    if ("first".equals(name)) { return "PPL_FIRST(" + visitExpr(node.getField()) + ")"; }
    if ("last".equals(name)) { return "PPL_LAST(" + visitExpr(node.getField()) + ")"; }
    if ("earliest".equals(name)) {
      return "ARG_MIN(" + visitExpr(node.getField()) + ", " + quoteId("@timestamp") + ")";
    }
    if ("latest".equals(name)) {
      return "ARG_MAX(" + visitExpr(node.getField()) + ", " + quoteId("@timestamp") + ")";
    }
    if ("take".equals(name)) { return "PPL_FIRST(" + visitExpr(node.getField()) + ")"; }
    if ("median".equals(name)) {
      return "PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY " + visitExpr(node.getField()) + ")";
    }
    if ("count_distinct_approx".equals(name) || "approx_count_distinct".equals(name)
        || "distinct_count_approx".equals(name)) {
      return "COUNT(DISTINCT " + visitExpr(node.getField()) + ")";
    }
    if ("percentile".equals(name) || "percentile_approx".equals(name)) {
      String fieldSql = visitExpr(node.getField());
      List<UnresolvedExpression> funcArgs = node.getArgList();
      if (funcArgs != null && !funcArgs.isEmpty()) {
        String pct = visitExpr(funcArgs.get(0));
        try {
          double pctLiteral = Double.parseDouble(pct) / 100.0;
          return "PERCENTILE_CONT(" + pctLiteral + ") WITHIN GROUP (ORDER BY " + fieldSql + ")";
        } catch (NumberFormatException e) {
          return "PERCENTILE_CONT(" + pct + " / 100.0) WITHIN GROUP (ORDER BY " + fieldSql + ")";
        }
      }
      return "PERCENTILE_CONT(0.5) WITHIN GROUP (ORDER BY " + fieldSql + ")";
    }

    String sqlName = FUNC_MAP.getOrDefault(name, name.toUpperCase());

    // COUNT() with no meaningful field → COUNT(*)
    if ("count".equals(name)) {
      UnresolvedExpression field = node.getField();
      if (field instanceof AllFields
          || (field instanceof Literal && ((Literal) field).getValue().equals(1))) {
        return "COUNT(*)";
      }
    }

    String fieldSql = visitExpr(node.getField());
    if (Boolean.TRUE.equals(node.getDistinct())) {
      return sqlName + "(DISTINCT " + fieldSql + ")";
    }
    return sqlName + "(" + fieldSql + ")";
  }

  @Override
  public String visitCompare(Compare node, Void ctx) {
    String op = node.getOperator();
    if ("REGEXP".equalsIgnoreCase(op)) {
      return "REGEXP_CONTAINS(" + visitExpr(node.getLeft()) + ", " + visitExpr(node.getRight()) + ")";
    }
    return "(" + visitExpr(node.getLeft()) + " " + op + " "
        + visitExpr(node.getRight()) + ")";
  }

  @Override
  public String visitAnd(And node, Void ctx) {
    return "(" + visitExpr(node.getLeft()) + " AND " + visitExpr(node.getRight()) + ")";
  }

  @Override
  public String visitOr(Or node, Void ctx) {
    return "(" + visitExpr(node.getLeft()) + " OR " + visitExpr(node.getRight()) + ")";
  }

  @Override
  public String visitNot(Not node, Void ctx) {
    return "(NOT " + visitExpr(node.getExpression()) + ")";
  }

  @Override
  public String visitInSubquery(InSubquery node, Void ctx) {
    List<UnresolvedExpression> values = node.getValue();
    String subSql = transpileSubPlan(node.getQuery());
    if (values.size() == 1) {
      return visitExpr(values.get(0)) + " IN (" + subSql + ")";
    }
    String lhs = values.stream().map(this::visitExpr).collect(Collectors.joining(", "));
    return "(" + lhs + ") IN (" + subSql + ")";
  }

  @Override
  public String visitScalarSubquery(ScalarSubquery node, Void ctx) {
    return "(" + transpileSubPlan(node.getQuery()) + ")";
  }

  @Override
  public String visitExistsSubquery(ExistsSubquery node, Void ctx) {
    return "EXISTS (" + transpileSubPlan(node.getQuery()) + ")";
  }

  @Override
  public String visitXor(Xor node, Void ctx) {
    // XOR(a, b) = (a OR b) AND NOT (a AND b)
    String l = visitExpr(node.getLeft());
    String r = visitExpr(node.getRight());
    return "((" + l + " OR " + r + ") AND NOT (" + l + " AND " + r + "))";
  }

  @Override
  public String visitIn(In node, Void ctx) {
    String field = visitExpr(node.getField());
    String vals =
        node.getValueList().stream().map(this::visitExpr).collect(Collectors.joining(", "));
    return field + " IN (" + vals + ")";
  }

  @Override
  public String visitBetween(Between node, Void ctx) {
    return visitExpr(node.getValue()) + " BETWEEN " + visitExpr(node.getLowerBound()) + " AND "
        + visitExpr(node.getUpperBound());
  }

  @Override
  public String visitCase(Case node, Void ctx) {
    StringBuilder sb = new StringBuilder("CASE");
    if (node.getCaseValue() != null) {
      sb.append(" ").append(visitExpr(node.getCaseValue()));
    }
    for (When when : node.getWhenClauses()) {
      sb.append(" WHEN ").append(visitExpr(when.getCondition()));
      sb.append(" THEN ").append(castVarcharIfStringLiteral(visitExpr(when.getResult())));
    }
    node.getElseClause().ifPresent(e -> sb.append(" ELSE ").append(castVarcharIfStringLiteral(visitExpr(e))));
    sb.append(" END");
    return sb.toString();
  }

  @Override
  public String visitCast(Cast node, Void ctx) {
    String expr = visitExpr(node.getExpression());
    String typeName = node.getConvertedType().toString().toUpperCase();
    switch (typeName) {
      case "STRING": typeName = "VARCHAR"; break;
      case "INT": case "INTEGER": typeName = "INTEGER"; break;
      case "LONG": typeName = "BIGINT"; break;
      case "FLOAT": typeName = "FLOAT"; break;
      case "DOUBLE": typeName = "DOUBLE"; break;
      case "BOOLEAN":
        if (node.getExpression() instanceof Literal
            && ((Literal) node.getExpression()).getType() != DataType.STRING) {
          return "(" + expr + " <> 0)";
        }
        return "CASE WHEN " + expr + " IS NULL THEN NULL"
            + " WHEN CAST(" + expr + " AS VARCHAR) IN ('true', '1') THEN TRUE"
            + " WHEN CAST(" + expr + " AS VARCHAR) IN ('false', '0') THEN FALSE"
            + " ELSE NULL END";
      case "DATE": typeName = "DATE"; break;
      case "TIME": typeName = "TIME"; break;
      case "TIMESTAMP": case "DATETIME": typeName = "TIMESTAMP"; break;
    }
    return "CAST(" + expr + " AS " + typeName + ")";
  }

  @Override
  public String visitSpan(Span node, Void ctx) {
    String field = visitExpr(node.getField());
    String value = visitExpr(node.getValue());
    SpanUnit unit = node.getUnit();
    if (unit == SpanUnit.NONE || !SpanUnit.isTimeUnit(unit)) {
      // Numeric bucketing: FLOOR(field / value) * value
      return "FLOOR(" + field + " / " + value + ") * " + value;
    }
    // Time-based bucketing
    String sqlUnit;
    String shortName = SpanUnit.getName(unit);
    switch (shortName) {
      case "s": sqlUnit = "SECOND"; break;
      case "m": sqlUnit = "MINUTE"; break;
      case "h": sqlUnit = "HOUR"; break;
      case "d": sqlUnit = "DAY"; break;
      case "w": sqlUnit = "WEEK"; break;
      case "M": sqlUnit = "MONTH"; break;
      case "q": sqlUnit = "QUARTER"; break;
      case "y": sqlUnit = "YEAR"; break;
      default: sqlUnit = "DAY"; break;
    }
    return "DATE_TRUNC('" + sqlUnit + "', " + field + ")";
  }

  @Override
  public String visitInterval(Interval node, Void ctx) {
    String value = visitExpr(node.getValue());
    String unit = node.getUnit().name();
    return "INTERVAL " + value + " " + unit;
  }

  @Override
  public String visitAllFields(AllFields node, Void ctx) {
    return "*";
  }

  // --- Helpers ---

  private static String quoteId(String id) {
    // Only quote if the identifier contains special characters or is a SQL keyword
    if (id.matches("[a-zA-Z_][a-zA-Z0-9_]*") && !SQL_KEYWORDS.contains(id.toUpperCase())) {
      return id;
    }
    return "\"" + id.replace("\"", "\"\"") + "\"";
  }

  /** Wrap a SQL expression with CAST(... AS VARCHAR) if it is a string literal, to avoid CHAR padding. */
  private static String castVarcharIfStringLiteral(String expr) {
    if (expr.startsWith("'") && expr.endsWith("'")) {
      return "CAST(" + expr + " AS VARCHAR)";
    }
    return expr;
  }

  private String buildGreatest(List<String> args) {
    if (args.size() == 1) return args.get(0);
    if (args.size() == 2) {
      String a = args.get(0), b = args.get(1);
      return "(CASE WHEN " + a + " IS NULL THEN " + b
          + " WHEN " + b + " IS NULL THEN " + a
          + " WHEN " + a + " >= " + b + " THEN " + a
          + " ELSE " + b + " END)";
    }
    String first = args.get(0);
    String rest = buildGreatest(args.subList(1, args.size()));
    return buildGreatest(List.of(first, rest));
  }

  private String buildLeast(List<String> args) {
    if (args.size() == 1) return args.get(0);
    if (args.size() == 2) {
      String a = args.get(0), b = args.get(1);
      return "(CASE WHEN " + a + " IS NULL THEN " + b
          + " WHEN " + b + " IS NULL THEN " + a
          + " WHEN " + a + " <= " + b + " THEN " + a
          + " ELSE " + b + " END)";
    }
    String first = args.get(0);
    String rest = buildLeast(args.subList(1, args.size()));
    return buildLeast(List.of(first, rest));
  }

  private static final java.util.Set<String> SQL_KEYWORDS =
      java.util.Set.of(
          "SELECT", "FROM", "WHERE", "GROUP", "BY", "ORDER", "LIMIT", "OFFSET", "AND", "OR",
          "NOT", "IN", "BETWEEN", "LIKE", "AS", "ON", "JOIN", "LEFT", "RIGHT", "INNER", "OUTER",
          "CROSS", "FULL", "HAVING", "UNION", "ALL", "DISTINCT", "CASE", "WHEN", "THEN", "ELSE",
          "END", "NULL", "TRUE", "FALSE", "IS", "ASC", "DESC", "EXISTS", "TABLE", "INDEX",
          "CREATE", "DROP", "ALTER", "INSERT", "UPDATE", "DELETE", "SET", "VALUES", "INTO",
          "DEFAULT", "PRIMARY", "KEY", "FOREIGN", "REFERENCES", "CHECK", "UNIQUE", "CONSTRAINT",
          "WITH", "RECURSIVE", "OVER", "PARTITION", "ROWS", "RANGE", "CURRENT", "ROW",
          "PRECEDING", "FOLLOWING", "UNBOUNDED", "FETCH", "NEXT", "FIRST", "LAST", "ONLY",
          "PERCENT", "TIES", "WINDOW", "FILTER", "WITHIN", "ROLLUP", "CUBE", "GROUPING",
          "SETS", "LATERAL", "UNNEST", "PIVOT", "UNPIVOT", "TABLESAMPLE", "MATCH", "NATURAL",
          "USING", "EXCEPT", "INTERSECT", "SOME", "ANY", "CAST", "TRIM", "POSITION",
          "SUBSTRING", "OVERLAY", "COLLECT", "FUSION", "INTERSECTION",
          "DATE", "TIME", "TIMESTAMP", "INTERVAL", "YEAR", "MONTH", "DAY", "HOUR", "MINUTE",
          "SECOND", "ZONE", "BOOLEAN", "INTEGER", "INT", "BIGINT", "SMALLINT", "TINYINT",
          "FLOAT", "DOUBLE", "DECIMAL", "NUMERIC", "REAL", "VARCHAR", "CHAR", "CHARACTER",
          "BINARY", "VARBINARY", "BLOB", "CLOB", "CURSOR", "MODULE", "VALUE",
          "NEW", "OLD", "BOTH", "LEADING", "TRAILING", "CONVERT", "TRANSLATE",
          "SYSTEM", "USER", "SESSION", "LOCAL", "GLOBAL", "TEMPORARY", "TEMP",
          "RESULT", "RETURN", "RETURNS", "SIGNAL", "CONDITION", "DECLARE",
          "AVG", "MIN", "MAX", "SUM", "COUNT", "ABS", "MOD", "UPPER", "LOWER",
          "LENGTH", "REPLACE", "ROUND", "FLOOR", "CEIL", "CEILING", "POWER",
          "SQRT", "LOG", "LN", "EXP", "SIGN", "TRUNCATE", "EXTRACT", "UNKNOWN",
          "METHOD", "SCOPE", "CATALOG", "SCHEMA", "DOMAIN", "ROLE", "AUTHORIZATION",
          "PRIVILEGES", "GRANT", "REVOKE", "COMMIT", "ROLLBACK", "TRANSACTION",
          "START", "WORK", "SAVEPOINT", "RELEASE", "PREPARE", "EXECUTE",
          "DESCRIBE", "OPEN", "CLOSE", "DEALLOCATE", "LANGUAGE", "EXTERNAL",
          "SPECIFIC", "PARAMETER", "FUNCTION", "PROCEDURE", "CALL", "TRIGGER",
          "EACH", "BEFORE", "AFTER", "FOR", "WHILE", "DO", "LOOP", "REPEAT",
          "UNTIL", "ELSEIF", "CONTINUE", "EXIT", "HANDLER", "FOUND",
          "SENSITIVE", "INSENSITIVE", "ASENSITIVE", "SCROLL", "NO", "HOLD",
          "RELATIVE", "ABSOLUTE", "PRIOR", "INPUT", "OUTPUT", "INOUT",
          "DYNAMIC", "STATIC", "DEPTH", "BREADTH", "SEARCH", "CYCLE",
          "NORMALIZE", "CLASSIFIER", "MEASURES", "PERMUTE", "RUNNING",
          "PREV", "DEFINE", "MATCH_RECOGNIZE", "PATTERN", "SUBSET",
          "INITIAL", "SEEK", "SKIP", "EMPTY", "GROUPS", "BEGIN");

  private static String nextAlias() {
    return "_t" + (++subqueryCounter);
  }

  /** Mutable builder that tracks the current SQL state. */
  private static class SelectBuilder {
    String from;
    List<String> select = new ArrayList<>();
    String where;
    List<String> groupBy;
    List<String> orderBy;
    String limit;
    String offset;
    boolean hasGroupBy;
    Map<String, String> computedColumns = new HashMap<>();
    /** Deferred columns from parse command — resolved inline by visitQualifiedName. */
    Map<String, String> deferredColumns = new HashMap<>();
    /** Pending renames: oldName -> newName. Used to exclude original columns from results. */
    java.util.LinkedHashMap<String, String> renames = new java.util.LinkedHashMap<>();
    boolean inJoin;
    java.util.Set<String> tableAliases = new java.util.HashSet<>();

    SelectBuilder() {
      select.add("*");
    }

    /** Wrap current state as a subquery and reset clauses. */
    void wrapAsSubquery() {
      String sql = build();
      String alias = nextAlias();
      from = "(" + sql + ") " + alias;
      select = new ArrayList<>();
      select.add("*");
      where = null;
      groupBy = null;
      orderBy = null;
      limit = null;
      offset = null;
      hasGroupBy = false;
      computedColumns = new HashMap<>();
      deferredColumns = new HashMap<>();
      renames = new java.util.LinkedHashMap<>();
      inJoin = false;
      tableAliases = new java.util.HashSet<>();
    }

    String build() {
      StringBuilder sb = new StringBuilder();
      // Encode pending renames as a SQL comment for post-processing
      if (!renames.isEmpty()) {
        List<String> pairs = new ArrayList<>();
        for (Map.Entry<String, String> e : renames.entrySet()) {
          pairs.add(e.getKey() + ":" + e.getValue());
        }
        sb.append("/* _RENAME_MAP:").append(String.join(",", pairs)).append(" */ ");
      }
      sb.append("SELECT ").append(String.join(", ", select));
      sb.append(" FROM ").append(from);
      if (where != null) {
        sb.append(" WHERE ").append(where);
      }
      if (groupBy != null && !groupBy.isEmpty()) {
        sb.append(" GROUP BY ").append(String.join(", ", groupBy));
      }
      if (orderBy != null && !orderBy.isEmpty()) {
        sb.append(" ORDER BY ").append(String.join(", ", orderBy));
      }
      if (limit != null) {
        sb.append(" LIMIT ").append(limit);
      }
      if (offset != null) {
        sb.append(" OFFSET ").append(offset);
      }
      return sb.toString();
    }
  }
}
