/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api;

import static org.opensearch.sql.calcite.utils.SqlNodeDSL.*;

import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlBasicFunction;
import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlSelectKeyword;
import org.apache.calcite.sql.SqlWindow;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlTypeFamily;
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
import org.opensearch.sql.ast.expression.PatternMethod;
import org.opensearch.sql.ast.expression.PatternMode;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.expression.SearchAnd;
import org.opensearch.sql.ast.expression.SearchComparison;
import org.opensearch.sql.ast.expression.SearchExpression;
import org.opensearch.sql.ast.expression.SearchGroup;
import org.opensearch.sql.ast.expression.SearchIn;
import org.opensearch.sql.ast.expression.SearchLiteral;
import org.opensearch.sql.ast.expression.SearchNot;
import org.opensearch.sql.ast.expression.SearchOr;
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
import org.opensearch.sql.ast.tree.AppendPipe;
import org.opensearch.sql.ast.tree.Bin;
import org.opensearch.sql.ast.tree.Chart;
import org.opensearch.sql.ast.tree.CountBin;
import org.opensearch.sql.ast.tree.Dedupe;
import org.opensearch.sql.ast.tree.DefaultBin;
import org.opensearch.sql.ast.tree.Eval;
import org.opensearch.sql.ast.tree.FillNull;
import org.opensearch.sql.ast.tree.Filter;
import org.opensearch.sql.ast.tree.Head;
import org.opensearch.sql.ast.tree.Join;
import org.opensearch.sql.ast.tree.Lookup;
import org.opensearch.sql.ast.tree.MinSpanBin;
import org.opensearch.sql.ast.tree.MvExpand;
import org.opensearch.sql.ast.tree.Multisearch;
import org.opensearch.sql.ast.tree.NoMv;
import org.opensearch.sql.ast.tree.Parse;
import org.opensearch.sql.ast.tree.Patterns;
import org.opensearch.sql.ast.tree.Project;
import org.opensearch.sql.ast.tree.RangeBin;
import org.opensearch.sql.ast.tree.RareTopN;
import org.opensearch.sql.ast.tree.Regex;
import org.opensearch.sql.ast.tree.Relation;
import org.opensearch.sql.ast.tree.Rename;
import org.opensearch.sql.ast.tree.Reverse;
import org.opensearch.sql.ast.tree.Replace;
import org.opensearch.sql.ast.tree.Rex;
import org.opensearch.sql.ast.tree.Search;
import org.opensearch.sql.ast.tree.ReplacePair;
import org.opensearch.sql.ast.tree.SPath;
import org.opensearch.sql.ast.tree.Sort;
import org.opensearch.sql.ast.tree.SpanBin;
import org.opensearch.sql.ast.tree.StreamWindow;
import org.opensearch.sql.ast.tree.SubqueryAlias;
import org.opensearch.sql.ast.tree.Trendline;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.ast.tree.Window;
import org.opensearch.sql.calcite.utils.SqlNodeDSL;
import org.opensearch.sql.expression.function.PPLBuiltinOperators;
import org.opensearch.sql.expression.parse.RegexCommonUtils;
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
    // Array/MV functions (most handled via SqlLibraryOperators in visitFunction)
    FUNC_MAP.put("mvjoin", "ARRAY_JOIN");
    FUNC_MAP.put("array_length", "ARRAY_LENGTH");
    // Crypto - SHA2 handled separately via PPLBuiltinOperators.SHA2 UDF
  }

  private static final Set<String> SQL_DATETIME_KEYWORDS =
      Set.of(
          "CURRENT_TIMESTAMP",
          "CURRENT_DATE",
          "CURRENT_TIME",
          "LOCALTIMESTAMP",
          "LOCALTIME");

  protected final AtomicInteger aliasCounter;

  /** When true, the next wrapAsSubquery() call is skipped (used after JOIN to preserve aliases). */
  protected boolean skipNextWrap = false;

  public PPLToSqlNodeConverter() {
    this.aliasCounter = new AtomicInteger(0);
  }

  protected PPLToSqlNodeConverter(AtomicInteger sharedCounter) {
    this.aliasCounter = sharedCounter;
  }

  protected PPLToSqlNodeConverter(AtomicInteger sharedCounter, Set<String> outerKnownAliases) {
    this.aliasCounter = sharedCounter;
    this.knownAliases.addAll(outerKnownAliases);
  }

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
              return (T) "LABEL";
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
    // If the final result is a raw SqlJoin (from visitJoin), wrap it in SELECT * first
    // so that ORDER BY / LIMIT and downstream processing work correctly.
    if (current instanceof SqlJoin) {
      current = select(star()).from(current).build();
      pipe = current;
    }
    // Apply any deferred ORDER BY / LIMIT that wasn't consumed by visitHead
    if (pendingOrderBy != null || pendingFetch != null) {
      current = applyPendingOrderBy(current);
    }
    return current;
  }

  /** Apply deferred ORDER BY and LIMIT to the given SqlNode. */
  protected SqlNode applyPendingOrderBy(SqlNode node) {
    SqlNodeList orderList = pendingOrderBy != null
        ? new SqlNodeList(pendingOrderBy, SqlParserPos.ZERO) : null;
    SqlNode result;
    if (orderList != null || pendingFetch != null) {
      result = new SqlOrderBy(SqlParserPos.ZERO, node,
          orderList != null ? orderList : SqlNodeList.EMPTY,
          pendingOffset, pendingFetch);
    } else {
      result = node;
    }
    pendingOrderBy = null;
    pendingFetch = null;
    pendingOffset = null;
    return result;
  }

  // -- Pipe state: the current SqlNode being built up --
  protected SqlNode pipe;

  // -- Deferred ORDER BY: stored here by visitSort, applied by visitHead or convert() --
  protected List<SqlNode> pendingOrderBy;
  protected SqlNode pendingFetch;
  protected SqlNode pendingOffset;

  // -- Bin alias tracking: maps original field name to bin expression alias --
  protected final Map<String, SqlNode> binReplacements = new HashMap<>();

  // -- MAP field tracking: field names produced by spath auto-extract (json_extract_all) --
  protected final Set<String> mapFields = new java.util.HashSet<>();

  // -- Known alias tracking: aliases from joins, subqueries, lookups --
  protected final Set<String> knownAliases = new java.util.HashSet<>();

  // -- Alias mapping: SubqueryAlias name → effective join alias (for rewriting references) --
  protected final Map<String, String> aliasMapping = new HashMap<>();

  // -- Pending SubqueryAlias: deferred until wrapAsSubquery maps it to the generated alias --
  protected String pendingSubqueryAlias = null;

  private String nextAlias() {
    String alias = "_t" + aliasCounter.incrementAndGet();
    knownAliases.add(alias);
    return alias;
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
    // If skipNextWrap is set (after JOIN), return the raw join directly
    // so that join aliases (a, b) remain visible to downstream pipes.
    if (skipNextWrap) {
      skipNextWrap = false;
      return pipe;
    }
    // If pipe is a raw SqlJoin, wrap in SELECT * first so the subquery
    // doesn't produce a bare join inside parentheses (which the parser rejects).
    if (pipe instanceof SqlJoin) {
      pipe = select(star()).from(pipe).build();
    }
    // Use the pending SubqueryAlias name as the wrapper alias if available,
    // so that alias.column references resolve correctly.
    String alias;
    if (pendingSubqueryAlias != null) {
      alias = pendingSubqueryAlias;
      knownAliases.add(alias);
      pendingSubqueryAlias = null;
    } else {
      alias = nextAlias();
    }
    return subquery(pipe, alias);
  }

  /** Consume the skipNextWrap flag and return pipe directly (used by visitProject after JOIN). */
  private SqlNode consumeSkipWrap() {
    skipNextWrap = false;
    return pipe;
  }

  // -- Visitor methods --

  @Override
  public SqlNode visitRelation(Relation node, Void ctx) {
    String tableName = node.getTableQualifiedName().toString();
    knownAliases.add(tableName);
    pipe = select(star()).from(table(tableName)).build();
    return pipe;
  }

  @Override
  public SqlNode visitSubqueryAlias(SubqueryAlias node, Void ctx) {
    knownAliases.add(node.getAlias());
    // Store the alias for use as the wrapper alias name in wrapAsSubquery.
    pendingSubqueryAlias = node.getAlias();
    return pipe;
  }

  @Override
  public SqlNode visitSearch(Search node, Void ctx) {
    SearchExpression expr = node.getOriginalExpression();
    if (expr != null) {
      SqlNode condition = convertSearchExpr(expr);
      if (condition != null) {
        pipe = select(star()).from(wrapAsSubquery()).where(condition).build();
      }
    }
    return pipe;
  }

  private SqlNode convertSearchExpr(SearchExpression expr) {
    if (expr instanceof SearchComparison) {
      SearchComparison cmp = (SearchComparison) expr;
      String fieldName = cmp.getField().getField().toString();
      SqlNode field = identifier(fieldName);
      Literal lit = (Literal) ((SearchLiteral) cmp.getValue()).getLiteral();
      Object rawObj = lit.getValue();
      String rawValue = rawObj.toString();
      SqlNode valueLiteral;
      if ("@timestamp".equals(fieldName)) {
        String resolved = resolveDateMathToTimestamp(rawValue);
        valueLiteral = SqlLiteral.createCharString(resolved, POS);
      } else {
        valueLiteral = literal(rawObj);
      }
      switch (cmp.getOperator()) {
        case GREATER_OR_EQUAL: return gte(field, valueLiteral);
        case LESS_OR_EQUAL: return lte(field, valueLiteral);
        case GREATER_THAN: return gt(field, valueLiteral);
        case LESS_THAN: return lt(field, valueLiteral);
        case EQUALS:
          if (hasUnescapedWildcard(rawValue)) {
            String pattern = convertWildcardToLike(rawValue).toLowerCase(java.util.Locale.ROOT);
            return new SqlBasicCall(SqlStdOperatorTable.LIKE,
                new SqlNode[]{call("LOWER", field),
                    SqlLiteral.createCharString(pattern, POS)}, POS);
          }
          return eq(field, valueLiteral);
        case NOT_EQUALS: return neq(field, valueLiteral);
        default: return null;
      }
    } else if (expr instanceof SearchIn) {
      SearchIn in = (SearchIn) expr;
      String fieldName = in.getField().getField().toString();
      SqlNode field = identifier(fieldName);
      boolean isTimestamp = "@timestamp".equals(fieldName);
      List<SqlNode> vals = new ArrayList<>();
      for (SearchLiteral lit : in.getValues()) {
        String raw = ((Literal) lit.getLiteral()).getValue().toString();
        vals.add(SqlLiteral.createCharString(isTimestamp ? resolveTimestampValue(raw) : raw, POS));
      }
      return new SqlBasicCall(SqlStdOperatorTable.IN,
          new SqlNode[]{field, new SqlNodeList(vals, POS)}, POS);
    } else if (expr instanceof SearchLiteral) {
      SearchLiteral lit = (SearchLiteral) expr;
      String val = lit.toQueryString().replace("'", "''");
      SqlBasicFunction matchPhraseFn = SqlBasicFunction.create(
          "match_phrase", ReturnTypes.BOOLEAN,
          OperandTypes.family(SqlTypeFamily.CHARACTER, SqlTypeFamily.CHARACTER));
      return new SqlBasicCall(matchPhraseFn,
          new SqlNode[]{SqlLiteral.createCharString("*", POS),
              SqlLiteral.createCharString(val, POS)}, POS);
    } else if (expr instanceof SearchAnd) {
      SearchAnd and = (SearchAnd) expr;
      SqlNode left = convertSearchExpr(and.getLeft());
      SqlNode right = convertSearchExpr(and.getRight());
      if (left == null) return right;
      if (right == null) return left;
      return and(left, right);
    } else if (expr instanceof SearchOr) {
      SearchOr or = (SearchOr) expr;
      SqlNode left = convertSearchExpr(or.getLeft());
      SqlNode right = convertSearchExpr(or.getRight());
      if (left == null) return right;
      if (right == null) return left;
      return or(left, right);
    } else if (expr instanceof SearchGroup) {
      return convertSearchExpr(((SearchGroup) expr).getExpression());
    } else if (expr instanceof SearchNot) {
      SqlNode inner = convertSearchExpr(((SearchNot) expr).getExpression());
      return inner != null ? new SqlBasicCall(SqlStdOperatorTable.IS_NOT_TRUE,
          new SqlNode[]{inner}, POS) : null;
    }
    return null;
  }

  /** Convert wildcard pattern (* → %, ? → _) for SQL LIKE. */
  private static String convertWildcardToLike(String value) {
    StringBuilder sb = new StringBuilder();
    for (int i = 0; i < value.length(); i++) {
      char c = value.charAt(i);
      if (c == '\\' && i + 1 < value.length() && (value.charAt(i + 1) == '*' || value.charAt(i + 1) == '?')) {
        sb.append(value.charAt(++i)); // literal * or ?
      } else if (c == '*') {
        sb.append('%');
      } else if (c == '?') {
        sb.append('_');
      } else {
        sb.append(c);
      }
    }
    return sb.toString();
  }

  /** Resolve a timestamp value preserving nanosecond precision for absolute timestamps. */
  private static String resolveTimestampValue(String raw) {
    if (raw.contains("T") || raw.matches("^\\d{4}-\\d{2}-\\d{2}.*")) {
      try {
        java.time.ZonedDateTime zdt = java.time.ZonedDateTime.parse(raw,
            java.time.format.DateTimeFormatter.ISO_DATE_TIME);
        return zdt.format(java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSSSSS"));
      } catch (Exception ignored) {}
    }
    return resolveDateMathToTimestamp(raw);
  }

  private static final java.time.format.DateTimeFormatter TS_FMT =
      java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

  private static final DateTimeFormatter EARLIEST_LATEST_FMT =
      DateTimeFormatter.ofPattern("MM/dd/yyyy:HH:mm:ss");

  /** Resolve time argument for earliest/latest filter functions to ISO timestamp string. */
  private static String resolveFilterTimeArg(SqlNode node) {
    String raw = null;
    if (node instanceof SqlLiteral) {
      raw = ((SqlLiteral) node).toValue();
    }
    if (raw == null) return resolveDateMathToTimestamp("now");
    // Try MM/dd/yyyy:HH:mm:ss format
    try {
      LocalDateTime dt = LocalDateTime.parse(raw, EARLIEST_LATEST_FMT);
      return dt.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
    } catch (Exception ignored) {}
    // Normalize @ to / for date math snap syntax, and prepend 'now' for relative expressions
    String normalized = raw.replace('@', '/');
    if (normalized.startsWith("-") || normalized.startsWith("+")) {
      normalized = "now" + normalized;
    }
    return resolveDateMathToTimestamp(normalized);
  }

  /** Resolve an OpenSearch date math expression to an actual timestamp string. */
  private static String resolveDateMathToTimestamp(String dateMath) {
    java.time.ZonedDateTime now = java.time.ZonedDateTime.now(java.time.ZoneOffset.UTC);
    if (dateMath == null || dateMath.isEmpty()) return now.format(TS_FMT);
    if ("now".equals(dateMath) || "now()".equals(dateMath)) return now.format(TS_FMT);
    // Unix millis
    if (dateMath.matches("^\\d+(\\.\\d+)?$")) {
      long millis = new java.math.BigDecimal(dateMath).longValue();
      return java.time.Instant.ofEpochMilli(millis).atZone(java.time.ZoneOffset.UTC).format(TS_FMT);
    }
    // ISO timestamp
    if (dateMath.contains("T") || dateMath.matches("^\\d{4}-\\d{2}-\\d{2}.*")) {
      try {
        return java.time.ZonedDateTime.parse(dateMath,
            java.time.format.DateTimeFormatter.ISO_DATE_TIME).format(TS_FMT);
      } catch (Exception e) {
        try {
          return java.time.LocalDateTime.parse(dateMath,
              java.time.format.DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")).format(TS_FMT);
        } catch (Exception e2) { return dateMath; }
      }
    }
    // OS date math: now[/unit][+-Nunit]...
    if (dateMath.startsWith("now")) {
      return resolveOsDateMath(dateMath.substring(3), now).format(TS_FMT);
    }
    return dateMath;
  }

  /** Parse and resolve OS date math operations: /unit for snap, +/-Nunit for offset. */
  private static java.time.ZonedDateTime resolveOsDateMath(String ops, java.time.ZonedDateTime t) {
    int i = 0;
    while (i < ops.length()) {
      char c = ops.charAt(i);
      if (c == '/') {
        // Snap: /unit
        int j = i + 1;
        while (j < ops.length() && Character.isLetterOrDigit(ops.charAt(j))) j++;
        t = applyOsSnap(t, ops.substring(i + 1, j));
        i = j;
      } else if (c == '+' || c == '-') {
        // Offset: +/-Nunit
        int j = i + 1;
        while (j < ops.length() && Character.isDigit(ops.charAt(j))) j++;
        int val = (j > i + 1) ? Integer.parseInt(ops.substring(i + 1, j)) : 1;
        int k = j;
        while (k < ops.length() && Character.isLetter(ops.charAt(k))) k++;
        String unit = ops.substring(j, k);
        t = applyOsOffset(t, c == '+' ? val : -val, unit);
        i = k;
      } else {
        break;
      }
    }
    return t;
  }

  private static java.time.ZonedDateTime applyOsSnap(java.time.ZonedDateTime t, String unit) {
    return switch (unit) {
      case "s" -> t.truncatedTo(java.time.temporal.ChronoUnit.SECONDS);
      case "m" -> t.truncatedTo(java.time.temporal.ChronoUnit.MINUTES);
      case "h", "H" -> t.truncatedTo(java.time.temporal.ChronoUnit.HOURS);
      case "d" -> t.truncatedTo(java.time.temporal.ChronoUnit.DAYS);
      case "w" -> t.minusDays((t.getDayOfWeek().getValue() % 7))
          .truncatedTo(java.time.temporal.ChronoUnit.DAYS);
      case "M" -> t.withDayOfMonth(1).truncatedTo(java.time.temporal.ChronoUnit.DAYS);
      case "y" -> t.withDayOfYear(1).truncatedTo(java.time.temporal.ChronoUnit.DAYS);
      default -> t;
    };
  }

  private static java.time.ZonedDateTime applyOsOffset(
      java.time.ZonedDateTime t, int val, String unit) {
    return switch (unit) {
      case "s" -> t.plusSeconds(val);
      case "m" -> t.plusMinutes(val);
      case "h", "H" -> t.plusHours(val);
      case "d" -> t.plusDays(val);
      case "w" -> t.plusWeeks(val);
      case "M" -> t.plusMonths(val);
      case "y" -> t.plusYears(val);
      default -> t;
    };
  }

  @Override
  public SqlNode visitFilter(Filter node, Void ctx) {
    SqlNode condition = node.getCondition().accept(this, null);
    // Preserve join context: if skipNextWrap is set (right after a join),
    // the WHERE is added to the join's SELECT and the flag is re-set
    // so downstream commands (fields, sort) can still see join aliases.
    boolean joinContext = skipNextWrap;
    // For correlated subqueries or qualified column references (table.column),
    // avoid wrapping a simple SELECT * FROM table so that the table name
    // remains visible for correlation resolution.
    if (isSimpleTableScan(pipe) && hasQualifiedOrSubqueryRefs(node.getCondition())) {
      SqlNode tableFrom = ((SqlSelect) pipe).getFrom();
      // If there's a pending SubqueryAlias, alias the table so that
      // alias.column references resolve within this SELECT.
      // Keep pendingSubqueryAlias set so wrapAsSubquery also uses it.
      if (pendingSubqueryAlias != null) {
        tableFrom = as(tableFrom, pendingSubqueryAlias);
      }
      pipe = select(star()).from(tableFrom).where(condition).build();
    } else {
      pipe = select(star()).from(wrapAsSubquery()).where(condition).build();
    }
    if (joinContext) {
      skipNextWrap = true;
    }
    return pipe;
  }

  @Override
  public SqlNode visitRegex(Regex node, Void ctx) {
    node.getChild().get(0).accept(this, ctx);
    pipe = wrapAsSubquery();
    SqlNode field = node.getField().accept(this, null);
    String pattern = ((Literal) node.getPattern()).getValue().toString();
    SqlNode regexCall = call("REGEXP_CONTAINS", field, literal(pattern));
    if (node.isNegated()) {
      regexCall = not(regexCall);
    }
    pipe = select(star()).from(pipe).where(regexCall).build();
    return pipe;
  }

  @Override
  public SqlNode visitProject(Project node, Void ctx) {
    if (node.getProjectList().size() == 1 && node.getProjectList().get(0) instanceof AllFields) {
      return pipe;
    }
    // Track seen leaf column names to detect duplicates (e.g. a.country vs b.country)
    Map<String, Integer> leafNameCount = new java.util.LinkedHashMap<>();
    for (UnresolvedExpression expr : node.getProjectList()) {
      if (expr instanceof Field) {
        String name = ((Field) expr).getField().toString();
        String leaf = name.contains(".") ? name.substring(name.lastIndexOf('.') + 1) : name;
        leafNameCount.merge(leaf, 1, Integer::sum);
      }
    }
    Set<String> seenLeafNames = new java.util.HashSet<>();
    List<SqlNode> colList = new ArrayList<>();
    for (UnresolvedExpression expr : node.getProjectList()) {
      SqlNode col = expr.accept(this, null);
      if (expr instanceof Field) {
        String name = ((Field) expr).getField().toString();
        if (binReplacements.containsKey(name)) {
          colList.add(as(col, name));
          continue;
        }
        if (col instanceof SqlBasicCall
            && ((SqlBasicCall) col).getOperator() == SqlStdOperatorTable.ITEM) {
          colList.add(as(col, name));
          continue;
        }
        if (name.contains(".")) {
          String leaf = name.substring(name.lastIndexOf('.') + 1);
          if (leafNameCount.getOrDefault(leaf, 0) > 1 && !seenLeafNames.add(leaf)) {
            // Duplicate leaf name: alias with qualified name (e.g. b.country)
            colList.add(as(col, name));
            continue;
          }
        }
      }
      colList.add(col);
    }
    // If in join context (skipNextWrap), replace the select list of the existing
    // SELECT-over-JOIN to keep join aliases visible.
    // Only do this when the existing select list is SELECT * (aliases still visible).
    // If the select list has explicit qualified columns (e.g. from field-list join with
    // overwrite), wrap as subquery so the qualified aliases resolve ambiguity.
    if (skipNextWrap && pipe instanceof SqlSelect && ((SqlSelect) pipe).getFrom() instanceof SqlJoin) {
      skipNextWrap = false;
      SqlSelect joinSelect = (SqlSelect) pipe;
      SqlNodeList selList = joinSelect.getSelectList();
      boolean isSelectStar = selList.size() == 1
          && selList.get(0) instanceof SqlIdentifier
          && ((SqlIdentifier) selList.get(0)).isStar();
      if (isSelectStar) {
        pipe = select(colList.toArray(new SqlNode[0]))
            .from(joinSelect.getFrom())
            .where(joinSelect.getWhere())
            .build();
      } else {
        pipe = select(colList.toArray(new SqlNode[0])).from(subquery(pipe, nextAlias())).build();
      }
    } else {
      pipe = select(colList.toArray(new SqlNode[0])).from(wrapAsSubquery()).build();
    }
    return pipe;
  }

  @Override
  public SqlNode visitSort(Sort node, Void ctx) {
    // If there's a pending LIMIT (from head), wrap it as subquery first
    if (pendingFetch != null) {
      pipe = applyPendingOrderBy(pipe);
      // Wrap the SqlOrderBy in a proper SELECT * FROM (...) subquery
      // so downstream sort doesn't produce a double-alias
      pipe = select(star()).from(subquery(pipe, nextAlias())).build();
    }
    List<SqlNode> orderItems = new ArrayList<>();
    for (Field f : node.getSortList()) {
      SqlNode col = f.getField().accept(this, null);
      boolean asc = true;
      boolean nullFirst = true;
      boolean nullFirstExplicit = false;
      for (Argument arg : f.getFieldArgs()) {
        if ("asc".equals(arg.getArgName())) {
          asc = Boolean.TRUE.equals(arg.getValue().getValue());
        } else if ("nullFirst".equals(arg.getArgName())) {
          nullFirst = Boolean.TRUE.equals(arg.getValue().getValue());
          nullFirstExplicit = true;
        }
      }
      if (!nullFirstExplicit) {
        nullFirst = asc;
      }
      if (!asc) col = desc(col);
      col = nullFirst ? nullsFirst(col) : nullsLast(col);
      orderItems.add(col);
    }
    // Defer ORDER BY — don't wrap as subquery. Store for later application.
    pendingOrderBy = orderItems;
    if (node.getCount() != null && node.getCount() > 0) {
      pendingFetch = intLiteral(node.getCount());
    }
    return pipe;
  }

  @Override
  public SqlNode visitReverse(Reverse node, Void ctx) {
    // Build ROW_NUMBER window ORDER BY from pending sort order (if any)
    SqlNodeList winOrderBy = SqlNodeList.EMPTY;
    if (pendingOrderBy != null) {
      winOrderBy = new SqlNodeList(pendingOrderBy, POS);
      pendingOrderBy = null;
    }
    if (pendingFetch != null) {
      pipe = applyPendingOrderBy(pipe);
    }
    // Step 1: Wrap current pipe as subquery, add ROW_NUMBER() OVER(ORDER BY ...) as __reverse_row_num__
    String rnCol = "__reverse_row_num__";
    SqlNode rowNum = as(
        window(new SqlBasicCall(SqlStdOperatorTable.ROW_NUMBER, new SqlNode[0], POS),
            SqlNodeList.EMPTY, winOrderBy), rnCol);
    pipe = select(star(), rowNum).from(wrapAsSubquery()).build();

    // Step 2: Wrap again, defer ORDER BY __reverse_row_num__ DESC
    pipe = select(star()).from(wrapAsSubquery()).build();
    pendingOrderBy = List.of(desc(identifier(rnCol)));
    return pipe;
  }

  @Override
  public SqlNode visitHead(Head node, Void ctx) {
    // Defer LIMIT — store for later application together with ORDER BY
    pendingFetch = intLiteral(node.getSize());
    if (node.getFrom() != null && node.getFrom() > 0) {
      pendingOffset = intLiteral(node.getFrom());
    }
    return pipe;
  }

  @Override
  public SqlNode visitAggregation(Aggregation node, Void ctx) {
    // Apply any pending FETCH (from head) before aggregation discards it.
    // Discard pendingOrderBy (sort before stats is semantically meaningless).
    if (pendingFetch != null) {
      pipe = applyPendingOrderBy(pipe);
      pipe = select(star()).from(subquery(pipe, nextAlias())).build();
    }
    pendingOrderBy = null;
    pendingFetch = null;
    pendingOffset = null;

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
    } else if (node.getSpan() != null && isTimeSpan(node.getSpan()) && !groupByItems.isEmpty()) {
      // Time-based spans always filter out NULL values (PPL semantics)
      SqlNode spanGroupBy = groupByItems.get(0);
      builder = builder.where(isNotNull(spanGroupBy));
    }

    pipe = builder.build();
    return pipe;
  }

  @Override
  public SqlNode visitEval(Eval node, Void ctx) {
    Map<String, SqlNode> evalAliases = new java.util.LinkedHashMap<>();
    // Collect eval expressions
    List<Map.Entry<String, SqlNode>> evalEntries = new ArrayList<>();
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
        // After substitution, fix ARRAY(ARRAY(...), ...) → ARRAY_CONCAT(...)
        expr = fixNestedArrayToConcat(expr);
      }
      // Handle fieldformat prefix/suffix concatenation
      expr = applyLetConcatPrefixSuffix(let, expr);
      evalAliases.put(varName, expr);
      evalEntries.add(Map.entry(varName, expr));
    }

    // Check if any eval variable overrides an existing column in the pipe's SELECT list
    Set<String> pipeColNames = new java.util.HashSet<>();
    // Unwrap SqlOrderBy to get the underlying SqlSelect for column detection
    SqlNode pipeForOverride = pipe;
    if (pipeForOverride instanceof SqlOrderBy) {
      pipeForOverride = ((SqlOrderBy) pipeForOverride).query;
    }
    if (pipeForOverride instanceof org.apache.calcite.sql.SqlSelect) {
      org.apache.calcite.sql.SqlSelect sel = (org.apache.calcite.sql.SqlSelect) pipeForOverride;
      if (sel.getSelectList() != null) {
        for (SqlNode item : sel.getSelectList()) {
          String name = extractPipeColumnName(item);
          if (name != null) pipeColNames.add(name);
        }
      }
    }

    boolean hasOverride = false;
    for (var entry : evalEntries) {
      if (pipeColNames.contains(entry.getKey())) {
        hasOverride = true;
        break;
      }
    }

    if (hasOverride && pipeForOverride instanceof org.apache.calcite.sql.SqlSelect) {
      org.apache.calcite.sql.SqlSelect sel = (org.apache.calcite.sql.SqlSelect) pipeForOverride;

      // If the pipe has GROUP BY, wrap as subquery first so eval expressions
      // can reference aggregated columns without GROUP BY restrictions.
      if (sel.getGroup() != null && sel.getGroup().size() > 0) {
        SqlNodeList oldList = sel.getSelectList();
        // Collect column names from the grouped query's SELECT list
        List<String> origColNames = new ArrayList<>();
        for (SqlNode item : oldList) {
          String name = extractPipeColumnName(item);
          if (name != null) origColNames.add(name);
        }
        // Wrap grouped query as subquery
        SqlNode sub = wrapAsSubquery();
        // Build SELECT list: for overridden columns use eval expr, others reference by name
        Set<String> overrideNames = new java.util.HashSet<>();
        for (var entry : evalEntries) {
          if (pipeColNames.contains(entry.getKey())) overrideNames.add(entry.getKey());
        }
        List<SqlNode> items = new ArrayList<>();
        for (String name : origColNames) {
          if (overrideNames.contains(name)) {
            items.add(as(evalAliases.get(name), name));
          } else {
            items.add(identifier(name));
          }
        }
        // Add new (non-override) eval columns
        for (var entry : evalEntries) {
          if (!pipeColNames.contains(entry.getKey())) {
            items.add(as(entry.getValue(), entry.getKey()));
          }
        }
        pipe = select(items.toArray(new SqlNode[0])).from(sub).build();
        return pipe;
      }

      // Non-GROUP BY override: modify the pipe's SELECT list in-place
      SqlNodeList oldList = sel.getSelectList();

      // Build a map of existing column name -> expression (for substitution)
      Map<String, SqlNode> existingExprs = new java.util.HashMap<>();
      for (SqlNode item : oldList) {
        String name = extractPipeColumnName(item);
        if (name != null && item instanceof SqlBasicCall) {
          SqlBasicCall asCall = (SqlBasicCall) item;
          if ("AS".equals(asCall.getOperator().getName()) && asCall.operandCount() >= 2) {
            existingExprs.put(name, asCall.operand(0)); // the expression before AS
          }
        }
      }

      Map<String, SqlNode> overrideMap = new java.util.HashMap<>();
      List<String> newCols = new ArrayList<>();
      for (var entry : evalEntries) {
        if (pipeColNames.contains(entry.getKey())) {
          SqlNode newExpr = entry.getValue();
          // Substitute references to the overridden column with the old expression
          SqlNode oldExpr = existingExprs.get(entry.getKey());
          if (oldExpr != null) {
            final String colName = entry.getKey();
            final SqlNode replacement = oldExpr;
            newExpr = newExpr.accept(new org.apache.calcite.sql.util.SqlShuttle() {
              @Override
              public SqlNode visit(org.apache.calcite.sql.SqlIdentifier id) {
                if (id.isSimple() && id.getSimple().equals(colName)) {
                  return replacement;
                }
                return id;
              }
            });
          }
          overrideMap.put(entry.getKey(), as(newExpr, entry.getKey()));
        } else {
          newCols.add(entry.getKey());
        }
      }
      List<SqlNode> newItems = new ArrayList<>();
      for (SqlNode item : oldList) {
        String name = extractPipeColumnName(item);
        if (name != null && overrideMap.containsKey(name)) {
          newItems.add(overrideMap.get(name));
        } else {
          newItems.add(item);
        }
      }
      for (String col : newCols) {
        newItems.add(as(evalAliases.get(col), col));
      }
      sel.setSelectList(new SqlNodeList(newItems, SqlParserPos.ZERO));
    } else {
      // Simple case: no overrides, just add new columns
      List<SqlNode> items = new ArrayList<>();
      items.add(star());
      for (var entry : evalEntries) {
        items.add(as(entry.getValue(), entry.getKey()));
      }
      pipe = select(items.toArray(new SqlNode[0])).from(wrapAsSubquery()).build();
    }
    return pipe;
  }

  /** Extract column name from a SELECT list item (AS alias or plain identifier). */
  protected String extractPipeColumnName(SqlNode node) {
    if (node instanceof SqlIdentifier) {
      SqlIdentifier id = (SqlIdentifier) node;
      if (id.isStar()) return null;
      return id.names.get(id.names.size() - 1);
    }
    if (node instanceof SqlBasicCall) {
      SqlBasicCall call = (SqlBasicCall) node;
      if ("AS".equals(call.getOperator().getName()) && call.operandCount() >= 2) {
        return extractPipeColumnName(call.operand(1));
      }
    }
    return null;
  }

  /** Wrap expression with CONCAT for fieldformat prefix/suffix. Returns null-safe result. */
  protected SqlNode applyLetConcatPrefixSuffix(Let let, SqlNode expr) {
    if (let.getConcatPrefix() == null && let.getConcatSuffix() == null) return expr;
    // Cast inner expression to VARCHAR for string concatenation
    SqlNode strExpr = cast(expr, typeSpec(SqlTypeName.VARCHAR));
    if (let.getConcatPrefix() != null && let.getConcatSuffix() != null) {
      SqlNode prefix = literal(let.getConcatPrefix().getValue());
      SqlNode suffix = literal(let.getConcatSuffix().getValue());
      return caseWhen(List.of(isNull(expr)), List.of(literal(null)),
          call("CONCAT", call("CONCAT", prefix, strExpr), suffix));
    }
    if (let.getConcatPrefix() != null) {
      SqlNode prefix = literal(let.getConcatPrefix().getValue());
      return caseWhen(List.of(isNull(expr)), List.of(literal(null)),
          call("CONCAT", prefix, strExpr));
    }
    SqlNode suffix = literal(let.getConcatSuffix().getValue());
    return caseWhen(List.of(isNull(expr)), List.of(literal(null)),
        call("CONCAT", strExpr, suffix));
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
    String effectiveLeftAlias;
    SqlNode leftSide;

    if (pipe instanceof SqlJoin) {
      // Multi-join chain: use the raw SqlJoin directly as the left side.
      leftSide = pipe;
      effectiveLeftAlias = leftAlias; // may be null for multi-join
    } else if (leftAlias != null) {
      leftSide = subquery(pipe, leftAlias);
      effectiveLeftAlias = leftAlias;
    } else {
      String genAlias = nextAlias();
      leftSide = subquery(pipe, genAlias);
      effectiveLeftAlias = genAlias;
    }

    String rightAlias = node.getRightAlias().orElse(null);
    SqlNode rightSide = resolveJoinRight(node.getRight(), rightAlias);
    String effectiveRightAlias = rightAlias != null ? rightAlias : extractAlias(rightSide);

    if (effectiveLeftAlias != null) knownAliases.add(effectiveLeftAlias);
    if (effectiveRightAlias != null) knownAliases.add(effectiveRightAlias);
    // Track all SubqueryAlias names from the right side and map them to the effective alias
    for (String saName : extractAllSubqueryAliasNames(node.getRight())) {
      knownAliases.add(saName);
      if (effectiveRightAlias != null && !saName.equals(effectiveRightAlias)) {
        aliasMapping.put(saName, effectiveRightAlias);
      }
    }
    // Track the left SubqueryAlias name too (source = table as tt | JOIN left=t1 ...)
    String leftSubqueryAlias = extractSubqueryAliasName(node.getLeft());
    if (leftSubqueryAlias != null && !leftSubqueryAlias.equals(effectiveLeftAlias)) {
      knownAliases.add(leftSubqueryAlias);
      if (effectiveLeftAlias != null) {
        aliasMapping.put(leftSubqueryAlias, effectiveLeftAlias);
      }
    }
    // Clear pending SubqueryAlias — the JOIN handles aliasing directly.
    pendingSubqueryAlias = null;

    SqlNode condition = null;
    if (node.getJoinCondition().isPresent()) {
      condition = node.getJoinCondition().get().accept(this, null);
    } else if (node.getJoinFields().isPresent() && !node.getJoinFields().get().isEmpty()) {
      condition = buildFieldListCondition(node.getJoinFields().get(),
          node.getLeftAlias().orElse(effectiveLeftAlias),
          node.getRightAlias().orElse(effectiveRightAlias));
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

    // Handle max option: limit right-side matches per join key group
    Literal maxLit = node.getArgumentMap().get("max");
    if (maxLit != null && !maxLit.equals(Literal.ZERO)) {
      int maxVal = (Integer) maxLit.getValue();
      rightSide = applyJoinMaxToRightSide(rightSide, maxVal, node, effectiveRightAlias);
    }

    SqlNode joinNode = SqlNodeDSL.join(leftSide, calciteJoinType, rightSide, condition);

    // Set pipe to raw SqlJoin so aliases stay visible to downstream stages.
    pipe = joinNode;
    skipNextWrap = true;
    return pipe;
  }

  /**
   * Wrap the right side of a join with ROW_NUMBER + filter to limit matches per join key group.
   * The resulting right side will contain an extra {@code _rn} column that must be removed
   * by downstream processing (e.g., DynamicPPLToSqlNodeConverter).
   */
  protected SqlNode applyJoinMaxToRightSide(
      SqlNode rightSide, int maxVal, Join node, String effectiveRightAlias) {
    // Determine partition keys for ROW_NUMBER
    List<SqlNode> partitionKeys = getJoinMaxPartitionKeys(node, effectiveRightAlias);
    if (partitionKeys.isEmpty()) return rightSide;

    // Extract inner query and alias from the right side (which is AS(inner, alias))
    String alias = extractAlias(rightSide);
    SqlNode inner;
    if (rightSide instanceof SqlBasicCall) {
      SqlBasicCall call = (SqlBasicCall) rightSide;
      if ("AS".equals(call.getOperator().getName())) {
        inner = call.operand(0);
      } else {
        inner = rightSide;
      }
    } else {
      inner = rightSide;
    }

    // Build: SELECT *, ROW_NUMBER() OVER (PARTITION BY keys ORDER BY keys) AS _rn FROM inner
    String innerAlias = "_rn_inner";
    SqlNodeList partBy = new SqlNodeList(partitionKeys, SqlParserPos.ZERO);
    SqlNode rowNum = as(
        window(call("ROW_NUMBER"), partBy, new SqlNodeList(partitionKeys, SqlParserPos.ZERO)),
        JOIN_MAX_RN_COLUMN);
    SqlNode withRn = select(star(), rowNum).from(subquery(inner, innerAlias)).build();

    // Build: SELECT * FROM (withRn) WHERE _rn <= maxVal
    String filterAlias = "_rn_filter";
    SqlNode filtered = select(star()).from(subquery(withRn, filterAlias))
        .where(lte(identifier(JOIN_MAX_RN_COLUMN), intLiteral(maxVal))).build();

    // Re-apply the original alias
    return alias != null ? subquery(filtered, alias) : subquery(filtered, effectiveRightAlias);
  }

  /** Column name used for the ROW_NUMBER in join max option. */
  protected static final String JOIN_MAX_RN_COLUMN = "_rn";

  /** Extract partition keys for the join max ROW_NUMBER window. */
  private List<SqlNode> getJoinMaxPartitionKeys(Join node, String effectiveRightAlias) {
    if (node.getJoinFields().isPresent() && !node.getJoinFields().get().isEmpty()) {
      // Field-list join: partition by field names (unqualified, on right side)
      return node.getJoinFields().get().stream()
          .map(f -> (SqlNode) identifier(f.getField().toString()))
          .collect(Collectors.toList());
    } else if (node.getJoinCondition().isPresent()) {
      // Criteria join: extract right-side column names from the ON condition
      return extractRightColumnsFromCondition(node.getJoinCondition().get(), effectiveRightAlias);
    }
    return List.of();
  }

  /** Extract right-side column references from a join condition expression. */
  private List<SqlNode> extractRightColumnsFromCondition(
      UnresolvedExpression condition, String rightAlias) {
    List<SqlNode> result = new ArrayList<>();
    collectRightColumns(condition, rightAlias, result);
    return result;
  }

  private void collectRightColumns(
      UnresolvedExpression expr, String rightAlias, List<SqlNode> result) {
    if (expr instanceof QualifiedName) {
      List<String> parts = ((QualifiedName) expr).getParts();
      if (parts.size() >= 2 && parts.get(0).equals(rightAlias)) {
        // Right-side qualified reference like r.country -> use unqualified name
        result.add(identifier(parts.get(parts.size() - 1)));
      }
    }
    for (var child : expr.getChild()) {
      if (child instanceof UnresolvedExpression) {
        collectRightColumns((UnresolvedExpression) child, rightAlias, result);
      }
    }
  }

  /** Extract alias from a SqlNode that is an AS expression (e.g., subquery(..., alias)). */
  protected static String extractAlias(SqlNode node) {
    if (node instanceof SqlBasicCall) {
      SqlBasicCall call = (SqlBasicCall) node;
      if ("AS".equals(call.getOperator().getName()) && call.operandCount() >= 2) {
        SqlNode aliasNode = call.operand(1);
        if (aliasNode instanceof SqlIdentifier) {
          return ((SqlIdentifier) aliasNode).getSimple();
        }
      }
    }
    if (node instanceof SqlIdentifier) {
      return ((SqlIdentifier) node).getSimple();
    }
    return null;
  }

  /** Extract the source table name from a pipe that is SELECT * FROM "tableName". */
  private static String extractTableNameFromPipe(SqlNode pipe) {
    if (!(pipe instanceof org.apache.calcite.sql.SqlSelect)) return null;
    org.apache.calcite.sql.SqlSelect sel = (org.apache.calcite.sql.SqlSelect) pipe;
    SqlNode from = sel.getFrom();
    if (from instanceof SqlIdentifier && !((SqlIdentifier) from).isStar()) {
      return ((SqlIdentifier) from).getSimple();
    }
    return null;
  }

  /** Extract SubqueryAlias name from an AST plan node. */
  private static String extractSubqueryAliasName(UnresolvedPlan plan) {
    if (plan instanceof SubqueryAlias) {
      return ((SubqueryAlias) plan).getAlias();
    }
    return null;
  }

  /** Extract all SubqueryAlias names from an AST plan node (including nested ones). */
  private static List<String> extractAllSubqueryAliasNames(UnresolvedPlan plan) {
    List<String> names = new ArrayList<>();
    collectSubqueryAliasNames(plan, names);
    return names;
  }

  private static void collectSubqueryAliasNames(UnresolvedPlan plan, List<String> names) {
    if (plan instanceof SubqueryAlias) {
      names.add(((SubqueryAlias) plan).getAlias());
    }
    for (Node child : plan.getChild()) {
      if (child instanceof UnresolvedPlan) {
        collectSubqueryAliasNames((UnresolvedPlan) child, names);
      }
    }
  }

  @Override
  public SqlNode visitLookup(Lookup node, Void ctx) {
    String leftAlias = "_l";
    String rightAlias = "_r";
    knownAliases.add(leftAlias);
    knownAliases.add(rightAlias);
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

    // Step 0: optional null filter
    if (!useNull) {
      pipe = wrapAsSubquery();
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

    // SELECT *, expr AS alias — works for new columns (trendline aliases are typically new)
    // DynamicPPLToSqlNodeConverter overrides this to use REPLACE for existing columns
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
      // Default ORDER BY CAST("_id" AS INTEGER) for deterministic row-by-row accumulation
      if (ordItems.isEmpty()) {
        ordItems.add(cast(identifier("_id"), typeSpec(SqlTypeName.INTEGER)));
      }
      SqlNodeList ordBy = new SqlNodeList(ordItems, POS);

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
    // Check if subsearch has a source (Relation node). If not, it's an empty search.
    if (!hasRelation(node.getSubSearch())) {
      return pipe;
    }
    SqlNode subSql;
    try {
      subSql = convertSubPlan(node.getSubSearch());
    } catch (Exception e) {
      // Empty subsearch or conversion failure — return main pipe unchanged
      return pipe;
    }
    if (subSql == null) {
      return pipe;
    }
    pipe = select(star()).from(subquery(unionAll(mainSql, subSql), nextAlias())).build();
    return pipe;
  }

  private static boolean hasRelation(UnresolvedPlan plan) {
    if (plan == null) return false;
    if (plan instanceof Relation) return true;
    for (Node child : plan.getChild()) {
      if (child instanceof UnresolvedPlan && hasRelation((UnresolvedPlan) child)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public SqlNode visitAppendPipe(AppendPipe node, Void ctx) {
    SqlNode originalPipe = pipe;
    // Apply sub-pipeline commands to a copy of the current pipe
    SqlNode subResult = convertSubPipeline(node.getSubQuery(), pipe);
    pipe = unionAll(originalPipe, subResult);
    return pipe;
  }

  /** Convert a sub-pipeline starting from the given initial pipe state. */
  protected SqlNode convertSubPipeline(UnresolvedPlan plan, SqlNode initialPipe) {
    PPLToSqlNodeConverter sub = new PPLToSqlNodeConverter(aliasCounter);
    sub.pipe = initialPipe;
    List<UnresolvedPlan> nodes = new ArrayList<>();
    flatten(plan, nodes);
    for (UnresolvedPlan n : nodes) {
      n.accept(sub, null);
    }
    if (sub.pendingOrderBy != null || sub.pendingFetch != null) {
      sub.pipe = sub.applyPendingOrderBy(sub.pipe);
    }
    return sub.pipe;
  }

  @Override
  public SqlNode visitBin(Bin node, Void ctx) {
    SqlNode field = node.getField().accept(this, null);
    String alias = node.getAlias() != null ? node.getAlias() : getFieldName(node.getField());

    SqlNode binSql;
    if (node instanceof SpanBin) {
      binSql = visitSpanBin((SpanBin) node, field);
    } else if (node instanceof CountBin) {
      binSql = visitCountBin((CountBin) node, field);
    } else if (node instanceof MinSpanBin) {
      binSql = visitMinSpanBin((MinSpanBin) node, field);
    } else if (node instanceof RangeBin) {
      binSql = visitRangeBin((RangeBin) node, field);
    } else if (node instanceof DefaultBin) {
      binSql = visitDefaultBin((DefaultBin) node, field);
    } else {
      throw new UnsupportedOperationException(
          "Unsupported bin type in V4: " + node.getClass().getSimpleName());
    }

    String fieldName = getFieldName(node.getField());
    // All bin types: subquery approach — materialize bin expression as a named column
    String tmpAlias = "_bin_" + fieldName.replaceAll("[^a-zA-Z0-9]", "_");
    pipe = select(star(), as(binSql, tmpAlias)).from(wrapAsSubquery()).build();
    // When there's an explicit alias, only map the alias — keep original field intact
    if (node.getAlias() != null && !alias.equals(fieldName)) {
      binReplacements.put(alias, identifier(tmpAlias));
    } else {
      binReplacements.put(fieldName, identifier(tmpAlias));
    }
    return pipe;
  }

  private SqlNode visitSpanBin(SpanBin spanBin, SqlNode field) {
    UnresolvedExpression spanExpr = spanBin.getSpan();
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
          return call("CONCAT",
              call("CONCAT", cast(cast(start, intType), varcharType), literal("-")),
              cast(cast(end, intType), varcharType));
        } else {
          return call("CONCAT",
              call("CONCAT", cast(start, varcharType), literal("-")),
              cast(end, varcharType));
        }
      } else {
        return transpileSpanToSqlNode(lit.getValue().toString(), field);
      }
    }
    return cast(field, typeSpec(SqlTypeName.VARCHAR));
  }

  /** CountBin: nice-number width algorithm using inline SQL. */
  private SqlNode visitCountBin(CountBin countBin, SqlNode field) {
    int bins = countBin.getBins() != null ? countBin.getBins() : 10;
    SqlNode dblField = cast(field, typeSpec(SqlTypeName.DOUBLE));
    SqlNode minOver = window(min(dblField), SqlNodeList.EMPTY, SqlNodeList.EMPTY);
    SqlNode maxOver = window(max(dblField), SqlNodeList.EMPTY, SqlNodeList.EMPTY);
    SqlNode dataRange = minus(maxOver, minOver);
    // Guard: if range <= 0, use width=1
    SqlNode safeRange = caseWhen(
        List.of(gt(dataRange, literal(0))),
        List.of(dataRange),
        literal(1.0));
    SqlNode targetWidth = divide(safeRange, literal(bins));
    // Guard LOG10 argument: must be > 0
    SqlNode safeTarget = caseWhen(
        List.of(gt(targetWidth, literal(0))),
        List.of(targetWidth),
        literal(1.0));
    SqlNode exponent = call("CEIL", call("LOG10", safeTarget));
    SqlNode width1 = call("POWER", literal(10.0), exponent);
    // Boundary check: if max falls on bin boundary, need extra bin
    // Use FLOOR(max/width)*width == max instead of MOD to avoid DECIMAL overflow
    SqlNode maxOnBoundary = caseWhen(
        List.of(eq(times(call("FLOOR", divide(maxOver, width1)), width1), maxOver)),
        List.of(literal(1.0)),
        literal(0.0));
    SqlNode actualBins = plus(call("CEIL", divide(safeRange, width1)), maxOnBoundary);
    // If actualBins > requested bins, go to next magnitude
    SqlNode width = caseWhen(
        List.of(gt(actualBins, literal(bins))),
        List.of(call("POWER", literal(10.0), plus(exponent, literal(1)))),
        width1);
    SqlNode binStart = times(call("FLOOR", divide(dblField, width)), width);
    SqlNode binEnd = plus(binStart, width);
    return formatBinRange(binStart, binEnd);
  }

  /** MinSpanBin: magnitude-based width with minimum span constraint. */
  private SqlNode visitMinSpanBin(MinSpanBin minSpanBin, SqlNode field) {
    SqlNode minspanVal = minSpanBin.getMinspan().accept(this, null);
    SqlNode dblField = cast(field, typeSpec(SqlTypeName.DOUBLE));
    SqlNode minOver = window(min(dblField), SqlNodeList.EMPTY, SqlNodeList.EMPTY);
    SqlNode maxOver = window(max(dblField), SqlNodeList.EMPTY, SqlNodeList.EMPTY);
    SqlNode dataRange = minus(maxOver, minOver);
    SqlNode safeRange = caseWhen(
        List.of(gt(dataRange, literal(0))),
        List.of(dataRange),
        literal(1.0));
    SqlNode dblMinspan = cast(minspanVal, typeSpec(SqlTypeName.DOUBLE));
    // minspanWidth = POWER(10, CEIL(LOG10(minspan)))
    SqlNode minspanWidth = call("POWER", literal(10.0), call("CEIL", call("LOG10", dblMinspan)));
    // defaultWidth = POWER(10, FLOOR(LOG10(dataRange)))
    SqlNode defaultWidth = call("POWER", literal(10.0), call("FLOOR", call("LOG10", safeRange)));
    // Use defaultWidth if >= minspan, else minspanWidth
    SqlNode width = caseWhen(
        List.of(gte(defaultWidth, dblMinspan)),
        List.of(defaultWidth),
        minspanWidth);
    SqlNode binStart = times(call("FLOOR", divide(dblField, width)), width);
    SqlNode binEnd = plus(binStart, width);
    return formatBinRange(binStart, binEnd);
  }

  /** RangeBin: magnitude-based width with start/end range constraints. */
  private SqlNode visitRangeBin(RangeBin rangeBin, SqlNode field) {
    SqlNode dblField = cast(field, typeSpec(SqlTypeName.DOUBLE));
    SqlNode minOver = window(min(dblField), SqlNodeList.EMPTY, SqlNodeList.EMPTY);
    SqlNode maxOver = window(max(dblField), SqlNodeList.EMPTY, SqlNodeList.EMPTY);
    SqlNode effectiveMin = rangeBin.getStart() != null
        ? cast(rangeBin.getStart().accept(this, null), typeSpec(SqlTypeName.DOUBLE)) : minOver;
    SqlNode effectiveMax = rangeBin.getEnd() != null
        ? cast(rangeBin.getEnd().accept(this, null), typeSpec(SqlTypeName.DOUBLE)) : maxOver;
    SqlNode dataRange = minus(effectiveMax, effectiveMin);
    SqlNode safeRange = caseWhen(
        List.of(gt(dataRange, literal(0))),
        List.of(dataRange),
        literal(1.0));
    SqlNode magnitude = call("FLOOR", call("LOG10", safeRange));
    SqlNode width = call("POWER", literal(10.0), magnitude);
    SqlNode widthInt = call("FLOOR", width);
    SqlNode binStart = times(call("FLOOR", divide(dblField, widthInt)), widthInt);
    SqlNode binEnd = plus(binStart, widthInt);
    return formatBinRange(binStart, binEnd);
  }

  /** DefaultBin: automatic magnitude-based binning. */
  private SqlNode visitDefaultBin(DefaultBin defaultBin, SqlNode field) {
    SqlNode dblField = cast(field, typeSpec(SqlTypeName.DOUBLE));
    SqlNode minOver = window(min(dblField), SqlNodeList.EMPTY, SqlNodeList.EMPTY);
    SqlNode maxOver = window(max(dblField), SqlNodeList.EMPTY, SqlNodeList.EMPTY);
    SqlNode dataRange = minus(maxOver, minOver);
    SqlNode safeRange = caseWhen(
        List.of(gt(dataRange, literal(0))),
        List.of(dataRange),
        literal(1.0));
    SqlNode magnitude = call("FLOOR", call("LOG10", safeRange));
    SqlNode width = call("POWER", literal(10.0), magnitude);
    SqlNode widthInt = call("FLOOR", width);
    SqlNode binStart = times(call("FLOOR", divide(dblField, widthInt)), widthInt);
    SqlNode binEnd = plus(binStart, widthInt);
    return formatBinRange(binStart, binEnd);
  }

  /** Format bin range as "start-end" string with integer casting. */
  private SqlNode formatBinRange(SqlNode binStart, SqlNode binEnd) {
    SqlDataTypeSpec intType = typeSpec(SqlTypeName.INTEGER);
    SqlDataTypeSpec vc = typeSpec(SqlTypeName.VARCHAR);
    return call("CONCAT",
        call("CONCAT", cast(cast(binStart, intType), vc), literal("-")),
        cast(cast(binEnd, intType), vc));
  }

  /** Build FLOOR(field TO timeUnit) using SqlStdOperatorTable.FLOOR + SqlIntervalQualifier. */
  private SqlNode floorToUnit(SqlNode field, String truncUnit) {
    TimeUnit tu = mapTruncUnit(truncUnit);
    return new SqlBasicCall(SqlStdOperatorTable.FLOOR,
        new SqlNode[] {field, new SqlIntervalQualifier(tu, null, POS)}, POS);
  }

  private static TimeUnit mapTruncUnit(String truncUnit) {
    switch (truncUnit) {
      case "SECOND": return TimeUnit.SECOND;
      case "MINUTE": return TimeUnit.MINUTE;
      case "HOUR": return TimeUnit.HOUR;
      case "DAY": return TimeUnit.DAY;
      case "WEEK": return TimeUnit.WEEK;
      case "MONTH": return TimeUnit.MONTH;
      case "QUARTER": return TimeUnit.QUARTER;
      case "YEAR": return TimeUnit.YEAR;
      default: return TimeUnit.SECOND;
    }
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
          cast(floorToUnit(field, truncUnit), typeSpec(SqlTypeName.VARCHAR)),
          intLiteral(1), intLiteral(7));
    }
    if (isMonth) {
      // Multi-month span: compute total months, floor to span, convert to year-month string
      // totalMonths = EXTRACT(YEAR FROM field) * 12 + EXTRACT(MONTH FROM field) - 1
      SqlNode yearPart = new SqlBasicCall(SqlStdOperatorTable.EXTRACT,
          new SqlNode[]{new SqlIntervalQualifier(TimeUnit.YEAR, null, POS), field}, POS);
      SqlNode monthPart = new SqlBasicCall(SqlStdOperatorTable.EXTRACT,
          new SqlNode[]{new SqlIntervalQualifier(TimeUnit.MONTH, null, POS), field}, POS);
      SqlNode totalMonths = plus(times(yearPart, intLiteral(12)),
          minus(monthPart, intLiteral(1)));
      // flooredMonths = FLOOR(totalMonths / value) * value
      SqlNode flooredMonths = times(call("FLOOR",
          divide(totalMonths, intLiteral(value))), intLiteral(value));
      // resultYear = flooredMonths / 12, resultMonth = flooredMonths % 12 + 1
      SqlNode resultYear = divide(flooredMonths, intLiteral(12));
      SqlNode resultMonth = plus(call("MOD", flooredMonths, intLiteral(12)), intLiteral(1));
      // Format as "YYYY-MM"
      SqlDataTypeSpec vc = typeSpec(SqlTypeName.VARCHAR);
      return call("CONCAT",
          call("CONCAT", cast(resultYear, vc), literal("-")),
          call("LPAD", cast(resultMonth, vc), intLiteral(2), literal("0")));
    }
    if (value == 1 && secondsPerUnit > 0) {
      return floorToUnit(field, truncUnit);
    }
    if (secondsPerUnit > 0) {
      long totalSeconds = (long) value * secondsPerUnit;
      SqlNode epoch = call("TIMESTAMPDIFF",
          identifier("SECOND"),
          cast(literal("1970-01-01 00:00:00"), typeSpec(SqlTypeName.TIMESTAMP)),
          field);
      SqlNode floored = times(call("FLOOR",
          divide(epoch, literal(totalSeconds))), literal(totalSeconds));
      return call("TIMESTAMPADD",
          identifier("SECOND"), cast(floored, typeSpec(SqlTypeName.INTEGER)),
          cast(literal("1970-01-01 00:00:00"), typeSpec(SqlTypeName.TIMESTAMP)));
    }
    // Fallback for sub-second or multi-month
    return floorToUnit(field, "SECOND");
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
        expr = buildReplacePairExpr(expr, pair);
      }
      selectItems.add(as(expr, fieldName));
    }
    pipe = select(selectItems.toArray(new SqlNode[0])).from(pipe).build();
    return pipe;
  }

  /** Escape regex special characters for use in REGEXP_REPLACE pattern. */
  protected static String escapeRegexChars(String s) {
    return s.replaceAll("([\\\\\\[\\]{}().*+?^$|])", "\\\\$1");
  }

  /** Build REGEXP_REPLACE expression for a replace pair (handles wildcard and literal patterns). */
  protected SqlNode buildReplacePairExpr(SqlNode expr, ReplacePair pair) {
    String pattern = pair.getPattern().getValue().toString();
    String replacement = pair.getReplacement().getValue().toString();
    if (hasUnescapedWildcard(pattern) || hasUnescapedWildcard(replacement)) {
      WildcardUtils.validateWildcardSymmetry(pattern, replacement);
      String regexPattern = WildcardUtils.convertWildcardPatternToRegex(pattern);
      String regexReplacement = WildcardUtils.convertWildcardReplacementToRegex(replacement);
      return call("REGEXP_REPLACE", expr, literal(regexPattern), literal(regexReplacement));
    }
    String unescapedPattern = pattern.replace("\\*", "*");
    String unescapedReplacement = replacement.replace("\\*", "*");
    String escapedPattern = escapeRegexChars(unescapedPattern);
    String escapedRepl = java.util.regex.Matcher.quoteReplacement(unescapedReplacement);
    return call("REGEXP_REPLACE", expr, literal(escapedPattern), literal(escapedRepl));
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
    if (node.getParseMethod() != ParseMethod.REGEX && node.getParseMethod() != ParseMethod.GROK) {
      throw new UnsupportedOperationException(
          "Unsupported PPL command: Parse (" + node.getParseMethod() + ")");
    }
    SqlNode sourceField = node.getSourceField().accept(this, null);
    String pattern = ((Literal) node.getPattern()).getValue().toString();

    String regexPattern;
    List<String> groupNames;

    if (node.getParseMethod() == ParseMethod.GROK) {
      // Compile grok pattern to Java regex with named groups
      org.opensearch.sql.common.grok.GrokCompiler grokCompiler =
          org.opensearch.sql.common.grok.GrokCompiler.newInstance();
      grokCompiler.registerDefaultPatterns();
      org.opensearch.sql.common.grok.Grok grok = grokCompiler.compile(pattern);
      regexPattern = grok.getNamedRegex();
      // Extract user-facing group names (filter out internal "UNWANTED" groups)
      groupNames = new ArrayList<>();
      for (String id : grok.namedGroups) {
        String name = grok.getNamedRegexCollectionById(id);
        if (name != null && !name.equals("UNWANTED")) {
          groupNames.add(name);
        }
      }
    } else {
      regexPattern = pattern;
      // Validate named group names (throws IllegalArgumentException for invalid names)
      RegexCommonUtils.getNamedGroupCandidates(pattern);
      // Extract named group names
      java.util.regex.Pattern namedGroupPattern =
          java.util.regex.Pattern.compile("\\(\\?<([a-zA-Z][a-zA-Z0-9]*)>");
      java.util.regex.Matcher matcher = namedGroupPattern.matcher(pattern);
      groupNames = new ArrayList<>();
      while (matcher.find()) groupNames.add(matcher.group(1));
    }

    pipe = wrapAsSubquery();
    List<SqlNode> selectItems = new ArrayList<>();
    selectItems.add(star());
    // For each named group, isolate it as the only capturing group and extract
    // We need to find each named group in the regex and convert others to non-capturing
    // First, extract all named groups from the regex pattern
    java.util.regex.Pattern namedGroupFinder =
        java.util.regex.Pattern.compile("\\(\\?<([a-zA-Z][a-zA-Z0-9]*)>");
    java.util.regex.Matcher allGroupsMatcher = namedGroupFinder.matcher(regexPattern);
    List<String> allRegexGroupNames = new ArrayList<>();
    while (allGroupsMatcher.find()) allRegexGroupNames.add(allGroupsMatcher.group(1));

    for (String groupName : groupNames) {
      // Find the regex group name that maps to this user-facing name
      // For REGEX parse, they're the same. For GROK, we need to find the internal ID.
      String targetRegexGroupName = groupName;
      if (node.getParseMethod() == ParseMethod.GROK) {
        org.opensearch.sql.common.grok.GrokCompiler gc =
            org.opensearch.sql.common.grok.GrokCompiler.newInstance();
        gc.registerDefaultPatterns();
        org.opensearch.sql.common.grok.Grok g = gc.compile(pattern);
        for (String id : g.namedGroups) {
          if (groupName.equals(g.getNamedRegexCollectionById(id))) {
            targetRegexGroupName = id;
            break;
          }
        }
      }
      String singleGroupPattern = regexPattern;
      // Convert other named groups to non-capturing
      for (String rgn : allRegexGroupNames) {
        if (!rgn.equals(targetRegexGroupName)) {
          singleGroupPattern = singleGroupPattern.replace("(?<" + rgn + ">", "(?:");
        }
      }
      // Convert remaining unnamed capturing groups to non-capturing
      singleGroupPattern =
          singleGroupPattern.replaceAll("(?<!\\\\)\\((?!\\?)", "(?:");
      // Convert target named group to unnamed capturing group
      singleGroupPattern = singleGroupPattern.replace("(?<" + targetRegexGroupName + ">", "(");
      SqlNode regexExpr = call("COALESCE",
          call("REGEXP_EXTRACT", sourceField, literal(singleGroupPattern), literal(1)),
          literal(""));
      selectItems.add(as(regexExpr, groupName));
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
      return new SqlBasicCall(
          SqlStdOperatorTable.COUNT,
          new SqlNode[] {args.get(0).accept(this, null)},
          POS,
          SqlLiteral.createSymbol(SqlSelectKeyword.DISTINCT, POS));
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
    if ("earliest".equals(name))
      return call("ARG_MIN", args.get(0).accept(this, null), identifier("@timestamp"));
    if ("latest".equals(name))
      return call("ARG_MAX", args.get(0).accept(this, null), identifier("@timestamp"));

    String sqlName = FUNC_MAP.getOrDefault(name, name.toUpperCase());
    if (args.isEmpty()) {
      // Now-like datetime functions: generate UTC literals at transpile time
      LocalDateTime utcNow = LocalDateTime.now(ZoneOffset.UTC);
      if (Set.of("now", "current_timestamp", "localtimestamp", "localtime", "utc_timestamp", "sysdate").contains(name))
        return literal(utcNow.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
      if (Set.of("curdate", "current_date", "utc_date").contains(name))
        return literal(utcNow.toLocalDate().toString());
      if (Set.of("curtime", "current_time", "utc_time").contains(name))
        return literal(utcNow.toLocalTime().format(DateTimeFormatter.ofPattern("HH:mm:ss")));
      return call(sqlName);
    }
    SqlNode[] sqlArgs = args.stream().map(a -> a.accept(this, null)).toArray(SqlNode[]::new);
    return call(sqlName, sqlArgs);
  }

  /**
   * Build week number expression for given mode.
   * Mode 0: Sunday-based, range 0-53, first week starts on first Sunday.
   * Mode 1: Monday-based, range 0-53, first week has 4+ days in year.
   * Mode 2: Sunday-based, range 1-53, first partial week = last week of prev year.
   * Mode 3: Monday-based, range 1-53, first week has 4+ days (ISO week).
   */
  private SqlNode buildWeekExpr(SqlNode d, int mode) {
    SqlNode doy = cast(call("DAYOFYEAR", d), typeSpec(SqlTypeName.INTEGER));
    SqlNode dow = cast(call("DAYOFWEEK", d), typeSpec(SqlTypeName.INTEGER));
    boolean sundayBased = (mode % 2 == 0);
    boolean fourDayRule = (mode == 1 || mode == 3 || mode == 5 || mode == 7);

    if (!fourDayRule) {
      // Modes 0, 2, 4, 6: simple formula FLOOR((doy - adjDow + 7) / 7)
      SqlNode adjDow = sundayBased ? dow
          : plus(mod(plus(dow, intLiteral(5)), intLiteral(7)), intLiteral(1));
      SqlNode weekNum = cast(call("FLOOR", divide(
          cast(plus(minus(doy, adjDow), intLiteral(7)), typeSpec(SqlTypeName.DOUBLE)),
          intLiteral(7))), typeSpec(SqlTypeName.INTEGER));
      if (mode == 2 || mode == 6) {
        // Range 1-53: if week=0, compute last week of previous year
        // Dec 31 of prev year = d - DAYOFYEAR(d) days
        SqlNode dec31 = tsAdd("DAY", new SqlBasicCall(SqlStdOperatorTable.UNARY_MINUS, new SqlNode[]{doy}, POS), d);
        SqlNode dec31Doy = cast(call("DAYOFYEAR", dec31), typeSpec(SqlTypeName.INTEGER));
        SqlNode dec31Dow = cast(call("DAYOFWEEK", dec31), typeSpec(SqlTypeName.INTEGER));
        SqlNode dec31AdjDow = sundayBased ? dec31Dow
            : plus(mod(plus(dec31Dow, intLiteral(5)), intLiteral(7)), intLiteral(1));
        SqlNode prevYearLastWeek = cast(call("FLOOR", divide(
            cast(plus(minus(dec31Doy, dec31AdjDow), intLiteral(7)), typeSpec(SqlTypeName.DOUBLE)),
            intLiteral(7))), typeSpec(SqlTypeName.INTEGER));
        return caseWhen(
            List.of(eq(weekNum, intLiteral(0))),
            List.of(prevYearLastWeek),
            weekNum);
      }
      return weekNum;
    }

    // Modes 1, 3, 5, 7: Monday-based, 4+ days rule
    // Modes 1, 3, 5, 7: Monday-based, 4+ days rule
    // Compute Monday-based mode 0 first: FLOOR((doy - adjDowMon + 7) / 7)
    SqlNode adjDowMon = plus(mod(plus(dow, intLiteral(5)), intLiteral(7)), intLiteral(1));
    SqlNode weekMode0Mon = cast(call("FLOOR", divide(
        cast(plus(minus(doy, adjDowMon), intLiteral(7)), typeSpec(SqlTypeName.DOUBLE)),
        intLiteral(7))), typeSpec(SqlTypeName.INTEGER));
    // jan1_dow_mon (Mon=0..Sun=6) = (adjDowMon - 1 - (doy - 1) % 7 + 7) % 7
    SqlNode jan1DowMon = mod(plus(minus(minus(adjDowMon, intLiteral(1)), mod(minus(doy, intLiteral(1)), intLiteral(7))), intLiteral(7)), intLiteral(7));
    // week_mode1 = weekMode0Mon + (jan1_dow_mon <= 3 ? 1 : 0)
    SqlNode adjustment = caseWhen(
        List.of(lte(jan1DowMon, intLiteral(3))),
        List.of(intLiteral(1)),
        intLiteral(0));
    return cast(plus(weekMode0Mon, adjustment), typeSpec(SqlTypeName.INTEGER));
  }

  private static String getFieldName(UnresolvedExpression expr) {
    if (expr instanceof Field) return ((Field) expr).getField().toString();
    if (expr instanceof QualifiedName) return ((QualifiedName) expr).toString();
    return expr.toString();
  }

  /** Check if a span expression uses a time unit. */
  private static boolean isTimeSpan(UnresolvedExpression expr) {
    if (expr instanceof Alias) return isTimeSpan(((Alias) expr).getDelegated());
    return expr instanceof Span && SpanUnit.isTimeUnit(((Span) expr).getUnit());
  }

  private SqlNode resolveJoinRight(UnresolvedPlan plan, String alias) {
    if (plan instanceof SubqueryAlias) {
      SubqueryAlias sa = (SubqueryAlias) plan;
      // Use the explicit join alias (right=t2) if provided, otherwise use SubqueryAlias name
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
    if (parts.size() > 1 && mapFields.contains(parts.get(0))) {
      String key = String.join(".", parts.subList(1, parts.size()));
      return new SqlBasicCall(SqlStdOperatorTable.ITEM,
          new SqlNode[]{identifier(parts.get(0)), SqlLiteral.createCharString(key, POS)}, POS);
    }
    if (parts.size() == 1 && binReplacements.containsKey(parts.get(0))) {
      return binReplacements.get(parts.get(0));
    }
    // Rewrite SubqueryAlias references to effective join aliases
    if (parts.size() > 1 && aliasMapping.containsKey(parts.get(0))) {
      List<String> rewritten = new ArrayList<>(parts);
      rewritten.set(0, aliasMapping.get(parts.get(0)));
      return identifier(rewritten.toArray(new String[0]));
    }
    // For multi-part names, check if the first part is a known alias (join, subquery, lookup).
    // If not, treat as a single dotted column name (OpenSearch object field).
    if (parts.size() > 1 && !knownAliases.contains(parts.get(0))) {
      return identifier(String.join(".", parts));
    }
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

  private static SqlTypeName extractCastType(SqlNode node) {
    if (!(node instanceof SqlBasicCall)) return null;
    SqlBasicCall call = (SqlBasicCall) node;
    if (call.getOperator() != SqlStdOperatorTable.CAST || call.operandCount() != 2) return null;
    if (!(call.operand(1) instanceof SqlDataTypeSpec)) return null;
    SqlDataTypeSpec spec = call.operand(1);
    if (!(spec.getTypeNameSpec() instanceof SqlBasicTypeNameSpec)) return null;
    String name = spec.getTypeNameSpec().getTypeName().getSimple();
    if (name == null) return null;
    switch (name.toUpperCase()) {
      case "DATE": return SqlTypeName.DATE;
      case "TIME": return SqlTypeName.TIME;
      case "TIMESTAMP": return SqlTypeName.TIMESTAMP;
      default: return null;
    }
  }

  /** Promote TIME to TIMESTAMP using today's date (PPL semantics). */
  private static SqlNode promoteTimeToTimestamp(SqlNode node) {
    if (!(node instanceof SqlBasicCall)) return cast(node, typeSpec(SqlTypeName.TIMESTAMP));
    SqlBasicCall call = (SqlBasicCall) node;
    if (call.getOperator() == SqlStdOperatorTable.CAST && call.operandCount() == 2) {
      SqlNode inner = call.operand(0);
      if (inner instanceof SqlLiteral) {
        String val = ((SqlLiteral) inner).toValue();
        if (val != null) {
          return cast(literal(java.time.LocalDate.now() + " " + val), typeSpec(SqlTypeName.TIMESTAMP));
        }
      }
    }
    return cast(node, typeSpec(SqlTypeName.TIMESTAMP));
  }

  private static SqlNode[] promoteDatetimeOperands(SqlNode left, SqlNode right) {
    SqlTypeName lt = extractCastType(left);
    SqlTypeName rt = extractCastType(right);
    if (lt != null && rt != null && lt != rt) {
      if (lt == SqlTypeName.TIME) left = promoteTimeToTimestamp(left);
      else if (lt != SqlTypeName.TIMESTAMP) left = cast(left, typeSpec(SqlTypeName.TIMESTAMP));
      if (rt == SqlTypeName.TIME) right = promoteTimeToTimestamp(right);
      else if (rt != SqlTypeName.TIMESTAMP) right = cast(right, typeSpec(SqlTypeName.TIMESTAMP));
    }
    return new SqlNode[] {left, right};
  }

  @Override
  public SqlNode visitCompare(org.opensearch.sql.ast.expression.Compare node, Void ctx) {
    SqlNode left = node.getLeft().accept(this, null);
    SqlNode right = node.getRight().accept(this, null);
    SqlNode[] promoted = promoteDatetimeOperands(left, right);
    left = promoted[0];
    right = promoted[1];
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
      case "ilike":
        return like(call("LOWER", left), call("LOWER", right));
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
    if ("+".equals(name) && args.size() == 2) {
      // If either operand is string-producing, use CONCAT instead of +
      if (isStringProducing(args.get(0)) || isStringProducing(args.get(1))) {
        return call("CONCAT", args.get(0), args.get(1));
      }
      return plus(args.get(0), args.get(1));
    }
    if ("-".equals(name) && args.size() == 2) return minus(args.get(0), args.get(1));
    if ("*".equals(name) && args.size() == 2) return times(args.get(0), args.get(1));
    if ("/".equals(name) && args.size() == 2) {
      return caseWhen(
          List.of(eq(args.get(1), literal(0))),
          List.of(literal(null)),
          divide(args.get(0), args.get(1)));
    }
    if ("%".equals(name) && args.size() == 2) {
      return caseWhen(
          List.of(eq(args.get(1), literal(0))),
          List.of(literal(null)),
          mod(args.get(0), args.get(1)));
    }
    // Special-case rewrites
    // earliest/latest as filter functions (2-arg form):
    // earliest(timeStr, field) → field >= resolved_time (field is at or after the earliest boundary)
    // latest(timeStr, field) → field <= resolved_time (field is at or before the latest boundary)
    if ("earliest".equals(name) && args.size() == 2) {
      String timeStr = resolveFilterTimeArg(args.get(0));
      return gte(args.get(1), cast(literal(timeStr), typeSpec(SqlTypeName.TIMESTAMP)));
    }
    if ("latest".equals(name) && args.size() == 2) {
      String timeStr = resolveFilterTimeArg(args.get(0));
      return lte(args.get(1), cast(literal(timeStr), typeSpec(SqlTypeName.TIMESTAMP)));
    }
    if ("if".equals(name))
      return caseWhen(List.of(args.get(0)), List.of(castVarcharIfStringLiteral(args.get(1))), castVarcharIfStringLiteral(args.get(2)));
    if ("ifnull".equals(name)) return call("COALESCE", args.get(0), args.get(1));
    if ("isnull".equals(name) || "is null".equals(name)) return isNull(args.get(0));
    if ("isnotnull".equals(name) || "is not null".equals(name)) return isNotNull(args.get(0));
    if ("ispresent".equals(name)) return isNotNull(args.get(0));
    if ("isempty".equals(name)) return or(isNull(args.get(0)), eq(args.get(0), literal("")));
    if ("isblank".equals(name)) return or(isNull(args.get(0)), eq(call("TRIM", args.get(0)), literal("")));
    if ("e".equals(name) && args.isEmpty()) return call("EXP", literal(1));
    if ("mod".equals(name) && args.size() == 2) {
      return caseWhen(
          List.of(eq(args.get(1), literal(0))),
          List.of(literal(null)),
          mod(args.get(0), args.get(1)));
    }
    if ("log".equals(name)) {
      if (args.size() == 2) return divide(call("LN", args.get(1)), call("LN", args.get(0)));
      return call("LN", args.get(0));
    }
    if ("log2".equals(name)) return divide(call("LN", args.get(0)), call("LN", literal(2)));
    if ("like".equals(name)) return like(args.get(0), args.get(1));
    if ("not like".equals(name)) return notLike(args.get(0), args.get(1));
    // Type constructor functions → CAST
    if ("date".equals(name) && args.size() == 1)
      return cast(args.get(0), typeSpec(SqlTypeName.DATE));
    if ("time".equals(name) && args.size() == 1)
      return cast(args.get(0), typeSpec(SqlTypeName.TIME));
    if ("timestamp".equals(name)) {
      if (args.size() == 1) return cast(args.get(0), typeSpec(SqlTypeName.TIMESTAMP));
      // timestamp(date, time) → CAST(CONCAT(CAST(date AS VARCHAR), ' ', CAST(time AS VARCHAR)) AS TIMESTAMP)
      SqlDataTypeSpec vc = typeSpec(SqlTypeName.VARCHAR);
      return cast(call("CONCAT", call("CONCAT", cast(args.get(0), vc), literal(" ")), cast(args.get(1), vc)),
          typeSpec(SqlTypeName.TIMESTAMP));
    }
    if ("datetime".equals(name)) {
      if (args.size() == 1) return cast(args.get(0), typeSpec(SqlTypeName.TIMESTAMP));
      return cast(SqlLiteral.createNull(POS), typeSpec(SqlTypeName.TIMESTAMP));
    }
    // EXTRACT-based datetime functions → CAST(EXTRACT(unit FROM CAST(arg AS TIMESTAMP)) AS INTEGER)
    if ("year".equals(name) && args.size() == 1)
      return castExtract("YEAR", args.get(0));
    if ("month".equals(name) && args.size() == 1)
      return castExtract("MONTH", args.get(0));
    if (("day".equals(name) || "dayofmonth".equals(name) || "day_of_month".equals(name)) && args.size() == 1)
      return castExtract("DAY", args.get(0));
    if ("hour".equals(name) && args.size() == 1)
      return castExtract("HOUR", args.get(0));
    if ("minute".equals(name) && args.size() == 1)
      return castExtract("MINUTE", args.get(0));
    if ("second".equals(name) && args.size() == 1)
      return castExtract("SECOND", args.get(0));
    if ("quarter".equals(name) && args.size() == 1)
      return castExtract("QUARTER", args.get(0));
    // week/weekofyear/week_of_year: PPL mode 0 (Sunday-based, range 0-53)
    // EXTRACT(WEEK) returns ISO week (Monday-based, range 1-53) — wrong for PPL default
    // Formula: FLOOR((DAYOFYEAR(d) - dow_adjusted + 7) / 7)
    if ("weekofyear".equals(name) || "week_of_year".equals(name) || "week".equals(name)) {
      SqlNode d = ensureTimestamp(args.get(0));
      int mode = 0;
      List<UnresolvedExpression> rawArgs = node.getFuncArgs();
      if (rawArgs.size() >= 2 && rawArgs.get(1) instanceof org.opensearch.sql.ast.expression.Literal) {
        mode = ((Number) ((org.opensearch.sql.ast.expression.Literal) rawArgs.get(1)).getValue()).intValue();
      }
      // For complex modes (1-7), try to compute at Java compile time if input is a literal
      if (mode != 0) {
        String dateStr = extractLiteralDateString(args.get(0));
        if (dateStr != null) {
          try {
            java.time.LocalDate ld = java.time.LocalDate.parse(dateStr.length() > 10 ? dateStr.substring(0, 10) : dateStr);
            int weekNum = computeMySQLWeek(ld, mode);
            return intLiteral(weekNum);
          } catch (Exception e) { /* fall through to SQL expression */ }
        }
      }
      return buildWeekExpr(d, mode);
    }
    // yearweek: YEAR*100 + WEEK
    if ("yearweek".equals(name)) {
      SqlNode d = ensureTimestamp(args.get(0));
      int mode = 0;
      List<UnresolvedExpression> rawArgs = node.getFuncArgs();
      if (rawArgs.size() >= 2 && rawArgs.get(1) instanceof org.opensearch.sql.ast.expression.Literal) {
        mode = ((Number) ((org.opensearch.sql.ast.expression.Literal) rawArgs.get(1)).getValue()).intValue();
      }
      // For complex modes, try to compute at Java compile time if input is a literal
      String dateStr = extractLiteralDateString(args.get(0));
      if (dateStr != null) {
        try {
          java.time.LocalDate ld = java.time.LocalDate.parse(dateStr.length() > 10 ? dateStr.substring(0, 10) : dateStr);
          int weekNum = computeMySQLWeek(ld, mode);
          int yearWeek = ld.getYear() * 100 + weekNum;
          return intLiteral(yearWeek);
        } catch (Exception e) { /* fall through to SQL expression */ }
      }
      SqlNode year = cast(call("YEAR", d), typeSpec(SqlTypeName.INTEGER));
      SqlNode week = buildWeekExpr(d, mode);
      return plus(times(year, intLiteral(100)), week);
    }
    if ("hour_of_day".equals(name) && args.size() == 1)
      return castExtract("HOUR", args.get(0));
    if ("minute_of_hour".equals(name) && args.size() == 1)
      return castExtract("MINUTE", args.get(0));
    if ("second_of_minute".equals(name) && args.size() == 1)
      return castExtract("SECOND", args.get(0));
    if ("month_of_year".equals(name) && args.size() == 1)
      return castExtract("MONTH", args.get(0));
    if (("dayofweek".equals(name) || "day_of_week".equals(name)) && args.size() == 1)
      return cast(call("DAYOFWEEK", cast(args.get(0), typeSpec(SqlTypeName.TIMESTAMP))), typeSpec(SqlTypeName.INTEGER));
    if (("dayofyear".equals(name) || "day_of_year".equals(name)) && args.size() == 1)
      return cast(call("DAYOFYEAR", cast(args.get(0), typeSpec(SqlTypeName.TIMESTAMP))), typeSpec(SqlTypeName.INTEGER));
    if ("minute_of_day".equals(name) && args.size() == 1) {
      SqlNode ts = cast(args.get(0), typeSpec(SqlTypeName.TIMESTAMP));
      SqlNode hours = new SqlBasicCall(SqlStdOperatorTable.EXTRACT,
          new SqlNode[]{new SqlIntervalQualifier(TimeUnit.HOUR, null, POS), ts}, POS);
      SqlNode minutes = new SqlBasicCall(SqlStdOperatorTable.EXTRACT,
          new SqlNode[]{new SqlIntervalQualifier(TimeUnit.MINUTE, null, POS), ts}, POS);
      return cast(plus(times(hours, intLiteral(60)), minutes), typeSpec(SqlTypeName.INTEGER));
    }
    if ("microsecond".equals(name) && args.size() == 1)
      return castExtract("MICROSECOND", args.get(0));
    // date_add / adddate
    if ("date_add".equals(name) || "adddate".equals(name)) {
      SqlNode intervalArg = args.get(1);
      if (intervalArg instanceof SqlBasicCall && ((SqlBasicCall) intervalArg).getOperator().getName().startsWith("INTERVAL_")) {
        String unit = ((SqlBasicCall) intervalArg).getOperator().getName().substring(9);
        SqlNode value = ((SqlBasicCall) intervalArg).operand(0);
        return cast(tsAdd(unit, value, ensureTimestampForDateArith(args.get(0))), typeSpec(SqlTypeName.TIMESTAMP));
      }
      // Plain days → preserve DATE type, cast TIME to TIMESTAMP
      SqlNode dateArg = isAlreadyCastToTime(args.get(0)) ? ensureTimestampForDateArith(args.get(0)) : args.get(0);
      return tsAdd("DAY", intervalArg, dateArg);
    }
    // date_sub / subdate
    if ("date_sub".equals(name) || "subdate".equals(name)) {
      SqlNode intervalArg = args.get(1);
      if (intervalArg instanceof SqlBasicCall && ((SqlBasicCall) intervalArg).getOperator().getName().startsWith("INTERVAL_")) {
        String unit = ((SqlBasicCall) intervalArg).getOperator().getName().substring(9);
        SqlNode value = ((SqlBasicCall) intervalArg).operand(0);
        return cast(tsAdd(unit, new SqlBasicCall(SqlStdOperatorTable.UNARY_MINUS, new SqlNode[]{value}, POS), ensureTimestampForDateArith(args.get(0))), typeSpec(SqlTypeName.TIMESTAMP));
      }
      // Plain days → preserve DATE type, cast TIME to TIMESTAMP
      SqlNode dateArg = isAlreadyCastToTime(args.get(0)) ? ensureTimestampForDateArith(args.get(0)) : args.get(0);
      return tsAdd("DAY", new SqlBasicCall(SqlStdOperatorTable.UNARY_MINUS, new SqlNode[]{intervalArg}, POS), dateArg);
    }
    // datediff
    if ("datediff".equals(name))
      return cast(call("TIMESTAMPDIFF", identifier("DAY"), cast(args.get(1), typeSpec(SqlTypeName.DATE)), cast(args.get(0), typeSpec(SqlTypeName.DATE))), typeSpec(SqlTypeName.BIGINT));
    // timestampdiff
    if ("timestampdiff".equals(name)) {
      SqlNode unit = args.get(0);
      String unitStr = (unit instanceof SqlLiteral) ? ((SqlLiteral) unit).toValue().replace("'", "") : unit.toString().replace("'", "").replace("`", "");
      // For MILLISECOND, use SECOND * 1000 to avoid int32 overflow on intervals > ~24 days
      if ("MILLISECOND".equalsIgnoreCase(unitStr)) {
        return times(cast(call("TIMESTAMPDIFF", identifier("SECOND"),
            ensureTimestamp(args.get(1)), ensureTimestamp(args.get(2))), typeSpec(SqlTypeName.BIGINT)),
            intLiteral(1000));
      }
      return cast(call("TIMESTAMPDIFF", identifier(unitStr), ensureTimestamp(args.get(1)), ensureTimestamp(args.get(2))), typeSpec(SqlTypeName.BIGINT));
    }
    // timestampadd
    if ("timestampadd".equals(name)) {
      SqlNode unit = args.get(0);
      String unitStr = (unit instanceof SqlLiteral) ? ((SqlLiteral) unit).toValue().replace("'", "") : unit.toString().replace("'", "").replace("`", "");
      return cast(call("TIMESTAMPADD", identifier(unitStr), args.get(1), ensureTimestamp(args.get(2))), typeSpec(SqlTypeName.TIMESTAMP));
    }
    // dayname / monthname
    if ("dayname".equals(name)) {
      SqlNode ts = ensureTimestamp(args.get(0));
      SqlNode dow = call("DAYOFWEEK", ts);
      return caseWhen(
          List.of(eq(dow, intLiteral(1)), eq(dow, intLiteral(2)), eq(dow, intLiteral(3)),
                  eq(dow, intLiteral(4)), eq(dow, intLiteral(5)), eq(dow, intLiteral(6)),
                  eq(dow, intLiteral(7))),
          List.of(cast(literal("Sunday"), typeSpec(SqlTypeName.VARCHAR)), cast(literal("Monday"), typeSpec(SqlTypeName.VARCHAR)), cast(literal("Tuesday"), typeSpec(SqlTypeName.VARCHAR)),
                  cast(literal("Wednesday"), typeSpec(SqlTypeName.VARCHAR)), cast(literal("Thursday"), typeSpec(SqlTypeName.VARCHAR)), cast(literal("Friday"), typeSpec(SqlTypeName.VARCHAR)),
                  cast(literal("Saturday"), typeSpec(SqlTypeName.VARCHAR))),
          cast(SqlLiteral.createNull(POS), typeSpec(SqlTypeName.VARCHAR)));
    }
    if ("monthname".equals(name)) {
      SqlNode ts = ensureTimestamp(args.get(0));
      SqlNode mo = new SqlBasicCall(SqlStdOperatorTable.EXTRACT, new SqlNode[]{new SqlIntervalQualifier(TimeUnit.MONTH, null, POS), ts}, POS);
      return caseWhen(
          List.of(eq(mo, intLiteral(1)), eq(mo, intLiteral(2)), eq(mo, intLiteral(3)),
                  eq(mo, intLiteral(4)), eq(mo, intLiteral(5)), eq(mo, intLiteral(6)),
                  eq(mo, intLiteral(7)), eq(mo, intLiteral(8)), eq(mo, intLiteral(9)),
                  eq(mo, intLiteral(10)), eq(mo, intLiteral(11)), eq(mo, intLiteral(12))),
          List.of(cast(literal("January"), typeSpec(SqlTypeName.VARCHAR)), cast(literal("February"), typeSpec(SqlTypeName.VARCHAR)), cast(literal("March"), typeSpec(SqlTypeName.VARCHAR)),
                  cast(literal("April"), typeSpec(SqlTypeName.VARCHAR)), cast(literal("May"), typeSpec(SqlTypeName.VARCHAR)), cast(literal("June"), typeSpec(SqlTypeName.VARCHAR)),
                  cast(literal("July"), typeSpec(SqlTypeName.VARCHAR)), cast(literal("August"), typeSpec(SqlTypeName.VARCHAR)), cast(literal("September"), typeSpec(SqlTypeName.VARCHAR)),
                  cast(literal("October"), typeSpec(SqlTypeName.VARCHAR)), cast(literal("November"), typeSpec(SqlTypeName.VARCHAR)), cast(literal("December"), typeSpec(SqlTypeName.VARCHAR))),
          cast(SqlLiteral.createNull(POS), typeSpec(SqlTypeName.VARCHAR)));
    }
    // weekday: 0=Monday...6=Sunday from DAYOFWEEK (1=Sunday...7=Saturday)
    if ("weekday".equals(name)) {
      SqlNode ts = ensureTimestamp(args.get(0));
      return caseWhen(
          List.of(isNull(args.get(0))),
          List.of(cast(SqlLiteral.createNull(POS), typeSpec(SqlTypeName.INTEGER))),
          cast(call("MOD", plus(call("DAYOFWEEK", ts), intLiteral(5)), intLiteral(7)), typeSpec(SqlTypeName.INTEGER)));
    }
    // unix_timestamp
    if ("unix_timestamp".equals(name)) {
      SqlNode epoch = cast(literal("1970-01-01 00:00:00"), typeSpec(SqlTypeName.TIMESTAMP));
      if (args.isEmpty())
        return cast(call("TIMESTAMPDIFF", identifier("SECOND"), epoch, identifier("CURRENT_TIMESTAMP")), typeSpec(SqlTypeName.DOUBLE));
      return cast(call("TIMESTAMPDIFF", identifier("SECOND"), epoch, cast(args.get(0), typeSpec(SqlTypeName.TIMESTAMP))), typeSpec(SqlTypeName.DOUBLE));
    }
    // from_unixtime
    if ("from_unixtime".equals(name)) {
      SqlNode epoch = cast(literal("1970-01-01 00:00:00"), typeSpec(SqlTypeName.TIMESTAMP));
      SqlNode result = call("TIMESTAMPADD", identifier("SECOND"), cast(args.get(0), typeSpec(SqlTypeName.INTEGER)), epoch);
      if (args.size() >= 2) {
        return cast(result, typeSpec(SqlTypeName.VARCHAR));
      }
      return cast(result, typeSpec(SqlTypeName.TIMESTAMP));
    }
    // to_days
    if ("to_days".equals(name)) {
      SqlNode origin = cast(literal("0001-01-01"), typeSpec(SqlTypeName.DATE));
      return caseWhen(
          List.of(isNull(args.get(0))),
          List.of(cast(SqlLiteral.createNull(POS), typeSpec(SqlTypeName.BIGINT))),
          cast(plus(call("TIMESTAMPDIFF", identifier("DAY"), origin, cast(args.get(0), typeSpec(SqlTypeName.DATE))), intLiteral(366)), typeSpec(SqlTypeName.BIGINT)));
    }
    // from_days
    if ("from_days".equals(name)) {
      SqlNode origin = cast(literal("0001-01-01"), typeSpec(SqlTypeName.DATE));
      return caseWhen(
          List.of(isNull(args.get(0))),
          List.of(cast(SqlLiteral.createNull(POS), typeSpec(SqlTypeName.DATE))),
          cast(call("TIMESTAMPADD", identifier("DAY"), cast(minus(args.get(0), intLiteral(366)), typeSpec(SqlTypeName.INTEGER)), origin), typeSpec(SqlTypeName.DATE)));
    }
    // to_seconds
    if ("to_seconds".equals(name)) {
      SqlNode origin = cast(literal("0001-01-01"), typeSpec(SqlTypeName.DATE));
      return caseWhen(
          List.of(isNull(args.get(0))),
          List.of(cast(SqlLiteral.createNull(POS), typeSpec(SqlTypeName.BIGINT))),
          cast(times(plus(call("TIMESTAMPDIFF", identifier("DAY"), origin, cast(args.get(0), typeSpec(SqlTypeName.DATE))), intLiteral(366)), intLiteral(86400)), typeSpec(SqlTypeName.BIGINT)));
    }
    // last_day
    if ("last_day".equals(name))
      return cast(call("LAST_DAY", ensureTimestamp(args.get(0))), typeSpec(SqlTypeName.DATE));
    // time_to_sec
    if ("time_to_sec".equals(name)) {
      SqlNode ts = ensureTimestamp(args.get(0));
      SqlNode h = new SqlBasicCall(SqlStdOperatorTable.EXTRACT, new SqlNode[]{new SqlIntervalQualifier(TimeUnit.HOUR, null, POS), ts}, POS);
      SqlNode m = new SqlBasicCall(SqlStdOperatorTable.EXTRACT, new SqlNode[]{new SqlIntervalQualifier(TimeUnit.MINUTE, null, POS), ts}, POS);
      SqlNode s = new SqlBasicCall(SqlStdOperatorTable.EXTRACT, new SqlNode[]{new SqlIntervalQualifier(TimeUnit.SECOND, null, POS), ts}, POS);
      return cast(plus(plus(times(h, intLiteral(3600)), times(m, intLiteral(60))), s), typeSpec(SqlTypeName.BIGINT));
    }
    // sec_to_time
    if ("sec_to_time".equals(name)) {
      SqlNode epoch = cast(literal("00:00:00"), typeSpec(SqlTypeName.TIME));
      return cast(call("TIMESTAMPADD", identifier("SECOND"), cast(args.get(0), typeSpec(SqlTypeName.INTEGER)), epoch), typeSpec(SqlTypeName.TIME));
    }
    // timediff
    if ("timediff".equals(name)) {
      SqlNode epoch = cast(literal("00:00:00"), typeSpec(SqlTypeName.TIME));
      SqlNode diff = call("TIMESTAMPDIFF", identifier("SECOND"), cast(args.get(1), typeSpec(SqlTypeName.TIMESTAMP)), cast(args.get(0), typeSpec(SqlTypeName.TIMESTAMP)));
      return caseWhen(
          List.of(or(isNull(args.get(0)), isNull(args.get(1)))),
          List.of(cast(SqlLiteral.createNull(POS), typeSpec(SqlTypeName.TIME))),
          cast(call("TIMESTAMPADD", identifier("SECOND"), diff, epoch), typeSpec(SqlTypeName.TIME)));
    }
    // makedate
    if ("makedate".equals(name)) {
      SqlNode yearStart = cast(call("CONCAT", cast(args.get(0), typeSpec(SqlTypeName.VARCHAR)), literal("-01-01")), typeSpec(SqlTypeName.DATE));
      SqlNode dayNum = cast(call("ROUND", args.get(1)), typeSpec(SqlTypeName.INTEGER));
      return cast(call("TIMESTAMPADD", identifier("DAY"), cast(minus(dayNum, intLiteral(1)), typeSpec(SqlTypeName.INTEGER)), yearStart), typeSpec(SqlTypeName.DATE));
    }
    // maketime
    if ("maketime".equals(name)) {
      SqlNode epoch = cast(literal("00:00:00"), typeSpec(SqlTypeName.TIME));
      SqlNode t1 = call("TIMESTAMPADD", identifier("HOUR"), cast(args.get(0), typeSpec(SqlTypeName.INTEGER)), epoch);
      SqlNode t2 = call("TIMESTAMPADD", identifier("MINUTE"), cast(args.get(1), typeSpec(SqlTypeName.INTEGER)), t1);
      return cast(call("TIMESTAMPADD", identifier("SECOND"), cast(args.get(2), typeSpec(SqlTypeName.INTEGER)), t2), typeSpec(SqlTypeName.TIME));
    }
    // addtime
    if ("addtime".equals(name)) {
      SqlNode ts = args.get(0);
      SqlNode h = new SqlBasicCall(SqlStdOperatorTable.EXTRACT, new SqlNode[]{new SqlIntervalQualifier(TimeUnit.HOUR, null, POS), args.get(1)}, POS);
      SqlNode m = new SqlBasicCall(SqlStdOperatorTable.EXTRACT, new SqlNode[]{new SqlIntervalQualifier(TimeUnit.MINUTE, null, POS), args.get(1)}, POS);
      SqlNode s = new SqlBasicCall(SqlStdOperatorTable.EXTRACT, new SqlNode[]{new SqlIntervalQualifier(TimeUnit.SECOND, null, POS), args.get(1)}, POS);
      SqlNode totalSec = cast(plus(plus(times(h, intLiteral(3600)), times(m, intLiteral(60))), s), typeSpec(SqlTypeName.INTEGER));
      return call("TIMESTAMPADD", identifier("SECOND"), totalSec, ts);
    }
    // subtime
    if ("subtime".equals(name)) {
      SqlNode ts = args.get(0);
      SqlNode h = new SqlBasicCall(SqlStdOperatorTable.EXTRACT, new SqlNode[]{new SqlIntervalQualifier(TimeUnit.HOUR, null, POS), args.get(1)}, POS);
      SqlNode m = new SqlBasicCall(SqlStdOperatorTable.EXTRACT, new SqlNode[]{new SqlIntervalQualifier(TimeUnit.MINUTE, null, POS), args.get(1)}, POS);
      SqlNode s = new SqlBasicCall(SqlStdOperatorTable.EXTRACT, new SqlNode[]{new SqlIntervalQualifier(TimeUnit.SECOND, null, POS), args.get(1)}, POS);
      SqlNode totalSec = cast(new SqlBasicCall(SqlStdOperatorTable.UNARY_MINUS, new SqlNode[]{plus(plus(times(h, intLiteral(3600)), times(m, intLiteral(60))), s)}, POS), typeSpec(SqlTypeName.INTEGER));
      return call("TIMESTAMPADD", identifier("SECOND"), totalSec, ts);
    }
    // convert_tz: convert timestamp from one timezone offset to another
    if ("convert_tz".equals(name)) {
      SqlNode nullTs = cast(SqlLiteral.createNull(POS), typeSpec(SqlTypeName.TIMESTAMP));
      // Both timezone args must be string literals for compile-time evaluation
      if (args.size() < 3
          || !(args.get(1) instanceof SqlCharStringLiteral)
          || !(args.get(2) instanceof SqlCharStringLiteral)) {
        return nullTs;
      }
      // Validate timestamp string literal at compile time (invalid dates like Feb 30 → NULL)
      if (args.get(0) instanceof SqlCharStringLiteral) {
        String tsStr = ((SqlCharStringLiteral) args.get(0)).getNlsString().getValue();
        try {
          java.time.LocalDateTime.parse(tsStr, DateTimeFormatter.ofPattern("uuuu-MM-dd HH:mm:ss")
              .withResolverStyle(java.time.format.ResolverStyle.STRICT));
        } catch (Exception e) {
          return nullTs;
        }
      }
      String fromTz = ((SqlCharStringLiteral) args.get(1)).getNlsString().getValue();
      String toTz = ((SqlCharStringLiteral) args.get(2)).getNlsString().getValue();
      java.util.regex.Pattern tzPat = java.util.regex.Pattern.compile("^([+-])(\\d{2}):(\\d{2})$");
      java.util.regex.Matcher mFrom = tzPat.matcher(fromTz);
      java.util.regex.Matcher mTo = tzPat.matcher(toTz);
      if (!mFrom.matches() || !mTo.matches()) return nullTs;
      int fromMin = (Integer.parseInt(mFrom.group(2)) * 60 + Integer.parseInt(mFrom.group(3)))
          * (mFrom.group(1).equals("-") ? -1 : 1);
      int toMin = (Integer.parseInt(mTo.group(2)) * 60 + Integer.parseInt(mTo.group(3)))
          * (mTo.group(1).equals("-") ? -1 : 1);
      // MySQL valid range is ±13:00 (±780 minutes)
      if (Math.abs(fromMin) > 780 || Math.abs(toMin) > 780) return nullTs;
      int delta = toMin - fromMin;
      SqlNode ts = cast(args.get(0), typeSpec(SqlTypeName.TIMESTAMP));
      SqlNode result = delta == 0 ? ts
          : cast(call("TIMESTAMPADD", identifier("MINUTE"), intLiteral(delta), ts), typeSpec(SqlTypeName.TIMESTAMP));
      return caseWhen(List.of(isNull(args.get(0))),
          List.of(nullTs), result);
    }
    // date_format / time_format
    if ("date_format".equals(name) || "time_format".equals(name)) {
      SqlDataTypeSpec ts6 = new SqlDataTypeSpec(new SqlBasicTypeNameSpec(SqlTypeName.TIMESTAMP, 6, POS), POS);
      // Upgrade any CAST(x AS TIMESTAMP) to CAST(x AS TIMESTAMP(6)) to preserve microseconds
      SqlNode arg0 = args.get(0);
      if (arg0 instanceof SqlBasicCall && ((SqlBasicCall) arg0).getOperator() == SqlStdOperatorTable.CAST) {
        SqlNode inner = ((SqlBasicCall) arg0).operand(0);
        arg0 = cast(inner, ts6);
      }
      SqlNode ts = cast(arg0, ts6);
      if (args.size() >= 2 && args.get(1) instanceof SqlCharStringLiteral) {
        String fmt = ((SqlCharStringLiteral) args.get(1)).getNlsString().getValue();
        SqlNode formatted = buildDateFormatExpr(ts, fmt, false, null);
        return caseWhen(List.of(isNull(args.get(0))),
            List.of(cast(SqlLiteral.createNull(POS), typeSpec(SqlTypeName.VARCHAR))), formatted);
      }
      return caseWhen(List.of(isNull(args.get(0))),
          List.of(cast(SqlLiteral.createNull(POS), typeSpec(SqlTypeName.VARCHAR))),
          cast(args.get(0), typeSpec(SqlTypeName.VARCHAR)));
    }
    // str_to_date
    if ("str_to_date".equals(name))
      return caseWhen(
          List.of(isNull(args.get(0))),
          List.of(cast(SqlLiteral.createNull(POS), typeSpec(SqlTypeName.TIMESTAMP))),
          cast(args.get(0), typeSpec(SqlTypeName.TIMESTAMP)));
    // period_add / period_diff
    if ("period_add".equals(name)) {
      SqlNode p = args.get(0);
      SqlNode n = args.get(1);
      // totalMonths = (p/100)*12 + (p%100) - 1 + n
      SqlNode yr = divide(p, intLiteral(100));
      SqlNode mo = call("MOD", p, intLiteral(100));
      SqlNode totalMonths = plus(plus(times(yr, intLiteral(12)), mo), minus(n, intLiteral(1)));
      // result = (totalMonths/12)*100 + (totalMonths%12) + 1
      SqlNode resYr = divide(totalMonths, intLiteral(12));
      SqlNode resMo = plus(call("MOD", totalMonths, intLiteral(12)), intLiteral(1));
      return caseWhen(
          List.of(or(isNull(p), isNull(n))),
          List.of(cast(SqlLiteral.createNull(POS), typeSpec(SqlTypeName.INTEGER))),
          cast(plus(times(resYr, intLiteral(100)), resMo), typeSpec(SqlTypeName.INTEGER)));
    }
    if ("period_diff".equals(name)) {
      SqlNode p1 = args.get(0);
      SqlNode p2 = args.get(1);
      // (p1/100*12 + p1%100) - (p2/100*12 + p2%100)
      SqlNode m1 = plus(times(divide(p1, intLiteral(100)), intLiteral(12)), call("MOD", p1, intLiteral(100)));
      SqlNode m2 = plus(times(divide(p2, intLiteral(100)), intLiteral(12)), call("MOD", p2, intLiteral(100)));
      return caseWhen(
          List.of(or(isNull(p1), isNull(p2))),
          List.of(cast(SqlLiteral.createNull(POS), typeSpec(SqlTypeName.INTEGER))),
          cast(minus(m1, m2), typeSpec(SqlTypeName.INTEGER)));
    }
    // extract (explicit function form)
    if ("extract".equals(name) && args.size() == 2) {
      String part;
      if (args.get(0) instanceof SqlLiteral) {
        part = ((SqlLiteral) args.get(0)).toValue().replace("'", "").toUpperCase();
      } else {
        part = args.get(0).toString().replace("'", "").replace("`", "").toUpperCase();
      }
      SqlNode ts = args.get(1);
      TimeUnit tu;
      switch (part) {
        case "YEAR": tu = TimeUnit.YEAR; break;
        case "MONTH": tu = TimeUnit.MONTH; break;
        case "DAY": tu = TimeUnit.DAY; break;
        case "HOUR": tu = TimeUnit.HOUR; break;
        case "MINUTE": tu = TimeUnit.MINUTE; break;
        case "SECOND": tu = TimeUnit.SECOND; break;
        case "QUARTER": tu = TimeUnit.QUARTER; break;
        case "WEEK": tu = TimeUnit.WEEK; break;
        case "MICROSECOND": tu = TimeUnit.MICROSECOND; break;
        default: tu = TimeUnit.DAY; break;
      }
      SqlIntervalQualifier qualifier = new SqlIntervalQualifier(tu, null, POS);
      SqlNode extractNode = new SqlBasicCall(SqlStdOperatorTable.EXTRACT, new SqlNode[]{qualifier, ts}, POS);
      return cast(extractNode, typeSpec(SqlTypeName.BIGINT));
    }
    // get_format: returns MySQL format string based on type and locale
    if ("get_format".equals(name)) {
      String type = args.get(0).toString().replace("'", "").replace("`", "").toUpperCase();
      String locale = args.size() > 1
          ? args.get(1).toString().replace("'", "").replace("`", "").toUpperCase() : "USA";
      return literal(getFormatString(type, locale));
    }
    // strftime
    if ("strftime".equals(name)) {
      SqlNode tsArg = args.get(0);
      String fmt = (args.size() > 1 && args.get(1) instanceof SqlCharStringLiteral)
          ? ((SqlCharStringLiteral) args.get(1)).getNlsString().getValue() : null;
      SqlNode origNumeric = isNumericExpression(tsArg) ? tsArg : null;
      SqlNode ts = convertStrftimeArg(tsArg);
      if (fmt != null) {
        SqlNode formatted = buildDateFormatExpr(ts, fmt, true, origNumeric);
        return caseWhen(List.of(isNull(tsArg)),
            List.of(cast(SqlLiteral.createNull(POS), typeSpec(SqlTypeName.VARCHAR))), formatted);
      }
      return caseWhen(List.of(isNull(tsArg)),
          List.of(cast(SqlLiteral.createNull(POS), typeSpec(SqlTypeName.VARCHAR))),
          cast(ts, typeSpec(SqlTypeName.VARCHAR)));
    }
    // Now-like datetime functions: generate UTC literals at transpile time
    LocalDateTime utcNow = LocalDateTime.now(ZoneOffset.UTC);
    if (Set.of("now", "current_timestamp", "localtimestamp", "localtime", "utc_timestamp", "sysdate").contains(name))
      return literal(utcNow.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss")));
    if (Set.of("curdate", "current_date", "utc_date").contains(name))
      return literal(utcNow.toLocalDate().toString());
    if (Set.of("curtime", "current_time", "utc_time").contains(name))
      return literal(utcNow.toLocalTime().format(DateTimeFormatter.ofPattern("HH:mm:ss")));
    // mvindex: 0-based PPL indexing → 1-based Calcite ITEM / ARRAY_SLICE
    if ("mvindex".equals(name)) {
      SqlNode arr = args.get(0);
      SqlNode idx = args.get(1);
      if (args.size() == 2) {
        // Single element: arr[idx+1] for positive, arr[ARRAY_LENGTH(arr)+idx+1] for negative
        SqlNode posIdx = plus(idx, intLiteral(1));
        SqlNode negIdx = plus(new SqlBasicCall(SqlLibraryOperators.ARRAY_LENGTH, new SqlNode[]{arr}, POS), plus(idx, intLiteral(1)));
        SqlNode adjustedIdx = caseWhen(
            List.of(lt(idx, intLiteral(0))),
            List.of(negIdx),
            posIdx);
        return new SqlBasicCall(SqlStdOperatorTable.ITEM, new SqlNode[]{arr, adjustedIdx}, POS);
      }
      // Range: mvindex(arr, start, end) → ARRAY_SLICE(arr, start+1, end-start+1)
      // Note: ARRAY_SLICE requires INTEGER args but arithmetic produces NUMERIC — engine-level type checker limitation
      SqlNode end = args.get(2);
      return new SqlBasicCall(SqlLibraryOperators.ARRAY_SLICE, new SqlNode[]{arr, cast(plus(idx, intLiteral(1)), typeSpec(SqlTypeName.INTEGER)), cast(plus(minus(end, idx), intLiteral(1)), typeSpec(SqlTypeName.INTEGER))}, POS);
    }
    // mvappend: single arg → ARRAY(arg), multiple → ARRAY_CONCAT(ensureArray(args)...) or ARRAY(args...)
    if ("mvappend".equals(name)) {
      if (args.size() == 1) return new SqlBasicCall(SqlLibraryOperators.ARRAY, new SqlNode[]{args.get(0)}, POS);
      // If any arg is already an array-producing expression, use ARRAY_CONCAT with scalar args wrapped
      boolean hasArrayExpr = args.stream().anyMatch(PPLToSqlNodeConverter::isArrayExpression);
      if (hasArrayExpr) {
        SqlNode[] wrapped = args.stream().map(PPLToSqlNodeConverter::ensureArrayArg).toArray(SqlNode[]::new);
        return new SqlBasicCall(SqlLibraryOperators.ARRAY_CONCAT, wrapped, POS);
      }
      // All args are scalars/identifiers — use ARRAY() to create array from all values
      return new SqlBasicCall(SqlLibraryOperators.ARRAY, args.toArray(new SqlNode[0]), POS);
    }
    // mvzip: map to ARRAYS_ZIP
    if ("mvzip".equals(name)) {
      if (args.size() >= 3) {
        return new SqlBasicCall(SqlLibraryOperators.ARRAYS_ZIP, new SqlNode[]{args.get(0), args.get(1), args.get(2)}, POS);
      }
      return new SqlBasicCall(SqlLibraryOperators.ARRAYS_ZIP, new SqlNode[]{args.get(0), args.get(1)}, POS);
    }
    // mvfind: use registered UDF operator
    if ("mvfind".equals(name)) {
      return new SqlBasicCall(PPLBuiltinOperators.MVFIND, args.toArray(new SqlNode[0]), POS);
    }
    // Array functions using SqlLibraryOperators for correct return types
    if ("mvdedup".equals(name)) {
      return new SqlBasicCall(SqlLibraryOperators.ARRAY_DISTINCT, new SqlNode[]{args.get(0)}, POS);
    }
    if ("array_compact".equals(name)) {
      return new SqlBasicCall(SqlLibraryOperators.ARRAY_COMPACT, new SqlNode[]{args.get(0)}, POS);
    }
    if ("split".equals(name)) {
      return new SqlBasicCall(SqlLibraryOperators.SPLIT, args.toArray(new SqlNode[0]), POS);
    }
    if ("mvsort".equals(name)) {
      return new SqlBasicCall(SqlLibraryOperators.SORT_ARRAY, new SqlNode[]{args.get(0)}, POS);
    }
    if ("array".equals(name)) {
      // Cast string literal args to VARCHAR to avoid CHAR padding
      SqlNode[] castArgs = args.stream()
          .map(a -> isStringLiteral(a) ? cast(a, typeSpec(SqlTypeName.VARCHAR)) : a)
          .toArray(SqlNode[]::new);
      return new SqlBasicCall(SqlLibraryOperators.ARRAY, castArgs, POS);
    }
    // Arithmetic function aliases
    if ("add".equals(name) && args.size() == 2) return plus(args.get(0), args.get(1));
    if ("subtract".equals(name) && args.size() == 2) return minus(args.get(0), args.get(1));
    if ("multiply".equals(name) && args.size() == 2) return times(args.get(0), args.get(1));
    if ("divide".equals(name) && args.size() == 2) {
      return caseWhen(
          List.of(eq(args.get(1), literal(0))),
          List.of(literal(null)),
          divide(args.get(0), args.get(1)));
    }
    if ("modulus".equals(name) && args.size() == 2) {
      return caseWhen(
          List.of(eq(args.get(1), literal(0))),
          List.of(literal(null)),
          mod(args.get(0), args.get(1)));
    }
    // Math: expm1(x) → EXP(x) - 1
    if ("expm1".equals(name) && args.size() == 1)
      return minus(call("EXP", args.get(0)), SqlLiteral.createExactNumeric("1", POS));
    // Math: rint(x) → ROUND(x)
    if ("rint".equals(name) && args.size() == 1) return call("ROUND", args.get(0));
    // Math: atan(y,x) 2-arg → ATAN2(y,x)
    if ("atan".equals(name) && args.size() == 2) return call("ATAN2", args.get(0), args.get(1));
    // String: position(substr, str) → POSITION(substr IN str)
    if ("position".equals(name) && args.size() == 2)
      return new SqlBasicCall(SqlStdOperatorTable.POSITION, new SqlNode[]{args.get(0), args.get(1)}, POS);
    // String: locate(substr, str[, pos])
    if ("locate".equals(name)) {
      if (args.size() == 3)
        return new SqlBasicCall(SqlStdOperatorTable.POSITION, new SqlNode[]{args.get(0), args.get(1), args.get(2)}, POS);
      if (args.size() == 2)
        return new SqlBasicCall(SqlStdOperatorTable.POSITION, new SqlNode[]{args.get(0), args.get(1)}, POS);
    }
    // String: strcmp(a,b) → CASE WHEN a < b THEN -1 WHEN a > b THEN 1 ELSE 0 END
    if ("strcmp".equals(name) && args.size() == 2)
      return caseWhen(
          List.of(lt(args.get(0), args.get(1)), gt(args.get(0), args.get(1))),
          List.of(intLiteral(-1), intLiteral(1)),
          intLiteral(0));
    // Eval: scalar_max / scalar_min with null-ignoring semantics
    if ("scalar_max".equals(name)) return buildGreatest(args);
    if ("scalar_min".equals(name)) return buildLeast(args);
    // SHA2: use registered UDF operator directly (generic call() won't resolve the UDF)
    if ("sha2".equals(name) && args.size() == 2)
      return new SqlBasicCall(PPLBuiltinOperators.SHA2,
          new SqlNode[]{args.get(0), cast(args.get(1), typeSpec(SqlTypeName.INTEGER))}, POS);
    // json_valid → expr IS JSON VALUE (postfix operator)
    if ("json_valid".equals(name))
      return new SqlBasicCall(SqlStdOperatorTable.IS_JSON_VALUE, new SqlNode[]{args.get(0)}, POS);
    // replace(field, pattern, replacement): convert \N back-references to $N for REGEXP_REPLACE
    if ("replace".equals(name) && args.size() == 3) {
      SqlNode replArg = args.get(2);
      if (replArg instanceof SqlCharStringLiteral) {
        String repl = ((SqlCharStringLiteral) replArg).getNlsString().getValue();
        String converted = repl.replaceAll("\\\\(\\d+)", "\\$$1");
        args.set(2, literal(converted));
      }
      return call("REGEXP_REPLACE", args.toArray(new SqlNode[0]));
    }
    // Default: FUNC_MAP lookup
    String sqlName = FUNC_MAP.getOrDefault(name, name.toUpperCase());
    return call(sqlName, args.toArray(new SqlNode[0]));
  }

  @Override
  public SqlNode visitAggregateFunction(AggregateFunction node, Void ctx) {
    String name = node.getFuncName().toLowerCase();
    if ("distinct_count".equals(name) || "dc".equals(name)) {
      return new SqlBasicCall(
          SqlStdOperatorTable.COUNT,
          new SqlNode[] {node.getField().accept(this, null)},
          POS,
          SqlLiteral.createSymbol(SqlSelectKeyword.DISTINCT, POS));
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
            new SqlNode[] {field.accept(this, null)},
            POS,
            SqlLiteral.createSymbol(SqlSelectKeyword.DISTINCT, POS));
      }
      return count(field.accept(this, null));
    }
    // PPL-specific aggregates
    if ("first".equals(name)) return call("PPL_FIRST", node.getField().accept(this, null));
    if ("last".equals(name)) return call("PPL_LAST", node.getField().accept(this, null));
    if ("earliest".equals(name))
      return call("ARG_MIN", node.getField().accept(this, null), identifier("@timestamp"));
    if ("latest".equals(name))
      return call("ARG_MAX", node.getField().accept(this, null), identifier("@timestamp"));
    if ("take".equals(name)) {
      SqlNode field = node.getField().accept(this, null);
      List<UnresolvedExpression> args = node.getArgList();
      SqlNode size = (args != null && !args.isEmpty()) ? args.get(0).accept(this, null) : intLiteral(10);
      return call("TAKE", field, size);
    }
    if ("list".equals(name)) {
      SqlNode field = node.getField().accept(this, null);
      return call("LIST", field);
    }
    if ("values".equals(name)) {
      SqlNode field = node.getField().accept(this, null);
      List<UnresolvedExpression> args = node.getArgList();
      if (args != null && !args.isEmpty()) {
        SqlNode limit = args.get(0).accept(this, null);
        return call("PPL_VALUES", field, limit);
      }
      return call("PPL_VALUES", field);
    }
    if ("median".equals(name))
      return call("percentile_approx", node.getField().accept(this, null), intLiteral(50));
    if ("count_distinct_approx".equals(name) || "approx_count_distinct".equals(name)
        || "distinct_count_approx".equals(name)) {
      return new SqlBasicCall(
          SqlStdOperatorTable.COUNT,
          new SqlNode[] {node.getField().accept(this, null)},
          POS,
          SqlLiteral.createSymbol(SqlSelectKeyword.DISTINCT, POS));
    }
    if ("percentile".equals(name) || "percentile_approx".equals(name)) {
      SqlNode field = node.getField().accept(this, null);
      List<UnresolvedExpression> funcArgs = node.getArgList();
      if (funcArgs != null && !funcArgs.isEmpty()) {
        SqlNode pct = funcArgs.get(0).accept(this, null);
        if (funcArgs.size() >= 2) {
          SqlNode compression = funcArgs.get(1).accept(this, null);
          return call("percentile_approx", field, pct, compression);
        }
        return call("percentile_approx", field, pct);
      }
      return call("percentile_approx", field, intLiteral(50));
    }
    if ("var_samp".equals(name)) {
      SqlNode field = node.getField().accept(this, null);
      return call("VAR_SAMP", cast(field, typeSpec(SqlTypeName.DOUBLE)));
    }
    if ("var_pop".equals(name)) {
      SqlNode field = node.getField().accept(this, null);
      return call("VAR_POP", cast(field, typeSpec(SqlTypeName.DOUBLE)));
    }
    if ("stddev_samp".equals(name)) {
      SqlNode field = node.getField().accept(this, null);
      return call("STDDEV_SAMP", cast(field, typeSpec(SqlTypeName.DOUBLE)));
    }
    if ("stddev_pop".equals(name)) {
      SqlNode field = node.getField().accept(this, null);
      return call("STDDEV_POP", cast(field, typeSpec(SqlTypeName.DOUBLE)));
    }
    if ("avg".equals(name)) {
      SqlNode field = node.getField().accept(this, null);
      return cast(call("AVG", cast(field, typeSpec(SqlTypeName.DOUBLE))), typeSpec(SqlTypeName.DOUBLE));
    }
    String sqlName = FUNC_MAP.getOrDefault(name, name.toUpperCase());
    SqlNode field = node.getField().accept(this, null);
    if (Boolean.TRUE.equals(node.getDistinct())) {
      return new SqlBasicCall(
          SqlStdOperatorTable.COUNT,
          new SqlNode[] {field},
          POS,
          SqlLiteral.createSymbol(SqlSelectKeyword.DISTINCT, POS));
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
      thens.add(castVarcharIfStringLiteral(w.getResult().accept(this, null)));
    }
    SqlNode elseExpr =
        node.getElseClause().map(e -> castVarcharIfStringLiteral(e.accept(this, null))).orElse(literal(null));
    return caseWhen(whens, thens, elseExpr);
  }

  @Override
  public SqlNode visitCast(Cast node, Void ctx) {
    SqlNode expr = node.getExpression().accept(this, null);
    String typeName = node.getConvertedType().toString().toUpperCase();
    SqlTypeName sqlType;
    switch (typeName) {
      case "STRING":
        return cast(expr, typeSpec(SqlTypeName.VARCHAR));
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
        // Calcite's CAST(int AS BOOLEAN) doesn't follow Spark/Postgres semantics.
        // Numeric: non-zero → true, zero → false
        // String: '1'/'true' → true, '0'/'false' → false, else → null
        // Field ref: try numeric comparison, fall back to string matching
        return castToBoolean(node, expr);
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

  /** Wrap string literals in CAST(... AS VARCHAR) to prevent Calcite CHAR(N) padding in CASE WHEN. */
  private static SqlNode castVarcharIfStringLiteral(SqlNode node) {
    if (node instanceof SqlLiteral && ((SqlLiteral) node).getTypeName() == SqlTypeName.CHAR) {
      return cast(node, typeSpec(SqlTypeName.VARCHAR));
    }
    return node;
  }

  /** Check if a SqlNode is a string literal. */
  private static boolean isStringLiteral(SqlNode node) {
    return node instanceof SqlLiteral && ((SqlLiteral) node).getTypeName() == SqlTypeName.CHAR;
  }

  /** Wrap a SqlNode in ARRAY() only if it is definitely a scalar (literal, cast, arithmetic). Field references pass through since they may already be arrays. */
  private static SqlNode ensureArrayArg(SqlNode node) {
    if (node instanceof SqlIdentifier) return node;
    if (isArrayExpression(node)) return node;
    return new SqlBasicCall(SqlLibraryOperators.ARRAY, new SqlNode[]{node}, POS);
  }

  /** Check if a SqlNode is known to produce an array value. */
  private static boolean isArrayExpression(SqlNode node) {
    if (node instanceof SqlBasicCall) {
      String op = ((SqlBasicCall) node).getOperator().getName();
      return "ARRAY".equals(op) || "ARRAY_CONCAT".equals(op) || "ARRAY_COMPACT".equals(op)
          || "ARRAY_DISTINCT".equals(op) || "SORT_ARRAY".equals(op) || "SPLIT".equals(op)
          || "ARRAYS_ZIP".equals(op) || "ARRAY_SLICE".equals(op);
    }
    return false;
  }

  /**
   * After eval substitution, ARRAY(ARRAY(1,2), ARRAY(3,4), 5) may appear.
   * Convert to ARRAY_CONCAT(ARRAY(1,2), ARRAY(3,4), ARRAY(5)) for proper flattening.
   */
  private static SqlNode fixNestedArrayToConcat(SqlNode node) {
    if (!(node instanceof SqlBasicCall)) return node;
    SqlBasicCall call = (SqlBasicCall) node;
    if (!"ARRAY".equals(call.getOperator().getName()) || call.operandCount() < 2) return node;
    boolean hasNestedArray = false;
    for (SqlNode operand : call.getOperandList()) {
      if (isArrayExpression(operand)) { hasNestedArray = true; break; }
    }
    if (!hasNestedArray) return node;
    SqlNode[] wrapped = call.getOperandList().stream()
        .map(PPLToSqlNodeConverter::ensureArrayArg)
        .toArray(SqlNode[]::new);
    return new SqlBasicCall(SqlLibraryOperators.ARRAY_CONCAT, wrapped, POS);
  }

  /** Check if a SqlNode produces a string value (literal, CONCAT, or CAST to VARCHAR). */
  private static boolean isStringProducing(SqlNode node) {
    if (isStringLiteral(node)) return true;
    if (node instanceof SqlBasicCall) {
      SqlBasicCall call = (SqlBasicCall) node;
      String opName = call.getOperator().getName();
      if ("CONCAT".equals(opName)) return true;
      if ("CAST".equals(opName) && call.operandCount() == 2
          && call.operand(1) instanceof SqlDataTypeSpec) {
        String typeName = call.operand(1).toString().toUpperCase();
        if (typeName.contains("VARCHAR") || typeName.contains("CHAR")) return true;
      }
    }
    return false;
  }

  /** Check if a string contains unescaped wildcard characters (* or ?). */
  protected static boolean hasUnescapedWildcard(String str) {
    if (str == null) return false;
    boolean escaped = false;
    for (char c : str.toCharArray()) {
      if (escaped) { escaped = false; continue; }
      if (c == '\\') { escaped = true; continue; }
      if (c == '*' || c == '?') return true;
    }
    return false;
  }

  /** CAST to BOOLEAN with Spark/Postgres semantics. */
  private SqlNode castToBoolean(Cast node, SqlNode expr) {
    UnresolvedExpression src = node.getExpression();
    if (src instanceof Literal) {
      Object val = ((Literal) src).getValue();
      if (val instanceof Number) {
        return neq(expr, literal(0));
      }
      if (val instanceof String) {
        return caseWhen(
            List.of(or(eq(expr, literal("1")), eq(call("UPPER", expr), literal("TRUE")))),
            List.of(literal(true)),
            caseWhen(
                List.of(or(eq(expr, literal("0")), eq(call("UPPER", expr), literal("FALSE")))),
                List.of(literal(false)),
                literal(null)));
      }
    }
    // Field reference: string-based boolean matching, null for non-boolean-like values
    SqlNode upper = call("UPPER", cast(expr, typeSpec(SqlTypeName.VARCHAR)));
    return caseWhen(
        List.of(isNull(expr)),
        List.of(literal(null)),
        caseWhen(
            List.of(or(eq(upper, literal("1")), eq(upper, literal("TRUE")))),
            List.of(literal(true)),
            caseWhen(
                List.of(or(eq(upper, literal("0")), eq(upper, literal("FALSE")))),
                List.of(literal(false)),
                literal(null))));
  }

  private SqlNode ensureTimestamp(SqlNode node) {
    if (isAlreadyCastToTime(node)) return node;
    return cast(node, typeSpec(SqlTypeName.TIMESTAMP));
  }

  /** Extract a literal date string from a CAST('...' AS DATE/TIMESTAMP) SqlNode. Returns null if not a literal. */
  private static String extractLiteralDateString(SqlNode node) {
    if (node instanceof SqlBasicCall) {
      SqlBasicCall c = (SqlBasicCall) node;
      if (c.getOperator() == SqlStdOperatorTable.CAST && c.operandCount() == 2) {
        SqlNode inner = c.operand(0);
        if (inner instanceof SqlLiteral) {
          String val = ((SqlLiteral) inner).toValue();
          if (val != null) return val;
        }
      }
    }
    return null;
  }

  /** Compute MySQL-compatible week number for a given date and mode. */
  private static int computeMySQLWeek(java.time.LocalDate d, int mode) {
    int doy = d.getDayOfYear();
    int dow = d.getDayOfWeek().getValue() % 7 + 1; // 1=Sun..7=Sat (MySQL convention)
    boolean sundayBased = (mode % 2 == 0);
    boolean fourDayRule = (mode == 1 || mode == 3 || mode == 5 || mode == 7);
    boolean range1to53 = (mode == 2 || mode == 3 || mode == 6 || mode == 7);

    int adjDow = sundayBased ? dow : (dow + 5) % 7 + 1;
    int weekNum = (doy - adjDow + 7) / 7;

    if (fourDayRule) {
      int adjDowMon = (dow + 5) % 7 + 1; // Mon=1..Sun=7
      int weekMode0Mon = (doy - adjDowMon + 7) / 7;
      int jan1DowMon = ((adjDowMon - 1) - (doy - 1) % 7 + 7) % 7; // Mon=0..Sun=6
      weekNum = weekMode0Mon + (jan1DowMon <= 3 ? 1 : 0);
    }

    if (range1to53 && weekNum == 0) {
      // Compute last week of previous year
      java.time.LocalDate dec31 = d.minusDays(doy);
      return computeMySQLWeek(dec31, mode - 2);
    }
    return weekNum;
  }

  /** Like ensureTimestamp but converts TIME to TIMESTAMP using today's date (for date arithmetic). */
  private SqlNode ensureTimestampForDateArith(SqlNode node) {
    if (isAlreadyCastToTime(node)) return promoteTimeToTimestamp(node);
    return cast(node, typeSpec(SqlTypeName.TIMESTAMP));
  }

  /** Build TIMESTAMPADD(unit, amount, ts) using Calcite's built-in operator with SqlIntervalQualifier. */
  private SqlNode tsAdd(String unit, SqlNode amount, SqlNode ts) {
    TimeUnit tu = mapTruncUnit(unit);
    SqlIntervalQualifier qualifier = new SqlIntervalQualifier(tu, null, POS);
    return SqlStdOperatorTable.TIMESTAMP_ADD.createCall(POS, qualifier, amount, ts);
  }

  /** CAST(EXTRACT(unit FROM arg) AS INTEGER) */
  private SqlNode castExtract(String unit, SqlNode arg) {
    TimeUnit tu;
    switch (unit) {
      case "YEAR": tu = TimeUnit.YEAR; break;
      case "MONTH": tu = TimeUnit.MONTH; break;
      case "DAY": tu = TimeUnit.DAY; break;
      case "HOUR": tu = TimeUnit.HOUR; break;
      case "MINUTE": tu = TimeUnit.MINUTE; break;
      case "SECOND": tu = TimeUnit.SECOND; break;
      case "QUARTER": tu = TimeUnit.QUARTER; break;
      case "WEEK": tu = TimeUnit.WEEK; break;
      case "MICROSECOND": tu = TimeUnit.MICROSECOND; break;
      default: tu = TimeUnit.DAY; break;
    }
    // If arg is already CAST to TIME, leave it (EXTRACT(HOUR FROM TIME) works).
    // Otherwise cast to TIMESTAMP (needed for raw string literals).
    SqlNode target;
    if (isAlreadyCastToTime(arg)) {
      target = arg;
    } else {
      target = cast(arg, typeSpec(SqlTypeName.TIMESTAMP));
    }
    SqlIntervalQualifier qualifier = new SqlIntervalQualifier(tu, null, POS);
    SqlNode extract = new SqlBasicCall(SqlStdOperatorTable.EXTRACT, new SqlNode[]{qualifier, target}, POS);
    return cast(extract, typeSpec(SqlTypeName.INTEGER));
  }

  /** Check if a SqlNode is CAST(... AS TIME) */
  private static boolean isAlreadyCastToTime(SqlNode node) {
    if (node instanceof SqlBasicCall) {
      SqlBasicCall c = (SqlBasicCall) node;
      if (c.getOperator() == SqlStdOperatorTable.CAST && c.getOperandList().size() == 2) {
        SqlNode typeNode = c.getOperandList().get(1);
        if (typeNode instanceof SqlDataTypeSpec) {
          String typeName = typeNode.toString().toUpperCase();
          return typeName.contains("TIME") && !typeName.contains("TIMESTAMP");
        }
      }
    }
    return false;
  }

  @Override
  public SqlNode visitSpan(Span node, Void ctx) {
    SqlNode field = node.getField().accept(this, null);
    SqlNode value = node.getValue().accept(this, null);
    SpanUnit unit = node.getUnit();
    if (unit == SpanUnit.NONE || !SpanUnit.isTimeUnit(unit)) {
      return times(call("FLOOR", divide(field, value)), value);
    }
    // For week unit, Calcite FLOOR(field TO WEEK) floors to Sunday.
    // Shift by -1 day, floor to week, then shift back +1 day to get Monday-based week.
    if ("w".equals(SpanUnit.getName(unit))) {
      SqlNode shifted = call("TIMESTAMPADD", identifier("DAY"), intLiteral(-1), field);
      SqlNode floored = new SqlBasicCall(SqlStdOperatorTable.FLOOR,
          new SqlNode[] {shifted, new SqlIntervalQualifier(TimeUnit.WEEK, null, POS)}, POS);
      return call("TIMESTAMPADD", identifier("DAY"), intLiteral(1), floored);
    }
    // Check if span value is 1 — use simple FLOOR(field TO unit)
    boolean isUnitSpan = false;
    if (node.getValue() instanceof org.opensearch.sql.ast.expression.Literal) {
      Object v = ((org.opensearch.sql.ast.expression.Literal) node.getValue()).getValue();
      if (v instanceof Number && ((Number) v).intValue() == 1) {
        isUnitSpan = true;
      }
    }
    if (isUnitSpan) {
      return new SqlBasicCall(SqlStdOperatorTable.FLOOR,
          new SqlNode[] {field, new SqlIntervalQualifier(spanUnitToTimeUnit(unit), null, POS)}, POS);
    }
    // For multi-unit time spans (e.g., span=2m), use:
    // TIMESTAMPADD(unit, FLOOR(TIMESTAMPDIFF(unit, epoch, field) / value) * value, epoch)
    String unitName = spanUnitToTimeUnit(unit).toString();
    SqlNode epoch = cast(literal("1970-01-01 00:00:00"), typeSpec(SqlTypeName.TIMESTAMP));
    SqlNode diff = call("TIMESTAMPDIFF", identifier(unitName), epoch, field);
    SqlNode floored = times(call("FLOOR", divide(cast(diff, typeSpec(SqlTypeName.DOUBLE)), value)), value);
    SqlNode flooredInt = cast(floored, typeSpec(SqlTypeName.INTEGER));
    return call("TIMESTAMPADD", identifier(unitName), flooredInt, epoch);
  }

  private static TimeUnit spanUnitToTimeUnit(SpanUnit u) {
    switch (SpanUnit.getName(u)) {
      case "s": return TimeUnit.SECOND;
      case "m": return TimeUnit.MINUTE;
      case "h": return TimeUnit.HOUR;
      case "d": return TimeUnit.DAY;
      case "w": return TimeUnit.WEEK;
      case "M": return TimeUnit.MONTH;
      case "q": return TimeUnit.QUARTER;
      case "y": return TimeUnit.YEAR;
      default: return TimeUnit.DAY;
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
    List<UnresolvedExpression> values = node.getValue();
    // Validate column count match
    int subCols = countSelectColumns(sub);
    if (subCols > 0 && subCols != values.size()) {
      throw new org.opensearch.sql.exception.SemanticCheckException(
          "The number of columns in the left hand side of an IN subquery does not match the number"
              + " of columns in the output of subquery");
    }
    if (values.size() == 1) {
      return inSub(values.get(0).accept(this, null), sub);
    }
    // Multi-value IN: (a, b) IN (subquery) → ROW(a, b) IN (subquery)
    SqlNode[] fields = values.stream().map(v -> v.accept(this, null)).toArray(SqlNode[]::new);
    SqlNode row = new SqlBasicCall(SqlStdOperatorTable.ROW, fields, POS);
    return inSub(row, sub);
  }

  @Override
  public SqlNode visitScalarSubquery(ScalarSubquery node, Void ctx) {
    return convertSubPlan(node.getQuery());
  }

  @Override
  public SqlNode visitExistsSubquery(ExistsSubquery node, Void ctx) {
    return exists(convertSubPlan(node.getQuery()));
  }

  @Override
  public SqlNode visitChart(Chart node, Void ctx) {
    // Parse chart arguments
    Argument.ArgumentMap args = Argument.ArgumentMap.of(node.getArguments());
    boolean useNull = args.get("usenull") == null
        || Boolean.TRUE.equals(args.get("usenull").getValue());
    String nullStr = args.get("nullstr") != null
        ? (String) args.get("nullstr").getValue() : "NULL";
    boolean useOther = args.get("useother") == null
        || Boolean.TRUE.equals(args.get("useother").getValue());
    String otherStr = args.get("otherstr") != null
        ? (String) args.get("otherstr").getValue() : "OTHER";
    int limit = args.get("limit") != null
        ? ((Number) args.get("limit").getValue()).intValue() : 10;
    boolean top = args.get("top") == null
        || Boolean.TRUE.equals(args.get("top").getValue());
    boolean hasExplicitLimit = args.get("limit") != null && limit > 0;

    // Determine if columnSplit is a Span (and whether it's numeric)
    boolean colSplitIsSpan = false;
    boolean colSplitIsNumericSpan = false;
    if (node.getColumnSplit() != null) {
      UnresolvedExpression colInner = node.getColumnSplit() instanceof Alias
          ? ((Alias) node.getColumnSplit()).getDelegated() : node.getColumnSplit();
      if (colInner instanceof Span) {
        colSplitIsSpan = true;
        SpanUnit su = ((Span) colInner).getUnit();
        colSplitIsNumericSpan = (su == SpanUnit.NONE || !SpanUnit.isTimeUnit(su));
      }
    }

    // Determine if limit logic will be applied
    boolean applyLimit = node.getColumnSplit() != null
        && node.getRowSplit() != null && limit > 0;

    // Fix 2: When no rowSplit and columnSplit is a numeric span, don't cast to VARCHAR
    boolean skipVarcharCast = (node.getRowSplit() == null && colSplitIsNumericSpan);

    List<SqlNode> selectItems = new ArrayList<>();
    List<SqlNode> groupByItems = new ArrayList<>();
    List<SqlNode> nullFilters = new ArrayList<>();

    // Row split (may be null for simple chart by field)
    if (node.getRowSplit() != null) {
      SqlNode rowSplitExpr = node.getRowSplit().accept(this, null);
      selectItems.add(rowSplitExpr);
      SqlNode rowGroupBy;
      if (node.getRowSplit() instanceof Alias) {
        rowGroupBy = ((Alias) node.getRowSplit()).getDelegated().accept(this, null);
      } else {
        rowGroupBy = node.getRowSplit().accept(this, null);
      }
      groupByItems.add(rowGroupBy);
      // Always filter null row splits
      nullFilters.add(isNotNull(rowGroupBy));
    }

    // Column split (optional)
    String colAlias = null;
    if (node.getColumnSplit() != null) {
      SqlNode colGroupBy;
      if (node.getColumnSplit() instanceof Alias) {
        colGroupBy = ((Alias) node.getColumnSplit()).getDelegated().accept(this, null);
      } else {
        colGroupBy = node.getColumnSplit().accept(this, null);
      }

      colAlias = node.getColumnSplit() instanceof Alias
          ? ((Alias) node.getColumnSplit()).getName() : null;

      SqlNode colSelectExpr;
      if (skipVarcharCast) {
        colSelectExpr = colGroupBy;
      } else if (applyLimit) {
        // When limit logic will be applied, cast to VARCHAR but defer COALESCE
        colSelectExpr = cast(colGroupBy, typeSpec(SqlTypeName.VARCHAR));
      } else {
        // No limit logic — apply full VARCHAR cast + COALESCE inline
        colSelectExpr = cast(colGroupBy, typeSpec(SqlTypeName.VARCHAR));
        if (useNull && !colSplitIsSpan) {
          colSelectExpr = call("COALESCE", colSelectExpr, literal(nullStr));
        }
      }

      // Fix 4: For span-based column splits, always filter out NULLs
      if (colSplitIsSpan) {
        nullFilters.add(isNotNull(colGroupBy));
      } else if (!useNull) {
        nullFilters.add(isNotNull(colGroupBy));
      }

      if (colAlias != null) {
        selectItems.add(as(colSelectExpr, colAlias));
      } else {
        selectItems.add(colSelectExpr);
      }
      groupByItems.add(colGroupBy);
    }

    // Aggregation function
    selectItems.add(node.getAggregationFunction().accept(this, null));

    SqlNodeDSL.SelectBuilder builder = select(selectItems.toArray(new SqlNode[0]))
        .from(wrapAsSubquery());
    if (!groupByItems.isEmpty()) {
      builder = builder.groupBy(groupByItems.toArray(new SqlNode[0]));
    }
    if (!nullFilters.isEmpty()) {
      SqlNode filter = nullFilters.stream().reduce((a, b) -> and(a, b)).get();
      builder = builder.where(filter);
    }
    pipe = builder.build();

    // Fix 3: Apply limit/top/bottom with useother
    if (applyLimit) {
      pipe = applyChartLimit(node, limit, top, useOther, otherStr, useNull, nullStr);
    }

    return pipe;
  }

  /**
   * Apply chart limit/top/bottom logic with optional OTHER aggregation.
   * Ranks column-split values globally by SUM of aggregation across all row-split groups,
   * then keeps top/bottom N and optionally aggregates the rest into otherStr.
   * NULL column-split values are excluded from ranking and re-included with nullStr if usenull.
   */
  private SqlNode applyChartLimit(Chart node, int limit, boolean top,
      boolean useOther, String otherStr, boolean useNull, String nullStr) {
    String rowAlias = node.getRowSplit() instanceof Alias
        ? ((Alias) node.getRowSplit()).getName() : null;
    String colAlias = node.getColumnSplit() instanceof Alias
        ? ((Alias) node.getColumnSplit()).getName() : null;
    String aggAlias = node.getAggregationFunction() instanceof Alias
        ? ((Alias) node.getAggregationFunction()).getName() : null;
    if (rowAlias == null || colAlias == null || aggAlias == null) return pipe;

    // Step 1: Wrap base aggregation as subquery
    SqlNode base = subquery(pipe, nextAlias());

    // Step 2: Add SUM(agg_val) OVER (PARTITION BY colSplit) for global ranking
    // NULL colSplit values will have NULL _col_total (excluded from ranking naturally)
    SqlNodeList colPartBy = new SqlNodeList(List.of(identifier(colAlias)), POS);
    SqlNode colTotal = as(
        window(sum(identifier(aggAlias)), colPartBy, SqlNodeList.EMPTY), "_col_total");
    SqlNode withTotal = select(star(), colTotal).from(base).build();
    SqlNode withTotalSub = subquery(withTotal, nextAlias());

    // Step 3: Add DENSE_RANK() OVER (ORDER BY _col_total DESC/ASC, colAlias NULLS LAST) as _rn
    SqlNode rankOrder = top ? desc(identifier("_col_total")) : identifier("_col_total");
    SqlNode rn = as(
        window(call("DENSE_RANK"), SqlNodeList.EMPTY,
            new SqlNodeList(List.of(nullsLast(rankOrder), identifier(colAlias)), POS)), "_rn");
    SqlNode ranked = select(identifier(rowAlias), identifier(colAlias),
        identifier(aggAlias), rn).from(withTotalSub).build();
    SqlNode rankedSub = subquery(ranked, nextAlias());

    if (!useOther) {
      // Just filter to top/bottom N (exclude NULLs which have _rn = NULL)
      pipe = select(identifier(rowAlias), identifier(colAlias), identifier(aggAlias))
          .from(rankedSub)
          .where(lte(identifier("_rn"), intLiteral(limit)))
          .orderBy(identifier(rowAlias), identifier(colAlias))
          .build();
    } else {
      // CASE WHEN colSplit IS NULL THEN nullStr (if usenull, checked first)
      //      WHEN _rn <= limit THEN colSplit
      //      ELSE otherStr END
      List<SqlNode> whens = new ArrayList<>();
      List<SqlNode> thens = new ArrayList<>();
      if (useNull) {
        whens.add(isNull(identifier(colAlias)));
        thens.add(literal(nullStr));
      }
      whens.add(lte(identifier("_rn"), intLiteral(limit)));
      thens.add(identifier(colAlias));
      SqlNode caseExpr = caseWhen(whens, thens, literal(otherStr));
      SqlNode casedCol = as(caseExpr, colAlias);

      // For re-aggregation: COUNT should use SUM (sum of counts), others use same function
      String aggFuncName = resolveAggFuncName(node.getAggregationFunction());
      String reaggFuncName = "COUNT".equals(aggFuncName) ? "SUM" : aggFuncName;
      SqlNode reagg = as(call(reaggFuncName, identifier(aggAlias)), aggAlias);

      // Same CASE expression for GROUP BY
      SqlNode caseGroupBy = caseWhen(
          new ArrayList<>(whens), new ArrayList<>(thens), literal(otherStr));

      pipe = select(identifier(rowAlias), casedCol, reagg)
          .from(rankedSub)
          .groupBy(identifier(rowAlias), caseGroupBy)
          .orderBy(identifier(rowAlias), identifier(colAlias))
          .build();
    }
    return pipe;
  }

  /** Extract the aggregation function name (MAX, MIN, SUM, AVG, COUNT) from the AST node. */
  private static String resolveAggFuncName(UnresolvedExpression aggExpr) {
    if (aggExpr instanceof Alias) {
      aggExpr = ((Alias) aggExpr).getDelegated();
    }
    if (aggExpr instanceof AggregateFunction) {
      return ((AggregateFunction) aggExpr).getFuncName().toUpperCase();
    }
    return "MAX"; // fallback
  }

  @Override
  public SqlNode visitNoMv(NoMv node, Void ctx) {
    // NoMv always overrides an existing field — delegate to visitEval which handles overrides
    return visitEval((Eval) node.rewriteAsEval(), ctx);
  }

  @Override
  public SqlNode visitSpath(SPath node, Void ctx) {
    node.getChild().get(0).accept(this, ctx);
    // Track fields produced by spath auto-extract (json_extract_all returns MAP)
    if (node.getPath() == null) {
      String output = (node.getOutField() != null) ? node.getOutField() : node.getInField();
      mapFields.add(output);
    }
    Eval evalNode = node.rewriteAsEval();
    return visitEval(evalNode, ctx);
  }

  @Override
  public SqlNode visitMvExpand(MvExpand node, Void ctx) {
    // MvExpand needs schema to enumerate columns — delegate to DynamicPPLToSqlNodeConverter
    return pipe;
  }

  @Override
  public SqlNode visitExpand(org.opensearch.sql.ast.tree.Expand node, Void ctx) {
    // Expand needs schema to enumerate columns — delegate to DynamicPPLToSqlNodeConverter
    return pipe;
  }

  @Override
  public SqlNode visitTranspose(org.opensearch.sql.ast.tree.Transpose node, Void ctx) {
    // Transpose needs schema to enumerate columns — delegate to DynamicPPLToSqlNodeConverter
    return pipe;
  }

  @Override
  public SqlNode visitPatterns(Patterns node, Void ctx) {
    if (node.getPatternMethod() == PatternMethod.BRAIN) {
      throw new UnsupportedOperationException(
          "Brain pattern method not supported in V4 converter");
    }

    pipe = wrapAsSubquery();
    SqlNode sourceField = node.getSourceField().accept(this, null);
    String alias = node.getAlias() != null ? node.getAlias() : "patterns_field";
    boolean showNumbered = false;
    if (node.getShowNumberedToken() instanceof Literal) {
      Object val = ((Literal) node.getShowNumberedToken()).getValue();
      showNumbered = Boolean.TRUE.equals(val) || "true".equalsIgnoreCase(String.valueOf(val));
    }
    String defaultPattern = showNumbered ? "[a-zA-Z]+" : "[a-zA-Z0-9]+";
    String pattern =
        node.getArguments().containsKey("pattern")
            ? node.getArguments().get("pattern").getValue().toString()
            : defaultPattern;

    // CASE WHEN sourceField IS NULL OR sourceField = '' THEN '' ELSE REGEXP_REPLACE(...) END
    SqlNode caseExpr =
        caseWhen(
            List.of(or(isNull(sourceField), eq(sourceField, literal("")))),
            List.of(literal("")),
            call("REGEXP_REPLACE", sourceField, literal(pattern), literal("<*>")));

    if (node.getPatternMode() == PatternMode.LABEL) {
      pipe =
          select(star(), as(caseExpr, alias))
              .from(pipe)
              .build();
    } else {
      // AGGREGATION mode: wrap label query, then GROUP BY + COUNT + TAKE
      pipe = select(star(), as(caseExpr, alias)).from(pipe).build();
      pipe = subquery(pipe, nextAlias());

      SqlNode patternsFieldId = identifier(alias);
      int maxSampleCount = 5;
      if (node.getPatternMaxSampleCount() instanceof Literal) {
        Object val = ((Literal) node.getPatternMaxSampleCount()).getValue();
        if (val instanceof Number) maxSampleCount = ((Number) val).intValue();
      }

      List<SqlNode> selectItems = new ArrayList<>();
      List<SqlNode> groupByItems = new ArrayList<>();

      // Add partition-by fields first
      if (node.getPartitionByList() != null) {
        for (UnresolvedExpression partExpr : node.getPartitionByList()) {
          SqlNode partField = partExpr.accept(this, null);
          selectItems.add(partField);
          groupByItems.add(partField);
        }
      }

      selectItems.add(patternsFieldId);
      groupByItems.add(patternsFieldId);
      selectItems.add(
          as(
              new SqlBasicCall(
                  SqlStdOperatorTable.COUNT, new SqlNode[] {patternsFieldId}, POS),
              "pattern_count"));
      selectItems.add(as(call("TAKE", sourceField, literal(maxSampleCount)), "sample_logs"));

      pipe =
          select(selectItems.toArray(new SqlNode[0]))
              .from(pipe)
              .groupBy(groupByItems.toArray(new SqlNode[0]))
              .build();
    }
    return pipe;
  }

  @Override
  public SqlNode visitRex(Rex node, Void ctx) {
    pipe = wrapAsSubquery();
    SqlNode sourceField = node.getField().accept(this, null);
    String pattern = ((Literal) node.getPattern()).getValue().toString();

    if (node.getMode() == Rex.RexMode.SED) {
      SqlNode replacement = parseSedToRegexpReplace(sourceField, pattern);
      String fieldName = getFieldName(node.getField());
      List<SqlNode> selectItems = new ArrayList<>();
      selectItems.add(star());
      selectItems.add(as(replacement, fieldName));
      pipe = select(selectItems.toArray(new SqlNode[0])).from(pipe).build();
      return pipe;
    }

    // EXTRACT mode
    // Validate group names (throws IllegalArgumentException for invalid names)
    RegexCommonUtils.getNamedGroupCandidates(pattern);

    java.util.regex.Pattern namedGroupPattern =
        java.util.regex.Pattern.compile("\\(\\?<([a-zA-Z][a-zA-Z0-9]*)>");
    java.util.regex.Matcher matcher = namedGroupPattern.matcher(pattern);
    List<String> groupNames = new ArrayList<>();
    while (matcher.find()) groupNames.add(matcher.group(1));

    if (groupNames.isEmpty()) {
      throw new IllegalArgumentException(
          "Rex pattern must contain at least one named capture group");
    }

    List<SqlNode> selectItems = new ArrayList<>();
    selectItems.add(star());
    for (int i = 0; i < groupNames.size(); i++) {
      String groupName = groupNames.get(i);
      // Step 1: Convert other named groups to non-capturing
      String singleGroupPattern = pattern;
      for (int j = 0; j < groupNames.size(); j++) {
        if (j != i) {
          singleGroupPattern = singleGroupPattern.replace("(?<" + groupNames.get(j) + ">", "(?:");
        }
      }
      // Step 2: Convert all unnamed capturing groups to non-capturing.
      // The target (?<name> starts with "(?" so it is NOT matched by this regex.
      singleGroupPattern =
          singleGroupPattern.replaceAll("(?<!\\\\)\\((?!\\?)", "(?:");
      // Step 3: Convert target named group to the sole unnamed capturing group.
      singleGroupPattern = singleGroupPattern.replace("(?<" + groupName + ">", "(");

      SqlNode regexExpr = call("COALESCE",
          call("REGEXP_EXTRACT", sourceField, literal(singleGroupPattern), literal(1)),
          literal(""));
      SqlIdentifier quotedAlias = new SqlIdentifier(groupName, SqlParserPos.QUOTED_ZERO);
      selectItems.add(new SqlBasicCall(SqlStdOperatorTable.AS,
          new SqlNode[] {regexExpr, quotedAlias}, POS));
    }
    pipe = select(selectItems.toArray(new SqlNode[0])).from(pipe).build();
    return pipe;
  }

  private SqlNode parseSedToRegexpReplace(SqlNode field, String sedExpr) {
    if (sedExpr.startsWith("s") && sedExpr.length() > 1) {
      char delim = sedExpr.charAt(1);
      String rest = sedExpr.substring(2);
      int idx1 = rest.indexOf(delim);
      if (idx1 >= 0) {
        String pat = rest.substring(0, idx1);
        String rest2 = rest.substring(idx1 + 1);
        int idx2 = rest2.indexOf(delim);
        String repl, flags;
        if (idx2 >= 0) {
          repl = rest2.substring(0, idx2);
          flags = rest2.substring(idx2 + 1);
        } else {
          repl = rest2;
          flags = "";
        }
        if (!flags.isEmpty()) {
          return call("REGEXP_REPLACE", field, literal(pat), literal(repl), literal(flags));
        }
        return call("REGEXP_REPLACE", field, literal(pat), literal(repl));
      }
    }
    return field;
  }

  @Override
  public SqlNode visitMultisearch(Multisearch node, Void ctx) {
    List<UnresolvedPlan> subsearches = node.getSubsearches();
    if (subsearches.isEmpty()) return pipe;

    SqlNode result = convertSubPlan(subsearches.get(0));
    for (int i = 1; i < subsearches.size(); i++) {
      result = unionAll(result, convertSubPlan(subsearches.get(i)));
    }
    pipe = select(star()).from(subquery(result, nextAlias())).build();
    return pipe;
  }

  /** Build null-ignoring GREATEST: CASE WHEN a IS NULL THEN b WHEN b IS NULL THEN a WHEN a >= b THEN a ELSE b END */
  private SqlNode buildGreatest(List<SqlNode> args) {
    if (args.size() == 1) return args.get(0);
    if (args.size() == 2) {
      SqlNode a = args.get(0), b = args.get(1);
      return caseWhen(
          List.of(isNull(a), isNull(b), gte(a, b)),
          List.of(b, a, a),
          b);
    }
    SqlNode rest = buildGreatest(args.subList(1, args.size()));
    return buildGreatest(List.of(args.get(0), rest));
  }

  /** Build null-ignoring LEAST: CASE WHEN a IS NULL THEN b WHEN b IS NULL THEN a WHEN a <= b THEN a ELSE b END */
  private SqlNode buildLeast(List<SqlNode> args) {
    if (args.size() == 1) return args.get(0);
    if (args.size() == 2) {
      SqlNode a = args.get(0), b = args.get(1);
      return caseWhen(
          List.of(isNull(a), isNull(b), lte(a, b)),
          List.of(b, a, a),
          b);
    }
    SqlNode rest = buildLeast(args.subList(1, args.size()));
    return buildLeast(List.of(args.get(0), rest));
  }

  protected SqlNode convertSubPlan(UnresolvedPlan plan) {
    PPLToSqlNodeConverter sub = new PPLToSqlNodeConverter(aliasCounter, knownAliases);
    return sub.convert(plan);
  }

  /** Count the number of output columns in a SqlSelect (or SqlOrderBy wrapping one). Returns -1 if unknown. */
  private static int countSelectColumns(SqlNode node) {
    SqlNode target = node;
    if (target instanceof org.apache.calcite.sql.SqlOrderBy) {
      target = ((org.apache.calcite.sql.SqlOrderBy) target).query;
    }
    if (target instanceof SqlSelect) {
      SqlNodeList selectList = ((SqlSelect) target).getSelectList();
      if (selectList != null && selectList.size() > 0) {
        // Check if it's SELECT * (single star means unknown column count)
        if (selectList.size() == 1 && selectList.get(0).toString().equals("*")) {
          return -1;
        }
        return selectList.size();
      }
    }
    return -1;
  }

  /** Convert strftime first arg: if numeric or numeric string, convert to timestamp via TIMESTAMPADD.
   *  For identifiers, use magnitude-based heuristic to detect seconds vs milliseconds. */
  private SqlNode convertStrftimeArg(SqlNode tsArg) {
    // String literal that looks like a timestamp (contains '-') → cast to TIMESTAMP
    if (tsArg instanceof SqlCharStringLiteral) {
      String val = ((SqlCharStringLiteral) tsArg).getNlsString().getValue();
      if (val.contains("-") || val.contains(":")) {
        return cast(tsArg, typeSpec(SqlTypeName.TIMESTAMP));
      }
      // Numeric string → convert to numeric literal
      try {
        Double.parseDouble(val);
        tsArg = SqlLiteral.createExactNumeric(val, POS);
      } catch (NumberFormatException e) {
        return cast(tsArg, typeSpec(SqlTypeName.TIMESTAMP));
      }
    }
    SqlNode epoch = cast(literal("1970-01-01 00:00:00"), typeSpec(SqlTypeName.TIMESTAMP));
    // For numeric expressions, use TIMESTAMPADD with FLOOR
    if (isNumericExpression(tsArg)) {
      return call("TIMESTAMPADD", identifier("SECOND"), cast(call("FLOOR", tsArg), typeSpec(SqlTypeName.INTEGER)), epoch);
    }
    // For identifiers: could be seconds (from unix_timestamp) or milliseconds (from timestamp field cast to bigint).
    // Use magnitude heuristic: if abs value > 10^10, assume milliseconds and divide by 1000.
    if (tsArg instanceof SqlIdentifier) {
      SqlNode bigVal = cast(tsArg, typeSpec(SqlTypeName.BIGINT));
      SqlNode absVal = call("ABS", bigVal);
      SqlNode seconds = caseWhen(
          List.of(gt(absVal, SqlLiteral.createExactNumeric("10000000000", POS))),
          List.of(cast(divide(bigVal, intLiteral(1000)), typeSpec(SqlTypeName.INTEGER))),
          cast(bigVal, typeSpec(SqlTypeName.INTEGER)));
      return call("TIMESTAMPADD", identifier("SECOND"), seconds, epoch);
    }
    return cast(tsArg, typeSpec(SqlTypeName.TIMESTAMP));
  }

  /** Determine if a SqlNode expression is numeric (for strftime unix timestamp detection). */
  private static boolean isNumericExpression(SqlNode node) {
    if (node instanceof SqlLiteral) {
      SqlTypeName tn = ((SqlLiteral) node).getTypeName();
      if (tn == SqlTypeName.INTEGER || tn == SqlTypeName.BIGINT || tn == SqlTypeName.DECIMAL) return true;
      if (tn == SqlTypeName.CHAR) {
        try { Double.parseDouble(((SqlCharStringLiteral) node).getNlsString().getValue()); return true; } catch (NumberFormatException e) { return false; }
      }
      return false;
    }
    if (node instanceof SqlBasicCall) {
      SqlBasicCall call = (SqlBasicCall) node;
      var op = call.getOperator();
      // Arithmetic operations produce numeric results
      if (op == SqlStdOperatorTable.PLUS || op == SqlStdOperatorTable.MINUS
          || op == SqlStdOperatorTable.MULTIPLY || op == SqlStdOperatorTable.DIVIDE
          || op == SqlStdOperatorTable.MOD || op == SqlStdOperatorTable.UNARY_MINUS) return true;
      // CAST to numeric type
      if (op == SqlStdOperatorTable.CAST && call.operandCount() == 2) {
        SqlNode typeNode = call.operand(1);
        if (typeNode instanceof SqlDataTypeSpec) {
          String typeName = typeNode.toString().toUpperCase();
          if (typeName.contains("INTEGER") || typeName.contains("BIGINT") || typeName.contains("DOUBLE")
              || typeName.contains("FLOAT") || typeName.contains("DECIMAL")) return true;
        }
      }
      // FLOOR, CEIL, ABS, ROUND, TIMESTAMPDIFF produce numeric
      String funcName = op.getName().toUpperCase();
      if (Set.of("FLOOR", "CEIL", "ABS", "ROUND", "TRUNCATE", "TIMESTAMPDIFF").contains(funcName)) return true;
    }
    return false;
  }

  private static final String[] ABBREV_DAYS = {"Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"};
  private static final String[] FULL_DAYS = {"Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"};
  private static final String[] ABBREV_MONTHS = {"Jan", "Feb", "Mar", "Apr", "May", "Jun", "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};
  private static final String[] FULL_MONTHS = {"January", "February", "March", "April", "May", "June", "July", "August", "September", "October", "November", "December"};

  /** Extract a date/time part from timestamp as integer. */
  private SqlNode extractPart(TimeUnit unit, SqlNode ts) {
    return cast(new SqlBasicCall(SqlStdOperatorTable.EXTRACT,
        new SqlNode[]{new SqlIntervalQualifier(unit, null, POS), ts}, POS), typeSpec(SqlTypeName.INTEGER));
  }

  /** LPAD(CAST(expr AS VARCHAR), width, '0') */
  private SqlNode zeroPad(SqlNode expr, int width) {
    return call("LPAD", cast(expr, typeSpec(SqlTypeName.VARCHAR)), intLiteral(width), literal("0"));
  }

  /** CASE WHEN for mapping integer to string array (1-based index). Cast to VARCHAR to avoid CHAR padding. */
  private SqlNode mapIntToString(SqlNode expr, String[] values, int startIndex) {
    List<SqlNode> whens = new ArrayList<>();
    List<SqlNode> thens = new ArrayList<>();
    for (int i = 0; i < values.length; i++) {
      whens.add(eq(expr, intLiteral(i + startIndex)));
      thens.add(cast(literal(values[i]), typeSpec(SqlTypeName.VARCHAR)));
    }
    return caseWhen(whens, thens, cast(literal(""), typeSpec(SqlTypeName.VARCHAR)));
  }

  /** MySQL GET_FORMAT format strings by type and locale. */
  private static String getFormatString(String type, String locale) {
    switch (type) {
      case "DATE":
        switch (locale) {
          case "USA": return "%m.%d.%Y";
          case "JIS": case "ISO": return "%Y-%m-%d";
          case "EUR": return "%d.%m.%Y";
          case "INTERNAL": return "%Y%m%d";
          default: return "%Y-%m-%d";
        }
      case "DATETIME": case "TIMESTAMP":
        switch (locale) {
          case "USA": return "%Y-%m-%d %H.%i.%s";
          case "JIS": case "ISO": return "%Y-%m-%d %H:%i:%s";
          case "EUR": return "%Y-%m-%d %H.%i.%s";
          case "INTERNAL": return "%Y%m%d%H%i%s";
          default: return "%Y-%m-%d %H:%i:%s";
        }
      case "TIME":
        switch (locale) {
          case "USA": return "%h:%i:%s %p";
          case "JIS": case "ISO": return "%H:%i:%s";
          case "EUR": return "%H.%i.%s";
          case "INTERNAL": return "%H%i%s";
          default: return "%H:%i:%s";
        }
      default: return "%Y-%m-%d";
    }
  }

  /** Build ordinal suffix: 1st, 2nd, 3rd, 4th, ... 11th, 12th, 13th, 21st, etc. */
  private SqlNode buildOrdinal(SqlNode dayExpr) {
    SqlNode dayStr = cast(dayExpr, typeSpec(SqlTypeName.VARCHAR));
    SqlNode mod10 = call("MOD", dayExpr, intLiteral(10));
    SqlNode mod100 = call("MOD", dayExpr, intLiteral(100));
    // 11th, 12th, 13th are exceptions
    return concatTwo(dayStr, caseWhen(
        List.of(and(gte(mod100, intLiteral(11)), lte(mod100, intLiteral(13))),
            eq(mod10, intLiteral(1)), eq(mod10, intLiteral(2)), eq(mod10, intLiteral(3))),
        List.of(cast(literal("th"), typeSpec(SqlTypeName.VARCHAR)), cast(literal("st"), typeSpec(SqlTypeName.VARCHAR)),
            cast(literal("nd"), typeSpec(SqlTypeName.VARCHAR)), cast(literal("rd"), typeSpec(SqlTypeName.VARCHAR))),
        cast(literal("th"), typeSpec(SqlTypeName.VARCHAR))));
  }

  /** 12-hour from 24-hour: ((h+11)%12)+1 */
  private SqlNode hour12(SqlNode hour24) {
    return plus(call("MOD", plus(hour24, intLiteral(11)), intLiteral(12)), intLiteral(1));
  }

  /** AM/PM from hour */
  private SqlNode amPm(SqlNode hour24, boolean upper) {
    return caseWhen(List.of(lt(hour24, intLiteral(12))),
        List.of(cast(literal(upper ? "AM" : "am"), typeSpec(SqlTypeName.VARCHAR))),
        cast(literal(upper ? "PM" : "pm"), typeSpec(SqlTypeName.VARCHAR)));
  }

  /** DAYOFWEEK returns 1=Sunday..7=Saturday */
  private SqlNode dayOfWeek(SqlNode ts) {
    return cast(call("DAYOFWEEK", ts), typeSpec(SqlTypeName.INTEGER));
  }

  private SqlNode concatTwo(SqlNode a, SqlNode b) {
    return call("CONCAT", a, b);
  }

  private SqlNode concatList(List<SqlNode> parts) {
    if (parts.isEmpty()) return literal("");
    SqlNode result = parts.get(0);
    for (int i = 1; i < parts.size(); i++) {
      result = concatTwo(result, parts.get(i));
    }
    return result;
  }

  /** Build a SQL expression tree that formats a timestamp according to a format string.
   *  isStrftime=true uses Python/C strftime conventions, false uses MySQL date_format conventions.
   *  origNumeric is the original numeric arg (for strftime %3Q millisecond extraction from fractional seconds). */
  private SqlNode buildDateFormatExpr(SqlNode ts, String fmt, boolean isStrftime, SqlNode origNumeric) {
    List<SqlNode> parts = new ArrayList<>();
    SqlNode year = extractPart(TimeUnit.YEAR, ts);
    SqlNode month = extractPart(TimeUnit.MONTH, ts);
    SqlNode day = extractPart(TimeUnit.DAY, ts);
    SqlNode hour = extractPart(TimeUnit.HOUR, ts);
    SqlNode minute = extractPart(TimeUnit.MINUTE, ts);
    SqlNode second = extractPart(TimeUnit.SECOND, ts);
    SqlNode dow = dayOfWeek(ts); // 1=Sun..7=Sat
    SqlNode h12 = hour12(hour);

    int i = 0;
    while (i < fmt.length()) {
      if (fmt.charAt(i) == '%' && i + 1 < fmt.length()) {
        char spec = fmt.charAt(i + 1);
        // Check for %3Q (milliseconds extension for strftime)
        if (spec == '3' && i + 2 < fmt.length() && fmt.charAt(i + 2) == 'Q') {
          // milliseconds from fractional part of original numeric value
          if (origNumeric != null) {
            // millis = FLOOR((val - FLOOR(val)) * 1000) MOD 1000
            SqlNode frac = minus(origNumeric, call("FLOOR", origNumeric));
            SqlNode millis = cast(call("MOD", cast(call("FLOOR", times(frac, intLiteral(1000))), typeSpec(SqlTypeName.INTEGER)), intLiteral(1000)), typeSpec(SqlTypeName.INTEGER));
            parts.add(zeroPad(millis, 3));
          } else {
            parts.add(literal("000"));
          }
          i += 3;
          continue;
        }
        switch (spec) {
          case 'Y': parts.add(cast(year, typeSpec(SqlTypeName.VARCHAR))); break;
          case 'y': // 2-digit year
            parts.add(zeroPad(call("MOD", year, intLiteral(100)), 2)); break;
          case 'm': parts.add(zeroPad(month, 2)); break;
          case 'c': parts.add(zeroPad(month, 2)); break; // month (padded to match expected behavior)
          case 'd': parts.add(zeroPad(day, 2)); break;
          case 'e': parts.add(cast(day, typeSpec(SqlTypeName.VARCHAR))); break; // day no pad
          case 'H': parts.add(zeroPad(hour, 2)); break;
          case 'k': parts.add(cast(hour, typeSpec(SqlTypeName.VARCHAR))); break; // hour 24h no pad
          case 'h': case 'I': parts.add(zeroPad(h12, 2)); break; // 12h padded
          case 'l': parts.add(cast(h12, typeSpec(SqlTypeName.VARCHAR))); break; // 12h no pad
          case 'S': case 's': parts.add(zeroPad(second, 2)); break;
          case 'p': parts.add(amPm(hour, true)); break;
          case 'a': // abbreviated weekday
            parts.add(mapIntToString(dow, ABBREV_DAYS, 1)); break;
          case 'b': // abbreviated month
            parts.add(mapIntToString(month, ABBREV_MONTHS, 1)); break;
          case 'Z': parts.add(literal("UTC")); break;
          case '%': parts.add(literal("%")); break;
          case 'M':
            if (isStrftime) { // minute
              parts.add(zeroPad(minute, 2));
            } else { // full month name (MySQL)
              parts.add(mapIntToString(month, FULL_MONTHS, 1));
            }
            break;
          case 'i': // MySQL minute
            parts.add(zeroPad(minute, 2)); break;
          case 'D': // day with ordinal suffix
            parts.add(buildOrdinal(day)); break;
          case 'f': // microseconds (6 digits) - MOD to get only fractional part
            parts.add(zeroPad(cast(call("MOD", extractPart(TimeUnit.MICROSECOND, ts), intLiteral(1000000)), typeSpec(SqlTypeName.INTEGER)), 6)); break;
          case 'j': // day of year (3 digits)
            parts.add(zeroPad(call("DAYOFYEAR", ts), 3)); break;
          case 'W':
            if (isStrftime) { // day of week number 0=Sun..6=Sat (but 'w' in strftime)
              parts.add(mapIntToString(dow, FULL_DAYS, 1));
            } else { // full weekday name (MySQL)
              parts.add(mapIntToString(dow, FULL_DAYS, 1));
            }
            break;
          case 'w':
            if (isStrftime) { // 0=Sunday..6=Saturday
              parts.add(cast(minus(dow, intLiteral(1)), typeSpec(SqlTypeName.VARCHAR)));
            } else { // MySQL: 0=Sunday..6=Saturday
              parts.add(cast(minus(dow, intLiteral(1)), typeSpec(SqlTypeName.VARCHAR)));
            }
            break;
          case 'r': // 12h time hh:mm:ss AM/PM
            parts.add(concatList(List.of(zeroPad(h12, 2), literal(":"), zeroPad(minute, 2), literal(":"), zeroPad(second, 2), literal(" "), amPm(hour, true))));
            break;
          case 'T': // 24h time HH:MM:SS
            parts.add(concatList(List.of(zeroPad(hour, 2), literal(":"), zeroPad(minute, 2), literal(":"), zeroPad(second, 2))));
            break;
          case 'F': // %Y-%m-%d shorthand (strftime)
            parts.add(concatList(List.of(cast(year, typeSpec(SqlTypeName.VARCHAR)), literal("-"), zeroPad(month, 2), literal("-"), zeroPad(day, 2))));
            break;
          case 'P':
            if (isStrftime) { parts.add(amPm(hour, false)); }
            else { parts.add(literal("P")); } // not valid MySQL specifier
            break;
          case 'n': parts.add(literal("\n")); break;
          case 't': parts.add(literal("\t")); break;
          case 'U': case 'V': // week of year (Sunday start)
            parts.add(cast(call("WEEK", ts), typeSpec(SqlTypeName.VARCHAR))); break;
          case 'u': case 'v': // week of year (ISO)
            parts.add(cast(call("WEEK", ts), typeSpec(SqlTypeName.VARCHAR))); break;
          case 'X': case 'x': // year for week
            parts.add(cast(year, typeSpec(SqlTypeName.VARCHAR))); break;
          default:
            parts.add(literal("%" + spec)); break;
        }
        i += 2;
      } else {
        // Collect consecutive literal characters
        StringBuilder sb = new StringBuilder();
        while (i < fmt.length() && (fmt.charAt(i) != '%' || i + 1 >= fmt.length())) {
          sb.append(fmt.charAt(i));
          i++;
          if (i < fmt.length() && fmt.charAt(i - 1) != '%') continue;
          break;
        }
        parts.add(literal(sb.toString()));
      }
    }
    if (parts.isEmpty()) return literal("");
    return concatList(parts);
  }

  /** Check if the pipe is a simple SELECT * FROM table (no WHERE, GROUP BY, etc.). */
  private static boolean isSimpleTableScan(SqlNode node) {
    if (!(node instanceof SqlSelect)) return false;
    SqlSelect sel = (SqlSelect) node;
    return sel.getFrom() instanceof SqlIdentifier
        && sel.getWhere() == null
        && sel.getGroup() == null
        && sel.getHaving() == null;
  }

  /** Check if an expression tree contains a subquery or qualified name (table.column) referencing a known alias. */
  private boolean hasQualifiedOrSubqueryRefs(UnresolvedExpression expr) {
    if (expr instanceof org.opensearch.sql.ast.expression.subquery.SubqueryExpression) return true;
    if (expr instanceof QualifiedName) {
      List<String> parts = ((QualifiedName) expr).getParts();
      return parts.size() > 1 && knownAliases.contains(parts.get(0));
    }
    if (expr instanceof org.opensearch.sql.ast.expression.Not) {
      return hasQualifiedOrSubqueryRefs(((org.opensearch.sql.ast.expression.Not) expr).getExpression());
    }
    if (expr instanceof org.opensearch.sql.ast.expression.And) {
      return hasQualifiedOrSubqueryRefs(((org.opensearch.sql.ast.expression.And) expr).getLeft())
          || hasQualifiedOrSubqueryRefs(((org.opensearch.sql.ast.expression.And) expr).getRight());
    }
    if (expr instanceof org.opensearch.sql.ast.expression.Or) {
      return hasQualifiedOrSubqueryRefs(((org.opensearch.sql.ast.expression.Or) expr).getLeft())
          || hasQualifiedOrSubqueryRefs(((org.opensearch.sql.ast.expression.Or) expr).getRight());
    }
    if (expr instanceof org.opensearch.sql.ast.expression.Compare) {
      return hasQualifiedOrSubqueryRefs(((org.opensearch.sql.ast.expression.Compare) expr).getLeft())
          || hasQualifiedOrSubqueryRefs(((org.opensearch.sql.ast.expression.Compare) expr).getRight());
    }
    if (expr instanceof Field) {
      return hasQualifiedOrSubqueryRefs(((Field) expr).getField());
    }
    return false;
  }
}
