/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api;

import static org.opensearch.sql.calcite.utils.SqlNodeDSL.*;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.JoinType;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlLiteral;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlOrderBy;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.ast.expression.AggregateFunction;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.Function;
import org.opensearch.sql.ast.expression.Let;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.expression.Span;
import org.opensearch.sql.ast.expression.SpanUnit;
import org.opensearch.sql.calcite.parser.SqlStarExceptReplace;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.ast.tree.AddColTotals;
import org.opensearch.sql.ast.tree.AddTotals;
import org.opensearch.sql.ast.tree.Dedupe;
import org.opensearch.sql.ast.tree.Eval;
import org.opensearch.sql.ast.tree.FillNull;
import org.opensearch.sql.ast.tree.Lookup;
import org.opensearch.sql.ast.tree.Multisearch;
import org.opensearch.sql.ast.tree.MvCombine;
import org.opensearch.sql.ast.tree.MvExpand;
import org.opensearch.sql.ast.tree.Project;
import org.opensearch.sql.ast.tree.Relation;
import org.opensearch.sql.ast.tree.Rename;
import org.opensearch.sql.ast.tree.Replace;
import org.opensearch.sql.ast.tree.ReplacePair;
import org.opensearch.sql.ast.tree.Expand;
import org.opensearch.sql.ast.tree.Flatten;
import org.opensearch.sql.ast.tree.Reverse;
import org.opensearch.sql.ast.tree.Transpose;
import org.opensearch.sql.ast.tree.Trendline;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.calcite.utils.WildcardUtils;
import org.opensearch.sql.utils.WildcardRenameUtils;

/**
 * Extends PPLToSqlNodeConverter with schema-dependent commands that need to resolve column names:
 * fields - (exclude), wildcard rename, fillnull all-fields, eval column override.
 */
public class DynamicPPLToSqlNodeConverter extends PPLToSqlNodeConverter {

  private final SchemaPlus defaultSchema;
  private final RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
  protected String tableName;
  /** Eval aliases being built in the current eval — used by visitFunction to avoid false NULL replacement. */
  private final Set<String> currentEvalAliases = new HashSet<>();
  /** Alias field name → target field name mapping (e.g. @timestamp → created_at). */
  private Map<String, String> aliasFieldMapping = Collections.emptyMap();
  /** When non-null, applyPendingOrderBy will strip _source_order after applying ORDER BY. */
  private List<String> pendingStripSourceOrder;
  /** Maps unqualified field names to their join alias for join condition resolution. */
  private Map<String, String> joinFieldToAlias = Collections.emptyMap();

  public DynamicPPLToSqlNodeConverter(SchemaPlus defaultSchema) {
    this.defaultSchema = defaultSchema;
  }

  private DynamicPPLToSqlNodeConverter(SchemaPlus defaultSchema, java.util.concurrent.atomic.AtomicInteger sharedCounter) {
    super(sharedCounter);
    this.defaultSchema = defaultSchema;
  }

  private List<String> resolveColumns(String tableName) {
    if (tableName == null) return Collections.emptyList();
    Table table = defaultSchema.getTable(tableName);
    if (table == null) {
      // Try case-insensitive lookup via getTableNames
      for (String name : defaultSchema.getTableNames()) {
        if (name.equalsIgnoreCase(tableName)) {
          table = defaultSchema.getTable(name);
          break;
        }
      }
    }
    if (table == null) {
      // SchemaPlus.getTable() may not trigger lazy fetch in AbstractSchema.getTableMap().
      // Access the underlying Schema object directly via CalciteSchema.
      try {
        org.apache.calcite.jdbc.CalciteSchema cs =
            org.apache.calcite.jdbc.CalciteSchema.from(defaultSchema);
        Schema underlying = cs.schema;
        if (underlying != null) {
          table = underlying.getTable(tableName);
        }
      } catch (Exception ignored) {}
    }
    if (table == null) return Collections.emptyList();
    return table.getRowType(typeFactory).getFieldList().stream()
        .map(f -> f.getName())
        .filter(n -> !n.startsWith("_"))
        .collect(Collectors.toList());
  }

  private boolean isArrayField(String fieldName) {
    if (tableName == null) return false;
    Table table = defaultSchema.getTable(tableName);
    if (table == null) {
      for (String name : defaultSchema.getTableNames()) {
        if (name.equalsIgnoreCase(tableName)) { table = defaultSchema.getTable(name); break; }
      }
    }
    if (table == null) {
      try {
        org.apache.calcite.jdbc.CalciteSchema cs =
            org.apache.calcite.jdbc.CalciteSchema.from(defaultSchema);
        Schema underlying = cs.schema;
        if (underlying != null) table = underlying.getTable(tableName);
      } catch (Exception ignored) {}
    }
    if (table == null) return false;
    org.apache.calcite.rel.type.RelDataTypeField f =
        table.getRowType(typeFactory).getField(fieldName, true, false);
    if (f == null) return false;
    org.apache.calcite.rel.type.RelDataType t = f.getType();
    return org.apache.calcite.sql.type.SqlTypeUtil.isArray(t)
        || org.apache.calcite.sql.type.SqlTypeUtil.isMultiset(t);
  }

  private static String fieldName(UnresolvedExpression expr) {
    if (expr instanceof Field) return ((Field) expr).getField().toString();
    if (expr instanceof QualifiedName) return ((QualifiedName) expr).toString();
    return expr.toString();
  }

  @Override
  public SqlNode visitRelation(Relation node, Void ctx) {
    this.tableName = node.getTableQualifiedName().toString();
    this.aliasFieldMapping = resolveAliasMapping(this.tableName);
    return super.visitRelation(node, ctx);
  }

  /**
   * Build a mapping of alias field names to their target field names by inspecting the table's
   * field types. OpenSearch alias fields have getOriginalPath() set to the target field path.
   */
  private Map<String, String> resolveAliasMapping(String tableName) {
    if (tableName == null) return Collections.emptyMap();
    Table table = resolveTable(tableName);
    if (table == null) return Collections.emptyMap();
    if (!(table instanceof org.opensearch.sql.storage.Table)) return Collections.emptyMap();
    Map<String, org.opensearch.sql.data.type.ExprType> fieldTypes =
        ((org.opensearch.sql.storage.Table) table).getFieldTypes();
    Map<String, String> mapping = new LinkedHashMap<>();
    for (Map.Entry<String, org.opensearch.sql.data.type.ExprType> entry : fieldTypes.entrySet()) {
      if (entry.getValue().getOriginalPath().isPresent()) {
        mapping.put(entry.getKey(), entry.getValue().getOriginalPath().get());
      }
    }
    return mapping;
  }

  @Override
  public SqlNode visitQualifiedName(org.opensearch.sql.ast.expression.QualifiedName node, Void ctx) {
    List<String> parts = node.getParts();
    // Resolve alias fields: replace alias field name with target field name
    if (parts.size() == 1 && aliasFieldMapping.containsKey(parts.get(0))) {
      return identifier(aliasFieldMapping.get(parts.get(0)));
    }
    // Qualify unqualified field names in join conditions with their table alias
    if (parts.size() == 1 && joinFieldToAlias.containsKey(parts.get(0))) {
      return identifier(joinFieldToAlias.get(parts.get(0)), parts.get(0));
    }
    // For multi-part names like skills.name, check if the root is a nested/MAP column
    if (parts.size() >= 2 && tableName != null && isArrayField(parts.get(0))) {
      // Root is a nested column; convert to ITEM chain: ITEM(skills, 'name')
      SqlNode result = identifier(parts.get(0));
      for (int i = 1; i < parts.size(); i++) {
        result = new SqlBasicCall(SqlStdOperatorTable.ITEM,
            new SqlNode[]{result, literal(parts.get(i))}, SqlParserPos.ZERO);
      }
      return result;
    }
    return super.visitQualifiedName(node, ctx);
  }

  @Override
  public SqlNode visitProject(Project node, Void ctx) {
    if (node.isExcluded()) {
      List<String> excludePatterns = node.getProjectList().stream()
          .map(e -> ((Field) e).getField().toString())
          .collect(Collectors.toList());
      List<String> pipeCols = extractPipeColumns();
      List<String> allCols = pipeCols.isEmpty() ? resolveColumns(tableName) : pipeCols;
      // Build compiled regex patterns for wildcard exclusions
      List<java.util.regex.Pattern> regexPatterns = excludePatterns.stream()
          .map(p -> java.util.regex.Pattern.compile("^" + p.replace("*", ".*") + "$"))
          .collect(Collectors.toList());
      SqlNode[] cols = allCols.stream()
          .filter(c -> regexPatterns.stream().noneMatch(p -> p.matcher(c).matches()))
          .map(c -> identifier(c))
          .toArray(SqlNode[]::new);
      pipe = select(cols).from(wrapAsSubquery()).build();
      return pipe;
    }
    // Check for wildcard patterns in field names
    boolean hasWildcard = false;
    for (UnresolvedExpression expr : node.getProjectList()) {
      if (expr instanceof Field) {
        String name = ((Field) expr).getField().toString();
        if (name.contains("*")) {
          hasWildcard = true;
          break;
        }
      }
    }
    if (hasWildcard) {
      List<String> allCols = resolveColumns(tableName);
      LinkedHashSet<String> seen = new LinkedHashSet<>();
      List<SqlNode> expanded = new ArrayList<>();
      for (UnresolvedExpression expr : node.getProjectList()) {
        if (expr instanceof Field) {
          String name = ((Field) expr).getField().toString();
          if (name.contains("*")) {
            String regex = "^" + name.replace("*", ".*") + "$";
            java.util.regex.Pattern pattern = java.util.regex.Pattern.compile(regex);
            boolean anyMatch = allCols.stream().anyMatch(c -> pattern.matcher(c).matches());
            if (!anyMatch) {
              throw new IllegalArgumentException(
                  "wildcard pattern [" + name + "] matches no fields");
            }
            for (String col : allCols) {
              if (pattern.matcher(col).matches() && seen.add(col)) {
                expanded.add(identifier(col));
              }
            }
            continue;
          }
          if (seen.add(name)) {
            expanded.add(expr.accept(this, null));
          }
          continue;
        }
        expanded.add(expr.accept(this, null));
      }
      pipe = select(expanded.toArray(new SqlNode[0])).from(wrapAsSubquery()).build();
      return pipe;
    }
    // Check if any field is a nested field access (e.g., skills.name) that needs ITEM aliasing
    boolean hasNestedAccess = false;
    for (UnresolvedExpression expr : node.getProjectList()) {
      if (expr instanceof Field) {
        String name = ((Field) expr).getField().toString();
        if (name.contains(".")) {
          String root = name.substring(0, name.indexOf('.'));
          if (isArrayField(root)) { hasNestedAccess = true; break; }
        }
      }
    }
    if (!hasNestedAccess) return super.visitProject(node, ctx);

    // Handle nested field access: alias ITEM expressions with the dotted name
    List<SqlNode> colList = new ArrayList<>();
    for (UnresolvedExpression expr : node.getProjectList()) {
      SqlNode col = expr.accept(this, null);
      if (expr instanceof Field) {
        String name = ((Field) expr).getField().toString();
        if (name.contains(".") && col instanceof SqlBasicCall
            && "ITEM".equals(((SqlBasicCall) col).getOperator().getName())) {
          colList.add(as(col, name));
          continue;
        }
      }
      colList.add(col);
    }
    pipe = select(colList.toArray(new SqlNode[0])).from(wrapAsSubquery()).build();
    return pipe;
  }

  @Override
  public SqlNode visitRename(Rename node, Void ctx) {
    // Try to get columns from the pipe's SELECT list first (handles narrowed pipes after fields)
    List<String> pipeCols = extractPipeColumns();
    List<String> allCols = pipeCols.isEmpty() ? resolveColumns(tableName) : pipeCols;
    if (allCols.isEmpty()) {
      return super.visitRename(node, ctx);
    }

    // Check for wildcard patterns
    boolean hasWildcard = false;
    for (org.opensearch.sql.ast.expression.Map mapping : node.getRenameList()) {
      if (WildcardRenameUtils.isWildcardPattern(fieldName(mapping.getOrigin()))) {
        hasWildcard = true;
        break;
      }
    }

    LinkedHashMap<String, String> mappings = new LinkedHashMap<>();
    if (hasWildcard) {
      // For wildcard expansion, use table columns if pipe columns are just *
      List<String> expandCols = new ArrayList<>(pipeCols.isEmpty() ? resolveColumns(tableName) : pipeCols);
      if (expandCols.isEmpty()) {
        return super.visitRename(node, ctx);
      }
      for (org.opensearch.sql.ast.expression.Map mapping : node.getRenameList()) {
        String sourcePattern = fieldName(mapping.getOrigin());
        String targetPattern = fieldName(mapping.getTarget());
        if (WildcardRenameUtils.isWildcardPattern(sourcePattern)) {
          List<String> matchingFields = WildcardRenameUtils.matchFieldNames(sourcePattern, expandCols);
          for (String fld : matchingFields) {
            String newName = WildcardRenameUtils.applyWildcardTransformation(
                sourcePattern, targetPattern, fld);
            // Track the original→final mapping through chains
            String origField = fld;
            for (Map.Entry<String, String> prev : mappings.entrySet()) {
              if (prev.getValue().equals(fld)) { origField = prev.getKey(); break; }
            }
            if (!origField.equals(fld)) {
              mappings.remove(origField);
            }
            mappings.put(origField.equals(fld) ? fld : origField, newName);
            // Update expandCols to reflect this rename for subsequent mappings
            int idx = expandCols.indexOf(fld);
            if (idx >= 0) expandCols.set(idx, newName);
          }
        } else {
          mappings.put(sourcePattern, targetPattern);
          int idx = expandCols.indexOf(sourcePattern);
          if (idx >= 0) expandCols.set(idx, targetPattern);
        }
      }
    } else {
      for (org.opensearch.sql.ast.expression.Map mapping : node.getRenameList()) {
        String origin = fieldName(mapping.getOrigin());
        String target = fieldName(mapping.getTarget());
        String realOrigin = null;
        for (Map.Entry<String, String> prev : mappings.entrySet()) {
          if (prev.getValue().equals(origin)) { realOrigin = prev.getKey(); break; }
        }
        if (realOrigin != null) mappings.put(realOrigin, target);
        else mappings.put(origin, target);
      }
    }
    mappings.entrySet().removeIf(e -> e.getKey().equals(e.getValue()));
    if (mappings.isEmpty()) return pipe;

    // Build explicit SELECT list to preserve column order
    pipe = wrapAsSubquery();
    Set<String> renamedTargets = new HashSet<>(mappings.values());
    List<SqlNode> selectItems = new ArrayList<>();
    for (String col : allCols) {
      if (mappings.containsKey(col)) {
        selectItems.add(as(identifier(col), mappings.get(col)));
      } else if (renamedTargets.contains(col)) {
        continue;
      } else {
        selectItems.add(identifier(col));
      }
    }
    pipe = select(selectItems.toArray(new SqlNode[0])).from(pipe).build();
    return pipe;
  }

  /** Extract column names from the current pipe's SELECT list. Returns empty if pipe is not a SELECT or uses *. */
  private List<String> extractPipeColumns() {
    SqlNode target = pipe;
    // Unwrap SqlOrderBy to get underlying SqlSelect
    if (target instanceof org.apache.calcite.sql.SqlOrderBy) {
      target = ((org.apache.calcite.sql.SqlOrderBy) target).query;
    }
    if (!(target instanceof org.apache.calcite.sql.SqlSelect)) return Collections.emptyList();
    org.apache.calcite.sql.SqlSelect sel = (org.apache.calcite.sql.SqlSelect) target;
    SqlNodeList selectList = sel.getSelectList();
    if (selectList == null) return Collections.emptyList();
    List<String> cols = new ArrayList<>();
    for (SqlNode item : selectList) {
      String name = extractColumnName(item);
      if (name == null) return Collections.emptyList(); // has * or unresolvable
      cols.add(name);
    }
    return cols;
  }

  /** Resolve table columns plus any extra aliased columns from the pipe's SELECT list (e.g. from eval). */
  private List<String> resolveColumnsWithPipeExtras() {
    List<String> cols = new ArrayList<>(resolveColumns(tableName));
    SqlNode target = pipe;
    if (target instanceof org.apache.calcite.sql.SqlOrderBy) {
      target = ((org.apache.calcite.sql.SqlOrderBy) target).query;
    }
    if (target instanceof org.apache.calcite.sql.SqlSelect) {
      SqlNodeList selectList = ((org.apache.calcite.sql.SqlSelect) target).getSelectList();
      if (selectList != null) {
        Set<String> existing = new LinkedHashSet<>(cols);
        for (SqlNode item : selectList) {
          String name = extractColumnName(item);
          if (name != null && !existing.contains(name)) {
            cols.add(name);
          }
        }
      }
    }
    return cols;
  }

  @Override
  public SqlNode visitReplace(Replace node, Void ctx) {
    // Check if pipe has been narrowed (e.g. by fields command)
    List<String> pipeCols = extractPipeColumns();
    if (!pipeCols.isEmpty()) {
      // Pipe is narrowed — use explicit column list
      pipe = wrapAsSubquery();
      Map<String, SqlNode> replaceExprs = new LinkedHashMap<>();
      for (Field field : node.getFieldList()) {
        String fieldName = field.getField().toString();
        SqlNode expr = identifier(fieldName);
        for (ReplacePair pair : node.getReplacePairs()) {
          expr = buildReplacePairExpr(expr, pair);
        }
        replaceExprs.put(fieldName, expr);
      }
      List<SqlNode> selectItems = new ArrayList<>();
      for (String col : pipeCols) {
        if (replaceExprs.containsKey(col)) {
          selectItems.add(as(replaceExprs.get(col), col));
        } else {
          selectItems.add(identifier(col));
        }
      }
      pipe = select(selectItems.toArray(new SqlNode[0])).from(pipe).build();
      return pipe;
    }
    // Check if any replace target field is NOT in the table schema (e.g. eval-created)
    List<String> schemaCols = resolveColumns(tableName);
    Set<String> schemaColSet = new LinkedHashSet<>(schemaCols);
    boolean allInSchema = true;
    for (Field field : node.getFieldList()) {
      if (!schemaColSet.contains(field.getField().toString())) {
        allInSchema = false;
        break;
      }
    }
    if (!allInSchema) {
      // Some replace targets are eval-created — use explicit column enumeration
      List<String> allCols = resolveColumnsWithPipeExtras();
      pipe = wrapAsSubquery();
      Map<String, SqlNode> replaceExprs = new LinkedHashMap<>();
      for (Field field : node.getFieldList()) {
        String fieldName = field.getField().toString();
        SqlNode expr = identifier(fieldName);
        for (ReplacePair pair : node.getReplacePairs()) {
          expr = buildReplacePairExpr(expr, pair);
        }
        replaceExprs.put(fieldName, expr);
      }
      List<SqlNode> selectItems = new ArrayList<>();
      for (String col : allCols) {
        if (replaceExprs.containsKey(col)) {
          selectItems.add(as(replaceExprs.get(col), col));
        } else {
          selectItems.add(identifier(col));
        }
      }
      pipe = select(selectItems.toArray(new SqlNode[0])).from(pipe).build();
      return pipe;
    }
    // Pipe is not narrowed and all targets in schema — use SqlStarExceptReplace with REPLACE
    pipe = wrapAsSubquery();
    List<SqlNode> replaceClauses = new ArrayList<>();
    for (Field field : node.getFieldList()) {
      String fieldName = field.getField().toString();
      SqlNode expr = identifier(fieldName);
      for (ReplacePair pair : node.getReplacePairs()) {
        expr = buildReplacePairExpr(expr, pair);
      }
      replaceClauses.add(as(expr, fieldName));
    }
    SqlNode starReplace = new SqlStarExceptReplace(
        SqlParserPos.ZERO, star(), null,
        new SqlNodeList(replaceClauses, SqlParserPos.ZERO));
    pipe = select(starReplace).from(pipe).build();
    return pipe;
  }

  @Override
  public SqlNode visitFillNull(FillNull node, Void ctx) {
    if (node.getReplacementForAll().isPresent()) {
      // All-fields fillnull needs to know ALL column names
      List<String> pipeCols = extractPipeColumns();
      List<String> allCols = pipeCols.isEmpty() ? resolveColumns(tableName) : pipeCols;
      if (allCols.isEmpty()) {
        return super.visitFillNull(node, ctx);
      }
      pipe = wrapAsSubquery();
      SqlNode value = node.getReplacementForAll().get().accept(this, null);
      List<SqlNode> replaceClauses = new ArrayList<>();
      for (String col : allCols) {
        replaceClauses.add(as(call("COALESCE", identifier(col), value), col));
      }
      if (!pipeCols.isEmpty()) {
        // Pipe is narrowed — use explicit column list
        pipe = select(replaceClauses.toArray(new SqlNode[0])).from(pipe).build();
      } else {
        SqlNode starReplace = new SqlStarExceptReplace(
            SqlParserPos.ZERO, star(), null,
            new SqlNodeList(replaceClauses, SqlParserPos.ZERO));
        pipe = select(starReplace).from(pipe).build();
      }
      return pipe;
    }
    // Per-field fillnull
    List<org.apache.commons.lang3.tuple.Pair<Field, UnresolvedExpression>> pairs =
        node.getReplacementPairs();
    if (!pairs.isEmpty()) {
      // Check if pipe has been narrowed (e.g. by fields command)
      List<String> pipeCols = extractPipeColumns();
      if (!pipeCols.isEmpty()) {
        // Pipe is narrowed — use explicit column list to avoid rewriter issues
        pipe = wrapAsSubquery();
        Map<String, SqlNode> fillExprs = new LinkedHashMap<>();
        for (org.apache.commons.lang3.tuple.Pair<Field, UnresolvedExpression> pair : pairs) {
          String fieldName = pair.getLeft().getField().toString();
          SqlNode value = pair.getRight().accept(this, null);
          fillExprs.put(fieldName, call("COALESCE", identifier(fieldName), value));
        }
        List<SqlNode> selectItems = new ArrayList<>();
        for (String col : pipeCols) {
          if (fillExprs.containsKey(col)) {
            selectItems.add(as(fillExprs.get(col), col));
          } else {
            selectItems.add(identifier(col));
          }
        }
        pipe = select(selectItems.toArray(new SqlNode[0])).from(pipe).build();
        return pipe;
      }
      // Pipe is not narrowed — use SqlStarExceptReplace with REPLACE
      pipe = wrapAsSubquery();
      List<SqlNode> replaceClauses = new ArrayList<>();
      for (org.apache.commons.lang3.tuple.Pair<Field, UnresolvedExpression> pair : pairs) {
        String fieldName = pair.getLeft().getField().toString();
        SqlNode value = pair.getRight().accept(this, null);
        replaceClauses.add(as(call("COALESCE", identifier(fieldName), value), fieldName));
      }
      SqlNode starReplace = new SqlStarExceptReplace(
          SqlParserPos.ZERO, star(), null,
          new SqlNodeList(replaceClauses, SqlParserPos.ZERO));
      pipe = select(starReplace).from(pipe).build();
      return pipe;
    }
    return pipe;
  }

  @Override
  public SqlNode visitEval(Eval node, Void ctx) {
    // Track eval aliases so visitFunction won't replace them with NULL
    for (Let let : node.getExpressionList()) {
      currentEvalAliases.add(let.getVar().getField().toString());
    }
    try {
      return visitEvalInternal(node, ctx);
    } finally {
      currentEvalAliases.clear();
    }
  }

  private SqlNode visitEvalInternal(Eval node, Void ctx) {
    List<String> allCols = resolveColumns(tableName);
    if (allCols.isEmpty()) {
      // No schema columns available — delegate to base which handles
      // in-place SELECT list replacement for overrides
      return super.visitEval(node, ctx);
    }

    Set<String> existingCols = new HashSet<>(allCols);
    boolean hasOverride = false;
    for (Let let : node.getExpressionList()) {
      if (existingCols.contains(let.getVar().getField().toString())) {
        hasOverride = true;
        break;
      }
    }
    if (!hasOverride) return super.visitEval(node, ctx);

    Map<String, SqlNode> evalAliases = new LinkedHashMap<>();
    Map<String, SqlNode> overrides = new LinkedHashMap<>();
    List<String> newCols = new ArrayList<>();
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
      // Handle fieldformat prefix/suffix concatenation
      expr = applyLetConcatPrefixSuffix(let, expr);
      evalAliases.put(varName, expr);
      if (existingCols.contains(varName)) {
        overrides.put(varName, expr);
      } else {
        newCols.add(varName);
      }
    }

    // Use SELECT * REPLACE(...) to avoid column ambiguity when eval
    // expression references the same column it overrides (e.g. eval date=WEEKDAY(date))
    List<SqlNode> replaceClauses = new ArrayList<>();
    for (Map.Entry<String, SqlNode> entry : overrides.entrySet()) {
      replaceClauses.add(as(entry.getValue(), entry.getKey()));
    }
    SqlNode starReplace = new SqlStarExceptReplace(
        SqlParserPos.ZERO, star(), null,
        new SqlNodeList(replaceClauses, SqlParserPos.ZERO));

    List<SqlNode> selectItems = new ArrayList<>();
    selectItems.add(starReplace);
    for (String col : newCols) {
      selectItems.add(as(evalAliases.get(col), col));
    }
    pipe = select(selectItems.toArray(new SqlNode[0])).from(wrapAsSubquery()).build();
    return pipe;
  }

  @Override
  public SqlNode visitTrendline(Trendline node, Void ctx) {
    // Call base to build window expressions and set pipe with SELECT *, expr AS alias
    super.visitTrendline(node, ctx);

    // Check if any trendline alias matches an existing column — if so, use REPLACE
    Set<String> existingCols = new HashSet<>(resolveColumns(tableName));
    List<String> overrideCols = new ArrayList<>();
    for (Trendline.TrendlineComputation comp : node.getComputations()) {
      if (existingCols.contains(comp.getAlias())) {
        overrideCols.add(comp.getAlias());
      }
    }
    if (overrideCols.isEmpty()) return pipe;

    // Rebuild: use SqlStarExceptReplace for overrides, keep new columns as-is
    // Extract the window expressions from the current pipe's SELECT list
    org.apache.calcite.sql.SqlSelect sel = (org.apache.calcite.sql.SqlSelect) pipe;
    SqlNodeList selectList = sel.getSelectList();
    // selectList is: [*, expr1 AS alias1, expr2 AS alias2, ...]
    List<SqlNode> replaceClauses = new ArrayList<>();
    List<SqlNode> newColExprs = new ArrayList<>();
    for (int i = 1; i < selectList.size(); i++) {
      SqlNode item = selectList.get(i);
      String name = extractColumnName(item);
      if (name != null && existingCols.contains(name)) {
        replaceClauses.add(item);
      } else {
        newColExprs.add(item);
      }
    }
    SqlNode starReplace = new SqlStarExceptReplace(
        SqlParserPos.ZERO, star(), null,
        new SqlNodeList(replaceClauses, SqlParserPos.ZERO));
    List<SqlNode> newSelectItems = new ArrayList<>();
    newSelectItems.add(starReplace);
    newSelectItems.addAll(newColExprs);
    sel.setSelectList(new SqlNodeList(newSelectItems, SqlParserPos.ZERO));
    return pipe;
  }

  @Override
  public SqlNode visitReverse(Reverse node, Void ctx) {
    // Capture column names before adding ROW_NUMBER
    List<String> cols = extractPipeColumns();
    if (cols.isEmpty()) {
      List<String> resolved = resolveColumns(tableName);
      cols = resolved.stream().filter(n -> !n.startsWith("_")).collect(Collectors.toList());
    }

    // If pending ORDER BY is from a prior reverse (__reverse_row_num__),
    // flip it to ASC (canceling the reverse) instead of adding another ROW_NUMBER
    if (pendingOrderBy != null && pendingOrderBy.size() == 1) {
      String pendingStr = pendingOrderBy.get(0).toString();
      if (pendingStr.contains("__reverse_row_num__")) {
        // Double reverse = cancel. Just flip DESC to ASC.
        pendingOrderBy = List.of(identifier("__reverse_row_num__"));
        return pipe;
      }
    }

    // Build ROW_NUMBER window ORDER BY from pending sort order (if any)
    SqlNodeList winOrderBy = SqlNodeList.EMPTY;
    if (pendingOrderBy != null) {
      winOrderBy = new SqlNodeList(pendingOrderBy, SqlParserPos.ZERO);
      pendingOrderBy = null;
    }
    // Apply any pending FETCH before ROW_NUMBER
    if (pendingFetch != null) {
      pipe = applyPendingOrderBy(pipe);
    }

    // Step 1: add ROW_NUMBER() OVER(ORDER BY ...) as __reverse_row_num__
    String rnCol = "__reverse_row_num__";
    SqlNode rowNum = as(
        window(new SqlBasicCall(SqlStdOperatorTable.ROW_NUMBER, new SqlNode[0], SqlParserPos.ZERO),
            SqlNodeList.EMPTY, winOrderBy), rnCol);
    pipe = select(star(), rowNum).from(wrapAsSubquery()).build();

    // Step 2: project only original columns, defer ORDER BY __reverse_row_num__ DESC
    pipe = wrapAsSubquery();
    List<SqlNode> items = cols.stream()
        .map(c -> (SqlNode) identifier(c))
        .collect(Collectors.toList());
    if (items.isEmpty()) {
      items = List.of(star());
    }
    pipe = select(items.toArray(new SqlNode[0])).from(pipe).build();
    pendingOrderBy = List.of(desc(identifier(rnCol)));
    return pipe;
  }

  @Override
  public SqlNode visitFlatten(Flatten node, Void ctx) {
    String fieldName = node.getField().getField().toString();
    List<String> allCols = resolveColumns(tableName);
    // Filter out metadata fields
    allCols = allCols.stream().filter(n -> !n.startsWith("_")).collect(Collectors.toList());

    // Sub-fields of the flattened field, sorted alphabetically
    List<String> subFields = allCols.stream()
        .filter(c -> c.startsWith(fieldName + "."))
        .sorted()
        .collect(Collectors.toList());

    // Top-level columns: those without dots (exclude ALL nested sub-fields), sorted alphabetically
    List<String> topLevelCols = allCols.stream()
        .filter(c -> !c.contains("."))
        .sorted()
        .collect(Collectors.toList());

    List<String> aliases = node.getAliases();
    List<String> outputNames;
    if (aliases != null && !aliases.isEmpty()) {
      if (aliases.size() != subFields.size()) {
        throw new IllegalArgumentException(
            String.format(
                "The number of aliases has to match the number of flattened fields."
                    + " Expected %d (%s), got %d (%s)",
                subFields.size(), String.join(", ", subFields),
                aliases.size(), String.join(", ", aliases)));
      }
      outputNames = aliases;
    } else {
      outputNames = subFields.stream()
          .map(f -> f.substring(fieldName.length() + 1))
          .collect(Collectors.toList());
    }

    SqlNode sub = wrapAsSubquery();
    List<SqlNode> selectItems = new ArrayList<>();
    // Add all top-level columns EXCEPT the flattened field first
    for (String col : topLevelCols) {
      if (!col.equals(fieldName)) {
        selectItems.add(identifier(col));
      }
    }
    // Then add the flattened field and its sub-fields at the end
    selectItems.add(identifier(fieldName));
    for (int i = 0; i < subFields.size(); i++) {
      selectItems.add(as(identifier(subFields.get(i)), outputNames.get(i)));
    }
    pipe = select(selectItems.toArray(new SqlNode[0])).from(sub).build();
    return pipe;
  }

  @Override
  public SqlNode visitMvCombine(MvCombine node, Void ctx) {
    String fieldName = node.getField().getField().toString();

    // Prefer pipe columns (handles narrowed pipes after fields command)
    List<String> allCols = extractPipeColumns();
    if (allCols.isEmpty()) {
      allCols = new ArrayList<>(resolveColumns(tableName));
    }

    // Validate field exists
    if (!allCols.isEmpty() && !allCols.contains(fieldName)) {
      throw new IllegalArgumentException("Field [" + fieldName + "] not found.");
    }

    // Fallback: extract columns from the current pipe's SELECT list
    if (allCols.isEmpty() && pipe instanceof org.apache.calcite.sql.SqlSelect) {
      org.apache.calcite.sql.SqlSelect sel = (org.apache.calcite.sql.SqlSelect) pipe;
      SqlNodeList selectList = sel.getSelectList();
      if (selectList != null) {
        allCols = new ArrayList<>();
        for (SqlNode item : selectList) {
          String colName = extractColumnName(item);
          if (colName != null) allCols.add(colName);
        }
      }
    }

    // GROUP BY all columns except the target field
    List<SqlNode> groupByCols = new ArrayList<>();
    List<SqlNode> selectItems = new ArrayList<>();
    for (String col : allCols) {
      if (col.equals(fieldName)) {
        selectItems.add(as(call("ARRAY_AGG", identifier(col)), col));
      } else {
        selectItems.add(identifier(col));
        groupByCols.add(identifier(col));
      }
    }

    if (selectItems.isEmpty()) {
      // Last resort: just ARRAY_AGG the target field
      selectItems.add(as(call("ARRAY_AGG", identifier(fieldName)), fieldName));
      pipe = select(selectItems.toArray(new SqlNode[0])).from(wrapAsSubquery()).build();
    } else if (groupByCols.isEmpty()) {
      pipe = select(selectItems.toArray(new SqlNode[0])).from(wrapAsSubquery()).build();
    } else {
      pipe = select(selectItems.toArray(new SqlNode[0]))
          .from(wrapAsSubquery())
          .groupBy(groupByCols.toArray(new SqlNode[0]))
          .build();
    }
    return pipe;
  }

  private String extractColumnName(SqlNode node) {
    if (node instanceof org.apache.calcite.sql.SqlIdentifier) {
      org.apache.calcite.sql.SqlIdentifier id = (org.apache.calcite.sql.SqlIdentifier) node;
      if (id.isStar()) return null; // Skip bare * — not a real column name
      return id.names.get(id.names.size() - 1);
    }
    if (node instanceof org.apache.calcite.sql.SqlBasicCall) {
      org.apache.calcite.sql.SqlBasicCall call = (org.apache.calcite.sql.SqlBasicCall) node;
      if ("AS".equals(call.getOperator().getName()) && call.operandCount() >= 2) {
        return extractColumnName(call.operand(1));
      }
    }
    return null;
  }

  @Override
  public SqlNode visitJoin(org.opensearch.sql.ast.tree.Join node, Void ctx) {
    boolean hasFieldList = node.getJoinFields().isPresent() && !node.getJoinFields().get().isEmpty();
    boolean hasCondition = node.getJoinCondition().isPresent();
    boolean isSelfJoin = isSelfJoinDetected(node);

    // Resolve unqualified field names in join condition to their table aliases
    if (hasCondition) {
      String leftTable = extractTableNameFromPlan(node.getLeft());
      String rightTable = extractTableNameFromPlan(node.getRight());
      String leftAlias0 = node.getLeftAlias().orElse(leftTable);
      String rightAlias0 = node.getRightAlias().orElse(rightTable);
      Map<String, String> fieldMap = new LinkedHashMap<>();
      for (String col : resolveColumns(leftTable)) fieldMap.put(col, leftAlias0);
      for (String col : resolveColumns(rightTable)) {
        if (fieldMap.containsKey(col)) fieldMap.remove(col); // ambiguous — leave unqualified
        else fieldMap.put(col, rightAlias0);
      }
      joinFieldToAlias = fieldMap;
    }

    // Delegate to base for the join construction
    super.visitJoin(node, ctx);
    joinFieldToAlias = Collections.emptyMap();

    // Fix 3: For no-field-list self-join, replace CROSS JOIN with natural join on all columns
    if (isSelfJoin && !hasFieldList && !hasCondition && pipe instanceof SqlJoin) {
      SqlJoin sqlJoin = (SqlJoin) pipe;
      String leftAlias = node.getLeftAlias().orElse(null);
      String rightAlias = node.getRightAlias().orElse(null);
      if (leftAlias == null) leftAlias = extractAlias(sqlJoin.getLeft());
      if (rightAlias == null) rightAlias = extractAlias(sqlJoin.getRight());
      String rightTable = extractTableNameFromPlan(node.getRight());
      List<String> cols = resolveColumns(rightTable);
      if (!cols.isEmpty() && leftAlias != null && rightAlias != null) {
        // Build ON condition for all common columns
        SqlNode condition = null;
        for (String col : cols) {
          SqlNode eqNode = eq(identifier(leftAlias, col), identifier(rightAlias, col));
          condition = condition == null ? eqNode : and(condition, eqNode);
        }
        pipe = join(sqlJoin.getLeft(), JoinType.INNER, sqlJoin.getRight(), condition);
        skipNextWrap = true;
      }
    }

    // Determine if max option was applied (adds _rn column to right side)
    org.opensearch.sql.ast.expression.Literal maxLit = node.getArgumentMap().get("max");
    boolean maxApplied = maxLit != null && !maxLit.equals(org.opensearch.sql.ast.expression.Literal.ZERO);

    // Handle column deduplication for field-list joins and self-joins
    if (hasFieldList || isSelfJoin) {
      String leftAlias = node.getLeftAlias().orElse(null);
      String rightAlias = node.getRightAlias().orElse(null);
      if (pipe instanceof SqlJoin) {
        SqlJoin sqlJoin = (SqlJoin) pipe;
        if (leftAlias == null) leftAlias = extractAlias(sqlJoin.getLeft());
        if (rightAlias == null) rightAlias = extractAlias(sqlJoin.getRight());
      }

      if (isSelfJoin && leftAlias != null && rightAlias != null) {
        // Self-join: select from one side to avoid duplicate columns
        org.opensearch.sql.ast.expression.Literal overwriteLit = node.getArgumentMap().get("overwrite");
        boolean overwrite = overwriteLit == null || Boolean.TRUE.equals(overwriteLit.getValue());
        String ref = overwrite ? rightAlias : leftAlias;
        if (maxApplied) {
          // Explicit column list to exclude _rn
          String rightTable = extractTableNameFromPlan(node.getRight());
          List<String> cols = resolveColumns(rightTable);
          if (!cols.isEmpty()) {
            Set<String> emitted = new HashSet<>(cols);
            List<SqlNode> selectItems = cols.stream()
                .map(col -> (SqlNode) as(identifier(ref, col), col))
                .collect(Collectors.toCollection(ArrayList::new));
            // Include computed columns from join field list not in physical schema
            if (hasFieldList) {
              for (Field f : node.getJoinFields().get()) {
                String col = f.getField().toString();
                if (!emitted.contains(col)) {
                  selectItems.add(as(identifier(ref, col), col));
                  emitted.add(col);
                }
              }
            }
            pipe = select(selectItems.toArray(new SqlNode[0])).from(pipe).build();
          } else {
            SqlNode starRef = new SqlIdentifier(java.util.Arrays.asList(ref, ""), SqlParserPos.ZERO);
            pipe = select(starRef).from(pipe).build();
          }
        } else {
          SqlNode starRef = new SqlIdentifier(java.util.Arrays.asList(ref, ""), SqlParserPos.ZERO);
          pipe = select(starRef).from(pipe).build();
        }
        skipNextWrap = true;
      } else if (hasFieldList && leftAlias != null && rightAlias != null) {
        // Fix 2: Expand columns explicitly instead of using SqlStarExceptReplace
        List<String> sharedFields = node.getJoinFields().get().stream()
            .map(f -> f.getField().toString()).collect(Collectors.toList());
        Set<String> sharedSet = new HashSet<>(sharedFields);
        org.opensearch.sql.ast.expression.Literal overwriteLit = node.getArgumentMap().get("overwrite");
        boolean overwrite = overwriteLit == null || Boolean.TRUE.equals(overwriteLit.getValue());

        String leftTable = extractTableNameFromPlan(node.getLeft());
        String rightTable = extractTableNameFromPlan(node.getRight());
        List<String> leftCols = resolveColumns(leftTable);
        List<String> rightCols = resolveColumns(rightTable);

        if (!leftCols.isEmpty() && !rightCols.isEmpty()) {
          List<SqlNode> selectItems = new ArrayList<>();
          Set<String> emitted = new HashSet<>();
          // Left side: all columns, with REPLACE for shared cols if overwrite
          for (String col : leftCols) {
            if (overwrite && sharedSet.contains(col)) {
              selectItems.add(as(identifier(rightAlias, col), col));
            } else {
              selectItems.add(as(identifier(leftAlias, col), col));
            }
            emitted.add(col);
          }
          // Right side: only columns not already in left side (exclude _rn from max option)
          for (String col : rightCols) {
            if (!emitted.contains(col) && !JOIN_MAX_RN_COLUMN.equals(col)) {
              selectItems.add(as(identifier(rightAlias, col), col));
              emitted.add(col);
            }
          }
          // Include computed columns from join field list not in physical schema
          for (String col : sharedFields) {
            if (!emitted.contains(col)) {
              selectItems.add(as(identifier(overwrite ? rightAlias : leftAlias, col), col));
              emitted.add(col);
            }
          }
          pipe = select(selectItems.toArray(new SqlNode[0])).from(pipe).build();
          skipNextWrap = true;
        }
      }
    } else if (maxApplied && hasCondition) {
      // Criteria join with max: need to exclude _rn from right side
      String leftAlias = node.getLeftAlias().orElse(null);
      String rightAlias = node.getRightAlias().orElse(null);
      if (pipe instanceof SqlJoin) {
        SqlJoin sqlJoin = (SqlJoin) pipe;
        if (leftAlias == null) leftAlias = extractAlias(sqlJoin.getLeft());
        if (rightAlias == null) rightAlias = extractAlias(sqlJoin.getRight());
      }
      if (leftAlias != null && rightAlias != null) {
        String leftTable = extractTableNameFromPlan(node.getLeft());
        String rightTable = extractTableNameFromPlan(node.getRight());
        List<String> leftCols = resolveColumns(leftTable);
        List<String> rightCols = resolveColumns(rightTable);
        if (!leftCols.isEmpty() && !rightCols.isEmpty()) {
          List<SqlNode> selectItems = new ArrayList<>();
          for (String col : leftCols) {
            selectItems.add(as(identifier(leftAlias, col), col));
          }
          // Right side: rename conflicting columns, exclude _rn
          for (String col : rightCols) {
            if (JOIN_MAX_RN_COLUMN.equals(col)) continue;
            if (leftCols.contains(col)) {
              selectItems.add(as(identifier(rightAlias, col), rightAlias + "." + col));
            } else {
              selectItems.add(as(identifier(rightAlias, col), col));
            }
          }
          pipe = select(selectItems.toArray(new SqlNode[0])).from(pipe).build();
          skipNextWrap = true;
        }
      }
    }
    return pipe;
  }

  /** Detect self-join: left and right reference the same table without explicit aliases. */
  private boolean isSelfJoinDetected(org.opensearch.sql.ast.tree.Join node) {
    String leftTable = extractTableNameFromPlan(node.getLeft());
    String rightTable = extractTableNameFromPlan(node.getRight());
    return leftTable != null && leftTable.equals(rightTable);
  }

  private static String extractTableNameFromPlan(org.opensearch.sql.ast.tree.UnresolvedPlan plan) {
    // Traverse through SubqueryAlias, Project, Eval, Filter etc. to find the base Relation
    while (plan != null) {
      if (plan instanceof org.opensearch.sql.ast.tree.SubqueryAlias) {
        plan = (org.opensearch.sql.ast.tree.UnresolvedPlan) plan.getChild().get(0);
        continue;
      }
      Relation rel = extractRelation(plan);
      if (rel != null) return rel.getTableQualifiedName().toString();
      // Traverse into child nodes (Project, Eval, Filter, etc.)
      if (!plan.getChild().isEmpty()) {
        plan = (org.opensearch.sql.ast.tree.UnresolvedPlan) plan.getChild().get(0);
      } else {
        break;
      }
    }
    return null;
  }

  @Override
  public SqlNode visitLookup(Lookup node, Void ctx) {
    Map<String, String> outputMap = node.getOutputAliasMap();
    boolean isReplace = node.getOutputStrategy() == Lookup.OutputStrategy.REPLACE;

    // Resolve lookup table columns
    Relation lookupRel = extractRelation(node.getLookupRelation());
    String lookupTable = lookupRel != null
        ? lookupRel.getTableQualifiedName().toString()
        : node.getLookupRelation().toString();
    Set<String> sourceCols = new HashSet<>(resolveColumns(tableName));
    List<String> lookupCols = resolveColumns(lookupTable);
    Set<String> mappingFields = new HashSet<>(node.getMappingAliasMap().keySet());

    String leftAlias = "_l";
    String rightAlias = "_r";
    knownAliases.add(leftAlias);
    knownAliases.add(rightAlias);
    SqlNode leftSide = subquery(pipe, leftAlias);
    SqlNode rightSide = subquery(
        select(star()).from(table(lookupTable)).build(), rightAlias);

    SqlNode onCondition = null;
    for (Map.Entry<String, String> e : node.getMappingAliasMap().entrySet()) {
      SqlNode cond = eq(
          identifier(leftAlias, e.getValue()),
          identifier(rightAlias, e.getKey()));
      onCondition = onCondition == null ? cond : and(onCondition, cond);
    }

    SqlNode joinNode = join(leftSide, JoinType.LEFT, rightSide, onCondition);

    if (outputMap.isEmpty()) {
      // No output spec: REPLACE all non-mapping lookup columns into source
      // Use _l.* REPLACE(_r.col AS col) for shared columns,
      // plus _r.col AS col for new columns
      List<SqlNode> replaceClauses = new ArrayList<>();
      List<SqlNode> newCols = new ArrayList<>();
      for (String col : lookupCols) {
        if (mappingFields.contains(col) || col.startsWith("_")) continue;
        SqlNode rRef = identifier(rightAlias, col);
        if (sourceCols.contains(col)) {
          // Shared column: REPLACE with lookup value (null when no match)
          replaceClauses.add(as(rRef, col));
        } else {
          // New column from lookup table
          newCols.add(as(rRef, col));
        }
      }
      List<SqlNode> selectItems = new ArrayList<>();
      if (!replaceClauses.isEmpty()) {
        SqlNode leftStar = new org.opensearch.sql.calcite.parser.SqlStarExceptReplace(
            SqlParserPos.ZERO,
            new org.apache.calcite.sql.SqlIdentifier(
                java.util.Arrays.asList(leftAlias, ""), SqlParserPos.ZERO),
            null,
            new SqlNodeList(replaceClauses, SqlParserPos.ZERO));
        selectItems.add(leftStar);
      } else {
        selectItems.add(new org.apache.calcite.sql.SqlIdentifier(
            java.util.Arrays.asList(leftAlias, ""), SqlParserPos.ZERO));
      }
      selectItems.addAll(newCols);
      pipe = select(selectItems.toArray(new SqlNode[0])).from(joinNode).build();
    } else {
      // Explicit output fields
      // Check pipe columns too (for eval-created columns like 'major')
      List<String> pipeCols = extractPipeColumns();
      Set<String> allKnownCols = new HashSet<>(sourceCols);
      allKnownCols.addAll(pipeCols);

      // Check if any target column is a pipe-only column (not in schema)
      // If so, we need explicit column enumeration instead of SqlStarExceptReplace
      boolean hasPipeOnlyTarget = false;
      for (Map.Entry<String, String> e : outputMap.entrySet()) {
        if (!sourceCols.contains(e.getValue()) && pipeCols.contains(e.getValue())) {
          hasPipeOnlyTarget = true;
          break;
        }
      }

      if (hasPipeOnlyTarget && !pipeCols.isEmpty()) {
        // Use explicit column enumeration
        // For REPLACE: skip target columns in source list, add at end
        Map<String, SqlNode> overrides = new LinkedHashMap<>();
        for (Map.Entry<String, String> e : outputMap.entrySet()) {
          SqlNode rRef = identifier(rightAlias, e.getKey());
          if (isReplace) {
            overrides.put(e.getValue(), as(rRef, e.getValue()));
          } else {
            if (allKnownCols.contains(e.getValue())) {
              overrides.put(e.getValue(), as(call("COALESCE", identifier(leftAlias, e.getValue()), rRef), e.getValue()));
            } else {
              overrides.put(e.getValue(), as(rRef, e.getValue()));
            }
          }
        }
        List<SqlNode> selectItems = new ArrayList<>();
        for (String col : pipeCols) {
          if (overrides.containsKey(col)) {
            // Skip — will be added at end
            continue;
          }
          selectItems.add(identifier(leftAlias, col));
        }
        // Add overridden/new columns at end
        for (SqlNode item : overrides.values()) {
          selectItems.add(item);
        }
        pipe = select(selectItems.toArray(new SqlNode[0])).from(joinNode).build();
      } else {
        List<SqlNode> replaceClauses = new ArrayList<>();
        List<SqlNode> newCols = new ArrayList<>();
        for (Map.Entry<String, String> e : outputMap.entrySet()) {
          SqlNode rRef = identifier(rightAlias, e.getKey());
          if (isReplace) {
            if (allKnownCols.contains(e.getValue())) {
              replaceClauses.add(as(rRef, e.getValue()));
            } else {
              newCols.add(as(rRef, e.getValue()));
            }
          } else {
            if (allKnownCols.contains(e.getValue())) {
              replaceClauses.add(as(call("COALESCE", identifier(leftAlias, e.getValue()), rRef), e.getValue()));
            } else {
              newCols.add(as(rRef, e.getValue()));
            }
          }
        }
        List<SqlNode> selectItems = new ArrayList<>();
        if (!replaceClauses.isEmpty()) {
          SqlNode leftStar = new org.opensearch.sql.calcite.parser.SqlStarExceptReplace(
              SqlParserPos.ZERO,
              new org.apache.calcite.sql.SqlIdentifier(
                  java.util.Arrays.asList(leftAlias, ""), SqlParserPos.ZERO),
              null,
              new SqlNodeList(replaceClauses, SqlParserPos.ZERO));
          selectItems.add(leftStar);
        } else {
          selectItems.add(new org.apache.calcite.sql.SqlIdentifier(
              java.util.Arrays.asList(leftAlias, ""), SqlParserPos.ZERO));
        }
        selectItems.addAll(newCols);
        pipe = select(selectItems.toArray(new SqlNode[0])).from(joinNode).build();
      }
    }
    return pipe;
  }

  private static Relation extractRelation(UnresolvedPlan plan) {
    if (plan instanceof Relation) return (Relation) plan;
    if (plan instanceof Project) {
      Project proj = (Project) plan;
      if (proj.getProjectList().size() == 1
          && proj.getProjectList().get(0) instanceof org.opensearch.sql.ast.expression.AllFields
          && !proj.getChild().isEmpty()) {
        UnresolvedPlan child = (UnresolvedPlan) proj.getChild().get(0);
        if (child instanceof Relation) return (Relation) child;
      }
    }
    return null;
  }

  @Override
  protected SqlNode convertSubPlan(UnresolvedPlan plan) {
    DynamicPPLToSqlNodeConverter sub = new DynamicPPLToSqlNodeConverter(defaultSchema, aliasCounter);
    return sub.convert(plan);
  }

  @Override
  protected SqlNode convertSubPipeline(UnresolvedPlan plan, SqlNode initialPipe) {
    DynamicPPLToSqlNodeConverter sub = new DynamicPPLToSqlNodeConverter(defaultSchema, aliasCounter);
    sub.pipe = initialPipe;
    sub.tableName = this.tableName;
    sub.aliasFieldMapping = this.aliasFieldMapping;
    List<UnresolvedPlan> nodes = new ArrayList<>();
    flattenPlan(plan, nodes);
    for (UnresolvedPlan n : nodes) {
      n.accept(sub, null);
    }
    if (sub.pendingOrderBy != null || sub.pendingFetch != null) {
      sub.pipe = sub.applyPendingOrderBy(sub.pipe);
    }
    return sub.pipe;
  }

  @Override
  public SqlNode visitAppend(org.opensearch.sql.ast.tree.Append node, Void ctx) {
    SqlNode mainSql = pipe;
    // Simplify subsearch AST: propagate empty source through joins/lookups
    UnresolvedPlan prunedSubSearch =
        node.getSubSearch().accept(new org.opensearch.sql.ast.EmptySourcePropagateVisitor(), null);
    // Check if simplified subsearch has a source. If not, it's an empty search.
    if (!hasRelation(prunedSubSearch)) {
      return pipe;
    }
    SqlNode subSql;
    try {
      subSql = convertSubPlan(prunedSubSearch);
    } catch (Exception e) {
      // Empty subsearch or conversion failure — return main pipe unchanged
      return pipe;
    }
    if (subSql == null) {
      return pipe;
    }
    List<String> mainCols = extractColumnsFromSqlNode(mainSql);
    List<String> subCols = extractColumnsFromSqlNode(subSql);
    if (subCols.isEmpty()) {
      subCols = extractSelectColumnNames(subSql);
    }
    // Detect type conflicts for common columns
    detectTypeConflicts(mainSql, subSql, mainCols, subCols);
    // Compute unified column set for alignment
    LinkedHashSet<String> unified = new LinkedHashSet<>(mainCols);
    unified.addAll(subCols);
    // Extract subsearch ORDER BY before alignment strips it
    SqlNodeList subOrderBy = extractOrderBy(subSql);
    if (!mainCols.isEmpty() && !subCols.isEmpty() && !mainCols.equals(subCols)) {
      mainSql = alignColumns(mainSql, mainCols, unified);
      subSql = alignColumns(subSql, subCols, unified);
    }
    // Strip subsearch ORDER BY (meaningless inside UNION ALL branch)
    subSql = stripOrderBy(subSql);
    // Add _source_order (0=main, 1=sub) to preserve main-first ordering
    mainSql = addSourceOrderColumn(mainSql, 0);
    subSql = addSourceOrderColumn(subSql, 1);
    String unionAlias = "_t" + aliasCounter.incrementAndGet();
    SqlNode union = unionAll(mainSql, subSql);
    // Build ORDER BY: _source_order first, then subsearch sort columns
    List<SqlNode> orderItems = new ArrayList<>();
    orderItems.add(identifier("_source_order"));
    if (subOrderBy != null) {
      for (SqlNode item : subOrderBy) {
        orderItems.add(item);
      }
    }
    // Keep _source_order visible in the pipe so ORDER BY can reference it.
    // Build: SELECT unified_cols, _source_order FROM (UNION ALL)
    List<SqlNode> projWithSource = new ArrayList<>();
    for (String col : unified) {
      projWithSource.add(identifier(col));
    }
    projWithSource.add(identifier("_source_order"));
    pipe = select(projWithSource.toArray(new SqlNode[0]))
        .from(subquery(
            select(star()).from(subquery(union, unionAlias)).build(),
            "_t" + aliasCounter.incrementAndGet()))
        .build();
    // Store order items as pending; they will be applied together with any
    // downstream head/limit.  After the ORDER BY is consumed we must strip
    // _source_order — that is done by overriding applyPendingOrderBy below.
    pendingOrderBy = orderItems;
    pendingStripSourceOrder = !unified.isEmpty() ? new ArrayList<>(unified) : null;
    return pipe;
  }

  /** Extract ORDER BY clause from a SqlNode, or null if none. */
  private SqlNodeList extractOrderBy(SqlNode node) {
    if (node instanceof SqlOrderBy) {
      SqlOrderBy ob = (SqlOrderBy) node;
      return (ob.orderList != null && ob.orderList.size() > 0) ? ob.orderList : null;
    }
    // Check if alignColumns preserved ORDER BY
    if (node instanceof SqlSelect) {
      SqlSelect sel = (SqlSelect) node;
      SqlNode from = sel.getFrom();
      if (from instanceof SqlBasicCall && "AS".equals(((SqlBasicCall) from).getOperator().getName())) {
        SqlNode inner = ((SqlBasicCall) from).operand(0);
        return extractOrderBy(inner);
      }
    }
    return null;
  }

  /** Strip ORDER BY from a SqlNode, returning the inner query. */
  private SqlNode stripOrderBy(SqlNode node) {
    if (node instanceof SqlOrderBy) {
      return ((SqlOrderBy) node).query;
    }
    return node;
  }

  /** Add a literal _source_order column to a SqlNode for append ordering. */
  private SqlNode addSourceOrderColumn(SqlNode node, int orderValue) {
    SqlNode target = node;
    if (target instanceof SqlOrderBy) {
      target = ((SqlOrderBy) target).query;
    }
    return select(star(), as(intLiteral(orderValue), "_source_order"))
        .from(subquery(target, "_t" + aliasCounter.incrementAndGet())).build();
  }

  /** Wrap a SqlOrderBy in a subquery to materialize its ordering before UNION ALL. */
  private SqlNode materializeOrderBy(SqlNode node) {
    if (node instanceof SqlOrderBy) {
      return select(star()).from(subquery(node, "_t" + aliasCounter.incrementAndGet())).build();
    }
    return node;
  }

  @Override
  protected SqlNode applyPendingOrderBy(SqlNode node) {
    SqlNode result = super.applyPendingOrderBy(node);
    if (pendingStripSourceOrder != null) {
      List<String> cols = pendingStripSourceOrder;
      pendingStripSourceOrder = null;
      List<SqlNode> projItems = new ArrayList<>();
      for (String col : cols) {
        projItems.add(identifier(col));
      }
      result = select(projItems.toArray(new SqlNode[0]))
          .from(subquery(result, "_t" + aliasCounter.incrementAndGet())).build();
    }
    return result;
  }

  /**
   * Detect type conflicts for common columns between main and sub queries.
   * Compares types from CAST expressions and schema-resolved types.
   * Throws IllegalArgumentException if incompatible types are found.
   */
  private void detectTypeConflicts(SqlNode mainSql, SqlNode subSql,
      List<String> mainCols, List<String> subCols) {
    Set<String> commonCols = new LinkedHashSet<>(mainCols);
    commonCols.retainAll(new LinkedHashSet<>(subCols));
    if (commonCols.isEmpty()) return;
    Map<String, SqlTypeName> mainTypes = extractColumnTypes(mainSql);
    Map<String, SqlTypeName> subTypes = extractColumnTypes(subSql);
    for (String col : commonCols) {
      SqlTypeName mainType = mainTypes.get(col);
      SqlTypeName subType = subTypes.get(col);
      if (mainType != null && subType != null && mainType != subType) {
        throw new IllegalArgumentException(
            String.format("Unable to process column '%s' due to incompatible types: '%s' and '%s'",
                col, mainType, subType));
      }
    }
  }

  /** Extract column types from a SqlNode by inspecting CAST expressions and schema. */
  private Map<String, SqlTypeName> extractColumnTypes(SqlNode node) {
    Map<String, SqlTypeName> types = new LinkedHashMap<>();
    SqlNode target = node;
    if (target instanceof SqlOrderBy) target = ((SqlOrderBy) target).query;
    if (!(target instanceof SqlSelect)) return types;
    SqlSelect sel = (SqlSelect) target;
    if (sel.getSelectList() == null) return types;
    for (SqlNode item : sel.getSelectList()) {
      String name = extractColumnName(item);
      if (name == null) continue;
      SqlTypeName typeName = extractTypeFromNode(item);
      if (typeName != null) types.put(name, typeName);
    }
    // Try resolving from source table schema for columns without explicit types
    if (types.size() < sel.getSelectList().size()) {
      Map<String, SqlTypeName> schemaTypes = resolveSchemaTypes(sel);
      for (Map.Entry<String, SqlTypeName> e : schemaTypes.entrySet()) {
        types.putIfAbsent(e.getKey(), e.getValue());
      }
    }
    // Recursively trace types from inner subqueries for columns still missing
    if (types.size() < sel.getSelectList().size()) {
      Map<String, SqlTypeName> innerTypes = resolveTypesFromInnerQuery(sel);
      for (Map.Entry<String, SqlTypeName> e : innerTypes.entrySet()) {
        types.putIfAbsent(e.getKey(), e.getValue());
      }
    }
    return types;
  }

  /** Recursively resolve column types from inner subqueries. */
  private Map<String, SqlTypeName> resolveTypesFromInnerQuery(SqlSelect sel) {
    SqlNode from = sel.getFrom();
    if (from == null) return Collections.emptyMap();
    if (from instanceof SqlBasicCall && "AS".equals(((SqlBasicCall) from).getOperator().getName())) {
      from = ((SqlBasicCall) from).operand(0);
    }
    if (from instanceof SqlSelect || from instanceof SqlOrderBy) {
      return extractColumnTypes(from);
    }
    return Collections.emptyMap();
  }

  /** Resolve column types from the source table schema. */
  private Map<String, SqlTypeName> resolveSchemaTypes(SqlSelect sel) {
    Map<String, SqlTypeName> types = new LinkedHashMap<>();
    SqlNode from = sel.getFrom();
    if (from == null) return types;
    // Unwrap AS alias
    if (from instanceof SqlBasicCall && "AS".equals(((SqlBasicCall) from).getOperator().getName())) {
      SqlNode inner = ((SqlBasicCall) from).operand(0);
      if (inner instanceof SqlSelect) return resolveSchemaTypes((SqlSelect) inner);
      from = inner;
    }
    if (from instanceof SqlIdentifier) {
      String tbl = ((SqlIdentifier) from).getSimple();
      Table table = defaultSchema.getTable(tbl);
      if (table == null) {
        for (String n : defaultSchema.getTableNames()) {
          if (n.equalsIgnoreCase(tbl)) { table = defaultSchema.getTable(n); break; }
        }
      }
      if (table != null) {
        for (org.apache.calcite.rel.type.RelDataTypeField f : table.getRowType(typeFactory).getFieldList()) {
          types.put(f.getName(), f.getType().getSqlTypeName());
        }
      }
    }
    return types;
  }

  /** Extract SqlTypeName from a CAST expression or AS(CAST(...), name). */
  private SqlTypeName extractTypeFromNode(SqlNode node) {
    if (node instanceof SqlBasicCall) {
      SqlBasicCall call = (SqlBasicCall) node;
      if ("AS".equals(call.getOperator().getName()) && call.operandCount() >= 2) {
        return extractTypeFromNode(call.operand(0));
      }
      if ("CAST".equals(call.getOperator().getName()) && call.operandCount() >= 2) {
        SqlNode typeNode = call.operand(1);
        if (typeNode instanceof SqlDataTypeSpec) {
          return ((SqlDataTypeSpec) typeNode).getTypeName().getSimple() != null
              ? SqlTypeName.get(((SqlDataTypeSpec) typeNode).getTypeName().getSimple())
              : null;
        }
      }
    }
    return null;
  }

  private static boolean hasRelation(org.opensearch.sql.ast.tree.UnresolvedPlan plan) {
    if (plan == null) return false;
    if (plan instanceof Relation) return true;
    for (org.opensearch.sql.ast.Node child : plan.getChild()) {
      if (child instanceof org.opensearch.sql.ast.tree.UnresolvedPlan
          && hasRelation((org.opensearch.sql.ast.tree.UnresolvedPlan) child)) {
        return true;
      }
    }
    return false;
  }

  @Override
  public SqlNode visitAppendPipe(org.opensearch.sql.ast.tree.AppendPipe node, Void ctx) {
    SqlNode originalPipe = pipe;
    SqlNode subResult = convertSubPipeline(node.getSubQuery(), pipe);
    // Extract columns from both sides to check if alignment is needed
    List<String> origCols = extractColumnsFromSqlNode(originalPipe);
    List<String> subCols = extractColumnsFromSqlNode(subResult);
    if (!origCols.isEmpty() && !subCols.isEmpty() && !origCols.equals(subCols)) {
      // Compute unified column set preserving order
      LinkedHashSet<String> unified = new LinkedHashSet<>(origCols);
      unified.addAll(subCols);
      originalPipe = alignColumns(originalPipe, origCols, unified);
      subResult = alignColumns(subResult, subCols, unified);
    }
    pipe = unionAll(originalPipe, subResult);
    return pipe;
  }

  private SqlNode alignColumns(SqlNode source, List<String> sourceCols, LinkedHashSet<String> targetCols) {
    Set<String> sourceSet = new HashSet<>(sourceCols);
    if (sourceSet.containsAll(targetCols)) return source;
    // Preserve ORDER BY if present
    SqlNodeList orderList = null;
    SqlNode fetch = null;
    SqlNode offset = null;
    SqlNode inner = source;
    if (inner instanceof SqlOrderBy) {
      SqlOrderBy ob = (SqlOrderBy) inner;
      orderList = ob.orderList;
      fetch = ob.fetch;
      offset = ob.offset;
      inner = ob.query;
    }
    List<SqlNode> items = new ArrayList<>();
    for (String col : targetCols) {
      if (sourceSet.contains(col)) {
        items.add(as(identifier(col), col));
      } else {
        items.add(as(SqlLiteral.createNull(SqlParserPos.ZERO), col));
      }
    }
    SqlNode aligned = select(items.toArray(new SqlNode[0])).from(subquery(inner, "_t" + aliasCounter.incrementAndGet())).build();
    if (orderList != null && orderList.size() > 0) {
      aligned = new SqlOrderBy(SqlParserPos.ZERO, aligned, orderList, offset, fetch);
    }
    return aligned;
  }

  private List<String> extractColumnsFromSqlNode(SqlNode node) {
    SqlNode target = node;
    if (target instanceof SqlOrderBy) target = ((SqlOrderBy) target).query;
    if (!(target instanceof SqlSelect)) return Collections.emptyList();
    SqlSelect sel = (SqlSelect) target;
    SqlNodeList selectList = sel.getSelectList();
    if (selectList == null) return Collections.emptyList();
    List<String> cols = new ArrayList<>();
    for (SqlNode item : selectList) {
      String name = extractColumnName(item);
      if (name == null) return Collections.emptyList();
      cols.add(name);
    }
    return cols;
  }

  private static void flattenPlan(UnresolvedPlan node, List<UnresolvedPlan> out) {
    List<? extends org.opensearch.sql.ast.Node> children = node.getChild();
    if (!children.isEmpty()) {
      flattenPlan((UnresolvedPlan) children.get(0), out);
    }
    out.add(node);
  }

  @Override
  public SqlNode visitMvExpand(MvExpand node, Void ctx) {
    String fieldName = node.getField().getField().toString();
    List<String> allCols = resolveColumns(tableName);
    if (allCols.isEmpty()) return pipe;

    // Check if the field is an array type; for non-array fields, mvexpand is a no-op
    if (!isArrayField(fieldName)) return pipe;

    // Predict the subquery alias that wrapAsSubquery() will assign
    String subqueryAlias = "_t" + (aliasCounter.get() + 1);

    // Build: SELECT _tN.cols..., _u.field FROM (pipe) AS _tN CROSS JOIN UNNEST(field) AS _u(field)
    String unnestAlias = "_u";
    SqlNode unnest = new SqlBasicCall(
        SqlStdOperatorTable.UNNEST,
        new SqlNode[]{identifier(subqueryAlias, fieldName)},
        SqlParserPos.ZERO);
    SqlNode aliasedUnnest = new SqlBasicCall(
        SqlStdOperatorTable.AS,
        new SqlNode[]{unnest, identifier(unnestAlias), identifier(fieldName)},
        SqlParserPos.ZERO);

    // Build select list: qualify all columns with subquery alias to avoid ambiguity
    List<SqlNode> selectItems = new ArrayList<>();
    for (String col : allCols) {
      if (col.equals(fieldName)) {
        selectItems.add(as(identifier(unnestAlias, fieldName), fieldName));
      } else {
        selectItems.add(identifier(subqueryAlias, col));
      }
    }

    SqlNode joinNode = join(wrapAsSubquery(), JoinType.CROSS, aliasedUnnest, null);
    pipe = select(selectItems.toArray(new SqlNode[0])).from(joinNode).build();

    if (node.getLimit() != null) {
      pipe = select(star()).from(wrapAsSubquery()).limit(intLiteral(node.getLimit())).build();
    }
    return pipe;
  }

  @Override
  public SqlNode visitExpand(Expand node, Void ctx) {
    String fieldName = node.getField().getField().toString();

    // Prefer pipe columns (handles eval-then-expand)
    List<String> allCols = extractPipeColumns();
    boolean usePipeCols = !allCols.isEmpty();
    if (!usePipeCols) {
      // extractPipeColumns returns empty when pipe has * — resolve * plus extra aliases
      allCols = resolveColumnsWithPipeExtras();
      allCols = allCols.stream().filter(n -> !n.startsWith("_") && !n.contains(".")).collect(Collectors.toList());
    }
    if (allCols.isEmpty()) return pipe;

    boolean isArray = isArrayField(fieldName);
    // If field is in table schema but not an array type, skip expand
    if (!isArray) {
      List<String> tableCols = resolveColumns(tableName);
      boolean fieldInTable = tableCols.stream().anyMatch(c -> c.equals(fieldName));
      if (fieldInTable) return pipe;
      // Field not in table schema (eval alias) — proceed with UNNEST
    }

    String alias = node.getAlias() != null ? node.getAlias() : fieldName;
    String subqueryAlias = "_t" + (aliasCounter.get() + 1);
    String unnestAlias = "_u";
    SqlNode unnest = new SqlBasicCall(
        SqlStdOperatorTable.UNNEST,
        new SqlNode[]{identifier(subqueryAlias, fieldName)},
        SqlParserPos.ZERO);
    SqlNode aliasedUnnest = new SqlBasicCall(
        SqlStdOperatorTable.AS,
        new SqlNode[]{unnest, identifier(unnestAlias), identifier(fieldName)},
        SqlParserPos.ZERO);

    List<SqlNode> selectItems = new ArrayList<>();
    for (String col : allCols) {
      if (col.equals(fieldName)) {
        selectItems.add(as(identifier(unnestAlias, fieldName), alias));
      } else {
        selectItems.add(identifier(subqueryAlias, col));
      }
    }

    SqlNode joinNode = join(wrapAsSubquery(), JoinType.CROSS, aliasedUnnest, null);
    pipe = select(selectItems.toArray(new SqlNode[0])).from(joinNode).build();
    return pipe;
  }

  @Override
  public SqlNode visitParse(org.opensearch.sql.ast.tree.Parse node, Void ctx) {
    // Call base to build the parse expressions
    super.visitParse(node, ctx);

    // Check if any group name matches an existing column — if so, use REPLACE
    Set<String> existingCols = new HashSet<>(resolveColumns(tableName));
    if (existingCols.isEmpty()) return pipe;

    String pattern =
        ((org.opensearch.sql.ast.expression.Literal) node.getPattern()).getValue().toString();
    java.util.regex.Pattern namedGroupPattern =
        java.util.regex.Pattern.compile("\\(\\?<([a-zA-Z][a-zA-Z0-9]*)>");
    java.util.regex.Matcher matcher = namedGroupPattern.matcher(pattern);
    List<String> overrideCols = new ArrayList<>();
    while (matcher.find()) {
      String gn = matcher.group(1);
      if (existingCols.contains(gn)) overrideCols.add(gn);
    }
    if (overrideCols.isEmpty()) return pipe;

    // Rebuild: use SqlStarExceptReplace for overrides, keep new columns as-is
    org.apache.calcite.sql.SqlSelect sel = (org.apache.calcite.sql.SqlSelect) pipe;
    SqlNodeList selectList = sel.getSelectList();
    // selectList is: [*, expr1 AS alias1, expr2 AS alias2, ...]
    List<SqlNode> replaceClauses = new ArrayList<>();
    List<SqlNode> newColExprs = new ArrayList<>();
    for (int i = 1; i < selectList.size(); i++) {
      SqlNode item = selectList.get(i);
      String name = extractColumnName(item);
      if (name != null && existingCols.contains(name)) {
        replaceClauses.add(item);
      } else {
        newColExprs.add(item);
      }
    }
    SqlNode starReplace = new SqlStarExceptReplace(
        SqlParserPos.ZERO, star(), null,
        new SqlNodeList(replaceClauses, SqlParserPos.ZERO));
    List<SqlNode> newSelectItems = new ArrayList<>();
    newSelectItems.add(starReplace);
    newSelectItems.addAll(newColExprs);
    sel.setSelectList(new SqlNodeList(newSelectItems, SqlParserPos.ZERO));
    return pipe;
  }

  @Override
  public SqlNode visitMultisearch(Multisearch node, Void ctx) {
    List<UnresolvedPlan> subsearches = node.getSubsearches();
    if (subsearches.isEmpty()) return pipe;

    // Convert each subsearch and extract column names
    List<SqlNode> converted = new ArrayList<>();
    List<List<String>> allColLists = new ArrayList<>();
    for (UnresolvedPlan sub : subsearches) {
      SqlNode result = convertSubPlan(sub);
      converted.add(result);
      List<String> cols = extractSelectColumnNames(result);
      allColLists.add(cols);
    }

    // If any subsearch has unresolvable columns (e.g. SELECT *), fall back to parent
    for (List<String> cols : allColLists) {
      if (cols.isEmpty()) {
        return super.visitMultisearch(node, ctx);
      }
    }

    // Compute unified column set preserving order: first subsearch columns first
    LinkedHashSet<String> unifiedCols = new LinkedHashSet<>();
    for (List<String> cols : allColLists) {
      unifiedCols.addAll(cols);
    }

    SqlDataTypeSpec varcharType =
        new SqlDataTypeSpec(new SqlBasicTypeNameSpec(SqlTypeName.VARCHAR, SqlParserPos.ZERO), SqlParserPos.ZERO);

    // Identify columns shared across multiple subsearches (candidates for CHAR padding)
    Map<String, Integer> colCount = new LinkedHashMap<>();
    for (String col : unifiedCols) colCount.put(col, 0);
    for (List<String> cols : allColLists) {
      for (String col : cols) colCount.merge(col, 1, Integer::sum);
    }

    // Collect all known table columns from subsearch source tables
    Set<String> allTableCols = new HashSet<>();
    for (UnresolvedPlan sub : subsearches) {
      String tbl = extractTableName(sub);
      if (tbl != null) allTableCols.addAll(resolveColumns(tbl));
    }

    // Shared computed columns (not in any table schema) need VARCHAR cast to prevent CHAR padding
    Set<String> castToVarchar = new HashSet<>();
    for (Map.Entry<String, Integer> e : colCount.entrySet()) {
      if (e.getValue() > 1 && !allTableCols.contains(e.getKey())) {
        castToVarchar.add(e.getKey());
      }
    }

    // Re-project each subsearch to the unified schema
    List<SqlNode> aligned = new ArrayList<>();
    for (int i = 0; i < converted.size(); i++) {
      Set<String> subCols = new LinkedHashSet<>(allColLists.get(i));
      String subAlias = "_t" + aliasCounter.incrementAndGet();
      SqlNode subquery = subquery(converted.get(i), subAlias);
      List<SqlNode> selectItems = new ArrayList<>();
      for (String col : unifiedCols) {
        if (subCols.contains(col)) {
          if (castToVarchar.contains(col)) {
            // Cast computed string columns to VARCHAR to prevent CHAR padding
            selectItems.add(as(
                SqlStdOperatorTable.CAST.createCall(SqlParserPos.ZERO, identifier(col), varcharType),
                col));
          } else {
            selectItems.add(as(identifier(col), col));
          }
        } else {
          // Null-fill missing columns with untyped NULL
          selectItems.add(as(SqlLiteral.createNull(SqlParserPos.ZERO), col));
        }
      }
      aligned.add(select(selectItems.toArray(new SqlNode[0])).from(subquery).build());
    }

    // UNION ALL the aligned projections
    SqlNode result = aligned.get(0);
    for (int i = 1; i < aligned.size(); i++) {
      result = unionAll(result, aligned.get(i));
    }
    pipe = select(star()).from(subquery(result, "_t" + aliasCounter.incrementAndGet())).build();
    return pipe;
  }

  /** Extract column names from a SqlSelect or SqlOrderBy result. */
  private List<String> extractSelectColumnNames(SqlNode node) {
    if (node instanceof SqlSelect) {
      SqlSelect sel = (SqlSelect) node;
      List<String> cols = new ArrayList<>();
      boolean hasStar = false;
      for (SqlNode item : sel.getSelectList()) {
        if (item instanceof SqlIdentifier && ((SqlIdentifier) item).isStar()) {
          hasStar = true;
          continue;
        }
        String name = extractColumnName(item);
        if (name == null) return Collections.emptyList();
        cols.add(name);
      }
      if (hasStar) {
        // Try to resolve * from the FROM clause's source table
        List<String> starCols = resolveStarColumns(sel);
        if (starCols.isEmpty()) return Collections.emptyList();
        // Merge: star columns first, then explicit non-star columns (excluding duplicates)
        LinkedHashSet<String> merged = new LinkedHashSet<>(starCols);
        merged.addAll(cols);
        return new ArrayList<>(merged);
      }
      return cols;
    }
    if (node instanceof SqlOrderBy) {
      return extractSelectColumnNames(((SqlOrderBy) node).query);
    }
    return Collections.emptyList();
  }

  /** Resolve columns for a SELECT * by tracing the FROM clause to find the source table. */
  private List<String> resolveStarColumns(SqlSelect sel) {
    SqlNode from = sel.getFrom();
    if (from == null) return Collections.emptyList();
    // Unwrap AS alias: (subquery) AS alias
    if (from instanceof SqlBasicCall
        && "AS".equals(((SqlBasicCall) from).getOperator().getName())) {
      from = ((SqlBasicCall) from).operand(0);
    }
    // If FROM is a subquery (SqlSelect), recurse
    if (from instanceof SqlSelect) {
      return extractSelectColumnNames(from);
    }
    // If FROM is a table identifier
    if (from instanceof SqlIdentifier) {
      return resolveColumns(((SqlIdentifier) from).getSimple());
    }
    return Collections.emptyList();
  }

  /** Extract the source table name from a subsearch plan by finding the Relation node. */
  private static String extractTableName(UnresolvedPlan plan) {
    if (plan instanceof Relation) return ((Relation) plan).getTableQualifiedName().toString();
    for (Object child : plan.getChild()) {
      if (child instanceof UnresolvedPlan) {
        String name = extractTableName((UnresolvedPlan) child);
        if (name != null) return name;
      }
    }
    return null;
  }

  private Table resolveTable(String tableName) {
    if (tableName == null) return null;
    Table table = defaultSchema.getTable(tableName);
    if (table == null) {
      for (String name : defaultSchema.getTableNames()) {
        if (name.equalsIgnoreCase(tableName)) { table = defaultSchema.getTable(name); break; }
      }
    }
    if (table == null) {
      try {
        org.apache.calcite.jdbc.CalciteSchema cs =
            org.apache.calcite.jdbc.CalciteSchema.from(defaultSchema);
        Schema underlying = cs.schema;
        if (underlying != null) table = underlying.getTable(tableName);
      } catch (Exception ignored) {}
    }
    return table;
  }

  private List<String> resolveNumericColumns(String tableName) {
    Table table = resolveTable(tableName);
    if (table == null) return Collections.emptyList();
    return table.getRowType(typeFactory).getFieldList().stream()
        .filter(f -> org.apache.calcite.sql.type.SqlTypeUtil.isNumeric(f.getType()))
        .map(f -> f.getName())
        .filter(n -> !n.startsWith("_"))
        .collect(Collectors.toList());
  }

  private static String getOptionValue(Map<String, Literal> options, String key, String defaultValue) {
    Literal literal = options.get(key);
    if (literal == null) return defaultValue;
    Object value = literal.getValue();
    if (value == null) return defaultValue;
    String s = value.toString();
    if (s.length() >= 2 && s.startsWith("'") && s.endsWith("'")) s = s.substring(1, s.length() - 1);
    return s;
  }

  private static boolean getBooleanOptionValue(Map<String, Literal> options, String key, boolean defaultValue) {
    if (!options.containsKey(key)) return defaultValue;
    Object value = options.get(key).getValue();
    if (value instanceof Boolean) return (Boolean) value;
    return Boolean.parseBoolean(value.toString());
  }

  @Override
  public SqlNode visitAddTotals(AddTotals node, Void ctx) {
    // Children already visited by convert()'s flatten loop — no need to re-visit

    // Apply pending FETCH/ORDER BY before building UNION ALL
    if (pendingFetch != null || pendingOrderBy != null) {
      pipe = applyPendingOrderBy(pipe);
    }

    Map<String, Literal> options = node.getOptions();
    String label = getOptionValue(options, "label", "Total");
    String labelField = getOptionValue(options, "labelfield", null);
    String fieldname = getOptionValue(options, "fieldname", "Total");
    boolean row = getBooleanOptionValue(options, "row", true);
    boolean col = getBooleanOptionValue(options, "col", false);

    List<Field> fieldsToAggregate = node.getFieldList();
    List<String> allCols = extractPipeColumns();
    if (allCols.isEmpty()) allCols = resolveColumns(tableName);
    Set<String> numericCols = resolveEffectiveNumericCols(allCols);

    // Determine which fields to sum
    List<String> fieldNames;
    if (fieldsToAggregate.isEmpty()) {
      fieldNames = allCols.stream().filter(numericCols::contains).collect(Collectors.toList());
    } else {
      fieldNames = fieldsToAggregate.stream()
          .map(f -> f.getField().toString())
          .filter(numericCols::contains)
          .collect(Collectors.toList());
    }

    if (row && !fieldNames.isEmpty()) {
      // Build row totals: SELECT *, (COALESCE(f1,0) + COALESCE(f2,0) + ...) AS fieldname
      SqlNode sumExpr = buildCoalescedSum(fieldNames);
      pipe = select(SqlIdentifier.star(SqlParserPos.ZERO), as(sumExpr, fieldname))
          .from(wrapAsSubquery()).build();
    }

    if (col) {
      buildColTotals(allCols, fieldNames, numericCols, labelField, label, row ? fieldname : null);
    }

    return pipe;
  }

  @Override
  public SqlNode visitAddColTotals(AddColTotals node, Void ctx) {
    // Children already visited by convert()'s flatten loop — no need to re-visit

    // Apply pending FETCH/ORDER BY before building UNION ALL
    if (pendingFetch != null || pendingOrderBy != null) {
      pipe = applyPendingOrderBy(pipe);
    }

    Map<String, Literal> options = node.getOptions();
    String label = getOptionValue(options, "label", "Total");
    String labelField = getOptionValue(options, "labelfield", null);

    List<Field> fieldsToAggregate = node.getFieldList();
    List<String> allCols = extractPipeColumns();
    if (allCols.isEmpty()) allCols = resolveColumns(tableName);
    Set<String> numericCols = resolveEffectiveNumericCols(allCols);

    List<String> fieldNames;
    if (fieldsToAggregate.isEmpty()) {
      fieldNames = allCols.stream().filter(numericCols::contains).collect(Collectors.toList());
    } else {
      fieldNames = fieldsToAggregate.stream()
          .map(f -> f.getField().toString())
          .filter(numericCols::contains)
          .collect(Collectors.toList());
    }

    buildColTotals(allCols, fieldNames, numericCols, labelField, label, null);
    return pipe;
  }

  private SqlNode buildCoalescedSum(List<String> fieldNames) {
    SqlNode result = null;
    for (String f : fieldNames) {
      SqlNode coalesced = new SqlBasicCall(SqlStdOperatorTable.COALESCE,
          new SqlNode[]{identifier(f), intLiteral(0)}, SqlParserPos.ZERO);
      result = result == null ? coalesced
          : new SqlBasicCall(SqlStdOperatorTable.PLUS, new SqlNode[]{result, coalesced}, SqlParserPos.ZERO);
    }
    return result;
  }

  /**
   * Resolve effective numeric columns: uses schema for table columns,
   * and assumes pipe-only columns (from stats/eval) are numeric if they appear
   * in the fields-to-aggregate list.
   */
  private Set<String> resolveEffectiveNumericCols(List<String> allCols) {
    Set<String> schemaCols = new LinkedHashSet<>(resolveNumericColumns(tableName));
    // Columns in the pipe but not in the schema (e.g., from stats/eval) — treat as numeric
    Set<String> schemaColNames = new HashSet<>(resolveColumns(tableName));
    for (String col : allCols) {
      if (!schemaColNames.contains(col)) {
        schemaCols.add(col);
      }
    }
    return schemaCols;
  }

  private void buildColTotals(List<String> allCols, List<String> fieldNames,
      Set<String> numericCols, String labelField, String label, String rowFieldname) {
    // Determine if labelField exists in current columns
    boolean labelFieldExists = labelField != null && allCols.contains(labelField);
    boolean labelFieldIsNew = labelField != null && !labelFieldExists;
    if (labelField != null && labelField.equals(rowFieldname)) {
      labelFieldExists = true;
      labelFieldIsNew = false;
    }

    // Wrap pipe as subquery once — used for both original data and aggregation
    SqlNode wrappedPipe = wrapAsSubquery();

    // Build the totals row first (references the wrapped pipe)
    SqlNode dataForAgg = subquery(pipe, "_t" + aliasCounter.incrementAndGet());

    // Determine the full column list
    List<String> fullCols = new ArrayList<>(allCols);
    if (rowFieldname != null && !allCols.contains(rowFieldname)) fullCols.add(rowFieldname);
    if (labelFieldIsNew) fullCols.add(labelField);

    Set<String> fieldsToSum = new LinkedHashSet<>(fieldNames);
    List<SqlNode> totalsItems = new ArrayList<>();
    for (String col : fullCols) {
      if (fieldsToSum.contains(col)) {
        totalsItems.add(as(new SqlBasicCall(SqlStdOperatorTable.SUM,
            new SqlNode[]{identifier(col)}, SqlParserPos.ZERO), col));
      } else if (col.equals(labelField)) {
        totalsItems.add(as(SqlLiteral.createCharString(label, SqlParserPos.ZERO), col));
      } else {
        totalsItems.add(as(SqlLiteral.createNull(SqlParserPos.ZERO), col));
      }
    }
    SqlNode totalsRow = select(totalsItems.toArray(new SqlNode[0])).from(dataForAgg).build();

    // Build original data side — add NULL labelField column if new
    SqlNode originalData;
    if (labelFieldIsNew) {
      List<SqlNode> origItems = new ArrayList<>();
      origItems.add(SqlIdentifier.star(SqlParserPos.ZERO));
      origItems.add(as(SqlLiteral.createNull(SqlParserPos.ZERO), labelField));
      originalData = select(origItems.toArray(new SqlNode[0]))
          .from(subquery(pipe, "_t" + aliasCounter.incrementAndGet())).build();
    } else {
      originalData = pipe;
    }

    pipe = unionAll(originalData, totalsRow);
  }

  @Override
  public SqlNode visitTranspose(Transpose node, Void ctx) {
    int maxRows = node.getMaxRows();
    String columnName = node.getColumnName();

    pipe = applyPendingOrderBy(pipe);

    List<String> cols = extractPipeColumns();
    if (cols.isEmpty()) {
      cols = resolveColumns(tableName).stream()
          .filter(n -> !n.startsWith("_") && !n.contains("."))
          .collect(Collectors.toList());
    }
    if (cols.isEmpty()) return pipe;

    // Add ROW_NUMBER() OVER() to the pipe
    String rnCol = "_rn";
    SqlNode rowNum = as(
        window(new SqlBasicCall(SqlStdOperatorTable.ROW_NUMBER, new SqlNode[0], SqlParserPos.ZERO),
            SqlNodeList.EMPTY, SqlNodeList.EMPTY), rnCol);
    pipe = select(star(), rowNum).from(wrapAsSubquery()).build();
    SqlNode numberedSubquery = wrapAsSubquery();

    // For each column, build a SELECT that transposes it
    List<SqlNode> unionParts = new ArrayList<>();
    for (String col : cols) {
      List<SqlNode> items = new ArrayList<>();
      items.add(as(new SqlBasicCall(SqlStdOperatorTable.CAST,
          new SqlNode[]{SqlLiteral.createCharString(col, SqlParserPos.ZERO),
              new SqlDataTypeSpec(new SqlBasicTypeNameSpec(SqlTypeName.VARCHAR, SqlParserPos.ZERO), SqlParserPos.ZERO)},
          SqlParserPos.ZERO), columnName));
      for (int i = 1; i <= maxRows; i++) {
        SqlNode caseExpr = caseWhen(
            List.of(new SqlBasicCall(SqlStdOperatorTable.EQUALS,
                new SqlNode[]{identifier(rnCol), intLiteral(i)}, SqlParserPos.ZERO)),
            List.of(new SqlBasicCall(SqlStdOperatorTable.CAST,
                new SqlNode[]{identifier(col),
                    new SqlDataTypeSpec(new SqlBasicTypeNameSpec(SqlTypeName.VARCHAR, SqlParserPos.ZERO), SqlParserPos.ZERO)},
                SqlParserPos.ZERO)),
            SqlLiteral.createNull(SqlParserPos.ZERO));
        SqlNode maxAgg = new SqlBasicCall(
            SqlStdOperatorTable.MAX,
            new SqlNode[]{caseExpr},
            SqlParserPos.ZERO);
        items.add(as(maxAgg, "row " + i));
      }
      SqlNode colSelect = select(items.toArray(new SqlNode[0]))
          .from(numberedSubquery)
          .build();
      unionParts.add(colSelect);
    }

    SqlNode result = unionParts.get(0);
    for (int i = 1; i < unionParts.size(); i++) {
      result = unionAll(result, unionParts.get(i));
    }
    pipe = result;
    return pipe;
  }

  @Override
  public SqlNode visitDedupe(Dedupe node, Void ctx) {
    super.visitDedupe(node, ctx);
    boolean consecutive =
        (Boolean) ((Literal) node.getOptions().get(2).getValue()).getValue();
    if (consecutive) {
      // Preserve document order so consecutive semantics are visible in the result
      SqlDataTypeSpec intType = new SqlDataTypeSpec(
          new SqlBasicTypeNameSpec(SqlTypeName.INTEGER, SqlParserPos.ZERO), SqlParserPos.ZERO);
      pendingOrderBy = List.of(cast(identifier("_id"), intType));
    }
    return pipe;
  }

  // -- Fix 1: COALESCE with non-existent fields → replace with NULL --

  @Override
  public SqlNode visitAggregateFunction(AggregateFunction node, Void ctx) {
    String name = node.getFuncName().toLowerCase();
    if (("earliest".equals(name) || "latest".equals(name))
        && !aliasFieldMapping.isEmpty()
        && aliasFieldMapping.containsKey("@timestamp")) {
      String resolved = aliasFieldMapping.get("@timestamp");
      SqlNode arg = node.getField().accept(this, null);
      return call("earliest".equals(name) ? "ARG_MIN" : "ARG_MAX", arg, identifier(resolved));
    }
    // PPL SUM returns BIGINT; cast integer args to BIGINT for Calcite type promotion
    if ("sum".equals(name)) {
      SqlNode field = node.getField().accept(this, null);
      String fn = fieldName(node.getField());
      // Resolve alias to target field for type lookup
      if (fn != null && aliasFieldMapping.containsKey(fn)) fn = aliasFieldMapping.get(fn);
      SqlTypeName ft = fn != null ? getFieldSqlType(fn) : null;
      if (ft == SqlTypeName.INTEGER || ft == SqlTypeName.SMALLINT || ft == SqlTypeName.TINYINT) {
        SqlDataTypeSpec bigintType = new SqlDataTypeSpec(
            new SqlBasicTypeNameSpec(SqlTypeName.BIGINT, SqlParserPos.ZERO), SqlParserPos.ZERO);
        return call("SUM", cast(field, bigintType));
      }
      return call("SUM", field);
    }
    return super.visitAggregateFunction(node, ctx);
  }

  @Override
  public SqlNode visitFunction(Function node, Void ctx) {
    // Resolve @timestamp alias for earliest/latest functions
    String name = node.getFuncName().toLowerCase();
    if (("earliest".equals(name) || "latest".equals(name))
        && !aliasFieldMapping.isEmpty()
        && aliasFieldMapping.containsKey("@timestamp")) {
      String resolved = aliasFieldMapping.get("@timestamp");
      SqlNode arg = node.getFuncArgs().get(0).accept(this, null);
      return call("earliest".equals(name) ? "ARG_MIN" : "ARG_MAX", arg, identifier(resolved));
    }
    if ("coalesce".equalsIgnoreCase(name)) {
      Set<String> cols = new HashSet<>(resolveColumnsWithPipeExtras());
      cols.addAll(currentEvalAliases);
      List<SqlNode> args = new ArrayList<>();
      for (UnresolvedExpression arg : node.getFuncArgs()) {
        String fieldName = null;
        if (arg instanceof Field) {
          fieldName = ((Field) arg).getField().toString();
        } else if (arg instanceof QualifiedName) {
          fieldName = ((QualifiedName) arg).toString();
        }
        if (fieldName != null && !cols.contains(fieldName)) {
          args.add(SqlLiteral.createNull(SqlParserPos.ZERO));
        } else {
          args.add(arg.accept(this, null));
        }
      }
      return call("COALESCE", args.toArray(new SqlNode[0]));
    }
    return super.visitFunction(node, ctx);
  }

  // -- Fix 3: Span type preservation for TIME/DATE fields --

  @Override
  public SqlNode visitSpan(Span node, Void ctx) {
    SqlNode result = super.visitSpan(node, ctx);
    if (tableName == null || node.getUnit() == SpanUnit.NONE || !SpanUnit.isTimeUnit(node.getUnit())) {
      return result;
    }
    String fieldName = node.getField() instanceof QualifiedName
        ? ((QualifiedName) node.getField()).toString()
        : (node.getField() instanceof Field ? ((Field) node.getField()).getField().toString() : null);
    if (fieldName == null) return result;
    SqlTypeName fieldType = getFieldSqlType(fieldName);
    if (fieldType == SqlTypeName.TIME) {
      return cast(result, new SqlDataTypeSpec(new SqlBasicTypeNameSpec(SqlTypeName.TIME, SqlParserPos.ZERO), SqlParserPos.ZERO));
    }
    if (fieldType == SqlTypeName.DATE) {
      return cast(result, new SqlDataTypeSpec(new SqlBasicTypeNameSpec(SqlTypeName.DATE, SqlParserPos.ZERO), SqlParserPos.ZERO));
    }
    return result;
  }

  private SqlTypeName getFieldSqlType(String fieldName) {
    Table table = resolveTable(tableName);
    if (table == null) return null;
    org.apache.calcite.rel.type.RelDataTypeField f =
        table.getRowType(typeFactory).getField(fieldName, true, false);
    return f != null ? f.getType().getSqlTypeName() : null;
  }
}
