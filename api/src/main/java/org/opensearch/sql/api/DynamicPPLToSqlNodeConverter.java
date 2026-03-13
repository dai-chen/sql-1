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
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.Let;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.calcite.parser.SqlStarExceptReplace;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.ast.tree.Eval;
import org.opensearch.sql.ast.tree.FillNull;
import org.opensearch.sql.ast.tree.Lookup;
import org.opensearch.sql.ast.tree.MvCombine;
import org.opensearch.sql.ast.tree.MvExpand;
import org.opensearch.sql.ast.tree.Project;
import org.opensearch.sql.ast.tree.Relation;
import org.opensearch.sql.ast.tree.Rename;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.utils.WildcardRenameUtils;

/**
 * Extends PPLToSqlNodeConverter with schema-dependent commands that need to resolve column names:
 * fields - (exclude), wildcard rename, fillnull all-fields, eval column override.
 */
public class DynamicPPLToSqlNodeConverter extends PPLToSqlNodeConverter {

  private final SchemaPlus defaultSchema;
  private final RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();
  protected String tableName;

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
        .collect(Collectors.toList());
  }

  private static String fieldName(UnresolvedExpression expr) {
    if (expr instanceof Field) return ((Field) expr).getField().toString();
    if (expr instanceof QualifiedName) return ((QualifiedName) expr).toString();
    return expr.toString();
  }

  @Override
  public SqlNode visitRelation(Relation node, Void ctx) {
    this.tableName = node.getTableQualifiedName().toString();
    return super.visitRelation(node, ctx);
  }

  @Override
  public SqlNode visitProject(Project node, Void ctx) {
    if (node.isExcluded()) {
      Set<String> excluded = node.getProjectList().stream()
          .map(e -> ((Field) e).getField().toString())
          .collect(Collectors.toSet());
      List<String> allCols = resolveColumns(tableName);
      SqlNode[] cols = allCols.stream()
          .filter(c -> !excluded.contains(c))
          .map(c -> identifier(c))
          .toArray(SqlNode[]::new);
      pipe = select(cols).from(wrapAsSubquery()).build();
      return pipe;
    }
    return super.visitProject(node, ctx);
  }

  @Override
  public SqlNode visitRename(Rename node, Void ctx) {
    boolean hasWildcard = false;
    for (org.opensearch.sql.ast.expression.Map mapping : node.getRenameList()) {
      if (WildcardRenameUtils.isWildcardPattern(fieldName(mapping.getOrigin()))) {
        hasWildcard = true;
        break;
      }
    }
    if (!hasWildcard) return super.visitRename(node, ctx);

    pipe = wrapAsSubquery();
    List<String> allCols = resolveColumns(tableName);
    LinkedHashMap<String, String> mappings = new LinkedHashMap<>();
    for (org.opensearch.sql.ast.expression.Map mapping : node.getRenameList()) {
      String sourcePattern = fieldName(mapping.getOrigin());
      String targetPattern = fieldName(mapping.getTarget());
      if (WildcardRenameUtils.isWildcardPattern(sourcePattern)) {
        List<String> matchingFields = WildcardRenameUtils.matchFieldNames(sourcePattern, allCols);
        for (String fld : matchingFields) {
          mappings.put(fld, WildcardRenameUtils.applyWildcardTransformation(
              sourcePattern, targetPattern, fld));
        }
      } else {
        mappings.put(sourcePattern, targetPattern);
      }
    }
    mappings.entrySet().removeIf(e -> e.getKey().equals(e.getValue()));
    if (mappings.isEmpty()) return pipe;

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
    if (node.getReplacementForAll().isPresent()) {
      pipe = wrapAsSubquery();
      SqlNode value = node.getReplacementForAll().get().accept(this, null);
      List<String> allCols = resolveColumns(tableName);
      List<SqlNode> selectItems = new ArrayList<>();
      for (String col : allCols) {
        selectItems.add(as(call("COALESCE", identifier(col), value), col));
      }
      pipe = select(selectItems.toArray(new SqlNode[0])).from(pipe).build();
      return pipe;
    }
    return super.visitFillNull(node, ctx);
  }

  @Override
  public SqlNode visitEval(Eval node, Void ctx) {
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
  public SqlNode visitMvCombine(MvCombine node, Void ctx) {
    String fieldName = node.getField().getField().toString();
    List<String> allCols = new ArrayList<>(resolveColumns(tableName));

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
  public SqlNode visitLookup(Lookup node, Void ctx) {
    Map<String, String> outputMap = node.getOutputAliasMap();
    if (outputMap.isEmpty() || node.getOutputStrategy() != Lookup.OutputStrategy.APPEND) {
      return super.visitLookup(node, ctx);
    }

    // APPEND mode with schema: only COALESCE columns that exist in the source table
    Set<String> sourceCols = new HashSet<>(resolveColumns(tableName));

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

    SqlNode joinNode = join(leftSide, JoinType.LEFT, rightSide, onCondition);

    List<SqlNode> selectItems = new ArrayList<>();
    selectItems.add(new org.apache.calcite.sql.SqlIdentifier(
        java.util.Arrays.asList(leftAlias, ""), SqlParserPos.ZERO));
    for (Map.Entry<String, String> e : outputMap.entrySet()) {
      SqlNode rRef = identifier(rightAlias, e.getKey());
      if (sourceCols.contains(e.getValue())) {
        selectItems.add(as(call("COALESCE", identifier(leftAlias, e.getValue()), rRef), e.getValue()));
      } else {
        selectItems.add(as(rRef, e.getValue()));
      }
    }
    pipe = select(selectItems.toArray(new SqlNode[0])).from(joinNode).build();
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
  public SqlNode visitMvExpand(MvExpand node, Void ctx) {
    String fieldName = node.getField().getField().toString();
    List<String> allCols = resolveColumns(tableName);
    if (allCols.isEmpty()) return pipe;

    // Build: SELECT cols..., _u.field FROM (pipe) CROSS JOIN UNNEST(field) AS _u(field)
    String unnestAlias = "_u";
    SqlNode unnest = new SqlBasicCall(
        SqlStdOperatorTable.UNNEST,
        new SqlNode[]{identifier(fieldName)},
        SqlParserPos.ZERO);
    // AS _u(field) — multi-column alias
    SqlNode aliasedUnnest = new SqlBasicCall(
        SqlStdOperatorTable.AS,
        new SqlNode[]{unnest, identifier(unnestAlias), identifier(fieldName)},
        SqlParserPos.ZERO);

    // Build select list: replace array field with unnested value
    List<SqlNode> selectItems = new ArrayList<>();
    for (String col : allCols) {
      if (col.equals(fieldName)) {
        selectItems.add(as(identifier(unnestAlias, fieldName), fieldName));
      } else {
        selectItems.add(identifier(col));
      }
    }

    SqlNode joinNode = join(wrapAsSubquery(), JoinType.CROSS, aliasedUnnest, null);
    pipe = select(selectItems.toArray(new SqlNode[0])).from(joinNode).build();

    if (node.getLimit() != null) {
      pipe = select(star()).from(wrapAsSubquery()).limit(intLiteral(node.getLimit())).build();
    }
    return pipe;
  }
}
