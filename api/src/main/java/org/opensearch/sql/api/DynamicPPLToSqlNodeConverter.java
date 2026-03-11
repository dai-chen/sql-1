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
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlNode;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.Let;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.ast.tree.Eval;
import org.opensearch.sql.ast.tree.FillNull;
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

  private List<String> resolveColumns(String tableName) {
    Table table = defaultSchema.getTable(tableName);
    if (table == null) {
      for (String name : defaultSchema.getTableNames()) {
        if (name.equalsIgnoreCase(tableName)) {
          table = defaultSchema.getTable(name);
          break;
        }
      }
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

    List<SqlNode> selectItems = new ArrayList<>();
    for (String col : allCols) {
      if (overrides.containsKey(col)) {
        selectItems.add(as(overrides.get(col), col));
      } else {
        selectItems.add(identifier(col));
      }
    }
    for (String col : newCols) {
      selectItems.add(as(evalAliases.get(col), col));
    }
    pipe = select(selectItems.toArray(new SqlNode[0])).from(wrapAsSubquery()).build();
    return pipe;
  }

  @Override
  protected SqlNode convertSubPlan(UnresolvedPlan plan) {
    DynamicPPLToSqlNodeConverter sub = new DynamicPPLToSqlNodeConverter(defaultSchema);
    return sub.convert(plan);
  }
}
