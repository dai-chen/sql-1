/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api;

import static org.opensearch.sql.calcite.utils.SqlNodeDSL.*;

import java.util.ArrayList;
import java.util.List;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.calcite.sql.SqlNode;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.Node;
import org.opensearch.sql.ast.expression.AllFields;
import org.opensearch.sql.ast.expression.Argument;
import org.opensearch.sql.ast.expression.Field;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.QualifiedName;
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

  // -- Expression visitors (minimal set for US-003) --

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
    if (node.getType() == org.opensearch.sql.ast.expression.DataType.NULL) {
      return literal(null);
    }
    return literal(node.getValue());
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
}
