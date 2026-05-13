/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.parser;

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import org.antlr.v4.runtime.tree.ParseTree;
import org.opensearch.sql.ast.expression.Argument;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.ast.statement.Query;
import org.opensearch.sql.ast.statement.Statement;
import org.opensearch.sql.ast.tree.Join;
import org.opensearch.sql.ast.tree.Join.JoinHint;
import org.opensearch.sql.ast.tree.Join.JoinType;
import org.opensearch.sql.ast.tree.Except;
import org.opensearch.sql.ast.tree.Union;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.sql.antlr.SQLSyntaxParser;
import org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser;
import org.opensearch.sql.sql.parser.AstBuilder;
import org.opensearch.sql.sql.parser.AstStatementBuilder;

/** SQL query parser that produces {@link UnresolvedPlan} using the V2 ANTLR grammar. */
public class SqlV2QueryParser implements UnifiedQueryParser<UnresolvedPlan> {

  /** Reusable ANTLR-based SQL syntax parser. Stateless and thread-safe. */
  private final SQLSyntaxParser syntaxParser = new SQLSyntaxParser();

  @Override
  public UnresolvedPlan parse(String query) {
    ParseTree cst = syntaxParser.parse(query);
    AstStatementBuilder astStmtBuilder =
        new AstStatementBuilder(
            new SqlV2AstBuilder(query),
            AstStatementBuilder.StatementBuilderContext.builder().build());
    Statement statement = cst.accept(astStmtBuilder);

    if (statement instanceof Query) {
      return ((Query) statement).getPlan();
    }
    throw new UnsupportedOperationException(
        "Only query statements are supported but got " + statement.getClass().getSimpleName());
  }

  /**
   * AstBuilder subclass that overrides JOIN/UNION/EXCEPT visit methods to produce proper AST nodes
   * instead of throwing SyntaxCheckException (which would trigger legacy engine fallback).
   */
  static class SqlV2AstBuilder extends AstBuilder {

    SqlV2AstBuilder(String query) {
      super(query);
    }

    @Override
    public UnresolvedPlan visitJoinClause(OpenSearchSQLParser.JoinClauseContext ctx) {
      JoinType joinType = resolveJoinType(ctx.joinType());
      UnresolvedPlan right = visit(ctx.relation());
      Optional<UnresolvedExpression> condition =
          ctx.expression() != null
              ? Optional.of(visitAstExpression(ctx.expression()))
              : Optional.empty();

      return new Join(
          right,
          Optional.empty(),
          Optional.empty(),
          joinType,
          condition,
          new JoinHint(),
          Optional.empty(),
          Argument.ArgumentMap.empty());
    }

    @Override
    public UnresolvedPlan visitUnionSelect(OpenSearchSQLParser.UnionSelectContext ctx) {
      List<UnresolvedPlan> datasets = new ArrayList<>();
      for (OpenSearchSQLParser.QuerySpecificationContext queryCtx : ctx.querySpecification()) {
        datasets.add(visit(queryCtx));
      }
      return new Union(datasets);
    }

    @Override
    public UnresolvedPlan visitExceptSelect(OpenSearchSQLParser.ExceptSelectContext ctx) {
      UnresolvedPlan left = visit(ctx.querySpecification(0));
      UnresolvedPlan right = visit(ctx.querySpecification(1));
      return new Except(left, right);
    }

    private JoinType resolveJoinType(OpenSearchSQLParser.JoinTypeContext ctx) {
      if (ctx == null) {
        return JoinType.INNER;
      }
      if (ctx.LEFT() != null) {
        return JoinType.LEFT;
      }
      if (ctx.RIGHT() != null) {
        return JoinType.RIGHT;
      }
      if (ctx.CROSS() != null) {
        return JoinType.CROSS;
      }
      return JoinType.INNER;
    }
  }
}
