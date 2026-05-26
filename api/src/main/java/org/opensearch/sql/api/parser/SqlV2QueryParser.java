/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.parser;

import static org.opensearch.sql.ast.dsl.AstDSL.existsSubquery;
import static org.opensearch.sql.ast.dsl.AstDSL.inSubquery;
import static org.opensearch.sql.ast.dsl.AstDSL.join;

import java.util.List;
import java.util.Locale;
import java.util.Optional;
import org.antlr.v4.runtime.tree.ParseTree;
import org.opensearch.sql.ast.expression.DataType;
import org.opensearch.sql.ast.expression.Function;
import org.opensearch.sql.ast.expression.Literal;
import org.opensearch.sql.ast.expression.Not;
import org.opensearch.sql.ast.expression.UnresolvedArgument;
import org.opensearch.sql.ast.expression.UnresolvedExpression;
import org.opensearch.sql.ast.statement.Query;
import org.opensearch.sql.ast.statement.Statement;
import org.opensearch.sql.ast.tree.Join.JoinType;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.sql.antlr.SQLSyntaxParser;
import org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser;
import org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.ExistsSubqueryExpressionAtomContext;
import org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.InSubqueryPredicateContext;
import org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.JoinClauseContext;
import org.opensearch.sql.sql.antlr.parser.OpenSearchSQLParser.SingleFieldRelevanceFunctionContext;
import org.opensearch.sql.sql.parser.AstBuilder;
import org.opensearch.sql.sql.parser.AstExpressionBuilder;
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
            new ExtendedAstBuilder(query),
            AstStatementBuilder.StatementBuilderContext.builder().build());
    Statement statement = cst.accept(astStmtBuilder);

    if (statement instanceof Query) {
      return ((Query) statement).getPlan();
    }
    throw new UnsupportedOperationException(
        "Only query statements are supported but got " + statement.getClass().getSimpleName());
  }

  /**
   * Extends the V2 AstBuilder with JOIN support that the base AstBuilder rejects with
   * SyntaxCheckException to trigger legacy engine fallback.
   */
  private static class ExtendedAstBuilder extends AstBuilder {

    ExtendedAstBuilder(String query) {
      super(query);
    }

    @Override
    protected AstExpressionBuilder createExpressionBuilder() {
      return new ExtendedAstExpressionBuilder();
    }

    @Override
    public UnresolvedPlan visitJoinClause(JoinClauseContext ctx) {
      JoinType joinType = toJoinType(ctx);
      UnresolvedPlan right = visit(ctx.relation());
      Optional<UnresolvedExpression> condition =
          Optional.ofNullable(ctx.expression()).map(this::visitAstExpression);
      return join(right, joinType, condition);
    }

    private JoinType toJoinType(JoinClauseContext ctx) {
      return switch (ctx.getStart().getType()) {
        case OpenSearchSQLParser.LEFT -> JoinType.LEFT;
        case OpenSearchSQLParser.RIGHT -> JoinType.RIGHT;
        case OpenSearchSQLParser.CROSS -> JoinType.CROSS;
        default -> JoinType.INNER;
      };
    }

    /**
     * Expression builder with IN/EXISTS subquery support. Accesses the enclosing AstBuilder to
     * visit subquery plan nodes. Must be created via {@link #createExpressionBuilder()} because the
     * enclosing {@code this} reference is not available during {@code super()} construction.
     */
    private class ExtendedAstExpressionBuilder extends AstExpressionBuilder {

      @Override
      public UnresolvedExpression visitSingleFieldRelevanceFunction(
          SingleFieldRelevanceFunctionContext ctx) {
        UnresolvedExpression result = super.visitSingleFieldRelevanceFunction(ctx);
        String funcName =
            ctx.singleFieldRelevanceFunctionName().getText().toLowerCase(Locale.ROOT);
        if ("wildcard_query".equals(funcName) || "wildcardquery".equals(funcName)) {
          return convertWildcardPattern((Function) result);
        }
        return result;
      }

      /** Convert SQL wildcards (%/_) to OpenSearch wildcards ( * /?) in wildcard_query pattern. */
      private Function convertWildcardPattern(Function func) {
        List<UnresolvedExpression> args = func.getFuncArgs();
        for (int i = 0; i < args.size(); i++) {
          if (args.get(i) instanceof UnresolvedArgument arg && "query".equals(arg.getArgName())) {
            if (arg.getValue() instanceof Literal lit && lit.getValue() instanceof String query) {
              String converted = sqlWildcardToLucene(query);
              if (!converted.equals(query)) {
                List<UnresolvedExpression> newArgs = new java.util.ArrayList<>(args);
                newArgs.set(
                    i,
                    new UnresolvedArgument(
                        "query", new Literal(converted, DataType.STRING)));
                return new Function(func.getFuncName(), newArgs);
              }
            }
          }
        }
        return func;
      }

      /** Converts SQL wildcards (% → *, _ → ?) respecting backslash escaping. */
      private String sqlWildcardToLucene(String text) {
        StringBuilder sb = new StringBuilder(text.length());
        boolean escaped = false;
        for (char c : text.toCharArray()) {
          if (c == '\\') {
            escaped = true;
            sb.append(c);
          } else if (c == '%' && !escaped) {
            sb.append('*');
          } else if (c == '_' && !escaped) {
            sb.append('?');
          } else {
            if (escaped && (c == '%' || c == '_')) {
              sb.deleteCharAt(sb.length() - 1); // remove escape char
            }
            sb.append(c);
            escaped = false;
          }
        }
        return sb.toString();
      }

      @Override
      public UnresolvedExpression visitInSubqueryPredicate(InSubqueryPredicateContext ctx) {
        UnresolvedPlan subquery = ExtendedAstBuilder.this.visit(ctx.querySpecification());
        UnresolvedExpression inExpr = inSubquery(subquery, visit(ctx.predicate()));
        return (ctx.NOT() != null) ? new Not(inExpr) : inExpr;
      }

      @Override
      public UnresolvedExpression visitExistsSubqueryExpressionAtom(
          ExistsSubqueryExpressionAtomContext ctx) {
        UnresolvedPlan subquery = ExtendedAstBuilder.this.visit(ctx.querySpecification());
        return existsSubquery(subquery);
      }
    }
  }
}
