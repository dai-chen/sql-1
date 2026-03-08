/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api;

import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.opensearch.sql.ast.statement.Query;
import org.opensearch.sql.ast.statement.Statement;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.calcite.CalciteRelNodeVisitor;
import org.opensearch.sql.calcite.parser.StarExceptReplaceRewriter;
import org.opensearch.sql.common.antlr.Parser;
import org.opensearch.sql.common.antlr.SyntaxCheckException;
import org.opensearch.sql.executor.QueryType;
import org.opensearch.sql.ppl.antlr.PPLSyntaxParser;
import org.opensearch.sql.sql.antlr.SQLSyntaxParser;

/**
 * {@code UnifiedQueryPlanner} provides a high-level API for parsing and analyzing queries using the
 * Calcite-based query engine. It serves as the primary integration point for external consumers
 * such as Spark or command-line tools, abstracting away Calcite internals.
 */
public class UnifiedQueryPlanner {
  /** The parser instance responsible for converting query text into a parse tree. */
  private final Parser parser;

  /** Unified query context containing CalcitePlanContext with all configuration. */
  private final UnifiedQueryContext context;

  /** Whether to use Calcite's native SQL parser instead of the ANTLR-based parser. */
  private final boolean useCalciteParser;

  /** AST-to-RelNode visitor that builds logical plans from the parsed AST. */
  private final CalciteRelNodeVisitor relNodeVisitor =
      new CalciteRelNodeVisitor(new EmptyDataSourceService());

  /**
   * Constructs a UnifiedQueryPlanner with a unified query context.
   *
   * @param context the unified query context containing CalcitePlanContext
   */
  public UnifiedQueryPlanner(UnifiedQueryContext context) {
    this.context = context;
    this.useCalciteParser =
        context.getPlanContext().queryType == QueryType.SQL && context.getConformance() != null;
    this.parser = useCalciteParser ? null : buildQueryParser(context.getPlanContext().queryType);
  }

  /**
   * Parses and analyzes a query string into a Calcite logical plan (RelNode).
   *
   * @param query the raw query string in PPL or SQL syntax
   * @return a logical plan representing the query
   */
  public RelNode plan(String query) {
    try {
      if (useCalciteParser) {
        return planWithCalcite(query);
      }
      return preserveCollation(analyze(parse(query)));
    } catch (SyntaxCheckException e) {
      throw e;
    } catch (Exception e) {
      throw new IllegalStateException("Failed to plan query", e);
    }
  }

  private Parser buildQueryParser(QueryType queryType) {
    return switch (queryType) {
      case PPL -> new PPLSyntaxParser();
      case SQL -> new SQLSyntaxParser();
    };
  }

  private UnresolvedPlan parse(String query) {
    ParseTree cst = parser.parse(query);
    Statement statement =
        switch (context.getPlanContext().queryType) {
          case PPL -> {
            var astBuilder =
                new org.opensearch.sql.ppl.parser.AstBuilder(query, context.getSettings());
            var stmtBuilder =
                new org.opensearch.sql.ppl.parser.AstStatementBuilder(
                    astBuilder,
                    org.opensearch.sql.ppl.parser.AstStatementBuilder.StatementBuilderContext
                        .builder()
                        .build());
            yield cst.accept(stmtBuilder);
          }
          case SQL -> {
            var astBuilder = new org.opensearch.sql.sql.parser.AstBuilder(query);
            var stmtBuilder =
                new org.opensearch.sql.sql.parser.AstStatementBuilder(
                    astBuilder,
                    org.opensearch.sql.sql.parser.AstStatementBuilder.StatementBuilderContext
                        .builder()
                        .build());
            yield cst.accept(stmtBuilder);
          }
        };

    if (statement instanceof Query) {
      return ((Query) statement).getPlan();
    }
    throw new UnsupportedOperationException(
        "Only query statements are supported but got " + statement.getClass().getSimpleName());
  }

  private RelNode planWithCalcite(String query) {
    try {
      Planner planner = Frameworks.getPlanner(context.getPlanContext().config);
      SqlNode parsed = planner.parse(query);

      // Rewrite EXCEPT/REPLACE before validation
      parsed = new StarExceptReplaceRewriter(
          context.getPlanContext().config.getDefaultSchema()).rewrite(parsed);

      SqlNode validated = planner.validate(parsed);
      RelRoot relRoot = planner.rel(validated);
      planner.close();

      // Run AggregateCaseToFilterRule to rewrite SUM(CASE WHEN cond THEN NULL ELSE expr END)
      // into SUM(expr) FILTER(WHERE NOT cond) for native OpenSearch filter aggregation pushdown
      RelNode rel = relRoot.rel;
      org.apache.calcite.plan.hep.HepPlanner hepPlanner =
          new org.apache.calcite.plan.hep.HepPlanner(
              new org.apache.calcite.plan.hep.HepProgramBuilder()
                  .addRuleInstance(org.apache.calcite.rel.rules.CoreRules.AGGREGATE_CASE_TO_FILTER)
                  .build());
      hepPlanner.setRoot(rel);
      return hepPlanner.findBestExp();
    } catch (Exception e) {
      throw new IllegalStateException("Failed to plan SQL query", e);
    }
  }

  private RelNode analyze(UnresolvedPlan ast) {
    return relNodeVisitor.analyze(ast, context.getPlanContext());
  }

  private RelNode preserveCollation(RelNode logical) {
    RelNode calcitePlan = logical;
    RelCollation collation = logical.getTraitSet().getCollation();
    if (!(logical instanceof Sort) && collation != RelCollations.EMPTY) {
      calcitePlan = LogicalSort.create(logical, collation, null, null);
    }
    return calcitePlan;
  }
}
