/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api;

import java.util.List;
import org.antlr.v4.runtime.tree.ParseTree;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.config.Lex;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.apache.calcite.tools.Program;
import org.apache.calcite.tools.Programs;
import org.opensearch.sql.ast.statement.Query;
import org.opensearch.sql.ast.statement.Statement;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.calcite.CalciteRelNodeVisitor;
import org.opensearch.sql.common.antlr.Parser;
import org.opensearch.sql.common.antlr.SyntaxCheckException;
import org.opensearch.sql.executor.QueryType;
import org.opensearch.sql.ppl.antlr.PPLSyntaxParser;
import org.opensearch.sql.ppl.parser.AstBuilder;
import org.opensearch.sql.ppl.parser.AstStatementBuilder;

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

  /** AST-to-RelNode visitor that builds logical plans from the parsed AST. */
  private final CalciteRelNodeVisitor relNodeVisitor =
      new CalciteRelNodeVisitor(new EmptyDataSourceService());

  /**
   * Constructs a UnifiedQueryPlanner with a unified query context.
   *
   * @param context the unified query context containing CalcitePlanContext
   */
  public UnifiedQueryPlanner(UnifiedQueryContext context) {
    this.parser =
        context.getPlanContext().queryType == QueryType.SQL
            ? null
            : buildQueryParser(context.getPlanContext().queryType);
    this.context = context;
  }

  /**
   * Parses and analyzes a query string into a Calcite logical plan (RelNode). TODO: Generate
   * optimal physical plan to fully unify query execution and leverage Calcite's optimizer.
   *
   * @param query the raw query string in PPL, SQL, or other supported syntax
   * @return a logical plan representing the query
   */
  public RelNode plan(String query) {
    try {
      if (context.getPlanContext().queryType == QueryType.SQL) {
        return planWithCalcite(query);
      }
      return preserveCollation(analyze(parse(query)));
    } catch (SyntaxCheckException e) {
      // Re-throw syntax error without wrapping
      throw e;
    } catch (Exception e) {
      throw new IllegalStateException("Failed to plan query", e);
    }
  }

  /**
   * Optimizes a logical plan using the VolcanoPlanner with rules registered by the schema's table
   * scan nodes. Adapter-specific pushdown rules (filter, project, aggregate) are applied here.
   *
   * @param logical the logical plan from {@link #plan(String)}
   * @return an optimized plan with adapter-specific physical operators
   */
  public RelNode optimize(RelNode logical) {
    try {
      RelTraitSet targetTraits = logical.getCluster().traitSetOf(EnumerableConvention.INSTANCE);
      Program program = Programs.standard();
      // Create a fresh VolcanoPlanner to avoid state conflicts with the HepPlanner from plan()
      return program.run(
          logical.getCluster().getPlanner(), logical, targetTraits, List.of(), List.of());
    } catch (Exception e) {
      throw new IllegalStateException("Failed to optimize plan", e);
    }
  }

  private RelNode planWithCalcite(String query) throws Exception {
    CalcitePlanContext planContext = context.getPlanContext();
    FrameworkConfig sqlConfig =
        Frameworks.newConfigBuilder(planContext.config)
            .parserConfig(SqlParser.config().withLex(Lex.JAVA))
            .build();
    Planner planner = Frameworks.getPlanner(sqlConfig);
    try {
      SqlNode parsed = planner.parse(query);
      SqlNode validated = planner.validate(parsed);
      RelRoot relRoot = planner.rel(validated);
      return relRoot.rel;
    } finally {
      planner.close();
    }
  }

  private Parser buildQueryParser(QueryType queryType) {
    if (queryType == QueryType.PPL) {
      return new PPLSyntaxParser();
    }
    throw new IllegalArgumentException("Unsupported query type: " + queryType);
  }

  private UnresolvedPlan parse(String query) {
    ParseTree cst = parser.parse(query);
    AstStatementBuilder astStmtBuilder =
        new AstStatementBuilder(
            new AstBuilder(query, context.getSettings()),
            AstStatementBuilder.StatementBuilderContext.builder().build());
    Statement statement = cst.accept(astStmtBuilder);

    if (statement instanceof Query) {
      return ((Query) statement).getPlan();
    }
    throw new UnsupportedOperationException(
        "Only query statements are supported but got " + statement.getClass().getSimpleName());
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
