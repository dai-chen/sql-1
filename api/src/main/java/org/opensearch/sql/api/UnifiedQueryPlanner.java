/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api;

import static org.opensearch.sql.monitor.profile.MetricName.ANALYZE;

import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.opensearch.sql.api.parser.UnifiedQueryParser;
import org.opensearch.sql.ast.tree.UnresolvedPlan;
import org.opensearch.sql.calcite.CalciteRelNodeVisitor;
import org.opensearch.sql.common.antlr.SyntaxCheckException;
import org.opensearch.sql.executor.QueryType;

/**
 * {@code UnifiedQueryPlanner} provides a high-level API for parsing and analyzing queries using the
 * Calcite-based query engine. It serves as the primary integration point for external consumers
 * such as Spark or command-line tools, abstracting away Calcite internals.
 */
public class UnifiedQueryPlanner {

  /** Planning strategy selected at construction time based on query type. */
  private final PlanningStrategy strategy;

  /** Unified query context for profiling support. */
  private final UnifiedQueryContext context;

  /**
   * Constructs a UnifiedQueryPlanner with a unified query context.
   *
   * @param context the unified query context containing CalcitePlanContext
   */
  public UnifiedQueryPlanner(UnifiedQueryContext context) {
    this.context = context;
    this.strategy =
        context.getPlanContext().queryType == QueryType.SQL
            ? new CalciteNativeStrategy(context)
            : new CustomVisitorStrategy(context);
  }

  /**
   * Constructs a UnifiedQueryPlanner that uses the custom visitor strategy (V2 parser frontend)
   * with the given parser, regardless of query type. This allows SQL queries to be parsed by the V2
   * ANTLR parser and converted to RelNode via CalciteRelNodeVisitor.
   *
   * @param context the unified query context
   * @param v2Parser a parser that produces UnresolvedPlan (e.g., SQL V2 ANTLR parser)
   */
  public UnifiedQueryPlanner(
      UnifiedQueryContext context, UnifiedQueryParser<UnresolvedPlan> v2Parser) {
    this.context = context;
    this.strategy = new CustomVisitorStrategy(context, v2Parser);
  }

  /**
   * Parses and analyzes a query string into a Calcite logical plan (RelNode). TODO: Generate
   * optimal physical plan to fully unify query execution and leverage Calcite's optimizer.
   *
   * @param query the raw query string in PPL or SQL syntax
   * @return a logical plan representing the query
   */
  public RelNode plan(String query) {
    try {
      return context.measure(
          ANALYZE,
          () -> {
            RelNode plan = strategy.plan(query);
            for (var shuttle : context.getLangSpec().postAnalysisRules()) {
              plan = plan.accept(shuttle);
            }
            return plan;
          });
    } catch (SyntaxCheckException | UnsupportedOperationException e) {
      throw e;
    } catch (Exception e) {
      throw new IllegalStateException("Failed to plan query: " + e.getMessage(), e);
    }
  }

  /** Strategy interface for language-specific planning logic. */
  private interface PlanningStrategy {
    RelNode plan(String query) throws Exception;
  }

  /**
   * SQL planning using a custom validate+convert pipeline. Consumes pre-parsed SqlNode from
   * UnifiedQueryParser and validates with BABEL_STRICT_GROUP_BY conformance (no ANY_VALUE wrapping,
   * no NPE on CASE in GROUP BY).
   */
  private static class CalciteNativeStrategy implements PlanningStrategy {
    private final UnifiedQueryContext context;
    private final UnifiedQueryParser<SqlNode> parser;

    @SuppressWarnings("unchecked")
    CalciteNativeStrategy(UnifiedQueryContext context) {
      this.context = context;
      this.parser = (UnifiedQueryParser<SqlNode>) context.getParser();
    }

    @Override
    public RelNode plan(String query) throws Exception {
      SqlNode parsed = parser.parse(query);
      if (!parsed.isA(SqlKind.QUERY)) {
        throw new UnsupportedOperationException(
            "Only query statements are supported. Got: " + parsed.getKind());
      }
      try (CalcitePlanner planner = new CalcitePlanner(context.getPlanContext().config)) {
        SqlNode validated = planner.validate(parsed);
        return planner.convert(validated);
      }
    }
  }

  /** AST-based planning via context-owned parser → UnresolvedPlan → CalciteRelNodeVisitor. */
  private static class CustomVisitorStrategy implements PlanningStrategy {
    private final UnifiedQueryContext context;
    private final UnifiedQueryParser<UnresolvedPlan> parser;
    private final CalciteRelNodeVisitor relNodeVisitor =
        new CalciteRelNodeVisitor(new EmptyDataSourceService());

    @SuppressWarnings("unchecked")
    CustomVisitorStrategy(UnifiedQueryContext context) {
      this.context = context;
      this.parser = (UnifiedQueryParser<UnresolvedPlan>) context.getParser();
    }

    CustomVisitorStrategy(UnifiedQueryContext context, UnifiedQueryParser<UnresolvedPlan> parser) {
      this.context = context;
      this.parser = parser;
    }

    @Override
    public RelNode plan(String query) {
      UnresolvedPlan ast = parser.parse(query);
      RelNode logical = relNodeVisitor.analyze(ast, context.getPlanContext());
      return preserveCollation(logical);
    }

    private RelNode preserveCollation(RelNode logical) {
      RelCollation collation = logical.getTraitSet().getCollation();
      if (!(logical instanceof Sort) && collation != RelCollations.EMPTY) {
        return LogicalSort.create(logical, collation, null, null);
      }
      return logical;
    }
  }
}
