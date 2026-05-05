/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api;

import static org.opensearch.sql.monitor.profile.MetricName.ANALYZE;

import java.util.Properties;
import org.apache.calcite.adapter.java.JavaTypeFactory;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable.ViewExpander;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.CalciteSqlValidator;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.RelBuilder;
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
   * Parses and analyzes a query string into a Calcite logical plan (RelNode). TODO: Generate
   * optimal physical plan to fully unify query execution and leverage Calcite's optimizer.
   *
   * @param query the raw query string in PPL or SQL syntax
   * @return a logical plan representing the query
   */
  public RelNode plan(String query) {
    try {
      return context.measure(ANALYZE, () -> strategy.plan(query));
    } catch (SyntaxCheckException | UnsupportedOperationException e) {
      throw e;
    } catch (Exception e) {
      throw new IllegalStateException("Failed to plan query", e);
    }
  }

  /** Strategy interface for language-specific planning logic. */
  private interface PlanningStrategy {
    RelNode plan(String query) throws Exception;
  }

  /**
   * SQL planning using a custom validate+convert pipeline. Consumes pre-parsed SqlNode from {@link
   * UnifiedQueryParser} and applies validation with a conformance that disables non-strict GROUP BY
   * (preventing unwanted ANY_VALUE wrapping and NPE on CASE expressions).
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
      return validateAndConvert(parsed);
    }

    private RelNode validateAndConvert(SqlNode parsed) {
      FrameworkConfig config = context.getPlanContext().config;

      SchemaPlus defaultSchema = config.getDefaultSchema();
      SchemaPlus rootSchema = rootSchema(defaultSchema);
      JavaTypeFactory typeFactory = new JavaTypeFactoryImpl(config.getTypeSystem());

      CalciteConnectionConfigImpl connectionConfig = buildConnectionConfig(config);
      CalciteCatalogReader catalogReader =
          new CalciteCatalogReader(
              CalciteSchema.from(rootSchema),
              CalciteSchema.from(defaultSchema).path(null),
              typeFactory,
              connectionConfig);

      SqlOperatorTable opTab = SqlOperatorTables.chain(config.getOperatorTable(), catalogReader);
      SqlValidator validator =
          new CalciteSqlValidator(
              opTab,
              catalogReader,
              typeFactory,
              config
                  .getSqlValidatorConfig()
                  .withDefaultNullCollation(connectionConfig.defaultNullCollation())
                  .withLenientOperatorLookup(connectionConfig.lenientOperatorLookup())
                  .withIdentifierExpansion(true));

      SqlNode validated = validator.validate(parsed);

      RelOptPlanner planner = new VolcanoPlanner(config.getCostFactory(), Contexts.empty());
      RelOptUtil.registerDefaultRules(planner, false, false);
      planner.addRelTraitDef(ConventionTraitDef.INSTANCE);

      RexBuilder rexBuilder = new RexBuilder(typeFactory);
      RelOptCluster cluster = RelOptCluster.create(planner, rexBuilder);

      SqlToRelConverter.Config converterConfig =
          config.getSqlToRelConverterConfig().withTrimUnusedFields(false);
      SqlToRelConverter sqlToRelConverter =
          new SqlToRelConverter(
              NOOP_VIEW_EXPANDER,
              validator,
              catalogReader,
              cluster,
              config.getConvertletTable(),
              converterConfig);

      RelRoot root = sqlToRelConverter.convertQuery(validated, false, true);
      root = root.withRel(sqlToRelConverter.flattenTypes(root.rel, true));

      RelBuilder relBuilder = converterConfig.getRelBuilderFactory().create(cluster, null);
      root = root.withRel(RelDecorrelator.decorrelateQuery(root.rel, relBuilder));
      return root.project();
    }

    private static CalciteConnectionConfigImpl buildConnectionConfig(FrameworkConfig config) {
      Properties props = new Properties();
      props.setProperty(
          CalciteConnectionProperty.CASE_SENSITIVE.camelName(),
          String.valueOf(config.getParserConfig().caseSensitive()));
      return new CalciteConnectionConfigImpl(props);
    }

    private static SchemaPlus rootSchema(SchemaPlus schema) {
      for (; ; ) {
        SchemaPlus parent = schema.getParentSchema();
        if (parent == null) {
          return schema;
        }
        schema = parent;
      }
    }

    private static final ViewExpander NOOP_VIEW_EXPANDER =
        (rowType, queryString, schemaPath, viewPath) -> {
          throw new UnsupportedOperationException("Views not supported");
        };
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
