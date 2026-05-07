/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api;

import java.util.Properties;
import lombok.Getter;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.config.CalciteConnectionProperty;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.plan.Contexts;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable.ViewExpander;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.prepare.CalciteSqlValidator;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.sql2rel.SqlToRelConverter;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.RelBuilder;

/**
 * Custom Calcite planner that separates validate and convert phases with independent conformance
 * control. Unlike Calcite's built-in {@code PlannerImpl} which couples validator conformance to
 * parser config via {@code connectionConfig.conformance()}.
 *
 * <p>Usage:
 *
 * <pre>{@code
 * try (CalcitePlanner planner = new CalcitePlanner(frameworkConfig)) {
 *   SqlNode validated = planner.validate(parsedSqlNode);
 *   RelNode relNode = planner.convert(validated);
 * }
 * }</pre>
 */
public class CalcitePlanner implements AutoCloseable {

  @Getter private final SqlValidator validator;
  private final CalciteCatalogReader catalogReader;
  private final RelOptCluster cluster;
  private final SqlToRelConverter.Config converterConfig;
  private final FrameworkConfig config;

  public CalcitePlanner(FrameworkConfig config) {
    this.config = config;
    var typeFactory = new JavaTypeFactoryImpl(config.getTypeSystem());
    var connectionConfig = connectionConfig(config);

    this.catalogReader =
        new CalciteCatalogReader(
            CalciteSchema.from(rootSchema(config.getDefaultSchema())),
            CalciteSchema.from(config.getDefaultSchema()).path(null),
            typeFactory,
            connectionConfig);

    // Key difference from PlannerImpl: we use config.getSqlValidatorConfig() directly
    // without overriding conformance from connectionConfig.
    var opTab = SqlOperatorTables.chain(config.getOperatorTable(), catalogReader);
    this.validator =
        new CalciteSqlValidator(
            opTab,
            catalogReader,
            typeFactory,
            config
                .getSqlValidatorConfig()
                .withDefaultNullCollation(connectionConfig.defaultNullCollation())
                .withLenientOperatorLookup(connectionConfig.lenientOperatorLookup())
                .withIdentifierExpansion(true));

    // Planner + cluster (same as PlannerImpl.ready())
    // RelOptCluster requires a RelOptPlanner (Calcite API constraint). VolcanoPlanner is not
    // used for optimization here — it will be reused when the unified query optimizer is added.
    var volcanoPlanner = new VolcanoPlanner(config.getCostFactory(), Contexts.empty());
    RelOptUtil.registerDefaultRules(volcanoPlanner, false, false);
    volcanoPlanner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    volcanoPlanner.addRelTraitDef(RelCollationTraitDef.INSTANCE);
    this.cluster = RelOptCluster.create(volcanoPlanner, new RexBuilder(typeFactory));

    this.converterConfig = config.getSqlToRelConverterConfig().withTrimUnusedFields(false);
  }

  /** Validate a parsed SqlNode. */
  public SqlNode validate(SqlNode parsed) {
    return validator.validate(parsed);
  }

  /** Convert a validated SqlNode to a logical RelNode. */
  public RelNode convert(SqlNode validated) {
    var converter =
        new SqlToRelConverter(
            NOOP_EXPANDER,
            validator,
            catalogReader,
            cluster,
            config.getConvertletTable(),
            converterConfig);

    RelRoot root = converter.convertQuery(validated, false, true);
    root = root.withRel(converter.flattenTypes(root.rel, true));
    RelBuilder relBuilder = converterConfig.getRelBuilderFactory().create(cluster, null);
    root = root.withRel(RelDecorrelator.decorrelateQuery(root.rel, relBuilder));
    // Flatten nested AND/OR in filter conditions to satisfy RexUtil.isFlat precondition
    // required by analytics-engine's LogicalFilter construction.
    root = root.withRel(flattenFilterConditions(root.rel));
    return root.project();
  }

  /** Flatten nested AND/OR in all Filter conditions so RexUtil.isFlat holds. */
  private RelNode flattenFilterConditions(RelNode rel) {
    RexBuilder rexBuilder = cluster.getRexBuilder();
    return rel.accept(
        new RelShuttleImpl() {
          @Override
          public RelNode visit(LogicalFilter filter) {
            RelNode newInput = filter.getInput().accept(this);
            RexNode condition = filter.getCondition();
            // Expand SEARCH operators (Sarg ranges) into individual comparisons,
            // then flatten nested AND/OR to satisfy RexUtil.isFlat precondition.
            RexNode expanded = RexUtil.expandSearch(rexBuilder, null, condition);
            RexNode flat = RexUtil.flatten(rexBuilder, expanded);
            return filter.copy(filter.getTraitSet(), newInput, flat);
          }

          @Override
          public RelNode visit(RelNode other) {
            // Handle non-LogicalFilter filter nodes (e.g., after decorrelation)
            if (other instanceof org.apache.calcite.rel.core.Filter f) {
              RelNode newInput = f.getInput().accept(this);
              RexNode condition = f.getCondition();
              RexNode expanded = RexUtil.expandSearch(rexBuilder, null, condition);
              RexNode flat = RexUtil.flatten(rexBuilder, expanded);
              return f.copy(f.getTraitSet(), newInput, flat);
            }
            return super.visit(other);
          }
        });
  }

  @Override
  public void close() {
    // No resources to release currently; exists for future extensibility.
  }

  private static CalciteConnectionConfigImpl connectionConfig(FrameworkConfig config) {
    var props = new Properties();
    props.setProperty(
        CalciteConnectionProperty.CASE_SENSITIVE.camelName(),
        String.valueOf(config.getParserConfig().caseSensitive()));
    return new CalciteConnectionConfigImpl(props);
  }

  private static SchemaPlus rootSchema(SchemaPlus schema) {
    while (schema.getParentSchema() != null) {
      schema = schema.getParentSchema();
    }
    return schema;
  }

  private static final ViewExpander NOOP_EXPANDER =
      (rowType, queryString, schemaPath, viewPath) -> {
        throw new UnsupportedOperationException("Views not supported");
      };
}
