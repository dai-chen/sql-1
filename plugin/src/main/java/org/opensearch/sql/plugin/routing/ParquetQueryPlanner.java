/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.routing;

import java.util.Map;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelRoot;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.tools.FrameworkConfig;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.Planner;
import org.opensearch.analytics.plan.BoundaryScan;
import org.opensearch.analytics.plan.UnsupportedFunctionValidator;
import org.opensearch.analytics.schema.ParquetTable;
import org.opensearch.analytics.spi.ParquetEngineCapabilities;
import org.opensearch.sql.api.UnifiedQueryContext;
import org.opensearch.sql.api.UnifiedQueryPlanner;
import org.opensearch.sql.executor.QueryType;

/** Plans PPL and SQL queries against mock Parquet schema with BoundaryScan absorption. */
public class ParquetQueryPlanner {

  private ParquetQueryPlanner() {}

  private static AbstractSchema createParquetSchema() {
    return new AbstractSchema() {
      @Override
      protected Map<String, Table> getTableMap() {
        return Map.of("parquet_index", new ParquetTable());
      }
    };
  }

  /** Parses PPL, generates RelNode, replaces scans with BoundaryScan, and runs absorption. */
  public static RelNode plan(String pplQuery) throws Exception {
    AbstractSchema schema = createParquetSchema();

    UnifiedQueryContext context =
        UnifiedQueryContext.builder()
            .language(QueryType.PPL)
            .catalog("parquet", schema)
            .defaultNamespace("parquet")
            .build();
    try {
      RelNode relNode = new UnifiedQueryPlanner(context).plan(pplQuery);
      UnsupportedFunctionValidator.validate(relNode, new ParquetEngineCapabilities());
      return optimize(replaceScanWithBoundary(relNode));
    } finally {
      context.close();
    }
  }

  /**
   * Parses ANSI SQL via Calcite's native Planner, replaces scans with BoundaryScan, and runs
   * absorption.
   */
  public static RelNode planSql(String sqlQuery) throws Exception {
    AbstractSchema schema = createParquetSchema();
    SchemaPlus rootSchema = CalciteSchema.createRootSchema(false).plus();
    SchemaPlus parquetSchema = rootSchema.add("parquet", schema);

    FrameworkConfig config =
        Frameworks.newConfigBuilder()
            .parserConfig(SqlParser.config().withLex(Lex.MYSQL))
            .defaultSchema(parquetSchema)
            .build();
    Planner planner = Frameworks.getPlanner(config);
    try {
      SqlNode parsed = planner.parse(sqlQuery);
      SqlNode validated = planner.validate(parsed);
      RelRoot relRoot = planner.rel(validated);
      RelNode relNode = relRoot.rel;
      UnsupportedFunctionValidator.validate(relNode, new ParquetEngineCapabilities());
      return optimize(replaceScanWithBoundary(relNode));
    } finally {
      planner.close();
    }
  }

  /** Formats a RelNode plan as a JSON explain string. */
  public static String formatExplain(RelNode relNode) {
    String plan = RelOptUtil.toString(relNode);
    return "{\"Parquet\":{\"plan\":\""
        + plan.replace("\\", "\\\\").replace("\"", "\\\"").replace("\n", "\\n")
        + "\"}}";
  }

  private static RelNode replaceScanWithBoundary(RelNode relNode) {
    return relNode.accept(
        new RelShuttleImpl() {
          @Override
          public RelNode visit(TableScan scan) {
            return BoundaryScan.create(
                scan.getCluster(), scan.getTable(), new ParquetEngineCapabilities());
          }
        });
  }

  private static RelNode optimize(RelNode relNode) {
    HepProgramBuilder builder = new HepProgramBuilder();
    builder.addRuleInstance(
        org.opensearch.analytics.plan.rules.FilterAbsorptionRule.Config.DEFAULT.toRule());
    builder.addRuleInstance(
        org.opensearch.analytics.plan.rules.ProjectAbsorptionRule.Config.DEFAULT.toRule());
    builder.addRuleInstance(
        org.opensearch.analytics.plan.rules.SortAbsorptionRule.Config.DEFAULT.toRule());
    builder.addRuleInstance(
        org.opensearch.analytics.plan.rules.AggregateAbsorptionRule.Config.DEFAULT.toRule());
    HepPlanner planner = new HepPlanner(builder.build());
    planner.setRoot(relNode);
    return planner.findBestExp();
  }
}
