/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.routing;

import java.util.Map;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.opensearch.analytics.plan.BoundaryScan;
import org.opensearch.analytics.schema.ParquetTable;
import org.opensearch.analytics.spi.ParquetEngineCapabilities;
import org.opensearch.sql.api.UnifiedQueryContext;
import org.opensearch.sql.api.UnifiedQueryPlanner;
import org.opensearch.sql.executor.QueryType;

/** Plans PPL queries against mock Parquet schema with BoundaryScan absorption. */
public class ParquetQueryPlanner {

  private ParquetQueryPlanner() {}

  /** Parses PPL, generates RelNode, replaces scans with BoundaryScan, and runs absorption. */
  public static RelNode plan(String pplQuery) throws Exception {
    AbstractSchema schema =
        new AbstractSchema() {
          @Override
          protected Map<String, Table> getTableMap() {
            return Map.of("parquet_index", new ParquetTable());
          }
        };

    UnifiedQueryContext context =
        UnifiedQueryContext.builder()
            .language(QueryType.PPL)
            .catalog("parquet", schema)
            .defaultNamespace("parquet")
            .build();
    try {
      RelNode relNode = new UnifiedQueryPlanner(context).plan(pplQuery);
      return optimize(replaceScanWithBoundary(relNode));
    } finally {
      context.close();
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
