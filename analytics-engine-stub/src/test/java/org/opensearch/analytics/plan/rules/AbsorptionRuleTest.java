/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.analytics.plan.rules;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.analytics.plan.BoundaryScan;
import org.opensearch.analytics.schema.MockParquetSchemaProvider;
import org.opensearch.analytics.spi.ParquetEngineCapabilities;

class AbsorptionRuleTest {

  private RelBuilder builder;
  private BoundaryScan boundaryScan;

  @BeforeEach
  void setUp() {
    SchemaPlus schema = new MockParquetSchemaProvider().buildSchema(null);
    builder = RelBuilder.create(Frameworks.newConfigBuilder().defaultSchema(schema).build());

    RelNode tableScan = builder.scan("parquet_index").build();
    RelOptTable table = ((TableScan) tableScan).getTable();
    RelOptCluster cluster = tableScan.getCluster();
    boundaryScan = BoundaryScan.create(cluster, table, new ParquetEngineCapabilities());
  }

  @Test
  void testFilterAbsorption() {
    RelNode tree =
        builder
            .push(boundaryScan)
            .filter(builder.equals(builder.field("status"), builder.literal(200)))
            .build();

    RelNode optimized = optimize(tree, FilterAbsorptionRule.Config.DEFAULT.toRule());

    assertInstanceOf(BoundaryScan.class, optimized);
    BoundaryScan result = (BoundaryScan) optimized;
    assertEquals(1, result.getAbsorbedOperators().size());
    assertInstanceOf(LogicalFilter.class, result.getAbsorbedOperators().get(0));
  }

  @Test
  void testProjectAbsorption() {
    RelNode tree =
        builder
            .push(boundaryScan)
            .project(builder.field("message"), builder.field("status"))
            .build();

    RelNode optimized = optimize(tree, ProjectAbsorptionRule.Config.DEFAULT.toRule());

    assertInstanceOf(BoundaryScan.class, optimized);
    BoundaryScan result = (BoundaryScan) optimized;
    assertEquals(1, result.getAbsorbedOperators().size());
    assertInstanceOf(LogicalProject.class, result.getAbsorbedOperators().get(0));
  }

  @Test
  void testSortAbsorption() {
    RelNode tree = builder.push(boundaryScan).sort(builder.field("status")).build();

    RelNode optimized = optimize(tree, SortAbsorptionRule.Config.DEFAULT.toRule());

    assertInstanceOf(BoundaryScan.class, optimized);
    BoundaryScan result = (BoundaryScan) optimized;
    assertEquals(1, result.getAbsorbedOperators().size());
    assertInstanceOf(LogicalSort.class, result.getAbsorbedOperators().get(0));
  }

  @Test
  void testAggregateAbsorption() {
    RelNode tree =
        builder
            .push(boundaryScan)
            .aggregate(builder.groupKey("status"), builder.count(false, "cnt"))
            .build();

    RelNode optimized = optimize(tree, AggregateAbsorptionRule.Config.DEFAULT.toRule());

    assertInstanceOf(BoundaryScan.class, optimized);
    BoundaryScan result = (BoundaryScan) optimized;
    assertEquals(1, result.getAbsorbedOperators().size());
    assertInstanceOf(LogicalAggregate.class, result.getAbsorbedOperators().get(0));
  }

  @Test
  void testUnsupportedOperatorStaysAboveBoundary() {
    RelNode tree =
        LogicalUnion.create(
            List.of(builder.push(boundaryScan).build(), builder.push(boundaryScan).build()), true);

    RelNode optimized =
        optimize(
            tree,
            FilterAbsorptionRule.Config.DEFAULT.toRule(),
            ProjectAbsorptionRule.Config.DEFAULT.toRule(),
            SortAbsorptionRule.Config.DEFAULT.toRule(),
            AggregateAbsorptionRule.Config.DEFAULT.toRule());

    assertInstanceOf(LogicalUnion.class, optimized);
  }

  private RelNode optimize(RelNode tree, org.apache.calcite.plan.RelOptRule... rules) {
    HepProgramBuilder programBuilder = new HepProgramBuilder();
    for (org.apache.calcite.plan.RelOptRule rule : rules) {
      programBuilder.addRuleInstance(rule);
    }
    HepPlanner planner = new HepPlanner(programBuilder.build());
    planner.setRoot(tree);
    return planner.findBestExp();
  }
}
