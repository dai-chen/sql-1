/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api;

import static org.apache.calcite.sql.type.SqlTypeName.INTEGER;
import static org.apache.calcite.sql.type.SqlTypeName.VARCHAR;
import static org.apache.calcite.test.Matchers.hasTree;
import static org.hamcrest.MatcherAssert.assertThat;

import java.util.List;
import java.util.Map;
import org.apache.calcite.adapter.enumerable.EnumerableConvention;
import org.apache.calcite.adapter.enumerable.EnumerableRel;
import org.apache.calcite.adapter.enumerable.EnumerableRelImplementor;
import org.apache.calcite.adapter.enumerable.PhysType;
import org.apache.calcite.adapter.enumerable.PhysTypeImpl;
import org.apache.calcite.linq4j.tree.Blocks;
import org.apache.calcite.linq4j.tree.Expressions;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptRule;
import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.sql.executor.QueryType;

/**
 * Demonstrates that {@link UnifiedQueryPlanner#optimize(RelNode)} triggers the VolcanoPlanner with
 * adapter-specific pushdown rules. Simulates a real Analytics engine that provides a custom
 * TableScan with a filter absorption rule.
 */
public class UnifiedQueryPlannerOptimizeTest {

  private UnifiedQueryContext context;
  private UnifiedQueryPlanner planner;

  @Before
  public void setUp() {
    AbstractSchema testSchema =
        new AbstractSchema() {
          @Override
          protected Map<String, Table> getTableMap() {
            return Map.of("test_table", new EngineTable());
          }
        };
    context =
        UnifiedQueryContext.builder()
            .language(QueryType.SQL)
            .catalog("catalog", testSchema)
            .build();
    planner = new UnifiedQueryPlanner(context);
  }

  @After
  public void tearDown() throws Exception {
    if (context != null) {
      context.close();
    }
  }

  @Test
  public void optimizePushesFilterIntoEngineScan() {
    RelNode logical = planner.plan("SELECT name FROM catalog.test_table WHERE id > 10");

    // Before: EngineTableScan (from TranslatableTable.toRel) with LogicalFilter on top
    assertThat(
        logical,
        hasTree(
            """
            LogicalProject(name=[$1])
              LogicalFilter(condition=[>($0, 10)])
                EngineTableScan(table=[[catalog, test_table]])
            """));

    // After: filter absorbed into EngineTableScan by the pushdown rule
    RelNode optimized = planner.optimize(logical);
    assertThat(
        optimized,
        hasTree(
            """
            EnumerableCalc(expr#0..1=[{inputs}], name=[$t1])
              EngineTableScan(table=[[catalog, test_table]], filter=[>($0, 10)])
            """));
  }

  // --- Simulated Analytics engine adapter ---

  /** Table that produces EngineTableScan via toRel(). */
  static class EngineTable extends AbstractTable
      implements org.apache.calcite.schema.TranslatableTable {
    @Override
    public RelDataType getRowType(RelDataTypeFactory typeFactory) {
      return typeFactory.builder().add("id", INTEGER).add("name", VARCHAR).build();
    }

    @Override
    public RelNode toRel(RelOptTable.ToRelContext ctx, RelOptTable table) {
      return new EngineTableScan(ctx.getCluster(), table, null);
    }
  }

  /** Engine-specific scan node with filter absorption. */
  static class EngineTableScan extends TableScan implements EnumerableRel {
    private final RexNode filter;

    EngineTableScan(RelOptCluster cluster, RelOptTable table, RexNode filter) {
      super(cluster, cluster.traitSetOf(EnumerableConvention.INSTANCE), List.of(), table);
      this.filter = filter;
    }

    @Override
    public void register(RelOptPlanner planner) {
      planner.addRule(EngineFilterAbsorptionRule.INSTANCE);
    }

    @Override
    public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
      return new EngineTableScan(getCluster(), getTable(), filter);
    }

    @Override
    public RelWriter explainTerms(RelWriter pw) {
      return super.explainTerms(pw).itemIf("filter", filter, filter != null);
    }

    @Override
    public Result implement(EnumerableRelImplementor implementor, Prefer pref) {
      PhysType physType =
          PhysTypeImpl.of(implementor.getTypeFactory(), getRowType(), pref.preferArray());
      return implementor.result(
          physType, Blocks.toBlock(Expressions.call(Expressions.constant(List.of()), "iterator")));
    }
  }

  /** Absorbs LogicalFilter into EngineTableScan. */
  static class EngineFilterAbsorptionRule extends RelOptRule {
    static final EngineFilterAbsorptionRule INSTANCE = new EngineFilterAbsorptionRule();

    EngineFilterAbsorptionRule() {
      super(operand(LogicalFilter.class, operand(EngineTableScan.class, none())));
    }

    @Override
    public void onMatch(RelOptRuleCall call) {
      LogicalFilter filter = call.rel(0);
      EngineTableScan scan = call.rel(1);
      call.transformTo(
          new EngineTableScan(scan.getCluster(), scan.getTable(), filter.getCondition()));
    }
  }
}
