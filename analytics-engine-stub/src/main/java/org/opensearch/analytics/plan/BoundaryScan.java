/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.analytics.plan;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptPlanner;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelWriter;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.opensearch.analytics.plan.rules.AggregateAbsorptionRule;
import org.opensearch.analytics.plan.rules.FilterAbsorptionRule;
import org.opensearch.analytics.plan.rules.ProjectAbsorptionRule;
import org.opensearch.analytics.plan.rules.SortAbsorptionRule;
import org.opensearch.analytics.spi.EngineCapabilities;

/** Custom TableScan that tracks absorbed operators for pushdown to an analytics engine. */
public class BoundaryScan extends TableScan {

  private final EngineCapabilities capabilities;
  private final List<RelNode> absorbedOperators;

  public BoundaryScan(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelOptTable table,
      EngineCapabilities capabilities) {
    this(cluster, traitSet, table, capabilities, Collections.emptyList());
  }

  private BoundaryScan(
      RelOptCluster cluster,
      RelTraitSet traitSet,
      RelOptTable table,
      EngineCapabilities capabilities,
      List<RelNode> absorbedOperators) {
    super(cluster, traitSet, Collections.emptyList(), table);
    this.capabilities = capabilities;
    this.absorbedOperators = Collections.unmodifiableList(new ArrayList<>(absorbedOperators));
  }

  public static BoundaryScan create(
      RelOptCluster cluster, RelOptTable table, EngineCapabilities capabilities) {
    return new BoundaryScan(cluster, cluster.traitSet(), table, capabilities);
  }

  /** Returns a new BoundaryScan with the given operator added to the absorbed list. */
  public BoundaryScan absorb(RelNode operator) {
    List<RelNode> newAbsorbed = new ArrayList<>(absorbedOperators);
    newAbsorbed.add(operator);
    return new BoundaryScan(getCluster(), getTraitSet(), getTable(), capabilities, newAbsorbed);
  }

  public List<RelNode> getAbsorbedOperators() {
    return absorbedOperators;
  }

  public EngineCapabilities getCapabilities() {
    return capabilities;
  }

  @Override
  public RelWriter explainTerms(RelWriter pw) {
    return super.explainTerms(pw).item("absorbed", absorbedOperators.size());
  }

  @Override
  public RelDataType deriveRowType() {
    if (!absorbedOperators.isEmpty()) {
      return absorbedOperators.get(absorbedOperators.size() - 1).getRowType();
    }
    return super.deriveRowType();
  }

  @Override
  public void register(RelOptPlanner planner) {
    planner.addRule(FilterAbsorptionRule.Config.DEFAULT.toRule());
    planner.addRule(ProjectAbsorptionRule.Config.DEFAULT.toRule());
    planner.addRule(SortAbsorptionRule.Config.DEFAULT.toRule());
    planner.addRule(AggregateAbsorptionRule.Config.DEFAULT.toRule());
  }

  @Override
  public RelNode copy(RelTraitSet traitSet, List<RelNode> inputs) {
    return new BoundaryScan(getCluster(), traitSet, getTable(), capabilities, absorbedOperators);
  }
}
