/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.analytics.plan.rules;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.logical.LogicalSort;
import org.opensearch.analytics.plan.BoundaryScan;

/** Absorbs a {@link LogicalSort} into a {@link BoundaryScan} if the engine supports it. */
public class SortAbsorptionRule extends RelRule<SortAbsorptionRule.Config> {

  protected SortAbsorptionRule(Config config) {
    super(config);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    LogicalSort sort = call.rel(0);
    BoundaryScan scan = call.rel(1);
    if (scan.getCapabilities().supportsOperator(LogicalSort.class)) {
      call.transformTo(scan.absorb(sort));
    }
  }

  /** Configuration for {@link SortAbsorptionRule}. */
  public static class Config extends SimpleAbsorptionConfig {
    public static final Config DEFAULT =
        new Config(
            b0 ->
                b0.operand(LogicalSort.class)
                    .oneInput(b1 -> b1.operand(BoundaryScan.class).noInputs()));

    private Config(OperandTransform operandSupplier) {
      super(operandSupplier);
    }

    @Override
    public SortAbsorptionRule toRule() {
      return new SortAbsorptionRule(this);
    }
  }
}
