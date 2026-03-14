/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.analytics.plan.rules;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.opensearch.analytics.plan.BoundaryScan;

/** Absorbs a {@link LogicalFilter} into a {@link BoundaryScan} if the engine supports it. */
public class FilterAbsorptionRule extends RelRule<FilterAbsorptionRule.Config> {

  protected FilterAbsorptionRule(Config config) {
    super(config);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    LogicalFilter filter = call.rel(0);
    BoundaryScan scan = call.rel(1);
    if (scan.getCapabilities().supportsOperator(LogicalFilter.class)) {
      call.transformTo(scan.absorb(filter));
    }
  }

  /** Configuration for {@link FilterAbsorptionRule}. */
  public static class Config extends SimpleAbsorptionConfig {
    public static final Config DEFAULT =
        new Config(
            b0 ->
                b0.operand(LogicalFilter.class)
                    .oneInput(b1 -> b1.operand(BoundaryScan.class).noInputs()));

    private Config(OperandTransform operandSupplier) {
      super(operandSupplier);
    }

    @Override
    public FilterAbsorptionRule toRule() {
      return new FilterAbsorptionRule(this);
    }
  }
}
