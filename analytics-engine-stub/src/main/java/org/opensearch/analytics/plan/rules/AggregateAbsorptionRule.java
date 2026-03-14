/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.analytics.plan.rules;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.opensearch.analytics.plan.BoundaryScan;

/** Absorbs a {@link LogicalAggregate} into a {@link BoundaryScan} if the engine supports it. */
public class AggregateAbsorptionRule extends RelRule<AggregateAbsorptionRule.Config> {

  protected AggregateAbsorptionRule(Config config) {
    super(config);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    LogicalAggregate aggregate = call.rel(0);
    BoundaryScan scan = call.rel(1);
    if (scan.getCapabilities().supportsOperator(LogicalAggregate.class)) {
      call.transformTo(scan.absorb(aggregate));
    }
  }

  /** Configuration for {@link AggregateAbsorptionRule}. */
  public static class Config extends SimpleAbsorptionConfig {
    public static final Config DEFAULT =
        new Config(
            b0 ->
                b0.operand(LogicalAggregate.class)
                    .oneInput(b1 -> b1.operand(BoundaryScan.class).noInputs()));

    private Config(OperandTransform operandSupplier) {
      super(operandSupplier);
    }

    @Override
    public AggregateAbsorptionRule toRule() {
      return new AggregateAbsorptionRule(this);
    }
  }
}
