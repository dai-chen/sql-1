/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.analytics.plan.rules;

import org.apache.calcite.plan.RelOptRuleCall;
import org.apache.calcite.plan.RelRule;
import org.apache.calcite.rel.logical.LogicalProject;
import org.opensearch.analytics.plan.BoundaryScan;

/** Absorbs a {@link LogicalProject} into a {@link BoundaryScan} if the engine supports it. */
public class ProjectAbsorptionRule extends RelRule<ProjectAbsorptionRule.Config> {

  protected ProjectAbsorptionRule(Config config) {
    super(config);
  }

  @Override
  public void onMatch(RelOptRuleCall call) {
    LogicalProject project = call.rel(0);
    BoundaryScan scan = call.rel(1);
    if (scan.getCapabilities().supportsOperator(LogicalProject.class)) {
      call.transformTo(scan.absorb(project));
    }
  }

  /** Configuration for {@link ProjectAbsorptionRule}. */
  public static class Config extends SimpleAbsorptionConfig {
    public static final Config DEFAULT =
        new Config(
            b0 ->
                b0.operand(LogicalProject.class)
                    .oneInput(b1 -> b1.operand(BoundaryScan.class).noInputs()));

    private Config(OperandTransform operandSupplier) {
      super(operandSupplier);
    }

    @Override
    public ProjectAbsorptionRule toRule() {
      return new ProjectAbsorptionRule(this);
    }
  }
}
