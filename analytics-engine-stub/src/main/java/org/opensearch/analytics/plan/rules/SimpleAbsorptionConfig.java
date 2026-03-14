/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.analytics.plan.rules;

import org.apache.calcite.plan.RelRule;
import org.apache.calcite.plan.RelRule.OperandTransform;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.tools.RelBuilderFactory;

/** Base {@link RelRule.Config} implementation for absorption rules (no Immutables needed). */
abstract class SimpleAbsorptionConfig implements RelRule.Config {

  private final OperandTransform operandSupplier;

  protected SimpleAbsorptionConfig(OperandTransform operandSupplier) {
    this.operandSupplier = operandSupplier;
  }

  @Override
  public OperandTransform operandSupplier() {
    return operandSupplier;
  }

  @Override
  public RelRule.Config withOperandSupplier(OperandTransform transform) {
    throw new UnsupportedOperationException();
  }

  @Override
  public String description() {
    return null;
  }

  @Override
  public RelRule.Config withDescription(String description) {
    throw new UnsupportedOperationException();
  }

  @Override
  public RelBuilderFactory relBuilderFactory() {
    return RelFactories.LOGICAL_BUILDER;
  }

  @Override
  public RelRule.Config withRelBuilderFactory(RelBuilderFactory factory) {
    throw new UnsupportedOperationException();
  }
}
