/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy;

import org.junit.rules.TestRule;
import org.junit.runner.Description;
import org.junit.runners.model.Statement;
import org.opensearch.sql.util.BackendCapabilities;
import org.opensearch.sql.util.Capability;
import org.opensearch.sql.util.RequiresCapability;

/**
 * Enforces {@link RequiresCapability} on tests run under RandomizedRunner, where the JUnit5 {@code
 * ExecutionCondition} hook does not fire. Class-level requirements are checked before method-level
 * ones; both delegate to {@link BackendCapabilities#requireCapability(Capability, String)} so skip
 * message and backend logic live in one place.
 */
public class CapabilityRule implements TestRule {

  @Override
  public Statement apply(Statement base, Description description) {
    return new Statement() {
      @Override
      public void evaluate() throws Throwable {
        Class<?> testClass = description.getTestClass();
        if (testClass != null) {
          enforce(testClass.getAnnotation(RequiresCapability.class));
        }
        enforce(description.getAnnotation(RequiresCapability.class));
        base.evaluate();
      }
    };
  }

  private static void enforce(RequiresCapability annotation) {
    if (annotation == null) {
      return;
    }
    String note = annotation.note().isBlank() ? null : annotation.note();
    for (Capability capability : annotation.value()) {
      BackendCapabilities.requireCapability(capability, note);
    }
  }
}
