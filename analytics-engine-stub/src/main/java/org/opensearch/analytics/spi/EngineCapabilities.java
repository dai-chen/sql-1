/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.analytics.spi;

import org.apache.calcite.rel.RelNode;

/** Declares the logical operators and functions a back-end engine supports. */
public interface EngineCapabilities {

  /** Returns {@code true} if the engine can execute the given logical operator. */
  boolean supportsOperator(Class<? extends RelNode> operatorClass);

  /** Returns {@code true} if the engine can evaluate the named function. */
  boolean supportsFunction(String functionName);
}
