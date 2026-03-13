/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.analytics.backend;

/**
 * JNI boundary interface between the query planner (Java) and a native execution engine.
 *
 * @param <LogicalPlan> logical plan type (e.g., Calcite RelNode)
 * @param <Fragment> serialised plan type (e.g., byte[] for Substrait)
 * @param <ResultStream> result stream handle
 */
public interface EngineBridge<LogicalPlan, Fragment, ResultStream> {

  /**
   * Converts a logical plan fragment into the native engine's serialised format.
   *
   * @param fragment the logical plan subtree to serialise
   * @return the serialised plan in the engine's wire format
   */
  Fragment convertFragment(LogicalPlan fragment);

  /**
   * Submits the serialised plan to the native engine for execution.
   *
   * @param fragment the serialised plan produced by {@link #convertFragment}
   * @return an opaque handle to the native result stream
   */
  ResultStream execute(Fragment fragment);
}
