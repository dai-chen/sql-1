/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.analytics.exec;

import java.util.stream.Stream;

/** Executes a logical query plan fragment against the underlying data store. */
@FunctionalInterface
public interface QueryPlanExecutor {

  /**
   * Executes the given logical plan and returns result rows.
   *
   * @param plan the logical subtree to execute (opaque Object for flexibility)
   * @param context execution context (opaque Object to avoid server dependency)
   * @return rows produced by the engine
   */
  Stream<?> execute(Object plan, Object context);
}
