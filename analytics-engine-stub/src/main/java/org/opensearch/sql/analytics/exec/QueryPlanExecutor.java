/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.analytics.exec;

/**
 * Executes a logical query plan fragment against the underlying data store.
 *
 * <p>Copied from analytics-framework SPI (opensearch-project/OpenSearch sandbox/libs).
 *
 * @param <LogicalPlan> logical plan type (e.g., Calcite RelNode)
 * @param <Stream> result stream type
 */
@FunctionalInterface
public interface QueryPlanExecutor<LogicalPlan, Stream> {

  /**
   * Executes the given logical fragment and returns result rows.
   *
   * @param plan the logical subtree to execute
   * @param context execution context (opaque Object to avoid server dependency)
   * @return rows produced by the engine
   */
  Stream execute(LogicalPlan plan, Object context);
}
