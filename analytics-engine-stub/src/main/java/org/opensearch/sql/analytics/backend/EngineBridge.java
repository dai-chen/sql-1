/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.analytics.backend;

/**
 * JNI boundary interface between the query planner (Java) and a native execution engine (e.g.,
 * DataFusion/Rust).
 *
 * <p>Copied from analytics-framework SPI (opensearch-project/OpenSearch sandbox/libs).
 *
 * @param <Fragment> serialised plan type (e.g., byte[] for Substrait)
 * @param <Stream> result stream handle
 * @param <LogicalPlan> logical plan type (e.g., Calcite RelNode)
 */
public interface EngineBridge<Fragment, Stream, LogicalPlan> {

  /** Converts a logical plan fragment into the native engine's serialised format. */
  Fragment convertFragment(LogicalPlan fragment);

  /** Submits the serialised plan to the native engine for execution. */
  Stream execute(Fragment fragment);
}
