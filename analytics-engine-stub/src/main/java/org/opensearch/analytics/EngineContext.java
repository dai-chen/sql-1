/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.analytics;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlOperatorTable;

/**
 * Context provided by the analytics engine to front-end plugins.
 *
 * <p>Provides everything a front-end needs for query validation and planning:
 *
 * <dl>
 *   <dt>{@link #getSchema()} — Calcite schema with tables/fields/types derived from cluster state
 *   <dt>{@link #operatorTable()} — supported functions/operators aggregated from all back-end
 *       engines
 * </dl>
 *
 * <p>Front-ends do not need to know about cluster state or individual back-end capabilities — this
 * context encapsulates both.
 */
public interface EngineContext {

  /** Returns a Calcite schema reflecting the current cluster state. */
  SchemaPlus getSchema();

  /**
   * Returns the operator table containing all functions supported by registered back-end engines.
   */
  SqlOperatorTable operatorTable();
}
