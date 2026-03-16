/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.analytics;

import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlOperatorTable;

/**
 * Context provided by the analytics engine to front-end plugins. Provides schema and operator table
 * for query validation and planning.
 *
 * <p>Copied from analytics-framework SPI (opensearch-project/OpenSearch sandbox/libs).
 */
public interface EngineContext {

  /** Returns a Calcite schema reflecting the current cluster state. */
  SchemaPlus getSchema();

  /** Returns the operator table containing all functions supported by registered back-ends. */
  SqlOperatorTable operatorTable();
}
