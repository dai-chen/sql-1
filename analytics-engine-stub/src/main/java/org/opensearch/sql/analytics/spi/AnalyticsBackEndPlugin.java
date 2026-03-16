/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.analytics.spi;

import org.apache.calcite.sql.SqlOperatorTable;
import org.opensearch.sql.analytics.backend.EngineBridge;

/**
 * SPI extension point for back-end query engines (DataFusion, Lucene, etc.).
 *
 * <p>Copied from analytics-framework SPI (opensearch-project/OpenSearch sandbox/libs).
 */
public interface AnalyticsBackEndPlugin {

  /** Unique engine name (e.g., "lucene", "datafusion"). */
  String name();

  /** JNI boundary for executing serialized plans, or null if not applicable. */
  EngineBridge bridge();

  /** Supported functions as a Calcite operator table, or null if none. */
  SqlOperatorTable operatorTable();
}
