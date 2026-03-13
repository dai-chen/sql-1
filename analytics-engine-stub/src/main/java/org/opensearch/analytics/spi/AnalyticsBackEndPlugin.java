/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.analytics.spi;

import org.apache.calcite.sql.SqlOperatorTable;
import org.opensearch.analytics.backend.EngineBridge;

/** SPI extension point for back-end query engines (DataFusion, Lucene, etc.). */
public interface AnalyticsBackEndPlugin {

  /** Unique engine name (e.g., "lucene", "datafusion"). */
  String name();

  /** JNI boundary for executing serialized plans, or null for engines without native execution. */
  EngineBridge<?, ?, ?> bridge();

  /** Supported functions as a Calcite operator table, or null if the back-end adds no functions. */
  SqlOperatorTable operatorTable();
}
