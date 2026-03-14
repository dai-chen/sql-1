/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.analytics.schema;

import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.schema.SchemaPlus;

/** Mock {@link SchemaProvider} that registers a {@link ParquetTable} as "parquet_index". */
public class MockParquetSchemaProvider implements SchemaProvider {

  @Override
  public SchemaPlus buildSchema(Object clusterState) {
    SchemaPlus rootSchema = CalciteSchema.createRootSchema(true).plus();
    rootSchema.add("parquet_index", new ParquetTable());
    return rootSchema;
  }
}
