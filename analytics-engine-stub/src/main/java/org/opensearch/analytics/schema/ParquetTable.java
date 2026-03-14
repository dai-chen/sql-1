/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.analytics.schema;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

/** Mock Calcite table representing a Parquet index with standard Calcite types. */
public class ParquetTable extends AbstractTable {

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return typeFactory
        .builder()
        .add("message", SqlTypeName.VARCHAR)
        .nullable(true)
        .add("status", SqlTypeName.BIGINT)
        .nullable(true)
        .add("timestamp", SqlTypeName.TIMESTAMP)
        .nullable(true)
        .add("active", SqlTypeName.BOOLEAN)
        .nullable(true)
        .build();
  }
}
