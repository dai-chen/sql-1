/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.stage;

import java.util.List;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.impl.AbstractTable;

/** Calcite table backed by rows supplied through a {@link DataContext}. */
final class CalciteRowsTable extends AbstractTable implements ScannableTable {

  private final RelDataType rowType;
  private final String dataKey;

  CalciteRowsTable(RelDataType rowType, String dataKey) {
    this.rowType = rowType;
    this.dataKey = dataKey;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    return rowType;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Enumerable<Object[]> scan(DataContext root) {
    Object rows = root.get(dataKey);
    if (rows instanceof Enumerable<?>) {
      return (Enumerable<Object[]>) rows;
    }
    if (rows instanceof List<?>) {
      return Linq4j.asEnumerable((List<Object[]>) rows);
    }
    throw new IllegalStateException("Missing Calcite rows for [" + dataKey + "]");
  }
}
