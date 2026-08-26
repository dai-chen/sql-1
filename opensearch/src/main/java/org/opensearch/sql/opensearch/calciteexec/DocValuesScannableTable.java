/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.calciteexec;

import java.util.List;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;

public class DocValuesScannableTable extends AbstractTable implements ScannableTable {

  public static final String ROW_DATA_KEY = "calcite_exec_rows";

  private final List<String> fieldNames;
  private final List<SqlTypeName> fieldTypes;

  public DocValuesScannableTable(List<String> fieldNames, List<SqlTypeName> fieldTypes) {
    this.fieldNames = fieldNames;
    this.fieldTypes = fieldTypes;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    RelDataTypeFactory.Builder builder = typeFactory.builder();
    for (int i = 0; i < fieldNames.size(); i++) {
      builder.add(fieldNames.get(i), fieldTypes.get(i)).nullable(true);
    }
    return builder.build();
  }

  @Override
  public Statistic getStatistic() {
    return Statistics.UNKNOWN;
  }

  @SuppressWarnings("unchecked")
  @Override
  public Enumerable<Object[]> scan(DataContext root) {
    Object data = root.get(ROW_DATA_KEY);
    if (data == null) {
      return Linq4j.emptyEnumerable();
    }
    return Linq4j.asEnumerable((List<Object[]>) data);
  }
}
