/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api;

import java.util.List;
import java.util.Map;
import lombok.Builder;
import lombok.Singular;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Linq4j;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.ScannableTable;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.Statistics;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.type.SqlTypeName;

/** Reusable scannable table with builder pattern for easy table creation. */
@Builder
public class SimpleTable implements ScannableTable {
  @Singular("col")
  private final Map<String, SqlTypeName> schema;

  @Singular private final List<Object[]> rows;

  @Override
  public RelDataType getRowType(RelDataTypeFactory typeFactory) {
    RelDataTypeFactory.Builder builder = typeFactory.builder();
    schema.forEach(builder::add);
    return builder.build();
  }

  @Override
  public Enumerable<Object[]> scan(DataContext root) {
    return Linq4j.asEnumerable(rows);
  }

  @Override
  public Statistic getStatistic() {
    return Statistics.UNKNOWN;
  }

  @Override
  public Schema.TableType getJdbcTableType() {
    return Schema.TableType.TABLE;
  }

  @Override
  public boolean isRolledUp(String column) {
    return false;
  }

  @Override
  public boolean rolledUpColumnValidInsideAgg(
      String column,
      SqlCall call,
      SqlNode parent,
      org.apache.calcite.config.CalciteConnectionConfig config) {
    return false;
  }
}
