/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.storage;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Statistic;
import org.apache.calcite.schema.TranslatableTable;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.data.type.ExprType;
import org.opensearch.sql.opensearch.storage.scan.CalciteNoPushdownIndexScan;
import org.opensearch.sql.planner.logical.LogicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.storage.Table;

/**
 * Wrapper around an OpenSearch table that maps date/time/timestamp fields to standard Calcite SQL
 * types instead of UDTs, and excludes metadata fields from the schema. Pushdown rules are disabled
 * so Calcite handles all planning with standard operators.
 */
public class AnsiSQLOpenSearchTable extends AbstractTable implements TranslatableTable, Table {

  private final org.apache.calcite.schema.Table calciteTable;
  private final Table storageTable;

  public AnsiSQLOpenSearchTable(org.apache.calcite.schema.Table table) {
    this.calciteTable = table;
    this.storageTable = (Table) table;
  }

  @Override
  public RelDataType getRowType(RelDataTypeFactory relDataTypeFactory) {
    OpenSearchTypeFactory typeFactory = OpenSearchTypeFactory.TYPE_FACTORY;
    List<String> fieldNameList = new ArrayList<>();
    List<RelDataType> typeList = new ArrayList<>();
    Map<String, ExprType> fieldTypes = new LinkedHashMap<>(storageTable.getFieldTypes());
    for (Map.Entry<String, ExprType> entry : fieldTypes.entrySet()) {
      if (entry.getValue().getOriginalPath().isPresent()) continue;
      fieldNameList.add(entry.getKey());
      typeList.add(convertToStandardType(typeFactory, entry.getValue()));
    }
    return typeFactory.createStructType(typeList, fieldNameList, true);
  }

  private static RelDataType convertToStandardType(
      OpenSearchTypeFactory typeFactory, ExprType fieldType) {
    if (fieldType instanceof ExprCoreType) {
      switch ((ExprCoreType) fieldType) {
        case DATE:
          return typeFactory.createSqlType(SqlTypeName.DATE, true);
        case TIME:
          return typeFactory.createSqlType(SqlTypeName.TIME, true);
        case TIMESTAMP:
          return typeFactory.createSqlType(SqlTypeName.TIMESTAMP, true);
        default:
          return OpenSearchTypeFactory.convertExprTypeToRelDataType(fieldType);
      }
    } else {
      String legacyName = fieldType.legacyTypeName().toLowerCase();
      if (legacyName.equals("timestamp")
          || legacyName.equals("date")
          || legacyName.equals("time")) {
        SqlTypeName sqlType =
            legacyName.equals("timestamp")
                ? SqlTypeName.TIMESTAMP
                : legacyName.equals("date") ? SqlTypeName.DATE : SqlTypeName.TIME;
        return typeFactory.createSqlType(sqlType, true);
      }
      if (legacyName.equals("binary")) {
        return typeFactory.createSqlType(SqlTypeName.VARBINARY, true);
      }
      return OpenSearchTypeFactory.convertExprTypeToRelDataType(fieldType);
    }
  }

  @Override
  public RelNode toRel(RelOptTable.ToRelContext context, RelOptTable relOptTable) {
    final RelOptCluster cluster = context.getCluster();
    return new CalciteNoPushdownIndexScan(cluster, relOptTable, (OpenSearchIndex) storageTable);
  }

  @Override
  public Statistic getStatistic() {
    return calciteTable.getStatistic();
  }

  @Override
  public Schema.TableType getJdbcTableType() {
    return calciteTable.getJdbcTableType();
  }

  @Override
  public Map<String, ExprType> getFieldTypes() {
    return storageTable.getFieldTypes();
  }

  @Override
  public Map<String, ExprType> getReservedFieldTypes() {
    return storageTable.getReservedFieldTypes();
  }

  @Override
  public boolean exists() {
    return storageTable.exists();
  }

  @Override
  public PhysicalPlan implement(LogicalPlan plan) {
    return storageTable.implement(plan);
  }
}
