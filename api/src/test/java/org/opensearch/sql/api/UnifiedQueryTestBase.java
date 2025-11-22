/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api;

import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Before;

/**
 * Base class for unified query tests providing common test schema and utilities.
 *
 * <p>This class sets up a simple test schema with an "employees" table that can be used across all
 * unified query tests for planner, transpiler, and facade testing.
 */
public abstract class UnifiedQueryTestBase {

  /** Test schema containing sample tables for testing */
  protected AbstractSchema testSchema;

  /**
   * Sets up the test schema before each test.
   *
   * <p>Creates an "employees" table with columns: id (INTEGER), name (VARCHAR), age (INTEGER),
   * department (VARCHAR)
   */
  @Before
  public void setUp() {
    testSchema =
        new AbstractSchema() {
          @Override
          protected Map<String, Table> getTableMap() {
            return Map.of("employees", createEmployeesTable());
          }
        };
  }

  /**
   * Creates the employees table with a standard schema.
   *
   * @return Table representing employees with id, name, age, and department columns
   */
  protected Table createEmployeesTable() {
    return new AbstractTable() {
      @Override
      public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return typeFactory.createStructType(
            List.of(
                typeFactory.createSqlType(SqlTypeName.INTEGER),
                typeFactory.createSqlType(SqlTypeName.VARCHAR),
                typeFactory.createSqlType(SqlTypeName.INTEGER),
                typeFactory.createSqlType(SqlTypeName.VARCHAR)),
            List.of("id", "name", "age", "department"));
      }
    };
  }

  /**
   * Helper method to create a table with custom columns.
   *
   * @param typeFactory The type factory to use for creating types
   * @param columns Map of column names to SQL type names
   * @return Table with the specified columns
   */
  protected Table createCustomTable(
      RelDataTypeFactory typeFactory, Map<String, SqlTypeName> columns) {
    return new AbstractTable() {
      @Override
      public RelDataType getRowType(RelDataTypeFactory factory) {
        List<SqlTypeName> types = List.copyOf(columns.values());
        List<String> names = List.copyOf(columns.keySet());

        return factory.createStructType(types.stream().map(factory::createSqlType).toList(), names);
      }
    };
  }

  /**
   * Helper method to create a schema with multiple tables.
   *
   * @param tables Map of table names to table instances
   * @return Schema containing the specified tables
   */
  protected AbstractSchema createSchemaWithTables(Map<String, Table> tables) {
    return new AbstractSchema() {
      @Override
      protected Map<String, Table> getTableMap() {
        return tables;
      }
    };
  }
}
