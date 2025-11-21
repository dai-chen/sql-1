/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.sql.executor.QueryType;

public class UnifiedQueryTest {

  private AbstractSchema testSchema;

  @Before
  public void setUp() {
    testSchema =
        new AbstractSchema() {
          @Override
          protected Map<String, Table> getTableMap() {
            return Map.of(
                "employees",
                new AbstractTable() {
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
                });
          }
        };
  }

  @Test
  public void testPlanAPI() {
    // Demonstrates using the plan() API to get a logical plan
    RelNode plan =
        UnifiedQuery.lang(QueryType.PPL)
            .catalog("catalog", testSchema)
            .defaultNamespace("catalog")
            .plan("source = employees | where age > 30 | fields name, age");

    assertNotNull("Plan should not be null", plan);
  }

  @Test
  public void testTranspileAPI() {
    // Demonstrates using the transpile() API to convert PPL to Spark SQL
    String sparkSql =
        UnifiedQuery.lang(QueryType.PPL)
            .catalog("catalog", testSchema)
            .defaultNamespace("catalog")
            .prettyPrint(false)
            .transpile(
                "source = employees | where age > 30 | fields name, age",
                SqlDialect.DatabaseProduct.SPARK);

    String expectedSql = "SELECT `name`, `age`\nFROM `catalog`.`employees`\nWHERE `age` > 30";
    assertEquals("Should generate correct Spark SQL", expectedSql, sparkSql);
  }
}
