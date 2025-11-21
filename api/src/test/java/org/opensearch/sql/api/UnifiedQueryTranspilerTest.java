/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api;

import static org.junit.Assert.assertEquals;

import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.sql.executor.QueryType;

public class UnifiedQueryTranspilerTest {

  private UnifiedQueryPlanner planner;
  private UnifiedQueryTranspiler transpiler;

  /** Test schema with sample table */
  private final AbstractSchema testSchema =
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

  @Before
  public void setUp() {
    planner =
        UnifiedQueryPlanner.builder()
            .language(QueryType.PPL)
            .catalog("catalog", testSchema)
            .defaultNamespace("catalog")
            .build();

    transpiler = new UnifiedQueryTranspiler();
  }

  @Test
  public void testSimpleSourceQuery() {
    String pplQuery = "source = employees";
    RelNode plan = planner.plan(pplQuery);

    TranspileOptions options = TranspileOptions.builder().prettyPrint(false).build();
    String sql = transpiler.toSql(plan, options);

    String expectedSql = "SELECT *\nFROM `catalog`.`employees`";
    assertEquals("Generated SQL should match expected", expectedSql, sql);
  }

  @Test
  public void testQueryWithFieldsProjection() {
    String pplQuery = "source = employees | fields name, age";
    RelNode plan = planner.plan(pplQuery);

    TranspileOptions options = TranspileOptions.builder().prettyPrint(false).build();
    String sql = transpiler.toSql(plan, options);

    String expectedSql = "SELECT `name`, `age`\nFROM `catalog`.`employees`";
    assertEquals("Generated SQL should match expected", expectedSql, sql);
  }

  @Test
  public void testQueryWithFilter() {
    String pplQuery = "source = employees | where age > 30";
    RelNode plan = planner.plan(pplQuery);

    TranspileOptions options = TranspileOptions.builder().prettyPrint(false).build();
    String sql = transpiler.toSql(plan, options);

    String expectedSql = "SELECT *\nFROM `catalog`.`employees`\nWHERE `age` > 30";
    assertEquals("Generated SQL should match expected", expectedSql, sql);
  }

  @Test
  public void testQueryWithAggregation() {
    String pplQuery = "source = employees | stats avg(age) by department";
    RelNode plan = planner.plan(pplQuery);

    TranspileOptions options = TranspileOptions.builder().prettyPrint(false).build();
    String sql = transpiler.toSql(plan, options);

    String expectedSql =
        "SELECT AVG(`age`) `avg(age)`, `department`\n"
            + "FROM `catalog`.`employees`\n"
            + "GROUP BY `department`";
    assertEquals("Generated SQL should match expected", expectedSql, sql);
  }
}
