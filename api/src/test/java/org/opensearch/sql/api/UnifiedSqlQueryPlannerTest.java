/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api;

import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

import java.util.Map;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.junit.Test;
import org.opensearch.sql.executor.QueryType;

public class UnifiedSqlQueryPlannerTest extends UnifiedQueryTestBase {

  @Override
  protected QueryType queryType() {
    return QueryType.SQL;
  }

  @Test
  public void testSelectAll() {
    RelNode plan =
        planner.plan(
            """
            SELECT *
            FROM catalog.employees\
            """);
    assertNotNull("Plan should be created", plan);
  }

  @Test
  public void testSelectWithFilter() {
    RelNode plan =
        planner.plan(
            """
            SELECT *
            FROM catalog.employees
            WHERE age > 30\
            """);
    assertNotNull("Plan should be created", plan);
  }

  @Test
  public void testSelectWithAggregation() {
    RelNode plan =
        planner.plan(
            """
            SELECT department, count(*)
            FROM catalog.employees
            GROUP BY department\
            """);
    assertNotNull("Plan should be created", plan);
  }

  @Test
  public void testSelectWithJoin() {
    RelNode plan =
        planner.plan(
            """
            SELECT a.id, b.name
            FROM catalog.employees a
            JOIN catalog.employees b ON a.id = b.age\
            """);
    assertNotNull("Plan should be created", plan);
  }

  @Test
  public void testSelectWithOrderBy() {
    RelNode plan =
        planner.plan(
            """
            SELECT *
            FROM catalog.employees
            ORDER BY age DESC\
            """);
    assertNotNull("Plan should be created", plan);
  }

  @Test
  public void testSelectWithSubquery() {
    RelNode plan =
        planner.plan(
            """
            SELECT *
            FROM catalog.employees
            WHERE age > (SELECT avg(age) FROM catalog.employees)\
            """);
    assertNotNull("Plan should be created", plan);
  }

  @Test
  public void testSelectWithCte() {
    RelNode plan =
        planner.plan(
            """
            WITH seniors AS (
              SELECT * FROM catalog.employees WHERE age > 30
            )
            SELECT *
            FROM seniors\
            """);
    assertNotNull("Plan should be created", plan);
  }

  @Test
  public void testCaseInsensitiveIdentifiers() {
    // Verify Casing.UNCHANGED: lowercase table/column names resolve correctly
    RelNode plan =
        planner.plan(
            """
            SELECT id, name
            FROM catalog.employees
            WHERE age > 30\
            """);
    assertNotNull("Plan should be created with lowercase identifiers", plan);
  }

  @Test
  public void testDefaultNamespace() {
    UnifiedQueryContext sqlContext =
        UnifiedQueryContext.builder()
            .language(QueryType.SQL)
            .catalog("catalog", testSchema)
            .defaultNamespace("catalog")
            .build();
    UnifiedQueryPlanner sqlPlanner = new UnifiedQueryPlanner(sqlContext);

    assertNotNull(
        "Plan should resolve unqualified table", sqlPlanner.plan("SELECT * FROM employees"));
  }

  @Test
  public void testMultipleCatalogs() {
    UnifiedQueryContext sqlContext =
        UnifiedQueryContext.builder()
            .language(QueryType.SQL)
            .catalog("catalog1", testSchema)
            .catalog("catalog2", testSchema)
            .build();
    UnifiedQueryPlanner sqlPlanner = new UnifiedQueryPlanner(sqlContext);

    RelNode plan =
        sqlPlanner.plan(
            """
            SELECT a.id
            FROM catalog1.employees a
            JOIN catalog2.employees b ON a.id = b.id\
            """);
    assertNotNull("Plan should be created with multiple catalogs", plan);
  }

  @Test
  public void testDefaultNamespaceMultiLevel() {
    AbstractSchema deepSchema =
        new AbstractSchema() {
          @Override
          protected Map<String, Schema> getSubSchemaMap() {
            return Map.of("opensearch", testSchema);
          }
        };
    UnifiedQueryContext sqlContext =
        UnifiedQueryContext.builder()
            .language(QueryType.SQL)
            .catalog("catalog", deepSchema)
            .defaultNamespace("catalog.opensearch")
            .build();
    UnifiedQueryPlanner sqlPlanner = new UnifiedQueryPlanner(sqlContext);

    assertNotNull(
        "Plan should resolve with multi-level default namespace",
        sqlPlanner.plan("SELECT * FROM employees"));
  }

  @Test
  public void testInvalidSqlThrowsException() {
    assertThrows(IllegalStateException.class, () -> planner.plan("SELECT FROM"));
  }
}
