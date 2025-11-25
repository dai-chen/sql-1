/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.SqlDialect;
import org.junit.Test;
import org.opensearch.sql.executor.QueryType;

public class UnifiedQueryTest extends UnifiedQueryTestBase {

  @Test
  public void testPlanAPI() {
    RelNode plan =
        UnifiedQuery.lang(QueryType.PPL)
            .catalog("catalog", testSchema)
            .defaultNamespace("catalog")
            .plan("source = employees | where age > 30 | fields name, age");

    assertNotNull("Plan should not be null", plan);
  }

  @Test
  public void testTranspileAPI() {
    String sparkSql =
        UnifiedQuery.lang(QueryType.PPL)
            .catalog("catalog", testSchema)
            .defaultNamespace("catalog")
            .transpile(
                "source = employees | where age > 30 | fields name, age",
                SqlDialect.DatabaseProduct.SPARK);

    String expectedSql = "SELECT `name`, `age`\nFROM `catalog`.`employees`\nWHERE `age` > 30";
    assertEquals("Should generate correct Spark SQL", expectedSql, sparkSql);
  }
}
