/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api;

import static org.apache.calcite.sql.type.SqlTypeName.DATE;
import static org.apache.calcite.sql.type.SqlTypeName.INTEGER;
import static org.apache.calcite.sql.type.SqlTypeName.TIME;
import static org.apache.calcite.sql.type.SqlTypeName.VARCHAR;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.util.Map;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.sql.executor.QueryType;

/**
 * Tests for PPLTypeCoercion registered via TypeCoercionFactory in UnifiedQueryContext.
 * Validates that DATE + TIME → TIMESTAMP coercion is applied during Calcite validation,
 * and that Calcite defaults handle STRING → NUMERIC comparisons.
 */
public class PPLTypeCoercionTest extends UnifiedQueryTestBase {

  @Override
  @Before
  public void setUp() {
    testSchema =
        new AbstractSchema() {
          @Override
          protected Map<String, Table> getTableMap() {
            return Map.of(
                "t",
                SimpleTable.builder()
                    .col("d", DATE)
                    .col("t", TIME)
                    .col("i", INTEGER)
                    .col("s", VARCHAR)
                    .build());
          }
        };

    context =
        UnifiedQueryContext.builder()
            .language(QueryType.SQL)
            .conformance(SqlConformanceEnum.DEFAULT)
            .catalog(DEFAULT_CATALOG, testSchema)
            .defaultNamespace(DEFAULT_CATALOG)
            .build();
    planner = new UnifiedQueryPlanner(context);
  }

  @Test
  public void testDateTimeComparisonCoercesToTimestamp() {
    RelNode plan = planner.plan("SELECT * FROM t WHERE d > t");
    assertNotNull(plan);
    String explain = plan.explain();
    // PPLTypeCoercion should inject CAST to TIMESTAMP for DATE + TIME comparison
    assertTrue("DATE+TIME comparison should produce CAST to TIMESTAMP: " + explain,
        explain.contains("CAST") && explain.contains("TIMESTAMP"));
  }

  @Test
  public void testIntegerStringComparisonCoerced() {
    // Calcite's built-in commonTypeForBinaryComparison handles STRING vs NUMERIC
    RelNode plan = planner.plan("SELECT * FROM t WHERE i > '25'");
    assertNotNull("Integer > string comparison should validate successfully", plan);
  }
}
