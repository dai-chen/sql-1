/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api;

import static org.junit.Assert.assertNotNull;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.sql.executor.QueryType;

public class StarExceptReplaceTest extends UnifiedQueryTestBase {

  @Override
  @Before
  public void setUp() {
    super.setUp();
    try {
      context.close();
    } catch (Exception e) {
      /* ignore */
    }

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
  public void testStandardSelectStarParsesWithCustomParser() {
    RelNode plan = planner.plan("SELECT * FROM employees");
    assertNotNull("SELECT * should parse with custom parser", plan);
  }

  @Test
  public void testStandardSelectColumnsParsesWithCustomParser() {
    RelNode plan = planner.plan("SELECT id, name FROM employees");
    assertNotNull("SELECT columns should parse with custom parser", plan);
  }

  @Test
  public void testStandardSelectWithWhereParsesWithCustomParser() {
    RelNode plan = planner.plan("SELECT * FROM employees WHERE age > 30");
    assertNotNull("SELECT with WHERE should parse with custom parser", plan);
  }
}
