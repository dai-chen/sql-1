/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.transpile;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import org.apache.calcite.sql.SqlDialect;
import org.apache.calcite.sql.dialect.PrestoSqlDialect;
import org.apache.calcite.sql.dialect.SparkSqlDialect;
import org.junit.Test;

public class TranspileOptionsTest {

  @Test
  public void testDefaultOptions() {
    TranspileOptions options = TranspileOptions.builder().build();

    assertTrue(options.isPrettyPrint());
    assertEquals(SparkSqlDialect.DEFAULT, options.getSqlDialect());
  }

  @Test
  public void testCustomDatabaseProduct() {
    TranspileOptions options =
        TranspileOptions.builder().databaseProduct(SqlDialect.DatabaseProduct.PRESTO).build();

    assertEquals(PrestoSqlDialect.DEFAULT, options.getSqlDialect());
  }

  @Test
  public void testPrettyPrintDisabled() {
    TranspileOptions options = TranspileOptions.builder().prettyPrint(false).build();
    assertFalse(options.isPrettyPrint());
  }

  @Test(expected = NullPointerException.class)
  public void testNullDatabaseProduct() {
    TranspileOptions.builder().databaseProduct(null).build();
  }
}
