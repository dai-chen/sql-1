/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.common.routing;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

import org.junit.Test;

public class ParquetIndexRoutingTest {

  @Test
  public void extractIndexNameBasic() {
    assertEquals("parquet_index", ParquetIndexRouting.extractIndexName("source = parquet_index"));
  }

  @Test
  public void extractIndexNameNoSpaces() {
    assertEquals("parquet_index", ParquetIndexRouting.extractIndexName("source=parquet_index"));
  }

  @Test
  public void extractIndexNameWithPipeline() {
    assertEquals(
        "parquet_index",
        ParquetIndexRouting.extractIndexName(
            "source = parquet_index | where status = 200 | fields timestamp, status"));
  }

  @Test
  public void extractIndexNameCaseInsensitive() {
    assertEquals("myindex", ParquetIndexRouting.extractIndexName("SOURCE = myindex"));
  }

  @Test
  public void extractIndexNameReturnsNullForNull() {
    assertNull(ParquetIndexRouting.extractIndexName(null));
  }

  @Test
  public void extractIndexNameReturnsNullForNoMatch() {
    assertNull(ParquetIndexRouting.extractIndexName("select * from table"));
  }

  @Test
  public void isParquetIndexTrue() {
    assertTrue(ParquetIndexRouting.isParquetIndex("parquet_index"));
  }

  @Test
  public void isParquetIndexFalseForRegularIndex() {
    assertFalse(ParquetIndexRouting.isParquetIndex("regular_index"));
  }

  @Test
  public void isParquetIndexFalseForNull() {
    assertFalse(ParquetIndexRouting.isParquetIndex(null));
  }

  @Test
  public void extractIndexNameFromSqlBasic() {
    assertEquals(
        "parquet_index",
        ParquetIndexRouting.extractIndexNameFromSql("SELECT * FROM parquet_index"));
  }

  @Test
  public void extractIndexNameFromSqlWithWhereClause() {
    assertEquals(
        "parquet_index",
        ParquetIndexRouting.extractIndexNameFromSql(
            "SELECT * FROM parquet_index WHERE status = 200"));
  }

  @Test
  public void extractIndexNameFromSqlCaseInsensitive() {
    assertEquals("myindex", ParquetIndexRouting.extractIndexNameFromSql("select * from myindex"));
  }

  @Test
  public void extractIndexNameFromSqlReturnsNullForNull() {
    assertNull(ParquetIndexRouting.extractIndexNameFromSql(null));
  }

  @Test
  public void extractIndexNameFromSqlReturnsNullForNoFromClause() {
    assertNull(ParquetIndexRouting.extractIndexNameFromSql("SHOW TABLES"));
  }
}
