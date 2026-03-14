/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.routing;

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
}
