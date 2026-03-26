/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.cli;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.instanceOf;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import org.apache.calcite.schema.Table;
import org.junit.Test;
import org.opensearch.sql.api.SimpleTable;

public class SampleDataLoaderTest {

  @Test
  public void testLoadHrDataset() throws Exception {
    Map<String, Table> tables = SampleDataLoader.loadFromClasspath("data/hr.json");
    assertThat(tables, hasKey("employees"));
    assertThat(tables, hasKey("departments"));
    assertThat(tables.get("employees"), instanceOf(SimpleTable.class));
    assertThat(tables.get("departments"), instanceOf(SimpleTable.class));
  }

  @Test
  public void testLoadFromInputStream() throws Exception {
    String json = "{\"users\": [{\"id\": 1, \"name\": \"Test\"}]}";
    InputStream is = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8));
    Map<String, Table> tables = SampleDataLoader.load(is);
    assertThat(tables, hasKey("users"));
    assertThat(tables.get("users"), instanceOf(SimpleTable.class));
  }

  @Test
  public void testTypeInference() throws Exception {
    String json = "{\"mixed\": [{\"i\": 1, \"s\": \"hello\", \"d\": 3.14, \"b\": true}]}";
    InputStream is = new ByteArrayInputStream(json.getBytes(StandardCharsets.UTF_8));
    Map<String, Table> tables = SampleDataLoader.load(is);
    assertThat(tables, hasKey("mixed"));
    assertThat(tables.get("mixed"), instanceOf(SimpleTable.class));
  }
}
