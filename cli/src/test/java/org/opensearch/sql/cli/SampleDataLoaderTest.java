/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.cli;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
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
  public void testLoadTextFile() throws Exception {
    String text = "line one\nline two\nline three";
    InputStream is = new ByteArrayInputStream(text.getBytes(StandardCharsets.UTF_8));
    Map<String, Table> tables = SampleDataLoader.loadTextFile(is, "logs");
    assertThat(tables, hasKey("logs"));
    assertThat(tables.get("logs"), instanceOf(SimpleTable.class));
  }

  @Test
  public void testLoadTextFileEmptyLines() throws Exception {
    String text = "first\n\nthird";
    InputStream is = new ByteArrayInputStream(text.getBytes(StandardCharsets.UTF_8));
    Map<String, Table> tables = SampleDataLoader.loadTextFile(is, "data");
    assertThat(tables, hasKey("data"));
  }

  @Test
  public void testLoadFileDetectsJson() throws Exception {
    Map<String, Table> tables = SampleDataLoader.loadFile("cli/examples/hr.json");
    assertThat(tables, hasKey("employees"));
    assertThat(tables, hasKey("departments"));
  }

  @Test
  public void testLoadFileDetectsText() throws Exception {
    Map<String, Table> tables = SampleDataLoader.loadFile("cli/examples/opensearch.log");
    assertThat(tables, hasKey("opensearch"));
  }

  @Test
  public void testLoadLogFileJoinsMultiLineStackTrace() throws Exception {
    String log =
        "[2024-01-15T10:30:04,345][ERROR][o.o.a.b.Action] failed\n"
            + "java.lang.NullPointerException: null\n"
            + "\tat org.opensearch.Foo.bar(Foo.java:10)\n"
            + "\tat org.opensearch.Foo.baz(Foo.java:20)";
    InputStream is = new ByteArrayInputStream(log.getBytes(StandardCharsets.UTF_8));
    Map<String, Table> tables = SampleDataLoader.loadLogFile(is, "app");
    SimpleTable table = (SimpleTable) tables.get("app");
    List<Object[]> rows = table.scan(null).toList();
    assertThat(rows.size(), is(1));
    String line = (String) rows.get(0)[0];
    assertThat(line, containsString("NullPointerException"));
    assertThat(line, containsString("\n\tat org.opensearch"));
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
