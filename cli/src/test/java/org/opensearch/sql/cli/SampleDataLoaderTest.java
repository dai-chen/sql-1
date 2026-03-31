/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.cli;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.contains;
import static org.hamcrest.Matchers.containsString;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.hasKey;
import static org.hamcrest.Matchers.instanceOf;
import static org.hamcrest.Matchers.is;

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.type.SqlTypeName;
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

  @Test
  public void testLoadCsvFile() throws Exception {
    String csv = "id,name,age\n1,Alice,30\n2,Bob,25\n";
    InputStream is = new ByteArrayInputStream(csv.getBytes(StandardCharsets.UTF_8));
    Map<String, Table> tables = SampleDataLoader.loadCsvFile(is, "people");
    assertThat(tables, hasKey("people"));
    RelDataType rowType = tables.get("people").getRowType(new JavaTypeFactoryImpl());
    assertThat(rowType.getFieldNames(), contains("id", "name", "age"));
    assertThat(rowType.getFieldList().get(0).getType().getSqlTypeName(), is(SqlTypeName.INTEGER));
    assertThat(rowType.getFieldList().get(1).getType().getSqlTypeName(), is(SqlTypeName.VARCHAR));
    assertThat(rowType.getFieldList().get(2).getType().getSqlTypeName(), is(SqlTypeName.INTEGER));
  }

  @Test
  public void testLoadCsvFileTypeInference() throws Exception {
    String csv = "name,price,active\nWidget,9.99,true\nGadget,19.99,false\n";
    InputStream is = new ByteArrayInputStream(csv.getBytes(StandardCharsets.UTF_8));
    Map<String, Table> tables = SampleDataLoader.loadCsvFile(is, "items");
    RelDataType rowType = tables.get("items").getRowType(new JavaTypeFactoryImpl());
    assertThat(rowType.getFieldList().get(0).getType().getSqlTypeName(), is(SqlTypeName.VARCHAR));
    assertThat(rowType.getFieldList().get(1).getType().getSqlTypeName(), is(SqlTypeName.DOUBLE));
    assertThat(rowType.getFieldList().get(2).getType().getSqlTypeName(), is(SqlTypeName.BOOLEAN));
  }

  @Test
  public void testLoadFileDetectsCsv() throws Exception {
    Map<String, Table> tables = SampleDataLoader.loadFile("cli/examples/orders.csv");
    assertThat(tables, hasKey("orders"));
    RelDataType rowType = tables.get("orders").getRowType(new JavaTypeFactoryImpl());
    assertThat(rowType.getFieldNames(), hasItem("customer"));
    assertThat(rowType.getFieldNames(), hasItem("price"));
  }
}
