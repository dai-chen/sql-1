/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.cli;

import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import org.junit.Test;

public class ResultSetFormatterTest {

  @Test
  public void testFormatBasicTable() throws Exception {
    ResultSet rs = mock(ResultSet.class);
    ResultSetMetaData meta = mock(ResultSetMetaData.class);
    when(rs.getMetaData()).thenReturn(meta);
    when(meta.getColumnCount()).thenReturn(2);
    when(meta.getColumnName(1)).thenReturn("name");
    when(meta.getColumnName(2)).thenReturn("age");
    when(rs.next()).thenReturn(true, true, false);
    when(rs.getObject(1)).thenReturn("Alice", "Bob");
    when(rs.getObject(2)).thenReturn(30, 25);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(baos);
    ResultSetFormatter.format(rs, out);
    String output = baos.toString();

    assertTrue(output.contains("| name"));
    assertTrue(output.contains("| age"));
    assertTrue(output.contains("+--"));
    assertTrue(output.contains("Alice"));
    assertTrue(output.contains("Bob"));
    assertTrue(output.contains("30"));
    assertTrue(output.contains("25"));
    assertTrue(output.contains("2 row(s) returned"));
  }

  @Test
  public void testFormatWithNullValues() throws Exception {
    ResultSet rs = mock(ResultSet.class);
    ResultSetMetaData meta = mock(ResultSetMetaData.class);
    when(rs.getMetaData()).thenReturn(meta);
    when(meta.getColumnCount()).thenReturn(2);
    when(meta.getColumnName(1)).thenReturn("name");
    when(meta.getColumnName(2)).thenReturn("age");
    when(rs.next()).thenReturn(true, false);
    when(rs.getObject(1)).thenReturn("Alice");
    when(rs.getObject(2)).thenReturn(null);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(baos);
    ResultSetFormatter.format(rs, out);
    String output = baos.toString();

    assertTrue(output.contains("NULL"));
    assertTrue(output.contains("1 row(s) returned"));
  }

  @Test
  public void testFormatEmptyResultSet() throws Exception {
    ResultSet rs = mock(ResultSet.class);
    ResultSetMetaData meta = mock(ResultSetMetaData.class);
    when(rs.getMetaData()).thenReturn(meta);
    when(meta.getColumnCount()).thenReturn(2);
    when(meta.getColumnName(1)).thenReturn("name");
    when(meta.getColumnName(2)).thenReturn("age");
    when(rs.next()).thenReturn(false);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(baos);
    ResultSetFormatter.format(rs, out);
    String output = baos.toString();

    assertTrue(output.contains("| name"));
    assertTrue(output.contains("+--"));
    assertTrue(output.contains("0 row(s) returned"));
  }

  @Test
  public void testFormatWithElapsedTime() throws Exception {
    ResultSet rs = mock(ResultSet.class);
    ResultSetMetaData meta = mock(ResultSetMetaData.class);
    when(rs.getMetaData()).thenReturn(meta);
    when(meta.getColumnCount()).thenReturn(1);
    when(meta.getColumnName(1)).thenReturn("id");
    when(rs.next()).thenReturn(true, false);
    when(rs.getObject(1)).thenReturn(1);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(baos);
    ResultSetFormatter.format(rs, out, 42);
    String output = baos.toString();

    assertTrue(output.contains("1 row(s) returned (42ms)"));
  }

  @Test
  public void testFormatWithBarChart() throws Exception {
    ResultSet rs = mock(ResultSet.class);
    ResultSetMetaData meta = mock(ResultSetMetaData.class);
    when(rs.getMetaData()).thenReturn(meta);
    when(meta.getColumnCount()).thenReturn(2);
    when(meta.getColumnName(1)).thenReturn("component");
    when(meta.getColumnName(2)).thenReturn("count");
    when(rs.next()).thenReturn(true, true, true, false);
    when(rs.getObject(1)).thenReturn("api", "core", "sql");
    when(rs.getObject(2)).thenReturn(10, 5, 2);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(baos);
    ResultSetFormatter.format(rs, out, -1, true);
    String output = baos.toString();

    assertTrue(output.contains("\u2588"));
    assertTrue(output.contains("chart"));
  }

  @Test
  public void testFormatWithBarChartTimeSeries() throws Exception {
    ResultSet rs = mock(ResultSet.class);
    ResultSetMetaData meta = mock(ResultSetMetaData.class);
    when(rs.getMetaData()).thenReturn(meta);
    when(meta.getColumnCount()).thenReturn(2);
    when(meta.getColumnName(1)).thenReturn("timestamp");
    when(meta.getColumnName(2)).thenReturn("count");
    when(rs.next()).thenReturn(true, true, false);
    when(rs.getObject(1)).thenReturn("2024-01-01 10:00:00", "2024-01-01 11:00:00");
    when(rs.getObject(2)).thenReturn(10, 5);

    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    PrintStream out = new PrintStream(baos);
    ResultSetFormatter.format(rs, out, -1, true);
    String output = baos.toString();

    assertTrue(output.contains("trend"));
    assertTrue(output.contains("\u2588"));
  }
}
