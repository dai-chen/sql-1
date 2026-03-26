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
}
