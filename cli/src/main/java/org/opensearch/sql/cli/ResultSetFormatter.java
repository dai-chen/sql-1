/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.cli;

import java.io.PrintStream;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

/** Formats a JDBC ResultSet as an ASCII table. */
public class ResultSetFormatter {

  private ResultSetFormatter() {}

  /** Print the ResultSet as an ASCII table to the given PrintStream. */
  public static void format(ResultSet rs, PrintStream out) throws SQLException {
    format(rs, out, -1);
  }

  /** Print the ResultSet as an ASCII table with elapsed time in the status line. */
  public static void format(ResultSet rs, PrintStream out, long elapsedMs) throws SQLException {
    ResultSetMetaData meta = rs.getMetaData();
    int colCount = meta.getColumnCount();

    String[] headers = new String[colCount];
    for (int i = 0; i < colCount; i++) {
      headers[i] = meta.getColumnName(i + 1);
    }

    List<Object[]> rows = new ArrayList<>();
    while (rs.next()) {
      Object[] row = new Object[colCount];
      for (int i = 0; i < colCount; i++) {
        row[i] = rs.getObject(i + 1);
      }
      rows.add(row);
    }

    int[] widths = new int[colCount];
    for (int i = 0; i < colCount; i++) {
      widths[i] = headers[i].length();
    }
    for (Object[] row : rows) {
      for (int i = 0; i < colCount; i++) {
        widths[i] = Math.max(widths[i], toStr(row[i]).length());
      }
    }

    out.println(formatRow(headers, widths));
    out.println(separatorLine(widths));
    for (Object[] row : rows) {
      String[] vals = new String[colCount];
      for (int i = 0; i < colCount; i++) {
        vals[i] = toStr(row[i]);
      }
      out.println(formatRow(vals, widths));
    }
    out.println();
    String status = rows.size() + " row(s) returned";
    if (elapsedMs >= 0) {
      status += " (" + elapsedMs + "ms)";
    }
    out.println(status);
  }

  private static String toStr(Object val) {
    return val == null ? "NULL" : String.valueOf(val);
  }

  private static String formatRow(String[] values, int[] widths) {
    StringBuilder sb = new StringBuilder("| ");
    for (int i = 0; i < values.length; i++) {
      if (i > 0) {
        sb.append(" | ");
      }
      sb.append(String.format("%-" + widths[i] + "s", values[i]));
    }
    sb.append(" |");
    return sb.toString();
  }

  private static String separatorLine(int[] widths) {
    StringBuilder sb = new StringBuilder("+-");
    for (int i = 0; i < widths.length; i++) {
      if (i > 0) {
        sb.append("-+-");
      }
      sb.append("-".repeat(widths[i]));
    }
    sb.append("-+");
    return sb.toString();
  }
}
