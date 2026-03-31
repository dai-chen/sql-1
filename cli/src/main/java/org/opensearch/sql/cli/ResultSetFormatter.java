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
    format(rs, out, elapsedMs, false);
  }

  /** Print the ResultSet as an ASCII table, optionally with inline bar charts for numeric data. */
  public static void format(ResultSet rs, PrintStream out, long elapsedMs, boolean chartEnabled)
      throws SQLException {
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

    // Find last numeric column for bar chart
    int numericCol = -1;
    if (chartEnabled && !rows.isEmpty()) {
      for (int i = colCount - 1; i >= 0; i--) {
        for (Object[] row : rows) {
          if (row[i] instanceof Number) {
            numericCol = i;
            break;
          }
        }
        if (numericCol >= 0) break;
      }
    }

    // Compute bars if applicable
    String[] bars = null;
    String barHeader = null;
    if (numericCol >= 0) {
      double maxVal = 0;
      for (Object[] row : rows) {
        if (row[numericCol] instanceof Number) {
          maxVal = Math.max(maxVal, ((Number) row[numericCol]).doubleValue());
        }
      }
      bars = new String[rows.size()];
      for (int r = 0; r < rows.size(); r++) {
        Object v = rows.get(r)[numericCol];
        if (v instanceof Number && ((Number) v).doubleValue() > 0 && maxVal > 0) {
          int w = (int) (((Number) v).doubleValue() / maxVal * MAX_BAR_WIDTH);
          bars[r] = "\u2588".repeat(Math.max(1, w));
        } else {
          bars[r] = "";
        }
      }
      // Detect time-series: exactly 2 columns and first column values look like timestamps
      barHeader = "chart";
      if (colCount == 2) {
        boolean allTimestamp = true;
        for (Object[] row : rows) {
          String s = toStr(row[0]);
          if (!(s.contains("-") && s.contains(":"))) {
            allTimestamp = false;
            break;
          }
        }
        if (allTimestamp) barHeader = "trend";
      }
    }

    // Compute column widths
    int totalCols = bars != null ? colCount + 1 : colCount;
    int[] widths = new int[totalCols];
    for (int i = 0; i < colCount; i++) {
      widths[i] = headers[i].length();
    }
    if (bars != null) {
      widths[colCount] = barHeader.length();
      for (String bar : bars) {
        widths[colCount] = Math.max(widths[colCount], bar.length());
      }
    }
    for (Object[] row : rows) {
      for (int i = 0; i < colCount; i++) {
        widths[i] = Math.max(widths[i], toStr(row[i]).length());
      }
    }

    // Build header row
    String[] allHeaders = new String[totalCols];
    System.arraycopy(headers, 0, allHeaders, 0, colCount);
    if (bars != null) allHeaders[colCount] = barHeader;

    out.println(formatRow(allHeaders, widths));
    out.println(separatorLine(widths));
    for (int r = 0; r < rows.size(); r++) {
      String[] vals = new String[totalCols];
      for (int i = 0; i < colCount; i++) {
        vals[i] = toStr(rows.get(r)[i]);
      }
      if (bars != null) vals[colCount] = bars[r];
      out.println(formatRow(vals, widths));
    }
    out.println();
    String status = rows.size() + " row(s) returned";
    if (elapsedMs >= 0) {
      status += " (" + elapsedMs + "ms)";
    }
    out.println(status);
  }

  private static final int MAX_BAR_WIDTH = 30;

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
