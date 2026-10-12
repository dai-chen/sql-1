/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.stage;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.List;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.sql.data.model.ExprIpValue;

/** Canonical transport representation for rows crossing a Calcite gather exchange. */
final class CalciteWireRowCodec {

  private static final byte GENERIC_CELL = 0;
  private static final byte DECIMAL_CELL = 1;

  private CalciteWireRowCodec() {}

  static List<Object[]> prepareForTransport(List<Object[]> input) {
    List<Object[]> rows = new ArrayList<>(input.size());
    for (Object[] row : input) {
      rows.add(prepareRow(row));
    }
    return List.copyOf(rows);
  }

  static List<Object[]> readRows(StreamInput in) throws IOException {
    int rowCount = in.readVInt();
    List<Object[]> rows = new ArrayList<>(rowCount);
    for (int row = 0; row < rowCount; row++) {
      int columnCount = in.readVInt();
      Object[] cells = new Object[columnCount];
      for (int column = 0; column < columnCount; column++) {
        cells[column] = readCell(in);
      }
      rows.add(cells);
    }
    return List.copyOf(rows);
  }

  static void writeRows(StreamOutput out, List<Object[]> rows) throws IOException {
    out.writeVInt(rows.size());
    for (Object[] row : rows) {
      out.writeVInt(row.length);
      for (Object cell : row) {
        writeCell(out, cell);
      }
    }
  }

  private static Object[] prepareRow(Object[] row) {
    Object[] prepared = row;
    for (int column = 0; column < row.length; column++) {
      if (row[column] instanceof ExprIpValue ip) {
        if (prepared == row) {
          prepared = row.clone();
        }
        prepared[column] = ip.value();
      }
    }
    return prepared;
  }

  private static void writeCell(StreamOutput out, Object cell) throws IOException {
    if (cell instanceof BigDecimal decimal) {
      out.writeByte(DECIMAL_CELL);
      out.writeString(decimal.toPlainString());
      return;
    }
    out.writeByte(GENERIC_CELL);
    out.writeGenericValue(cell);
  }

  private static Object readCell(StreamInput in) throws IOException {
    return switch (in.readByte()) {
      case GENERIC_CELL -> in.readGenericValue();
      case DECIMAL_CELL -> new BigDecimal(in.readString());
      default -> throw new IOException("Unknown calcite_exec cell encoding");
    };
  }
}
