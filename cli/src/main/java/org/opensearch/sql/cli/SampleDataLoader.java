/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.cli;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.api.SimpleTable;

/** Loads sample datasets from JSON files into Calcite tables. */
public class SampleDataLoader {

  private SampleDataLoader() {}

  private static final ObjectMapper MAPPER = new ObjectMapper();

  /** Load tables from a JSON input stream. */
  public static Map<String, Table> load(InputStream inputStream) throws IOException {
    Map<String, List<Map<String, Object>>> dataset =
        MAPPER.readValue(inputStream, new TypeReference<>() {});
    Map<String, Table> tables = new LinkedHashMap<>();
    dataset.forEach((tableName, rows) -> tables.put(tableName, buildTable(tableName, rows)));
    return tables;
  }

  /** Load tables from a classpath resource. */
  public static Map<String, Table> loadFromClasspath(String resourcePath) throws IOException {
    try (InputStream is =
        SampleDataLoader.class.getClassLoader().getResourceAsStream(resourcePath)) {
      if (is == null) {
        throw new IOException("Resource not found: " + resourcePath);
      }
      return load(is);
    }
  }

  private static SimpleTable buildTable(String tableName, List<Map<String, Object>> rows) {
    if (rows.isEmpty()) {
      return SimpleTable.builder().build();
    }

    // Infer schema from first row
    Map<String, Object> firstRow = rows.get(0);
    List<String> columns = new ArrayList<>();
    Map<String, SqlTypeName> schema = new LinkedHashMap<>();

    for (Map.Entry<String, Object> entry : firstRow.entrySet()) {
      SqlTypeName type = inferType(entry.getValue());
      if (type == null) {
        System.err.println(
            "Warning: skipping nested column '"
                + entry.getKey()
                + "' in table '"
                + tableName
                + "'");
        continue;
      }
      columns.add(entry.getKey());
      schema.put(entry.getKey(), type);
    }

    SimpleTable.SimpleTableBuilder builder = SimpleTable.builder();
    schema.forEach(builder::col);

    for (Map<String, Object> row : rows) {
      Object[] values = new Object[columns.size()];
      for (int i = 0; i < columns.size(); i++) {
        Object val = row.get(columns.get(i));
        SqlTypeName colType = schema.get(columns.get(i));
        values[i] = convertValue(val, colType);
      }
      builder.row(values);
    }

    return builder.build();
  }

  private static SqlTypeName inferType(Object value) {
    if (value == null) {
      return SqlTypeName.VARCHAR;
    }
    if (value instanceof Integer) {
      return SqlTypeName.INTEGER;
    }
    if (value instanceof Long) {
      long l = (Long) value;
      return (l >= Integer.MIN_VALUE && l <= Integer.MAX_VALUE)
          ? SqlTypeName.INTEGER
          : SqlTypeName.BIGINT;
    }
    if (value instanceof Float || value instanceof Double) {
      return SqlTypeName.DOUBLE;
    }
    if (value instanceof Boolean) {
      return SqlTypeName.BOOLEAN;
    }
    if (value instanceof String) {
      return SqlTypeName.VARCHAR;
    }
    // Nested object or array
    return null;
  }

  private static Object convertValue(Object value, SqlTypeName type) {
    if (value == null) {
      return null;
    }
    if (type == SqlTypeName.INTEGER && value instanceof Long) {
      return ((Long) value).intValue();
    }
    return value;
  }
}
