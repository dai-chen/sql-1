/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.cli;

import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import java.io.BufferedReader;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
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

  /** Load a text file where each line becomes a row with a single 'line' VARCHAR column. */
  public static Map<String, Table> loadTextFile(InputStream inputStream, String tableName)
      throws IOException {
    List<String> lines =
        new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))
            .lines()
            .collect(Collectors.toList());
    SimpleTable.SimpleTableBuilder builder = SimpleTable.builder().col("line", SqlTypeName.VARCHAR);
    for (String line : lines) {
      builder.row(new Object[] {line});
    }
    return Map.of(tableName, builder.build());
  }

  /** Load a log file with multi-line joining. Lines not starting with '[' are continuations. */
  public static Map<String, Table> loadLogFile(InputStream inputStream, String tableName)
      throws IOException {
    List<String> lines =
        new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))
            .lines()
            .collect(Collectors.toList());
    List<String> entries = new ArrayList<>();
    for (String line : lines) {
      if (entries.isEmpty() || line.startsWith("[")) {
        entries.add(line);
      } else {
        entries.set(entries.size() - 1, entries.get(entries.size() - 1) + "\n" + line);
      }
    }
    SimpleTable.SimpleTableBuilder builder = SimpleTable.builder().col("line", SqlTypeName.VARCHAR);
    for (String entry : entries) {
      builder.row(new Object[] {entry});
    }
    return Map.of(tableName, builder.build());
  }

  /** Load a file, detecting type by extension. .json uses JSON loader, others use line-per-row. */
  public static Map<String, Table> loadFile(String path) throws IOException {
    String lower = path.toLowerCase();
    String tableName = deriveTableName(path);
    if (lower.endsWith(".json")) {
      try (FileInputStream fis = new FileInputStream(path)) {
        return load(fis);
      }
    } else if (lower.endsWith(".log")) {
      try (FileInputStream fis = new FileInputStream(path)) {
        return loadLogFile(fis, tableName);
      }
    } else {
      try (FileInputStream fis = new FileInputStream(path)) {
        return loadTextFile(fis, tableName);
      }
    }
  }

  private static String deriveTableName(String path) {
    String fileName = Path.of(path).getFileName().toString();
    int dot = fileName.lastIndexOf('.');
    return dot > 0 ? fileName.substring(0, dot) : fileName;
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
