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

  /** Load a CSV file with header-based schema and type inference. */
  public static Map<String, Table> loadCsvFile(InputStream inputStream, String tableName)
      throws IOException {
    List<String> lines =
        new BufferedReader(new InputStreamReader(inputStream, StandardCharsets.UTF_8))
            .lines()
            .filter(l -> !l.isEmpty())
            .collect(Collectors.toList());
    if (lines.isEmpty()) {
      return Map.of(tableName, SimpleTable.builder().build());
    }
    String[] headers = lines.get(0).split(",", -1);
    for (int i = 0; i < headers.length; i++) {
      headers[i] = stripQuotes(headers[i].trim());
    }
    // Infer types from first data row
    SqlTypeName[] types = new SqlTypeName[headers.length];
    if (lines.size() > 1) {
      String[] firstRow = lines.get(1).split(",", -1);
      for (int i = 0; i < headers.length; i++) {
        String val = i < firstRow.length ? stripQuotes(firstRow[i].trim()) : "";
        types[i] = inferCsvType(val);
      }
    } else {
      for (int i = 0; i < headers.length; i++) {
        types[i] = SqlTypeName.VARCHAR;
      }
    }
    SimpleTable.SimpleTableBuilder builder = SimpleTable.builder();
    for (int i = 0; i < headers.length; i++) {
      builder.col(headers[i], types[i]);
    }
    for (int r = 1; r < lines.size(); r++) {
      String[] parts = lines.get(r).split(",", -1);
      Object[] row = new Object[headers.length];
      for (int i = 0; i < headers.length; i++) {
        String val = i < parts.length ? stripQuotes(parts[i].trim()) : null;
        row[i] = convertCsvValue(val, types[i]);
      }
      builder.row(row);
    }
    return Map.of(tableName, builder.build());
  }

  private static String stripQuotes(String value) {
    if (value != null && value.length() >= 2 && value.startsWith("\"") && value.endsWith("\"")) {
      return value.substring(1, value.length() - 1);
    }
    return value;
  }

  private static SqlTypeName inferCsvType(String value) {
    if (value == null || value.isEmpty()) {
      return SqlTypeName.VARCHAR;
    }
    try {
      Integer.parseInt(value);
      return SqlTypeName.INTEGER;
    } catch (NumberFormatException e) {
      // not an integer
    }
    try {
      Double.parseDouble(value);
      return SqlTypeName.DOUBLE;
    } catch (NumberFormatException e) {
      // not a double
    }
    if (value.equalsIgnoreCase("true") || value.equalsIgnoreCase("false")) {
      return SqlTypeName.BOOLEAN;
    }
    return SqlTypeName.VARCHAR;
  }

  private static Object convertCsvValue(String value, SqlTypeName type) {
    if (value == null || value.isEmpty()) {
      return null;
    }
    switch (type) {
      case INTEGER:
        return Integer.parseInt(value);
      case DOUBLE:
        return Double.parseDouble(value);
      case BOOLEAN:
        return Boolean.parseBoolean(value);
      default:
        return value;
    }
  }

  /** Load a log file with multi-line joining and structured parsing using a LogFormat. */
  public static Map<String, Table> loadFormattedLogFile(
      InputStream inputStream, String tableName, LogFormat format) throws IOException {
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
    String[] columns = format.getColumns();
    SimpleTable.SimpleTableBuilder builder = SimpleTable.builder();
    for (String col : columns) {
      builder.col(col, SqlTypeName.VARCHAR);
    }
    java.util.regex.Pattern pattern = format.getPattern();
    for (String entry : entries) {
      java.util.regex.Matcher m = pattern.matcher(entry);
      if (m.matches()) {
        Object[] row = new Object[columns.length];
        for (int i = 0; i < columns.length; i++) {
          row[i] = m.group(i + 1).trim();
        }
        builder.row(row);
      } else {
        Object[] row = new Object[columns.length];
        row[columns.length - 1] = entry;
        builder.row(row);
      }
    }
    return Map.of(tableName, builder.build());
  }

  /** Load a file with optional format hint. */
  public static Map<String, Table> loadFile(String path, String format) throws IOException {
    if (format != null) {
      LogFormat logFormat = LogFormat.get(format);
      if (logFormat == null) {
        throw new IOException("Unknown format: " + format);
      }
      String tableName = deriveTableName(path);
      try (FileInputStream fis = new FileInputStream(path)) {
        return loadFormattedLogFile(fis, tableName, logFormat);
      }
    }
    return loadFile(path);
  }

  /** Load a file, detecting type by extension. .json uses JSON loader, others use line-per-row. */
  public static Map<String, Table> loadFile(String path) throws IOException {
    String lower = path.toLowerCase();
    String tableName = deriveTableName(path);
    if (lower.endsWith(".json")) {
      try (FileInputStream fis = new FileInputStream(path)) {
        return load(fis);
      }
    } else if (lower.endsWith(".csv")) {
      try (FileInputStream fis = new FileInputStream(path)) {
        return loadCsvFile(fis, tableName);
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
