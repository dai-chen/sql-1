/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.common.routing;

import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/** Utility class for detecting and routing queries targeting Parquet indices. */
public class ParquetIndexRouting {

  private static final Pattern SOURCE_PATTERN =
      Pattern.compile("source\\s*=\\s*(\\w+)", Pattern.CASE_INSENSITIVE);

  private static final Pattern FROM_PATTERN =
      Pattern.compile("FROM\\s+(\\w+)", Pattern.CASE_INSENSITIVE);

  private static final Set<String> PARQUET_INDICES = Set.of("parquet_index");

  private ParquetIndexRouting() {}

  /**
   * Extract the source index name from a PPL query string.
   *
   * @param query PPL query string (e.g. "source = parquet_index | where status = 200")
   * @return the index name, or null if not found
   */
  public static String extractIndexName(String query) {
    if (query == null) {
      return null;
    }
    Matcher matcher = SOURCE_PATTERN.matcher(query);
    return matcher.find() ? matcher.group(1) : null;
  }

  /**
   * Extract the target index name from a SQL query string.
   *
   * @param query SQL query string (e.g. "SELECT * FROM parquet_index WHERE status = 200")
   * @return the index name, or null if not found
   */
  public static String extractIndexNameFromSql(String query) {
    if (query == null) {
      return null;
    }
    Matcher matcher = FROM_PATTERN.matcher(query);
    return matcher.find() ? matcher.group(1) : null;
  }

  /**
   * Check if the given index name is a known Parquet index.
   *
   * @param indexName the index name to check
   * @return true if it is a Parquet index
   */
  public static boolean isParquetIndex(String indexName) {
    return indexName != null && PARQUET_INDICES.contains(indexName);
  }
}
