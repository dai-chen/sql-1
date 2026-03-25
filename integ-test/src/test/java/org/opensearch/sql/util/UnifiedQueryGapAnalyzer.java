/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.util;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.json.JSONArray;
import org.json.JSONObject;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.opensearch.client.RestClient;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.sql.api.UnifiedQueryContext;
import org.opensearch.sql.api.UnifiedQueryPlanner;
import org.opensearch.sql.api.compiler.UnifiedQueryCompiler;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.executor.QueryType;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.client.OpenSearchRestClient;
import org.opensearch.sql.opensearch.storage.OpenSearchIndex;

/**
 * Utility that replays queries through the UnifiedQueryPlanner + UnifiedQueryCompiler pipeline for
 * gap analysis. Controlled by system property {@code unified.gap.analysis} (default false).
 */
public class UnifiedQueryGapAnalyzer {

  private static final String PROPERTY = "unified.gap.analysis";

  /** Result of a unified pipeline execution attempt. */
  public static class GapResult {
    public enum Phase {
      PLAN,
      COMPILE,
      EXECUTE,
      RESULT_MISMATCH
    }

    public final Phase phase;
    public final boolean success;
    public final String errorMessage;
    public final String exceptionClass;

    private GapResult(Phase phase, boolean success, String errorMessage, String exceptionClass) {
      this.phase = phase;
      this.success = success;
      this.errorMessage = errorMessage;
      this.exceptionClass = exceptionClass;
    }

    public static GapResult success() {
      return new GapResult(Phase.EXECUTE, true, null, null);
    }

    public static GapResult failure(Phase phase, Throwable t) {
      Throwable root = t;
      while (root.getCause() != null && root.getCause() != root) {
        root = root.getCause();
      }
      return new GapResult(phase, false, root.getMessage(), root.getClass().getName());
    }

    public static GapResult resultMismatch(String diff) {
      return new GapResult(Phase.RESULT_MISMATCH, false, diff, null);
    }
  }

  private static final String CATALOG = "opensearch";
  private static final String CATALOG_DOT = CATALOG + ".";

  // Matches source=<index> with optional spaces, case-insensitive.
  // Skips source=[subquery] (bracket indicates subquery, not an index name).
  private static final Pattern SOURCE_PATTERN =
      Pattern.compile(
          "(?i)(\\bsource\\s*=\\s*)(?!\\[)(?!" + Pattern.quote(CATALOG_DOT) + ")([\\w.*-]+)");

  // Matches join ... on <condition> <index> — the index is the word after the ON clause
  private static final Pattern JOIN_PATTERN =
      Pattern.compile(
          "(?i)(\\bjoin\\b\\s+(?:(?:left|right|semi|anti|cross|inner|outer|full)\\s+)*"
              + "(?:on\\s+\\S+\\s+))(?!"
              + Pattern.quote(CATALOG_DOT)
              + ")([\\w.*-]+)");

  // Matches lookup <index> (the index right after lookup keyword)
  private static final Pattern LOOKUP_PATTERN =
      Pattern.compile(
          "(?i)(\\blookup\\s+)(?!" + Pattern.quote(CATALOG_DOT) + ")([\\w.*-]+)");

  public static boolean isEnabled() {
    return Boolean.getBoolean(PROPERTY);
  }

  /**
   * Transforms a PPL query to add the opensearch catalog prefix to index names. Handles
   * source=index, join index, and lookup index patterns. Does not modify queries that already have
   * the catalog prefix.
   */
  public static String transformPPLQuery(String query) {
    // Unescape JSON encoding — test queries are pre-escaped for buildRequest()'s JSON template,
    // but the unified pipeline receives the raw string without a JSON decode round-trip.
    String result = unescapeJsonString(query);

    // Transform source=<index> (but not source=[subquery...])
    result =
        SOURCE_PATTERN
            .matcher(result)
            .replaceAll(m -> m.group(1) + CATALOG_DOT + m.group(2));

    // Transform lookup <index>
    result =
        LOOKUP_PATTERN.matcher(result).replaceAll(m -> m.group(1) + CATALOG_DOT + m.group(2));

    // Transform join ... <index>
    result = JOIN_PATTERN.matcher(result).replaceAll(m -> m.group(1) + CATALOG_DOT + m.group(2));

    return result;
  }

  /**
   * Attempts to run the query through the unified pipeline. Never throws — all exceptions are
   * caught and returned as a GapResult. Returns null when gap analysis is disabled.
   */
  public static GapResult tryUnifiedExecution(RestClient restClient, String query, QueryType qt) {
    return tryUnifiedExecution(restClient, query, qt, null);
  }

  /**
   * Attempts to run the query through the unified pipeline and optionally compares the result with
   * the V2 response. Never throws — all exceptions are caught and returned as a GapResult. Returns
   * null when gap analysis is disabled.
   */
  public static GapResult tryUnifiedExecution(
      RestClient restClient, String query, QueryType qt, String v2Response) {
    if (!isEnabled()) {
      return null;
    }
    try {
      OpenSearchClient osClient =
          new OpenSearchRestClient(new InternalRestHighLevelClient(restClient));
      // Build context with a lazy schema that resolves tables on demand
      Settings[] settingsHolder = new Settings[1];
      String catalogName = "opensearch";
      AbstractSchema schema = createSchema(osClient, settingsHolder);
      UnifiedQueryContext context =
          UnifiedQueryContext.builder()
              .language(qt)
              .catalog(catalogName, schema)
              .defaultNamespace(catalogName)
              .setting("plugins.query.size_limit", 200)
              .setting("plugins.query.buckets", 1000)
              .setting("search.max_buckets", 65535)
              .setting("plugins.sql.cursor.keep_alive", TimeValue.timeValueMinutes(1))
              .setting("plugins.query.field_type_tolerance", true)
              .setting("plugins.calcite.enabled", true)
              .setting("plugins.calcite.pushdown.enabled", true)
              .setting("plugins.calcite.pushdown.rowcount.estimation.factor", 0.9)
              .build();
      settingsHolder[0] = context.getSettings();

      try {
        UnifiedQueryPlanner planner = new UnifiedQueryPlanner(context);
        UnifiedQueryCompiler compiler = new UnifiedQueryCompiler(context);

        RelNode plan;
        try {
          plan = planner.plan(query);
        } catch (Exception e) {
          return GapResult.failure(GapResult.Phase.PLAN, e);
        }

        PreparedStatement stmt;
        try {
          stmt = compiler.compile(plan);
        } catch (Exception e) {
          return GapResult.failure(GapResult.Phase.COMPILE, e);
        }

        try (stmt) {
          ResultSet rs = stmt.executeQuery();
          List<List<Object>> calciteRows = extractResultSetData(rs);
          if (v2Response != null) {
            List<List<Object>> v2Rows = extractV2Data(v2Response);
            if (v2Rows != null) {
              String diff = compareResults(v2Rows, calciteRows, hasOrderBy(query));
              if (diff != null) {
                return GapResult.resultMismatch(diff);
              }
            }
          }
        } catch (Exception e) {
          return GapResult.failure(GapResult.Phase.EXECUTE, e);
        }

        return GapResult.success();
      } finally {
        context.close();
      }
    } catch (Exception e) {
      return GapResult.failure(GapResult.Phase.PLAN, e);
    }
  }

  /**
   * Unescape JSON string encoding. Test queries are pre-escaped for the REST buildRequest() JSON
   * template (e.g., \" becomes \\\"), but the unified pipeline bypasses the JSON round-trip.
   */
  public static String unescapeJsonString(String s) {
    return s.replace("\\\"", "\"").replace("\\'", "'").replace("\\\\", "\\");
  }

  // Matches unquoted table names containing hyphens in FROM/JOIN clauses.
  // Handles: FROM name, JOIN name, FROM name AS alias
  private static final Pattern HYPHENATED_TABLE =
      Pattern.compile(
          "(?i)(\\bFROM\\s+|\\bJOIN\\s+)([\\w][\\w.-]*-[\\w.-]*)");

  /**
   * Double-quote hyphenated table names so Calcite's SQL parser treats them as identifiers instead
   * of expressions with minus operators. E.g., {@code FROM opensearch-sql_test_index_account}
   * becomes {@code FROM "opensearch-sql_test_index_account"}.
   */
  public static String quoteHyphenatedTableNames(String sql) {
    return HYPHENATED_TABLE.matcher(sql).replaceAll(m -> m.group(1) + "\"" + m.group(2) + "\"");
  }

  private static List<List<Object>> extractResultSetData(ResultSet rs) throws Exception {
    List<List<Object>> rows = new ArrayList<>();
    ResultSetMetaData meta = rs.getMetaData();
    int colCount = meta.getColumnCount();
    while (rs.next()) {
      List<Object> row = new ArrayList<>();
      for (int i = 1; i <= colCount; i++) {
        row.add(rs.getObject(i));
      }
      rows.add(row);
    }
    return rows;
  }

  private static List<List<Object>> extractV2Data(String v2Response) {
    JSONObject json = new JSONObject(v2Response);
    if (!json.has("datarows")) return null;
    JSONArray datarows = json.getJSONArray("datarows");
    List<List<Object>> rows = new ArrayList<>();
    for (int i = 0; i < datarows.length(); i++) {
      JSONArray row = datarows.getJSONArray(i);
      List<Object> rowList = new ArrayList<>();
      for (int j = 0; j < row.length(); j++) {
        rowList.add(row.isNull(j) ? null : row.get(j));
      }
      rows.add(rowList);
    }
    return rows;
  }

  private static String compareResults(
      List<List<Object>> v2Rows, List<List<Object>> calciteRows, boolean ordered) {
    if (v2Rows.size() != calciteRows.size()) {
      return String.format("Row count: V2=%d, Calcite=%d", v2Rows.size(), calciteRows.size());
    }
    List<List<String>> v2Norm = normalize(v2Rows);
    List<List<String>> calciteNorm = normalize(calciteRows);
    if (!ordered) {
      v2Norm.sort(Comparator.comparing(Object::toString));
      calciteNorm.sort(Comparator.comparing(Object::toString));
    }
    for (int i = 0; i < v2Norm.size(); i++) {
      if (!v2Norm.get(i).equals(calciteNorm.get(i))) {
        return String.format(
            "Row %d differs: V2=%s, Calcite=%s", i, v2Norm.get(i), calciteNorm.get(i));
      }
    }
    return null;
  }

  private static List<List<String>> normalize(List<List<Object>> rows) {
    return rows.stream()
        .map(
            row ->
                row.stream()
                    .map(
                        v -> {
                          if (v == null) return "NULL";
                          if (v instanceof Float || v instanceof Double) {
                            return String.format("%.2f", ((Number) v).doubleValue());
                          }
                          if (v instanceof Number) {
                            return String.valueOf(((Number) v).longValue());
                          }
                          return v.toString();
                        })
                    .collect(Collectors.toList()))
        .collect(Collectors.toList());
  }

  private static boolean hasOrderBy(String query) {
    return query.toUpperCase().contains("ORDER BY");
  }

  private static AbstractSchema createSchema(
      OpenSearchClient osClient, Settings[] settingsHolder) {
    // Single shared map instance — avoids re-creating on every getTableMap() call,
    // which caused redundant REST _mapping fetches and race conditions under concurrency.
    Map<String, Table> tableMap =
        new HashMap<>() {
          @Override
          public Table get(Object key) {
            if (!super.containsKey(key)) {
              String indexName = (String) key;
              super.put(indexName, new OpenSearchIndex(osClient, settingsHolder[0], indexName));
            }
            return super.get(key);
          }
        };
    return new AbstractSchema() {
      @Override
      protected Map<String, Table> getTableMap() {
        return tableMap;
      }
    };
  }
}
