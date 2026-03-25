/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.util;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;
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
      EXECUTE
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
          rs.next();
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
