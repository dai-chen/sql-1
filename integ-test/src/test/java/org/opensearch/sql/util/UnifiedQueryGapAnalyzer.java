/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.util;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.util.HashMap;
import java.util.Map;
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
      return new GapResult(phase, false, t.getMessage(), t.getClass().getName());
    }
  }

  public static boolean isEnabled() {
    return Boolean.getBoolean(PROPERTY);
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

  private static AbstractSchema createSchema(
      OpenSearchClient osClient, Settings[] settingsHolder) {
    return new AbstractSchema() {
      @Override
      protected Map<String, Table> getTableMap() {
        return new HashMap<>() {
          @Override
          public Table get(Object key) {
            if (!super.containsKey(key)) {
              String indexName = (String) key;
              super.put(indexName, new OpenSearchIndex(osClient, settingsHolder[0], indexName));
            }
            return super.get(key);
          }
        };
      }
    };
  }
}
