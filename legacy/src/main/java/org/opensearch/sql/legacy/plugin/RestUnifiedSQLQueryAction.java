/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.plugin;

import static org.opensearch.core.rest.RestStatus.OK;
import static org.opensearch.sql.opensearch.executor.OpenSearchQueryManager.SQL_WORKER_THREAD_POOL_NAME;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.validate.SqlConformanceEnum;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.common.inject.Injector;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.sql.api.UnifiedQueryContext;
import org.opensearch.sql.api.UnifiedQueryPlanner;
import org.opensearch.sql.api.compiler.UnifiedQueryCompiler;
import org.opensearch.sql.opensearch.storage.AnsiSQLOpenSearchSchema;
import org.opensearch.sql.calcite.OpenSearchSchema;
import org.opensearch.sql.datasource.DataSourceService;
import org.opensearch.sql.executor.QueryType;
import org.opensearch.sql.sql.domain.SQLQueryRequest;
import org.opensearch.transport.client.node.NodeClient;

/**
 * REST handler that routes SQL queries through the unified query API. Without mode=ansi: OpenSearch
 * SQL (ANTLR parser, supports match() etc.) With mode=ansi: Calcite-native ANSI SQL (supports
 * JOINs, standard SQL)
 */
public class RestUnifiedSQLQueryAction {

  private static final Logger LOG = LogManager.getLogger(RestUnifiedSQLQueryAction.class);

  private final DataSourceService dataSourceService;

  public RestUnifiedSQLQueryAction(Injector injector) {
    this.dataSourceService = injector.getInstance(DataSourceService.class);
  }

  /**
   * Schedule SQL query execution on the sql-worker thread pool to avoid blocking transport threads.
   */
  public void execute(SQLQueryRequest request, RestChannel channel, NodeClient client) {
    client
        .threadPool()
        .schedule(
            () -> {
              try {
                String result = executeWithUnifiedAPI(request);
                channel.sendResponse(
                    new BytesRestResponse(OK, "application/json; charset=UTF-8", result));
              } catch (Exception e) {
                LOG.error("Failed to execute unified SQL query", e);
                channel.sendResponse(
                    new BytesRestResponse(
                        org.opensearch.core.rest.RestStatus.INTERNAL_SERVER_ERROR,
                        "application/json; charset=UTF-8",
                        formatError(e)));
              }
            },
            new TimeValue(0),
            SQL_WORKER_THREAD_POOL_NAME);
  }

  private String executeWithUnifiedAPI(SQLQueryRequest request) throws Exception {
    String catalogName = OpenSearchSchema.OPEN_SEARCH_SCHEMA_NAME;
    OpenSearchSchema baseSchema = new OpenSearchSchema(dataSourceService);
    var builder =
        UnifiedQueryContext.builder()
            .language(QueryType.SQL)
            .catalog(
                catalogName,
                request.isOpenSearchMode()
                    ? baseSchema
                    : new AnsiSQLOpenSearchSchema(baseSchema))
            .defaultNamespace(catalogName);

    // PoC: ANSI SQL (Calcite) is default; mode=opensearch falls back to OpenSearch SQL (ANTLR)
    if (!request.isOpenSearchMode()) {
      builder.conformance(SqlConformanceEnum.LENIENT);
    }

    try (UnifiedQueryContext context = builder.build()) {
      UnifiedQueryPlanner planner = new UnifiedQueryPlanner(context);
      UnifiedQueryCompiler compiler = new UnifiedQueryCompiler(context);

      String query = request.getQuery().trim();
      // Handle EXPLAIN: strip prefix, plan, show logical + optimized physical plan
      if (query.regionMatches(true, 0, "EXPLAIN ", 0, 8)) {
        String innerQuery = query.substring(query.toUpperCase().indexOf("SELECT"));
        RelNode plan = planner.plan(innerQuery);
        String logical = org.apache.calcite.plan.RelOptUtil.toString(
            plan, org.apache.calcite.sql.SqlExplainLevel.EXPPLAN_ATTRIBUTES);

        // Capture optimized physical plan via Hook during compilation
        java.util.concurrent.atomic.AtomicReference<String> physical =
            new java.util.concurrent.atomic.AtomicReference<>();
        try (org.apache.calcite.runtime.Hook.Closeable ignored =
            org.apache.calcite.runtime.Hook.PLAN_BEFORE_IMPLEMENTATION.addThread(obj -> {
              org.apache.calcite.rel.RelRoot relRoot = (org.apache.calcite.rel.RelRoot) obj;
              physical.set(org.apache.calcite.plan.RelOptUtil.toString(
                  relRoot.rel, org.apache.calcite.sql.SqlExplainLevel.EXPPLAN_ATTRIBUTES));
            })) {
          try (PreparedStatement stmt = compiler.compile(plan)) {
            // triggers optimization pipeline and the hook
          }
        }

        org.json.JSONObject result = new org.json.JSONObject();
        result.put("logical", logical);
        if (physical.get() != null) {
          result.put("physical", physical.get());
        }
        return result.toString(2);
      }

      RelNode plan = planner.plan(query);
      List<String> udtTypes = extractUdtTypes(plan);
      try (PreparedStatement statement = compiler.compile(plan)) {
        ResultSet rs;
        try {
          rs = statement.executeQuery();
        } catch (ExceptionInInitializerError e) {
          // Calcite's generated code can throw this for invalid data (e.g. bad date literals).
          // Catch it here to prevent crashing the OpenSearch node.
          Throwable cause = e.getCause() != null ? e.getCause() : e;
          throw new IllegalStateException("Failed to compile logical plan", cause);
        }
        return formatAsJdbc(rs, udtTypes);
      }
    }
  }

  private static List<String> extractUdtTypes(RelNode plan) {
    List<String> types = new ArrayList<>();
    for (RelDataTypeField field : plan.getRowType().getFieldList()) {
      RelDataType t = field.getType();
      String typeName = t.toString();
      if (typeName.contains("EXPR_IP")) {
        types.add("ip");
      } else if (t.getSqlTypeName() == SqlTypeName.MAP) {
        types.add("struct");
      } else {
        types.add(null);
      }
    }
    return types;
  }

  private String formatAsJdbc(ResultSet rs, List<String> udtTypes) throws Exception {
    ResultSetMetaData meta = rs.getMetaData();
    int columnCount = meta.getColumnCount();

    StringBuilder json = new StringBuilder();
    json.append("{\"schema\":[");
    for (int i = 1; i <= columnCount; i++) {
      if (i > 1) json.append(",");
      String udtOverride = (i - 1 < udtTypes.size()) ? udtTypes.get(i - 1) : null;
      json.append("{\"name\":\"")
          .append(escape(meta.getColumnLabel(i)))
          .append("\",\"type\":\"")
          .append(udtOverride != null ? udtOverride : jdbcTypeToString(meta.getColumnType(i), meta.getColumnTypeName(i)))
          .append("\"}");
    }
    json.append("],");

    json.append("\"datarows\":[");
    List<String> rows = new ArrayList<>();
    while (rs.next()) {
      StringBuilder row = new StringBuilder("[");
      for (int i = 1; i <= columnCount; i++) {
        if (i > 1) row.append(",");
        Object val = rs.getObject(i);
        if (val == null) {
          row.append("null");
        } else if (val instanceof Number) {
          row.append(val);
        } else if (val instanceof Boolean) {
          row.append(val);
        } else if (val instanceof java.util.List) {
          row.append(new org.json.JSONArray((java.util.List<?>) val));
        } else if (val instanceof java.sql.Array) {
          Object arr = ((java.sql.Array) val).getArray();
          if (arr instanceof Object[]) {
            row.append(new org.json.JSONArray(java.util.Arrays.asList((Object[]) arr)));
          } else {
            row.append(new org.json.JSONArray(val.toString()));
          }
        } else if (val instanceof java.util.Map) {
          row.append(new org.json.JSONObject((java.util.Map<?, ?>) val));
        } else {
          row.append("\"").append(escape(val.toString())).append("\"");
        }
      }
      row.append("]");
      rows.add(row.toString());
    }
    json.append(String.join(",", rows));
    json.append("],");

    json.append("\"total\":")
        .append(rows.size())
        .append(",\"size\":")
        .append(rows.size())
        .append(",\"status\":200}");

    return json.toString();
  }

  private static String formatError(Exception e) {
    String reason = e.getMessage() != null ? escape(e.getMessage()) : "Unknown error";
    return "{\"error\":{\"type\":\""
        + e.getClass().getSimpleName()
        + "\",\"reason\":\""
        + reason
        + "\"},\"status\":500}";
  }

  private static String escape(String s) {
    return s.replace("\\", "\\\\").replace("\"", "\\\"");
  }

  private static String jdbcTypeToString(int type, String typeName) {
    if (typeName != null && typeName.contains("EXPR_IP")) return "ip";
    return switch (type) {
      case java.sql.Types.VARCHAR, java.sql.Types.CHAR, java.sql.Types.LONGVARCHAR -> "keyword";
      case java.sql.Types.INTEGER -> "integer";
      case java.sql.Types.BIGINT -> "long";
      case java.sql.Types.SMALLINT -> "short";
      case java.sql.Types.TINYINT -> "byte";
      case java.sql.Types.FLOAT, java.sql.Types.REAL -> "float";
      case java.sql.Types.DOUBLE, java.sql.Types.DECIMAL, java.sql.Types.NUMERIC -> "double";
      case java.sql.Types.BOOLEAN -> "boolean";
      case java.sql.Types.DATE -> "date";
      case java.sql.Types.TIME -> "time";
      case java.sql.Types.TIMESTAMP -> "timestamp";
      case java.sql.Types.ARRAY -> "array";
      default -> "keyword";
    };
  }
}
