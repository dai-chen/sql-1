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
import java.util.List;
import org.apache.calcite.rel.RelNode;
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
    var builder =
        UnifiedQueryContext.builder()
            .language(QueryType.SQL)
            .catalog(catalogName, new OpenSearchSchema(dataSourceService))
            .defaultNamespace(catalogName);

    if (request.isAnsiMode()) {
      builder.conformance(SqlConformanceEnum.DEFAULT);
    }

    try (UnifiedQueryContext context = builder.build()) {
      UnifiedQueryPlanner planner = new UnifiedQueryPlanner(context);
      UnifiedQueryCompiler compiler = new UnifiedQueryCompiler(context);

      RelNode plan = planner.plan(request.getQuery());
      try (PreparedStatement statement = compiler.compile(plan)) {
        ResultSet rs = statement.executeQuery();
        return formatAsJdbc(rs);
      }
    }
  }

  private String formatAsJdbc(ResultSet rs) throws Exception {
    ResultSetMetaData meta = rs.getMetaData();
    int columnCount = meta.getColumnCount();

    StringBuilder json = new StringBuilder();
    json.append("{\"schema\":[");
    for (int i = 1; i <= columnCount; i++) {
      if (i > 1) json.append(",");
      json.append("{\"name\":\"")
          .append(escape(meta.getColumnLabel(i)))
          .append("\",\"type\":\"")
          .append(jdbcTypeToString(meta.getColumnType(i)))
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

  private static String jdbcTypeToString(int type) {
    return switch (type) {
      case java.sql.Types.VARCHAR, java.sql.Types.CHAR, java.sql.Types.LONGVARCHAR -> "keyword";
      case java.sql.Types.INTEGER -> "integer";
      case java.sql.Types.BIGINT -> "long";
      case java.sql.Types.SMALLINT -> "short";
      case java.sql.Types.TINYINT -> "byte";
      case java.sql.Types.FLOAT, java.sql.Types.REAL -> "float";
      case java.sql.Types.DOUBLE -> "double";
      case java.sql.Types.BOOLEAN -> "boolean";
      case java.sql.Types.DATE -> "date";
      case java.sql.Types.TIME -> "time";
      case java.sql.Types.TIMESTAMP -> "timestamp";
      default -> "keyword";
    };
  }
}
