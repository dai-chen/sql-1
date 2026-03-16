/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.plugin;

import static org.opensearch.core.rest.RestStatus.OK;
import static org.opensearch.sql.executor.ExecutionEngine.ExplainResponse;
import static org.opensearch.sql.executor.ExecutionEngine.QueryResponse;
import static org.opensearch.sql.protocol.response.format.JsonResponseFormatter.Style.PRETTY;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.sql.analytics.schema.OpenSearchSchemaBuilder;
import org.opensearch.sql.api.UnifiedQueryContext;
import org.opensearch.sql.api.UnifiedQueryPlanner;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.executor.AnalyticsExecutionEngine;
import org.opensearch.sql.executor.QueryType;
import org.opensearch.sql.protocol.response.QueryResult;
import org.opensearch.sql.protocol.response.format.JdbcResponseFormatter;
import org.opensearch.sql.protocol.response.format.JsonResponseFormatter;
import org.opensearch.sql.protocol.response.format.ResponseFormatter;

/**
 * REST handler for queries routed to the Analytics engine via the unified query pipeline. Parses
 * SQL/PPL queries using {@link UnifiedQueryPlanner} to generate a Calcite {@link RelNode}, then
 * hands off to {@link AnalyticsExecutionEngine} for execution.
 */
public class RestUnifiedQueryAction {

  private static final Logger LOG = LogManager.getLogger(RestUnifiedQueryAction.class);
  private static final String SCHEMA_NAME = "opensearch";

  private final AnalyticsExecutionEngine analyticsEngine = new AnalyticsExecutionEngine();
  private final ClusterService clusterService;

  public RestUnifiedQueryAction(ClusterService clusterService) {
    this.clusterService = clusterService;
  }

  /**
   * Execute a query through the unified query pipeline.
   *
   * @param query the query string
   * @param queryType SQL or PPL
   * @param channel the REST channel for sending the response
   * @param isExplain whether this is an explain request
   */
  public void execute(String query, QueryType queryType, RestChannel channel, boolean isExplain) {
    try {
      SchemaPlus schema = OpenSearchSchemaBuilder.buildSchema(clusterService.state());

      try (UnifiedQueryContext context =
          UnifiedQueryContext.builder()
              .language(queryType)
              .catalog(SCHEMA_NAME, schema)
              .defaultNamespace(SCHEMA_NAME)
              .build()) {

        UnifiedQueryPlanner planner = new UnifiedQueryPlanner(context);
        RelNode plan = planner.plan(query);
        CalcitePlanContext planContext = context.getPlanContext();

        if (isExplain) {
          analyticsEngine.explain(
              plan,
              org.opensearch.sql.ast.statement.ExplainMode.STANDARD,
              planContext,
              createExplainListener(channel));
        } else {
          analyticsEngine.execute(plan, planContext, createQueryListener(channel));
        }
      }
    } catch (Exception e) {
      reportError(channel, e);
    }
  }

  private ResponseListener<QueryResponse> createQueryListener(RestChannel channel) {
    ResponseFormatter<QueryResult> formatter = new JdbcResponseFormatter(PRETTY);
    return new ResponseListener<>() {
      @Override
      public void onResponse(QueryResponse response) {
        String result =
            formatter.format(
                new QueryResult(response.getSchema(), response.getResults(), response.getCursor()));
        channel.sendResponse(new BytesRestResponse(OK, formatter.contentType(), result));
      }

      @Override
      public void onFailure(Exception e) {
        reportError(channel, e);
      }
    };
  }

  private ResponseListener<ExplainResponse> createExplainListener(RestChannel channel) {
    return new ResponseListener<>() {
      @Override
      public void onResponse(ExplainResponse response) {
        var formatter =
            new JsonResponseFormatter<ExplainResponse>(PRETTY) {
              @Override
              protected Object buildJsonObject(ExplainResponse resp) {
                return resp;
              }
            };
        channel.sendResponse(
            new BytesRestResponse(OK, formatter.contentType(), formatter.format(response)));
      }

      @Override
      public void onFailure(Exception e) {
        reportError(channel, e);
      }
    };
  }

  private static void reportError(RestChannel channel, Exception e) {
    LOG.error("Error in unified query pipeline", e);
    channel.sendResponse(
        new BytesRestResponse(
            org.opensearch.core.rest.RestStatus.INTERNAL_SERVER_ERROR,
            "application/json; charset=UTF-8",
            "{\"error\":{\"type\":\""
                + e.getClass().getSimpleName()
                + "\",\"reason\":\""
                + (e.getMessage() != null ? e.getMessage().replace("\"", "\\\"") : "Unknown error")
                + "\"},\"status\":500}"));
  }
}
