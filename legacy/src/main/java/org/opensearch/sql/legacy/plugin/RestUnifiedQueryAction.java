/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy.plugin;

import static org.opensearch.core.rest.RestStatus.OK;
import static org.opensearch.sql.executor.ExecutionEngine.ExplainResponse;
import static org.opensearch.sql.executor.ExecutionEngine.QueryResponse;
import static org.opensearch.sql.opensearch.executor.OpenSearchQueryManager.SQL_WORKER_THREAD_POOL_NAME;
import static org.opensearch.sql.protocol.response.format.JsonResponseFormatter.Style.PRETTY;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.analytics.exec.QueryPlanExecutor;
import org.opensearch.analytics.schema.OpenSearchSchemaBuilder;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.rest.BytesRestResponse;
import org.opensearch.rest.RestChannel;
import org.opensearch.sql.api.UnifiedQueryContext;
import org.opensearch.sql.api.UnifiedQueryPlanner;
import org.opensearch.sql.calcite.CalcitePlanContext;
import org.opensearch.sql.common.response.ResponseListener;
import org.opensearch.sql.executor.AnalyticsExecutionEngine;
import org.opensearch.sql.executor.QueryType;
import org.opensearch.sql.legacy.metrics.MetricName;
import org.opensearch.sql.legacy.metrics.Metrics;
import org.opensearch.sql.protocol.response.QueryResult;
import org.opensearch.sql.protocol.response.format.JdbcResponseFormatter;
import org.opensearch.sql.protocol.response.format.JsonResponseFormatter;
import org.opensearch.sql.protocol.response.format.ResponseFormatter;
import org.opensearch.transport.client.node.NodeClient;

/**
 * REST handler for queries routed to the Analytics engine via the unified query pipeline. Parses
 * SQL/PPL queries using {@link UnifiedQueryPlanner} to generate a Calcite {@link RelNode}, then
 * hands off to {@link AnalyticsExecutionEngine} for execution.
 */
public class RestUnifiedQueryAction {

  private static final Logger LOG = LogManager.getLogger(RestUnifiedQueryAction.class);
  private static final String SCHEMA_NAME = "opensearch";

  private final AnalyticsExecutionEngine analyticsEngine;
  private final ClusterService clusterService;
  private final NodeClient client;

  public RestUnifiedQueryAction(
      ClusterService clusterService,
      NodeClient client,
      QueryPlanExecutor<RelNode, Iterable<Object[]>> planExecutor) {
    this.clusterService = clusterService;
    this.client = client;
    this.analyticsEngine = new AnalyticsExecutionEngine(planExecutor);
  }

  /**
   * PoC: Check if the query should be routed to the unified query pipeline. Currently uses a
   * hardcoded prefix convention ("parquet_" in table name). In production, this would check index
   * settings to determine the storage engine.
   */
  public static boolean isUnifiedQueryPath(String query) {
    return query != null && query.toLowerCase().contains("parquet_");
  }

  /**
   * Execute a query through the unified query pipeline on the sql-worker thread pool.
   *
   * @param query the query string
   * @param queryType SQL or PPL
   * @param channel the REST channel for sending the response
   * @param isExplain whether this is an explain request
   */
  public void execute(String query, QueryType queryType, RestChannel channel, boolean isExplain) {
    client
        .threadPool()
        .schedule(
            () -> doExecute(query, queryType, channel, isExplain),
            new TimeValue(0),
            SQL_WORKER_THREAD_POOL_NAME);
  }

  private void doExecute(
      String query, QueryType queryType, RestChannel channel, boolean isExplain) {
    try {
      long startTime = System.nanoTime();

      SchemaPlus schema = OpenSearchSchemaBuilder.buildSchema(clusterService.state());

      try (UnifiedQueryContext context =
          UnifiedQueryContext.builder()
              .language(queryType)
              .catalog(SCHEMA_NAME, schema)
              .defaultNamespace(SCHEMA_NAME)
              .build()) {

        UnifiedQueryPlanner planner = new UnifiedQueryPlanner(context);
        RelNode plan = planner.plan(query);
        long planTime = System.nanoTime();
        LOG.info(
            "[unified] Planning completed in {}ms for {} query",
            (planTime - startTime) / 1_000_000,
            queryType);

        CalcitePlanContext planContext = context.getPlanContext();

        if (isExplain) {
          analyticsEngine.explain(
              plan,
              org.opensearch.sql.ast.statement.ExplainMode.STANDARD,
              planContext,
              createExplainListener(channel));
        } else {
          analyticsEngine.execute(plan, planContext, createQueryListener(channel, planTime));
        }
      }
    } catch (Exception e) {
      recordFailureMetric(e);
      reportError(channel, e);
    }
  }

  private ResponseListener<QueryResponse> createQueryListener(
      RestChannel channel, long planEndTime) {
    ResponseFormatter<QueryResult> formatter = new JdbcResponseFormatter(PRETTY);
    return new ResponseListener<>() {
      @Override
      public void onResponse(QueryResponse response) {
        long execTime = System.nanoTime();
        LOG.info(
            "[unified] Execution completed in {}ms, {} rows returned",
            (execTime - planEndTime) / 1_000_000,
            response.getResults().size());
        Metrics.getInstance().getNumericalMetric(MetricName.REQ_TOTAL).increment();
        Metrics.getInstance().getNumericalMetric(MetricName.REQ_COUNT_TOTAL).increment();
        String result =
            formatter.format(
                new QueryResult(response.getSchema(), response.getResults(), response.getCursor()));
        channel.sendResponse(new BytesRestResponse(OK, formatter.contentType(), result));
      }

      @Override
      public void onFailure(Exception e) {
        recordFailureMetric(e);
        reportError(channel, e);
      }
    };
  }

  private ResponseListener<ExplainResponse> createExplainListener(RestChannel channel) {
    return new ResponseListener<>() {
      @Override
      public void onResponse(ExplainResponse response) {
        Metrics.getInstance().getNumericalMetric(MetricName.REQ_TOTAL).increment();
        Metrics.getInstance().getNumericalMetric(MetricName.REQ_COUNT_TOTAL).increment();
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
        recordFailureMetric(e);
        reportError(channel, e);
      }
    };
  }

  private static void recordFailureMetric(Exception e) {
    LOG.error("[unified] Query execution failed", e);
    Metrics.getInstance().getNumericalMetric(MetricName.FAILED_REQ_COUNT_SYS).increment();
  }

  private static void reportError(RestChannel channel, Exception e) {
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
