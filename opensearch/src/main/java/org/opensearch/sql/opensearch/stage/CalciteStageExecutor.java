/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.stage;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.SqlExplainLevel;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.storage.scan.AbstractCalciteIndexScan;
import org.opensearch.transport.client.node.NodeClient;

/** Dispatches shard fragments and executes their coordinator continuation. */
public final class CalciteStageExecutor {

  private static final String AGGREGATION_NAME = "calcite_stage";

  private final OpenSearchClient client;

  public CalciteStageExecutor(OpenSearchClient client) {
    this.client = Objects.requireNonNull(client);
  }

  public List<Object[]> execute(StagePlan plan, long currentTimeNanos) {
    NodeClient nodeClient =
        client
            .getNodeClient()
            .orElseThrow(() -> new IllegalStateException("Staged execution requires a NodeClient"));
    List<StagePlan.GatherExchange> exchanges = plan.exchanges();
    List<ActionFuture<SearchResponse>> futures = new ArrayList<>(exchanges.size());
    for (StagePlan.GatherExchange exchange : exchanges) {
      futures.add(nodeClient.search(buildSearchRequest(exchange.source(), currentTimeNanos)));
    }

    List<CoordinatorTreeExecutor.GatheredRows> inputs = new ArrayList<>(exchanges.size());
    for (int index = 0; index < exchanges.size(); index++) {
      InternalCalciteExec result = stageResult(futures.get(index).actionGet());
      StagePlan.GatherExchange exchange = exchanges.get(index);
      inputs.add(
          new CoordinatorTreeExecutor.GatheredRows(
              exchange.target(), exchange.source().outputRowType(), result.rows()));
    }

    return CoordinatorTreeExecutor.execute(plan.coordinatorTree(), inputs, currentTimeNanos);
  }

  public static String formatPhysicalPlan(StagePlan plan, SqlExplainLevel level) {
    StringBuilder result = new StringBuilder("CalciteCoordinator\n");
    result.append(RelOptUtil.toString(plan.coordinatorTree(), level).indent(2));
    for (StagePlan.GatherExchange exchange : plan.exchanges()) {
      StagePlan.ShardFragment fragment = exchange.source();
      result
          .append("  CalciteExecAggregation(indices=")
          .append(Arrays.toString(fragment.scan().getOsIndex().getIndexName().getIndexNames()))
          .append(", fragment=")
          .append(fragment.outputRowType().getFullTypeString())
          .append(", pagination=false)\n");
    }
    return result.toString();
  }

  private static InternalCalciteExec stageResult(SearchResponse response) {
    InternalAggregation result = response.getAggregations().get(AGGREGATION_NAME);
    if (result instanceof InternalCalciteExec calciteExec) {
      return calciteExec;
    }
    throw new IllegalStateException(
        "Expected InternalCalciteExec aggregation in staged response, got: "
            + (result == null ? "null" : result.getClass().getName()));
  }

  /** Builds a size-zero internal aggregation request without PIT or pagination state. */
  private static SearchRequest buildSearchRequest(
      StagePlan.ShardFragment fragment, long currentTimeNanos) {
    AbstractCalciteIndexScan scan = fragment.scan();
    RelDataType scanRowType = scan.getRowType();
    Map<String, OpenSearchDataType> fieldTypes =
        OpenSearchDataType.traverseAndFlatten(scan.getOsIndex().getFieldOpenSearchTypes());
    List<CalciteExecAggregationBuilder.FieldDescriptor> fields =
        new ArrayList<>(scanRowType.getFieldCount());
    for (RelDataTypeField field : scanRowType.getFieldList()) {
      OpenSearchDataType osType = fieldTypes.get(field.getName());
      String mappingType = osType == null ? "keyword" : osType.getMappingType().toString();
      fields.add(new CalciteExecAggregationBuilder.FieldDescriptor(field.getName(), mappingType));
    }

    CalciteExecAggregationBuilder aggregation =
        new CalciteExecAggregationBuilder(AGGREGATION_NAME)
            .fields(fields)
            .fragmentTableName(fragment.tableName())
            .fragmentJson(fragment.planJson())
            .inputRowTypeJson(fragment.inputRowTypeJson())
            .currentTimeNanos(currentTimeNanos);
    SearchSourceBuilder source = new SearchSourceBuilder().size(0).aggregation(aggregation);

    org.opensearch.sql.opensearch.request.OpenSearchRequestBuilder requestBuilder =
        scan.getPushDownContext().createRequestBuilder();
    QueryBuilder query = requestBuilder.getSourceBuilder().query();
    if (query != null) {
      source.query(query);
    }

    SearchRequest request =
        new SearchRequest()
            .indices(scan.getOsIndex().getIndexName().getIndexNames())
            .source(source);
    request.allowPartialSearchResults(false);
    return request;
  }
}
