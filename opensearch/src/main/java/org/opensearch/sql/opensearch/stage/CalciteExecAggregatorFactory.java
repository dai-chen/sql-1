/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.stage;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.AggregatorFactory;
import org.opensearch.search.aggregations.CardinalityUpperBound;
import org.opensearch.search.internal.SearchContext;

/** Creates the shard-local Calcite aggregation. */
final class CalciteExecAggregatorFactory extends AggregatorFactory {

  private final List<CalciteExecAggregationBuilder.FieldDescriptor> fields;
  private final String tableName;
  private final String fragmentJson;
  private final String inputRowTypeJson;
  private final long currentTimeNanos;

  CalciteExecAggregatorFactory(
      String name,
      List<CalciteExecAggregationBuilder.FieldDescriptor> fields,
      String tableName,
      String fragmentJson,
      String inputRowTypeJson,
      long currentTimeNanos,
      QueryShardContext queryShardContext,
      AggregatorFactory parent,
      AggregatorFactories.Builder subfactoriesBuilder,
      Map<String, Object> metadata)
      throws IOException {
    super(name, queryShardContext, parent, subfactoriesBuilder, metadata);
    this.fields = fields;
    this.tableName = tableName;
    this.fragmentJson = fragmentJson;
    this.inputRowTypeJson = inputRowTypeJson;
    this.currentTimeNanos = currentTimeNanos;
  }

  @Override
  protected Aggregator createInternal(
      SearchContext searchContext,
      Aggregator parent,
      CardinalityUpperBound cardinality,
      Map<String, Object> metadata)
      throws IOException {
    return new CalciteExecAggregator(
        name,
        fields,
        tableName,
        fragmentJson,
        inputRowTypeJson,
        currentTimeNanos,
        searchContext,
        parent,
        metadata);
  }
}
