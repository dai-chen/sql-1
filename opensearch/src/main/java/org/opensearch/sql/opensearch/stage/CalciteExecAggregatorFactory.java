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

/**
 * Factory that instantiates a {@link CalciteExecAggregator} on each shard. Carries the plan,
 * fields, combine descriptor, row budget, and forcing operator through to the aggregator.
 */
public class CalciteExecAggregatorFactory extends AggregatorFactory {

  private final String plan;
  private final List<CalciteExecAggregationBuilder.FieldDescriptor> fields;
  private final CombineDescriptor combine;
  private final int rowBudget;
  private final String forcingOperator;

  public CalciteExecAggregatorFactory(
      String name,
      String plan,
      List<CalciteExecAggregationBuilder.FieldDescriptor> fields,
      CombineDescriptor combine,
      int rowBudget,
      String forcingOperator,
      QueryShardContext queryShardContext,
      AggregatorFactory parent,
      AggregatorFactories.Builder subfactoriesBuilder,
      Map<String, Object> metadata)
      throws IOException {
    super(name, queryShardContext, parent, subfactoriesBuilder, metadata);
    this.plan = plan;
    this.fields = fields;
    this.combine = combine;
    this.rowBudget = rowBudget;
    this.forcingOperator = forcingOperator;
  }

  @Override
  protected Aggregator createInternal(
      SearchContext searchContext,
      Aggregator parent,
      CardinalityUpperBound cardinality,
      Map<String, Object> metadata)
      throws IOException {
    return new CalciteExecAggregator(
        name, plan, fields, combine, rowBudget, forcingOperator, searchContext, parent, metadata);
  }
}
