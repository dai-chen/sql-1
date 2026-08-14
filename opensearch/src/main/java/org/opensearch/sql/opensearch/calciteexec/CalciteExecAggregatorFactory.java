/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.calciteexec;

import org.opensearch.index.query.QueryShardContext;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.AggregatorFactories;
import org.opensearch.search.aggregations.AggregatorFactory;
import org.opensearch.search.aggregations.CardinalityUpperBound;
import org.opensearch.search.internal.SearchContext;

import java.io.IOException;
import java.util.List;
import java.util.Map;

public class CalciteExecAggregatorFactory extends AggregatorFactory {

  private final List<String> fields;
  private final boolean probe;
  private final String plan;
  private final List<String> schema;

  CalciteExecAggregatorFactory(
      String name,
      List<String> fields,
      boolean probe,
      String plan,
      List<String> schema,
      QueryShardContext queryShardContext,
      AggregatorFactory parent,
      AggregatorFactories.Builder subFactories,
      Map<String, Object> metadata
  ) throws IOException {
    super(name, queryShardContext, parent, subFactories, metadata);
    this.fields = fields;
    this.probe = probe;
    this.plan = plan;
    this.schema = schema;
  }

  @Override
  protected Aggregator createInternal(
      SearchContext searchContext,
      Aggregator parent,
      CardinalityUpperBound cardinality,
      Map<String, Object> metadata
  ) throws IOException {
    return new CalciteExecAggregator(name, fields, probe, plan, schema, searchContext, parent, metadata);
  }
}
