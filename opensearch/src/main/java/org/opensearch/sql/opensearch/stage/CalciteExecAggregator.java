/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.stage;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import org.apache.lucene.index.LeafReaderContext;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.aggregations.metrics.MetricsAggregator;
import org.opensearch.search.internal.SearchContext;

/**
 * Single-value (non-bucket) aggregator for the staged Calcite execution. In this skeleton story
 * (US-002) the aggregator does no collection — it returns an empty {@link InternalCalciteExec}.
 */
public class CalciteExecAggregator extends MetricsAggregator {

  private final String plan;
  private final List<CalciteExecAggregationBuilder.FieldDescriptor> fields;
  private final CombineDescriptor combine;
  private final int rowBudget;

  public CalciteExecAggregator(
      String name,
      String plan,
      List<CalciteExecAggregationBuilder.FieldDescriptor> fields,
      CombineDescriptor combine,
      int rowBudget,
      SearchContext searchContext,
      Aggregator parent,
      Map<String, Object> metadata)
      throws IOException {
    super(name, searchContext, parent, metadata);
    this.plan = plan;
    this.fields = fields;
    this.combine = combine;
    this.rowBudget = rowBudget;
  }

  @Override
  protected LeafBucketCollector getLeafCollector(LeafReaderContext ctx, LeafBucketCollector sub)
      throws IOException {
    return new LeafBucketCollector() {
      @Override
      public void collect(int doc, long owningBucketOrd) {
        // US-004: materialize a row here
      }
    };
  }

  @Override
  public InternalAggregation buildAggregation(long owningBucketOrd) {
    return new InternalCalciteExec(name, combine, 0L, 0L, List.of(), metadata());
  }

  @Override
  public InternalAggregation buildEmptyAggregation() {
    return new InternalCalciteExec(name, combine, 0L, 0L, List.of(), metadata());
  }
}
