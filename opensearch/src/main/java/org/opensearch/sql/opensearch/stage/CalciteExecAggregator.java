/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.stage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.lucene.index.LeafReaderContext;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.aggregations.metrics.MetricsAggregator;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.lookup.SourceLookup;

/**
 * Single-value (non-bucket) aggregator for the staged Calcite execution. US-004: materializes one
 * row per matching document using per-type doc_values readers with a _source fallback, and reports
 * rowsCollected/rowsEmitted via {@link InternalCalciteExec}.
 */
public class CalciteExecAggregator extends MetricsAggregator {

  private final String plan;
  private final List<CalciteExecAggregationBuilder.FieldDescriptor> fields;
  private final CombineDescriptor combine;
  private final int rowBudget;

  /** Accumulated rows — one List&lt;Object&gt; per matching document, in collect() order. */
  private final List<List<Object>> rows = new ArrayList<>();

  /** Count of rows collected (equals rows.size() — US-009 will enforce rowBudget). */
  private long rowsCollected;

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
    SourceLookup sourceLookup =
        context.getQueryShardContext().lookup().getLeafSearchLookup(ctx).source();
    ShardRowReader rowReader = ShardRowReader.create(ctx, fields, sourceLookup);

    return new LeafBucketCollector() {
      @Override
      public void collect(int doc, long owningBucketOrd) throws IOException {
        // Position _source on the current document before reading
        sourceLookup.setSegmentAndDocument(ctx, doc);
        Object[] row = rowReader.readRow(doc);
        rows.add(Arrays.asList(row));
        rowsCollected++;
        // TODO US-009: enforce rowBudget
      }
    };
  }

  @Override
  public InternalAggregation buildAggregation(long owningBucketOrd) {
    return new InternalCalciteExec(name, combine, rowsCollected, rowsCollected, rows, metadata());
  }

  @Override
  public InternalAggregation buildEmptyAggregation() {
    return new InternalCalciteExec(name, combine, 0L, 0L, List.of(), metadata());
  }
}
