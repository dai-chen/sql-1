/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.stage;

import java.io.IOException;
import java.io.UncheckedIOException;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.calcite.DataContext;
import org.apache.calcite.linq4j.AbstractEnumerable;
import org.apache.calcite.linq4j.Enumerable;
import org.apache.calcite.linq4j.Enumerator;
import org.apache.lucene.index.LeafReaderContext;
import org.opensearch.search.aggregations.Aggregator;
import org.opensearch.search.aggregations.InternalAggregation;
import org.opensearch.search.aggregations.LeafBucketCollector;
import org.opensearch.search.aggregations.metrics.MetricsAggregator;
import org.opensearch.search.internal.SearchContext;
import org.opensearch.search.lookup.SourceLookup;

/**
 * Collects matching document IDs and executes a trusted Calcite fragment over their field values.
 *
 * <p>Rows are materialized lazily while Calcite consumes the table, so the collector retains only
 * primitive document IDs before aggregation.
 */
final class CalciteExecAggregator extends MetricsAggregator {

  private static final int INITIAL_DOC_CAPACITY = 256;

  private final List<CalciteExecAggregationBuilder.FieldDescriptor> fields;
  private final long currentTimeNanos;
  private final List<SegmentRows> segments = new ArrayList<>();
  private final CalciteFragmentSerde.DecodedPlan decodedPlan;
  private final EnumerableFragmentExecutor.CacheKey fragmentCacheKey;
  private final BitSet requiredFields;

  CalciteExecAggregator(
      String name,
      List<CalciteExecAggregationBuilder.FieldDescriptor> fields,
      String tableName,
      String fragmentJson,
      String inputRowTypeJson,
      long currentTimeNanos,
      SearchContext searchContext,
      Aggregator parent,
      Map<String, Object> metadata)
      throws IOException {
    super(name, searchContext, parent, metadata);
    this.fields = List.copyOf(fields);
    this.currentTimeNanos = currentTimeNanos;
    this.fragmentCacheKey = new EnumerableFragmentExecutor.CacheKey(fragmentJson, inputRowTypeJson);
    this.decodedPlan =
        CalciteFragmentSerde.deserialize(fragmentJson, inputRowTypeJson, tableName, matchingRows());
    this.requiredFields =
        CalciteFragmentSerde.requiredInputFields(decodedPlan.plan(), this.fields.size());
  }

  @Override
  protected LeafBucketCollector getLeafCollector(LeafReaderContext context, LeafBucketCollector sub)
      throws IOException {
    SourceLookup source =
        this.context.getQueryShardContext().lookup().getLeafSearchLookup(context).source();
    SegmentRows segment =
        new SegmentRows(
            ShardRowReader.create(context, fields, requiredFields, source), new DocBuffer());
    segments.add(segment);
    return new LeafBucketCollector() {
      @Override
      public void collect(int doc, long owningBucketOrd) {
        long allocatedBytes = segment.docs().add(doc);
        if (allocatedBytes != 0) {
          addRequestCircuitBreakerBytes(allocatedBytes);
        }
      }
    };
  }

  @Override
  public InternalAggregation buildAggregation(long owningBucketOrd) {
    return executeFragment();
  }

  @Override
  public InternalAggregation buildEmptyAggregation() {
    return executeFragment();
  }

  private InternalCalciteExec executeFragment() {
    Map<String, Object> dataContextValues = new HashMap<>(decodedPlan.dataContextValues());
    dataContextValues.put(DataContext.Variable.UTC_TIMESTAMP.camelName, currentTimeNanos);
    List<Object[]> output =
        EnumerableFragmentExecutor.executeCached(
            fragmentCacheKey, decodedPlan.plan(), decodedPlan.rootSchema(), dataContextValues);
    return new InternalCalciteExec(name, output, metadata());
  }

  private Enumerable<Object[]> matchingRows() {
    return new AbstractEnumerable<>() {
      @Override
      public Enumerator<Object[]> enumerator() {
        return new Enumerator<>() {
          private int segmentIndex;
          private int docIndex;
          private Object[] current;

          @Override
          public Object[] current() {
            return current;
          }

          @Override
          public boolean moveNext() {
            while (segmentIndex < segments.size()) {
              SegmentRows segment = segments.get(segmentIndex);
              if (docIndex < segment.docs().size()) {
                int doc = segment.docs().get(docIndex++);
                current = readRow(segment.reader(), doc);
                return true;
              }
              segmentIndex++;
              docIndex = 0;
            }
            current = null;
            return false;
          }

          @Override
          public void reset() {
            segmentIndex = 0;
            docIndex = 0;
            current = null;
          }

          @Override
          public void close() {}
        };
      }
    };
  }

  private Object[] readRow(ShardRowReader reader, int doc) {
    Object[] row = new Object[fields.size()];
    try {
      for (int field = requiredFields.nextSetBit(0);
          field >= 0;
          field = requiredFields.nextSetBit(field + 1)) {
        row[field] = reader.readField(field, doc);
      }
      return row;
    } catch (IOException e) {
      throw new UncheckedIOException("Unable to read Calcite fragment input row", e);
    }
  }

  private record SegmentRows(ShardRowReader reader, DocBuffer docs) {}

  private static final class DocBuffer {
    private int[] values = new int[0];
    private int size;

    private long add(int value) {
      long allocatedBytes = 0;
      if (size == values.length) {
        int oldCapacity = values.length;
        int newCapacity =
            oldCapacity == 0 ? INITIAL_DOC_CAPACITY : Math.multiplyExact(oldCapacity, 2);
        int[] grown = new int[newCapacity];
        System.arraycopy(values, 0, grown, 0, size);
        values = grown;
        allocatedBytes = (long) (newCapacity - oldCapacity) * Integer.BYTES;
      }
      values[size++] = value;
      return allocatedBytes;
    }

    private int get(int index) {
      return values[index];
    }

    private int size() {
      return size;
    }
  }
}
