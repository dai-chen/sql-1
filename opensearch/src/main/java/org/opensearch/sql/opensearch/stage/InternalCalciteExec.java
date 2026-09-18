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
import java.util.Objects;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.search.aggregations.InternalAggregation;

/** Generic rows emitted by a shard-local Calcite fragment. */
public final class InternalCalciteExec extends InternalAggregation {

  private final List<Object[]> rows;

  public InternalCalciteExec(String name, List<Object[]> rows, Map<String, Object> metadata) {
    super(name, metadata);
    this.rows = CalciteWireRowCodec.prepareForTransport(rows);
  }

  public InternalCalciteExec(StreamInput in) throws IOException {
    super(in);
    rows = CalciteWireRowCodec.readRows(in);
  }

  @Override
  protected void doWriteTo(StreamOutput out) throws IOException {
    CalciteWireRowCodec.writeRows(out, rows);
  }

  @Override
  public String getWriteableName() {
    return CalciteExecAggregationBuilder.NAME;
  }

  @Override
  public InternalAggregation reduce(
      List<InternalAggregation> aggregations, ReduceContext reduceContext) {
    int rowCount =
        aggregations.stream()
            .map(InternalCalciteExec.class::cast)
            .mapToInt(aggregation -> aggregation.rows.size())
            .sum();
    List<Object[]> gathered = new ArrayList<>(rowCount);
    for (InternalAggregation aggregation : aggregations) {
      gathered.addAll(((InternalCalciteExec) aggregation).rows);
    }
    return new InternalCalciteExec(getName(), getMetadata(), List.copyOf(gathered));
  }

  private InternalCalciteExec(
      String name, Map<String, Object> metadata, List<Object[]> normalizedRows) {
    super(name, metadata);
    this.rows = normalizedRows;
  }

  @Override
  protected boolean mustReduceOnSingleInternalAgg() {
    return false;
  }

  @Override
  public Object getProperty(List<String> path) {
    if (path.isEmpty()) {
      return this;
    }
    throw new IllegalArgumentException("Unknown property [" + String.join(".", path) + "]");
  }

  @Override
  public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
    builder.startArray("rows");
    for (Object[] row : rows) {
      builder.value(row);
    }
    return builder.endArray();
  }

  List<Object[]> rows() {
    return rows;
  }

  @Override
  public boolean equals(Object other) {
    if (this == other) {
      return true;
    }
    return other instanceof InternalCalciteExec that
        && super.equals(other)
        && Arrays.deepEquals(rows.toArray(), that.rows.toArray());
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), Arrays.deepHashCode(rows.toArray()));
  }
}
