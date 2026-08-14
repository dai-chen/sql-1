/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.calciteexec;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.search.aggregations.InternalAggregation;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

public class InternalCalciteExec extends InternalAggregation {

  private final List<Object[]> rows;
  private final String probeResult;

  public InternalCalciteExec(String name, List<Object[]> rows, String probeResult, Map<String, Object> metadata) {
    super(name, metadata);
    this.rows = rows;
    this.probeResult = probeResult;
  }

  public InternalCalciteExec(StreamInput in) throws IOException {
    super(in);
    int rowCount = in.readVInt();
    this.rows = new ArrayList<>(rowCount);
    for (int i = 0; i < rowCount; i++) {
      int colCount = in.readVInt();
      Object[] row = new Object[colCount];
      for (int j = 0; j < colCount; j++) {
        row[j] = in.readGenericValue();
      }
      rows.add(row);
    }
    this.probeResult = in.readOptionalString();
  }

  @Override
  protected void doWriteTo(StreamOutput out) throws IOException {
    out.writeVInt(rows.size());
    for (Object[] row : rows) {
      out.writeVInt(row.length);
      for (Object val : row) {
        out.writeGenericValue(val);
      }
    }
    out.writeOptionalString(probeResult);
  }

  @Override
  public String getWriteableName() {
    return CalciteExecAggregationBuilder.NAME;
  }

  @Override
  public InternalAggregation reduce(List<InternalAggregation> aggregations, ReduceContext reduceContext) {
    List<Object[]> combined = new ArrayList<>();
    String firstProbe = null;
    for (InternalAggregation agg : aggregations) {
      InternalCalciteExec internal = (InternalCalciteExec) agg;
      combined.addAll(internal.rows);
      if (firstProbe == null && internal.probeResult != null) {
        firstProbe = internal.probeResult;
      }
    }
    return new InternalCalciteExec(getName(), combined, firstProbe, getMetadata());
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
    throw new IllegalArgumentException("path not supported for [" + getName() + "]: " + path);
  }

  @Override
  public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
    builder.startArray("rows");
    for (Object[] row : rows) {
      builder.startArray();
      for (Object val : row) {
        builder.value(val);
      }
      builder.endArray();
    }
    builder.endArray();
    if (probeResult != null) {
      builder.field("probe_result", probeResult);
    }
    return builder;
  }

  public List<Object[]> getRows() {
    return rows;
  }

  public String getProbeResult() {
    return probeResult;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null || getClass() != obj.getClass()) return false;
    if (!super.equals(obj)) return false;
    InternalCalciteExec other = (InternalCalciteExec) obj;
    return Objects.equals(probeResult, other.probeResult) && rowsEqual(rows, other.rows);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), probeResult, rows.size());
  }

  private static boolean rowsEqual(List<Object[]> a, List<Object[]> b) {
    if (a.size() != b.size()) return false;
    for (int i = 0; i < a.size(); i++) {
      if (!java.util.Arrays.equals(a.get(i), b.get(i))) return false;
    }
    return true;
  }
}
