/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.stage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.search.aggregations.InternalAggregation;

/**
 * The shard-level result of a staged Calcite execution aggregation. Carries the echoed combine
 * descriptor, collection stats, and a rows payload.
 *
 * <p>The wire format for rows will be revisited in US-004; for now each row is a {@code
 * List<Object>} serialized with {@link StreamOutput#writeGenericValue}.
 */
public class InternalCalciteExec extends InternalAggregation {

  private final CombineDescriptor combine;
  private final long rowsCollected;
  private final long rowsEmitted;
  // Rows payload — empty in this story (US-002).
  private final List<List<Object>> rows;

  public InternalCalciteExec(
      String name,
      CombineDescriptor combine,
      long rowsCollected,
      long rowsEmitted,
      List<List<Object>> rows,
      Map<String, Object> metadata) {
    super(name, metadata);
    this.combine = Objects.requireNonNull(combine);
    this.rowsCollected = rowsCollected;
    this.rowsEmitted = rowsEmitted;
    this.rows = rows == null ? List.of() : List.copyOf(rows);
  }

  public InternalCalciteExec(StreamInput in) throws IOException {
    super(in);
    this.combine = new CombineDescriptor(in);
    this.rowsCollected = in.readVLong();
    this.rowsEmitted = in.readVLong();
    int rowCount = in.readVInt();
    List<List<Object>> readRows = new ArrayList<>(rowCount);
    for (int i = 0; i < rowCount; i++) {
      int colCount = in.readVInt();
      List<Object> row = new ArrayList<>(colCount);
      for (int j = 0; j < colCount; j++) {
        row.add(in.readGenericValue());
      }
      readRows.add(row);
    }
    this.rows = List.copyOf(readRows);
  }

  @Override
  protected void doWriteTo(StreamOutput out) throws IOException {
    combine.writeTo(out);
    out.writeVLong(rowsCollected);
    out.writeVLong(rowsEmitted);
    out.writeVInt(rows.size());
    for (List<Object> row : rows) {
      out.writeVInt(row.size());
      for (Object cell : row) {
        out.writeGenericValue(cell);
      }
    }
  }

  @Override
  public String getWriteableName() {
    return CalciteExecAggregationBuilder.NAME;
  }

  /**
   * Reduce all shard-level InternalCalciteExec instances into one.
   *
   * <p>ASSOCIATIVITY REQUIREMENT: This method is called in batches of batched_reduce_size (512) by
   * QueryPhaseResultConsumer.PendingReduces with partially-reduced inputs. The output must be the
   * same wire type and re-reducible. For CONCAT, we simply concatenate rows and SUM the stats.
   */
  @Override
  public InternalAggregation reduce(
      List<InternalAggregation> aggregations, ReduceContext reduceContext) {
    switch (combine.getMode()) {
      case CONCAT:
        // TODO(US-013): enforce rowBudget during reduce; CONCAT currently accumulates unbounded.
        long totalCollected = 0;
        long totalEmitted = 0;
        List<List<Object>> allRows = new ArrayList<>();
        for (InternalAggregation agg : aggregations) {
          InternalCalciteExec other = (InternalCalciteExec) agg;
          totalCollected += other.rowsCollected;
          totalEmitted += other.rowsEmitted;
          allRows.addAll(other.rows);
        }
        return new InternalCalciteExec(
            getName(), combine, totalCollected, totalEmitted, allRows, getMetadata());

      case MERGE_AGG:
        throw new UnsupportedOperationException("MERGE_AGG reduce is implemented in US-012");
      case LIMIT:
        throw new UnsupportedOperationException("LIMIT reduce is implemented in US-013");
      case TOP_N:
        throw new UnsupportedOperationException("TOP_N reduce is implemented in US-014");
      case RANK_LIMIT:
        throw new UnsupportedOperationException("RANK_LIMIT reduce is implemented in US-015");
      default:
        throw new IllegalStateException("Unknown combine mode: " + combine.getMode());
    }
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
    throw new IllegalArgumentException(
        "Unknown property path [" + String.join(".", path) + "] on " + getWriteableName());
  }

  @Override
  public XContentBuilder doXContentBody(XContentBuilder builder, Params params) throws IOException {
    builder.field("rowsCollected", rowsCollected);
    builder.field("rowsEmitted", rowsEmitted);
    builder.field("combine");
    combine.toXContent(builder, params);
    builder.startArray("rows");
    for (List<Object> row : rows) {
      builder.startArray();
      for (Object cell : row) {
        builder.value(cell);
      }
      builder.endArray();
    }
    builder.endArray();
    return builder;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj) return true;
    if (obj == null || getClass() != obj.getClass()) return false;
    if (!super.equals(obj)) return false;
    InternalCalciteExec other = (InternalCalciteExec) obj;
    return rowsCollected == other.rowsCollected
        && rowsEmitted == other.rowsEmitted
        && Objects.equals(combine, other.combine)
        && Objects.equals(rows, other.rows);
  }

  @Override
  public int hashCode() {
    return Objects.hash(super.hashCode(), combine, rowsCollected, rowsEmitted, rows);
  }

  /** Returns the gathered rows from shard execution. */
  public List<List<Object>> getRows() {
    return rows;
  }
}
