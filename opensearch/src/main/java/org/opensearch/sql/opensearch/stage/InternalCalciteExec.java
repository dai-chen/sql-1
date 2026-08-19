/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.stage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
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
        row.add(readTaggedCell(in));
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
        writeTaggedCell(out, cell);
      }
    }
  }

  /**
   * Out-of-band discriminator for cell wire encoding. A byte tag is written BEFORE each cell value
   * so that types not supported by writeGenericValue (BigDecimal) can be carried losslessly without
   * in-band sentinel values that could collide with real document data.
   *
   * <p>Tags: 0 = generic (delegates to writeGenericValue/readGenericValue), 1 = BigDecimal (carried
   * as its exact toPlainString, reconstructed with new BigDecimal(String)).
   */
  private static final byte CELL_TAG_GENERIC = 0;

  private static final byte CELL_TAG_BIGDECIMAL = 1;

  private static void writeTaggedCell(StreamOutput out, Object cell) throws IOException {
    if (cell instanceof java.math.BigDecimal bd) {
      out.writeByte(CELL_TAG_BIGDECIMAL);
      out.writeString(bd.toPlainString());
    } else {
      out.writeByte(CELL_TAG_GENERIC);
      out.writeGenericValue(cell);
    }
  }

  private static Object readTaggedCell(StreamInput in) throws IOException {
    byte tag = in.readByte();
    return switch (tag) {
      case CELL_TAG_GENERIC -> in.readGenericValue();
      case CELL_TAG_BIGDECIMAL -> new java.math.BigDecimal(in.readString());
      default -> throw new IOException("Unknown cell wire tag: " + tag);
    };
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
        return reduceMergeAgg(aggregations);
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

  /**
   * MERGE_AGG reduce: groups incoming rows by group-key tuple and merges each agg column with the
   * spec's function. Associative: output is the same wire type (InternalCalciteExec) re-reducible
   * by a subsequent reduce call, meeting the batched_reduce_size=512 contract.
   */
  private InternalCalciteExec reduceMergeAgg(List<InternalAggregation> aggregations) {
    List<Integer> groupKeyIndices = combine.getIntListParam();
    List<String> aggSpecs = combine.getStringListParam();

    // Parse agg specs: "SUM(2)" -> (MergeOp.SUM, 2)
    MergeSpec[] specs = new MergeSpec[aggSpecs.size()];
    for (int i = 0; i < aggSpecs.size(); i++) {
      specs[i] = MergeSpec.parse(aggSpecs.get(i));
    }

    // Accumulator: GroupKey -> merged row (full width)
    Map<GroupKey, Object[]> accumulator = new LinkedHashMap<>();
    long totalCollected = 0;
    long totalEmitted = 0;

    for (InternalAggregation agg : aggregations) {
      InternalCalciteExec other = (InternalCalciteExec) agg;
      totalCollected += other.rowsCollected;
      totalEmitted += other.rowsEmitted;

      for (List<Object> row : other.rows) {
        // Extract group key
        Object[] keyArr = new Object[groupKeyIndices.size()];
        for (int i = 0; i < groupKeyIndices.size(); i++) {
          keyArr[i] = row.get(groupKeyIndices.get(i));
        }
        GroupKey key = new GroupKey(keyArr);

        Object[] accRow = accumulator.get(key);
        if (accRow == null) {
          // Initialize: copy the whole row as the baseline
          accRow = row.toArray(new Object[0]);
          // For SUM0 specs, initialize the column to 0 if currently null
          for (MergeSpec spec : specs) {
            if (spec.op == MergeOp.SUM0 && accRow[spec.colIdx] == null) {
              accRow[spec.colIdx] = 0L;
            }
          }
          accumulator.put(key, accRow);
        } else {
          // Merge each agg column
          for (MergeSpec spec : specs) {
            accRow[spec.colIdx] = merge(accRow[spec.colIdx], row.get(spec.colIdx), spec.op);
          }
        }
      }
    }

    // Convert accumulator to output rows
    List<List<Object>> resultRows = new ArrayList<>(accumulator.size());
    for (Object[] accRow : accumulator.values()) {
      resultRows.add(Arrays.asList(accRow));
    }

    return new InternalCalciteExec(
        getName(), combine, totalCollected, totalEmitted, resultRows, getMetadata());
  }

  /**
   * Merges a single cell value into the accumulator. Null-skip semantics: null inputs are ignored;
   * null accumulator means "no non-null value seen yet" (except SUM0 which initializes to 0).
   */
  private static Object merge(Object acc, Object val, MergeOp op) {
    if (val == null) {
      return acc;
    }
    if (acc == null) {
      return val;
    }
    return switch (op) {
      case SUM, SUM0 -> addNumbers((Number) acc, (Number) val);
      case MIN -> compareNumbers((Number) acc, (Number) val) <= 0 ? acc : val;
      case MAX -> compareNumbers((Number) acc, (Number) val) >= 0 ? acc : val;
    };
  }

  /** Addition preserving BigDecimal exactness, Long when both are integral, Double otherwise. */
  private static Number addNumbers(Number a, Number b) {
    if (a instanceof java.math.BigDecimal || b instanceof java.math.BigDecimal) {
      java.math.BigDecimal bdA =
          a instanceof java.math.BigDecimal
              ? (java.math.BigDecimal) a
              : java.math.BigDecimal.valueOf(a.doubleValue());
      java.math.BigDecimal bdB =
          b instanceof java.math.BigDecimal
              ? (java.math.BigDecimal) b
              : java.math.BigDecimal.valueOf(b.doubleValue());
      return bdA.add(bdB);
    }
    if (a instanceof Double || b instanceof Double) {
      return a.doubleValue() + b.doubleValue();
    }
    return a.longValue() + b.longValue();
  }

  /** Comparison preserving BigDecimal exactness, promotes to Double when either is Double. */
  private static int compareNumbers(Number a, Number b) {
    if (a instanceof java.math.BigDecimal || b instanceof java.math.BigDecimal) {
      java.math.BigDecimal bdA =
          a instanceof java.math.BigDecimal
              ? (java.math.BigDecimal) a
              : java.math.BigDecimal.valueOf(a.doubleValue());
      java.math.BigDecimal bdB =
          b instanceof java.math.BigDecimal
              ? (java.math.BigDecimal) b
              : java.math.BigDecimal.valueOf(b.doubleValue());
      return bdA.compareTo(bdB);
    }
    if (a instanceof Double || b instanceof Double) {
      return Double.compare(a.doubleValue(), b.doubleValue());
    }
    return Long.compare(a.longValue(), b.longValue());
  }

  /** Parsed merge specification: an operation and the column index it applies to. */
  private record MergeSpec(MergeOp op, int colIdx) {
    static MergeSpec parse(String spec) {
      // Format: "SUM(2)" or "MIN(3)"
      int paren = spec.indexOf('(');
      String opName = spec.substring(0, paren);
      int colIdx = Integer.parseInt(spec.substring(paren + 1, spec.length() - 1));
      return new MergeSpec(MergeOp.valueOf(opName), colIdx);
    }
  }

  private enum MergeOp {
    SUM,
    SUM0,
    MIN,
    MAX
  }

  /**
   * Group key for MERGE_AGG reduce. Treats nulls as equal (SQL GROUP BY semantics: all NULLs group
   * together).
   */
  private record GroupKey(Object[] keys) {
    @Override
    public boolean equals(Object o) {
      if (this == o) return true;
      if (!(o instanceof GroupKey other)) return false;
      return Arrays.deepEquals(keys, other.keys);
    }

    @Override
    public int hashCode() {
      return Arrays.deepHashCode(keys);
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
