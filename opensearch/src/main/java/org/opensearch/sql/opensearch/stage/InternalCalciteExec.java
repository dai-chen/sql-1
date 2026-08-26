/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.stage;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
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
        // Concatenate rows from all inputs and truncate at n. Must be associative: reduce()
        // runs in batches of 512 and its output is fed back in.
        int limitN = combine.getIntParam();
        long limitCollected = 0;
        List<List<Object>> limitRows = new ArrayList<>();
        for (InternalAggregation agg : aggregations) {
          InternalCalciteExec other = (InternalCalciteExec) agg;
          limitCollected += other.rowsCollected;
          limitRows.addAll(other.rows);
        }
        // Truncate to n — copy so the truncated list does not pin the full backing ArrayList
        if (limitRows.size() > limitN) {
          limitRows = new ArrayList<>(limitRows.subList(0, limitN));
        }
        return new InternalCalciteExec(
            getName(), combine, limitCollected, (long) limitRows.size(), limitRows, getMetadata());
      case TOP_N:
        return reduceTopN(aggregations);
      case RANK_LIMIT:
        return reduceRankLimit(aggregations);
      default:
        throw new IllegalStateException("Unknown combine mode: " + combine.getMode());
    }
  }

  /**
   * TOP_N reduce (US-014): merges the sorted runs shipped by each shard into one sorted run of at
   * most n rows.
   *
   * <p>Associative: the union of per-run top-n contains the global top-n, so truncating an
   * intermediate result can never discard a row the next reduce needed. reduce() runs in batches of
   * 512 and feeds its own output back in, and the output here is another sorted run of the same
   * wire type. Ties between rows with equal sort keys may be resolved differently across batch
   * groupings — that is inherent to a non-stable global order and does not change the row SET,
   * which the coordinator's own Sort then re-orders authoritatively.
   */
  private InternalCalciteExec reduceTopN(List<InternalAggregation> aggregations) {
    int n = combine.getIntParam();
    Comparator<List<Object>> comparator =
        buildTopNComparator(combine.getIntListParam(), combine.getStringListParam());

    long totalCollected = 0;
    List<List<Object>> merged = new ArrayList<>();
    for (InternalAggregation agg : aggregations) {
      InternalCalciteExec other = (InternalCalciteExec) agg;
      totalCollected += other.rowsCollected;
      merged.addAll(other.rows);
    }
    merged.sort(comparator);
    if (merged.size() > n) {
      // Copy so the truncated list does not pin the full backing ArrayList
      merged = new ArrayList<>(merged.subList(0, n));
    }
    return new InternalCalciteExec(
        getName(), combine, totalCollected, (long) merged.size(), merged, getMetadata());
  }

  /**
   * Builds the row comparator for TOP_N from the descriptor's key indices and direction specs. Each
   * spec is {@code <Direction>:<NullDirection>} as written by StagePlanner; the null direction is
   * always explicit (never UNSPECIFIED) so no default has to be re-derived here.
   */
  private static Comparator<List<Object>> buildTopNComparator(
      List<Integer> keys, List<String> dirs) {
    if (keys.size() != dirs.size()) {
      throw new IllegalStateException(
          "TOP_N descriptor has " + keys.size() + " keys but " + dirs.size() + " direction specs");
    }
    Comparator<List<Object>> result = null;
    for (int i = 0; i < keys.size(); i++) {
      int colIdx = keys.get(i);
      String spec = dirs.get(i);
      int colon = spec.indexOf(':');
      if (colon < 0) {
        throw new IllegalStateException("Malformed TOP_N direction spec: " + spec);
      }
      boolean descending = spec.substring(0, colon).contains("DESCENDING");
      boolean nullsFirst = "FIRST".equals(spec.substring(colon + 1));
      Comparator<List<Object>> keyComparator =
          (a, b) -> compareCells(a.get(colIdx), b.get(colIdx), descending, nullsFirst);
      result = result == null ? keyComparator : result.thenComparing(keyComparator);
    }
    // An empty key list cannot reach here (StagePlanner refuses an empty collation), but a
    // no-op comparator is the safe reading of "no ordering" rather than an exception.
    return result == null ? (a, b) -> 0 : result;
  }

  /** Compares two cells honouring the key's direction and null placement. */
  private static int compareCells(Object a, Object b, boolean descending, boolean nullsFirst) {
    if (a == null || b == null) {
      if (a == null && b == null) {
        return 0;
      }
      // Null placement is absolute: it is NOT flipped by the descending direction, because
      // StagePlanner already resolved the collation's own nullDirection for that direction.
      return (a == null) == nullsFirst ? -1 : 1;
    }
    int cmp = compareValues(a, b);
    return descending ? -cmp : cmp;
  }

  /**
   * Compares two non-null cell values. Numbers compare numerically across boxed types; otherwise
   * both values must be mutually comparable instances of the same class. Anything else throws
   * rather than degrading to an arbitrary order — a wrong order is a wrong answer.
   */
  @SuppressWarnings("unchecked")
  private static int compareValues(Object a, Object b) {
    if (a instanceof Number na && b instanceof Number nb) {
      return compareNumbers(na, nb);
    }
    if (a.getClass() == b.getClass() && a instanceof Comparable<?>) {
      return ((Comparable<Object>) a).compareTo(b);
    }
    throw new IllegalStateException(
        "TOP_N cannot compare values of types "
            + a.getClass().getName()
            + " and "
            + b.getClass().getName());
  }

  /**
   * RANK_LIMIT reduce (US-015, rule 3): recomputes rank over the union of shard outputs and
   * re-applies the inclusive bound k. Nothing here reads a shipped rank column — the shard dropped
   * it because a shard-local rank is meaningless once rows from several shards are combined — so
   * the rank is derived afresh from the partition and order key values.
   *
   * <p>Associative. Every row this method discards had a rank above k within a SUBSET of the
   * partition, and rank is monotone in the row set, so it could not have qualified over the full
   * set either; feeding the output back in therefore cannot change the answer. Ranks are
   * non-decreasing along the sorted run, which is why the scan can stop at the first row past k.
   *
   * <p>For the unordered ROW_NUMBER case the answer is not unique — that is the nondeterminism the
   * split rule relies on — but this implementation is still deterministic across batch groupings,
   * because each reduce keeps a PREFIX of the concatenation and a prefix of a prefix is the same
   * prefix.
   */
  private InternalCalciteExec reduceRankLimit(List<InternalAggregation> aggregations) {
    int k = combine.getIntParam();
    List<Integer> partitionKeys = combine.getIntListParam();
    List<OrderKeySpec> orderKeys = OrderKeySpec.parseAll(combine.getStringListParam());
    RankFunction rankFunction = RankFunction.valueOf(combine.getRankFunction());

    long totalCollected = 0;
    Map<GroupKey, List<List<Object>>> partitions = new LinkedHashMap<>();
    for (InternalAggregation agg : aggregations) {
      InternalCalciteExec other = (InternalCalciteExec) agg;
      totalCollected += other.rowsCollected;
      for (List<Object> row : other.rows) {
        Object[] keyArr = new Object[partitionKeys.size()];
        for (int i = 0; i < partitionKeys.size(); i++) {
          keyArr[i] = row.get(partitionKeys.get(i));
        }
        partitions.computeIfAbsent(new GroupKey(keyArr), key -> new ArrayList<>()).add(row);
      }
    }

    Comparator<List<Object>> order = buildOrderComparator(orderKeys);
    List<List<Object>> result = new ArrayList<>();
    for (List<List<Object>> rows : partitions.values()) {
      if (order != null) {
        // List.sort is stable, so rows with equal order keys keep their encounter order.
        rows.sort(order);
      }
      retainByRank(rows, k, rankFunction, order, result);
    }
    return new InternalCalciteExec(
        getName(), combine, totalCollected, (long) result.size(), result, getMetadata());
  }

  /**
   * Appends the rows of one already-sorted partition whose recomputed rank is at most k. The three
   * ranking functions differ only in how a tie advances the rank: ROW_NUMBER never ties, RANK jumps
   * to the row's ordinal, DENSE_RANK advances by one per distinct order key.
   */
  private static void retainByRank(
      List<List<Object>> sorted,
      int k,
      RankFunction rankFunction,
      Comparator<List<Object>> order,
      List<List<Object>> out) {
    int rank = 0;
    for (int i = 0; i < sorted.size(); i++) {
      List<Object> row = sorted.get(i);
      boolean tie = i > 0 && order != null && order.compare(sorted.get(i - 1), row) == 0;
      rank =
          switch (rankFunction) {
            case ROW_NUMBER -> i + 1;
            case RANK -> tie ? rank : i + 1;
            case DENSE_RANK -> tie ? rank : rank + 1;
          };
      if (rank > k) {
        // Ranks never decrease along a sorted run, so no later row can qualify.
        return;
      }
      out.add(row);
    }
  }

  /** The ranking functions rule 3 can decompose. */
  private enum RankFunction {
    ROW_NUMBER,
    RANK,
    DENSE_RANK
  }

  /**
   * One parsed RANK_LIMIT order key: {@code <colIdx>:<Direction>:<NullDirection>} as written by
   * StagePlanner. The null direction is always explicit on the wire, so no default is re-derived
   * here.
   */
  private record OrderKeySpec(int colIdx, boolean descending, boolean nullsFirst) {
    static List<OrderKeySpec> parseAll(List<String> specs) {
      List<OrderKeySpec> parsed = new ArrayList<>(specs.size());
      for (String spec : specs) {
        String[] parts = spec.split(":", -1);
        if (parts.length != 3) {
          throw new IllegalStateException("Malformed RANK_LIMIT order key spec: " + spec);
        }
        parsed.add(
            new OrderKeySpec(
                Integer.parseInt(parts[0]),
                parts[1].contains("DESCENDING"),
                "FIRST".equals(parts[2])));
      }
      return parsed;
    }
  }

  /** Builds the intra-partition order comparator, or null for an unordered window. */
  private static Comparator<List<Object>> buildOrderComparator(List<OrderKeySpec> orderKeys) {
    Comparator<List<Object>> result = null;
    for (OrderKeySpec key : orderKeys) {
      Comparator<List<Object>> keyComparator =
          (a, b) ->
              compareCells(
                  a.get(key.colIdx()), b.get(key.colIdx()), key.descending(), key.nullsFirst());
      result = result == null ? keyComparator : result.thenComparing(keyComparator);
    }
    return result;
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
    if (a instanceof Double || b instanceof Double || a instanceof Float || b instanceof Float) {
      return a.doubleValue() + b.doubleValue();
    }
    return a.longValue() + b.longValue();
  }

  /**
   * Comparison preserving BigDecimal exactness, promotes to Double for any floating-point input.
   */
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
    if (a instanceof Double || b instanceof Double || a instanceof Float || b instanceof Float) {
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

  /** Rows buffered by the shard before the fragment ran, summed across reduced aggregations. */
  public long getRowsCollected() {
    return rowsCollected;
  }

  /** Rows this aggregation carries, i.e. the fragment/combine output size. */
  public long getRowsEmitted() {
    return rowsEmitted;
  }
}
