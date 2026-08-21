/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.stage;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.math.BigDecimal;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;

/**
 * Unit tests for {@link InternalCalciteExec#reduce} in MERGE_AGG mode. The key property tested is
 * ASSOCIATIVITY: reducing the same input in two different batch groupings must produce identical
 * output, because OpenSearch calls reduce() in batches of 512 (batched_reduce_size).
 */
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
public class InternalCalciteExecReduceTest {

  private static final String NAME = "calcite_stage";
  private static final Map<String, Object> META = Map.of();

  @Test
  void merge_agg_groups_and_sums_across_shards() {
    // Two shards, each with two groups: ("web", count=3), ("api", count=2)
    //                                    ("web", count=5), ("api", count=1)
    CombineDescriptor combine = CombineDescriptor.mergeAgg(List.of(0), List.of("SUM(1)"));

    InternalCalciteExec shard1 =
        new InternalCalciteExec(
            NAME, combine, 10L, 5L, List.of(List.of("web", 3L), List.of("api", 2L)), META);
    InternalCalciteExec shard2 =
        new InternalCalciteExec(
            NAME, combine, 8L, 5L, List.of(List.of("web", 5L), List.of("api", 1L)), META);

    InternalCalciteExec result = (InternalCalciteExec) shard1.reduce(List.of(shard1, shard2), null);

    assertEquals(2, result.getRows().size());
    // web: 3+5=8, api: 2+1=3
    assertEquals(List.of("web", 8L), result.getRows().get(0));
    assertEquals(List.of("api", 3L), result.getRows().get(1));
  }

  @Test
  void merge_agg_null_handling_yields_null_only_if_all_inputs_null() {
    CombineDescriptor combine = CombineDescriptor.mergeAgg(List.of(0), List.of("SUM(1)"));

    InternalCalciteExec shard1 =
        new InternalCalciteExec(NAME, combine, 2L, 1L, List.of(Arrays.asList("x", null)), META);
    InternalCalciteExec shard2 =
        new InternalCalciteExec(NAME, combine, 2L, 1L, List.of(Arrays.asList("x", 5L)), META);

    InternalCalciteExec result = (InternalCalciteExec) shard1.reduce(List.of(shard1, shard2), null);

    // null + 5 = 5 (null skipped)
    assertEquals(Arrays.asList("x", 5L), result.getRows().get(0));
  }

  @Test
  void merge_agg_all_null_yields_null() {
    CombineDescriptor combine = CombineDescriptor.mergeAgg(List.of(0), List.of("SUM(1)"));

    InternalCalciteExec shard1 =
        new InternalCalciteExec(NAME, combine, 1L, 1L, List.of(Arrays.asList("y", null)), META);
    InternalCalciteExec shard2 =
        new InternalCalciteExec(NAME, combine, 1L, 1L, List.of(Arrays.asList("y", null)), META);

    InternalCalciteExec result = (InternalCalciteExec) shard1.reduce(List.of(shard1, shard2), null);

    assertEquals(Arrays.asList("y", null), result.getRows().get(0));
  }

  @Test
  void merge_agg_min_max() {
    CombineDescriptor combine = CombineDescriptor.mergeAgg(List.of(0), List.of("MIN(1)", "MAX(2)"));

    InternalCalciteExec shard1 =
        new InternalCalciteExec(NAME, combine, 5L, 1L, List.of(List.of("g1", 10L, 100L)), META);
    InternalCalciteExec shard2 =
        new InternalCalciteExec(NAME, combine, 5L, 1L, List.of(List.of("g1", 3L, 200L)), META);

    InternalCalciteExec result = (InternalCalciteExec) shard1.reduce(List.of(shard1, shard2), null);

    assertEquals(List.of("g1", 3L, 200L), result.getRows().get(0));
  }

  /**
   * ASSOCIATIVITY PROOF: reduce the same 3 shards in two different batch groupings and assert
   * identical output. This is the load-bearing test per Design Invariant 6: reduce() is called in
   * batches of 512, so the output of one reduce is fed back into another.
   */
  @Test
  void merge_agg_is_associative_across_batch_groupings() {
    CombineDescriptor combine =
        CombineDescriptor.mergeAgg(List.of(0), List.of("SUM(1)", "MIN(2)", "MAX(3)"));

    InternalCalciteExec s1 =
        new InternalCalciteExec(
            NAME,
            combine,
            10L,
            2L,
            List.of(List.of("a", 5L, 1L, 10L), List.of("b", 3L, 2L, 8L)),
            META);
    InternalCalciteExec s2 =
        new InternalCalciteExec(
            NAME,
            combine,
            10L,
            2L,
            List.of(List.of("a", 7L, 3L, 15L), List.of("b", 1L, 5L, 6L)),
            META);
    InternalCalciteExec s3 =
        new InternalCalciteExec(
            NAME,
            combine,
            10L,
            2L,
            List.of(List.of("a", 2L, 0L, 20L), List.of("b", 4L, 1L, 9L)),
            META);

    // Grouping A: reduce(s1, s2) then reduce(result, s3)
    InternalCalciteExec intermediate = (InternalCalciteExec) s1.reduce(List.of(s1, s2), null);
    InternalCalciteExec resultA =
        (InternalCalciteExec) intermediate.reduce(List.of(intermediate, s3), null);

    // Grouping B: reduce(s1, s3) then reduce(result, s2)
    InternalCalciteExec intermediateB = (InternalCalciteExec) s1.reduce(List.of(s1, s3), null);
    InternalCalciteExec resultB =
        (InternalCalciteExec) intermediateB.reduce(List.of(intermediateB, s2), null);

    // Both must produce the same rows (order may differ, so sort by key)
    List<List<Object>> rowsA =
        resultA.getRows().stream()
            .sorted((a, b) -> ((String) a.get(0)).compareTo((String) b.get(0)))
            .toList();
    List<List<Object>> rowsB =
        resultB.getRows().stream()
            .sorted((a, b) -> ((String) a.get(0)).compareTo((String) b.get(0)))
            .toList();
    assertEquals(rowsA, rowsB);
    // Verify actual values: a: sum=14, min=0, max=20; b: sum=8, min=1, max=9
    assertEquals(List.of("a", 14L, 0L, 20L), rowsA.get(0));
    assertEquals(List.of("b", 8L, 1L, 9L), rowsA.get(1));
  }

  @Test
  void merge_agg_with_null_group_key_groups_nulls_together() {
    CombineDescriptor combine = CombineDescriptor.mergeAgg(List.of(0), List.of("SUM(1)"));

    InternalCalciteExec shard1 =
        new InternalCalciteExec(
            NAME, combine, 2L, 2L, List.of(Arrays.asList(null, 4L), Arrays.asList("x", 1L)), META);
    InternalCalciteExec shard2 =
        new InternalCalciteExec(
            NAME, combine, 2L, 2L, List.of(Arrays.asList(null, 6L), Arrays.asList("x", 2L)), META);

    InternalCalciteExec result = (InternalCalciteExec) shard1.reduce(List.of(shard1, shard2), null);

    // Null group key: 4+6=10, x: 1+2=3
    assertEquals(2, result.getRows().size());
  }

  @Test
  void limit_truncates_to_n() {
    CombineDescriptor combine = CombineDescriptor.limit(3);

    InternalCalciteExec shard1 =
        new InternalCalciteExec(
            NAME,
            combine,
            5L,
            5L,
            List.of(List.of("a", 1L), List.of("b", 2L), List.of("c", 3L)),
            META);
    InternalCalciteExec shard2 =
        new InternalCalciteExec(
            NAME,
            combine,
            3L,
            3L,
            List.of(List.of("d", 4L), List.of("e", 5L), List.of("f", 6L)),
            META);

    InternalCalciteExec result = (InternalCalciteExec) shard1.reduce(List.of(shard1, shard2), null);

    // Total rows available = 6, but limit is 3 → only first 3 kept
    assertEquals(3, result.getRows().size());
    assertEquals(List.of("a", 1L), result.getRows().get(0));
    assertEquals(List.of("b", 2L), result.getRows().get(1));
    assertEquals(List.of("c", 3L), result.getRows().get(2));
  }

  /**
   * ASSOCIATIVITY PROOF for LIMIT: reduce() runs in batches of 512 and its own output is fed back
   * in. The key associativity property is: reduce([reduce([s1,s2]), s3]) == reduce([s1, s2, s3])
   * when the concatenation order is the same. For LIMIT, this means truncating at intermediate
   * stages is safe because no new rows are created — only discarded.
   */
  @Test
  void limit_is_associative_across_batch_groupings() {
    CombineDescriptor combine = CombineDescriptor.limit(4);

    InternalCalciteExec s1 =
        new InternalCalciteExec(
            NAME, combine, 2L, 2L, List.of(List.of("a", 1L), List.of("b", 2L)), META);
    InternalCalciteExec s2 =
        new InternalCalciteExec(
            NAME, combine, 2L, 2L, List.of(List.of("c", 3L), List.of("d", 4L)), META);
    InternalCalciteExec s3 =
        new InternalCalciteExec(
            NAME, combine, 2L, 2L, List.of(List.of("e", 5L), List.of("f", 6L)), META);

    // Grouping A: reduce(s1, s2, s3) all at once
    InternalCalciteExec resultAll = (InternalCalciteExec) s1.reduce(List.of(s1, s2, s3), null);

    // Grouping B: reduce(s1, s2) then reduce(result, s3) — same order as A
    InternalCalciteExec intermediate = (InternalCalciteExec) s1.reduce(List.of(s1, s2), null);
    InternalCalciteExec resultBatched =
        (InternalCalciteExec) intermediate.reduce(List.of(intermediate, s3), null);

    // Both must produce 4 rows (the limit) and the same rows — because concatenation order is
    // identical (s1 then s2 then s3) regardless of batching.
    assertEquals(4, resultAll.getRows().size());
    assertEquals(4, resultBatched.getRows().size());
    assertEquals(resultAll.getRows(), resultBatched.getRows());
  }

  // --- TOP_N reduce tests (US-014, rule 2) ---

  @Test
  void top_n_merges_sorted_runs_and_keeps_the_global_top_n() {
    // Two shards each shipped their own ordered top-3 run on column 1 ascending. The global top-3
    // interleaves the two runs — which is the whole point of merging rather than concatenating.
    CombineDescriptor combine = CombineDescriptor.topN(List.of(1), List.of("ASCENDING:LAST"), 3);

    InternalCalciteExec shard1 =
        new InternalCalciteExec(
            NAME,
            combine,
            100L,
            3L,
            List.of(List.of("a", 10L), List.of("c", 30L), List.of("e", 50L)),
            META);
    InternalCalciteExec shard2 =
        new InternalCalciteExec(
            NAME,
            combine,
            80L,
            3L,
            List.of(List.of("b", 20L), List.of("d", 40L), List.of("f", 60L)),
            META);

    InternalCalciteExec result = (InternalCalciteExec) shard1.reduce(List.of(shard1, shard2), null);

    assertEquals(
        List.of(List.of("a", 10L), List.of("b", 20L), List.of("c", 30L)), result.getRows());
    assertEquals(180L, result.getRowsCollected());
    assertEquals(3L, result.getRowsEmitted());
  }

  @Test
  void top_n_honours_descending_direction_and_null_placement() {
    // dirs is DESCENDING:LAST, so the largest value wins and nulls go last regardless of direction.
    CombineDescriptor combine = CombineDescriptor.topN(List.of(0), List.of("DESCENDING:LAST"), 3);

    InternalCalciteExec shard1 =
        new InternalCalciteExec(
            NAME,
            combine,
            3L,
            3L,
            List.of(List.of(30L), List.of(10L), Arrays.asList((Object) null)),
            META);
    InternalCalciteExec shard2 =
        new InternalCalciteExec(NAME, combine, 2L, 2L, List.of(List.of(40L), List.of(20L)), META);

    InternalCalciteExec result = (InternalCalciteExec) shard1.reduce(List.of(shard1, shard2), null);

    assertEquals(List.of(List.of(40L), List.of(30L), List.of(20L)), result.getRows());
  }

  @Test
  void top_n_orders_by_the_second_key_when_the_first_ties() {
    CombineDescriptor combine =
        CombineDescriptor.topN(List.of(0, 1), List.of("ASCENDING:LAST", "DESCENDING:LAST"), 3);

    InternalCalciteExec shard =
        new InternalCalciteExec(
            NAME,
            combine,
            4L,
            4L,
            List.of(List.of("x", 1L), List.of("x", 3L), List.of("x", 2L), List.of("y", 9L)),
            META);

    InternalCalciteExec result = (InternalCalciteExec) shard.reduce(List.of(shard), null);

    assertEquals(List.of(List.of("x", 3L), List.of("x", 2L), List.of("x", 1L)), result.getRows());
  }

  /**
   * ASSOCIATIVITY PROOF for TOP_N. reduce() runs in batches of 512 and its own output is fed back
   * in, so the property that matters is that reducing the same input in two DIFFERENT batch
   * groupings yields identical output. It holds because the global top-n over the union of per-run
   * top-n is exactly the global top-n: an intermediate truncation can only discard rows that were
   * already beaten by n others.
   */
  @Test
  void top_n_is_associative_across_batch_groupings() {
    CombineDescriptor combine = CombineDescriptor.topN(List.of(1), List.of("ASCENDING:LAST"), 4);

    InternalCalciteExec s1 =
        new InternalCalciteExec(
            NAME,
            combine,
            9L,
            3L,
            List.of(List.of("a", 5L), List.of("b", 50L), List.of("c", 90L)),
            META);
    InternalCalciteExec s2 =
        new InternalCalciteExec(
            NAME,
            combine,
            9L,
            3L,
            List.of(List.of("d", 10L), List.of("e", 60L), List.of("f", 95L)),
            META);
    InternalCalciteExec s3 =
        new InternalCalciteExec(
            NAME,
            combine,
            9L,
            3L,
            List.of(List.of("g", 1L), List.of("h", 70L), List.of("i", 99L)),
            META);

    InternalCalciteExec resultAll = (InternalCalciteExec) s1.reduce(List.of(s1, s2, s3), null);

    InternalCalciteExec intermediate = (InternalCalciteExec) s1.reduce(List.of(s1, s2), null);
    InternalCalciteExec resultBatched =
        (InternalCalciteExec) intermediate.reduce(List.of(intermediate, s3), null);

    // A third grouping, batching the LAST two first, to prove grouping-independence rather than
    // just left-associativity.
    InternalCalciteExec intermediate2 = (InternalCalciteExec) s2.reduce(List.of(s2, s3), null);
    InternalCalciteExec resultBatched2 =
        (InternalCalciteExec) s1.reduce(List.of(s1, intermediate2), null);

    assertEquals(
        List.of(List.of("g", 1L), List.of("a", 5L), List.of("d", 10L), List.of("b", 50L)),
        resultAll.getRows());
    assertEquals(resultAll.getRows(), resultBatched.getRows());
    assertEquals(resultAll.getRows(), resultBatched2.getRows());
    assertEquals(27L, resultAll.getRowsCollected());
    assertEquals(27L, resultBatched.getRowsCollected());
    assertEquals(27L, resultBatched2.getRowsCollected());
  }

  // --- Wire round-trip tests (Critical B fix) ---

  @Test
  void wire_round_trip_preserves_bigDecimal_cell_and_normal_cell() throws IOException {
    // A BigDecimal cell (from DECIMAL-typed aggregation) and a normal Long cell must
    // round-trip through the out-of-band tagged wire format with whole-object equality.
    CombineDescriptor combine = CombineDescriptor.concat();
    BigDecimal precise = new BigDecimal("123456789.123456789012345678");
    InternalCalciteExec original =
        new InternalCalciteExec(
            NAME, combine, 5L, 3L, List.of(Arrays.asList("hello", precise, 42L, null)), META);

    // Serialize to bytes
    BytesStreamOutput out = new BytesStreamOutput();
    original.writeTo(out);

    // Deserialize from bytes
    StreamInput in = out.bytes().streamInput();
    InternalCalciteExec deserialized = new InternalCalciteExec(in);

    // Whole-object equality — the load-bearing assertion per the symmetric checklist
    assertEquals(original, deserialized);
    // Also verify individual cells to confirm types are preserved
    List<Object> row = deserialized.getRows().get(0);
    assertEquals("hello", row.get(0));
    assertEquals(precise, row.get(1));
    assertEquals(42L, row.get(2));
    assertEquals(null, row.get(3));
  }

  @Test
  void merge_agg_bigDecimal_addition_is_exact() {
    // BigDecimal SUM must stay exact beyond ~15 significant digits (the lossy threshold for
    // doubleValue()). This tests the NIT fix to addNumbers.
    CombineDescriptor combine = CombineDescriptor.mergeAgg(List.of(0), List.of("SUM(1)"));
    BigDecimal a = new BigDecimal("99999999999999999.99");
    BigDecimal b = new BigDecimal("0.01");

    InternalCalciteExec shard1 =
        new InternalCalciteExec(NAME, combine, 1L, 1L, List.of(Arrays.asList("k", a)), META);
    InternalCalciteExec shard2 =
        new InternalCalciteExec(NAME, combine, 1L, 1L, List.of(Arrays.asList("k", b)), META);

    InternalCalciteExec result = (InternalCalciteExec) shard1.reduce(List.of(shard1, shard2), null);

    // Exact: 99999999999999999.99 + 0.01 = 100000000000000000.00
    BigDecimal expected = new BigDecimal("100000000000000000.00");
    assertEquals(expected, result.getRows().get(0).get(1));
  }
}
