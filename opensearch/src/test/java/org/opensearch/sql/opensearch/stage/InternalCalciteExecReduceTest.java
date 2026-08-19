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
