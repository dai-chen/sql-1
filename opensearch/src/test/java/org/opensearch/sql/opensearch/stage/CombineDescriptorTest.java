/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.stage;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.List;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
public class CombineDescriptorTest {

  @Test
  void concat_describes_as_CONCAT() {
    CombineDescriptor concat = CombineDescriptor.concat();
    assertEquals("CONCAT", concat.describe());
  }

  @Test
  void merge_agg_describes_with_groupKeys_and_aggs() {
    CombineDescriptor mergeAgg =
        new CombineDescriptor(
            CombineDescriptor.Mode.MERGE_AGG, List.of(0, 1), List.of("SUM", "COUNT"), 0);
    assertEquals("MERGE_AGG{groupKeys:[0, 1], aggs:[SUM, COUNT]}", mergeAgg.describe());
  }

  @Test
  void top_n_describes_with_keys_dirs_n() {
    CombineDescriptor topN =
        new CombineDescriptor(
            CombineDescriptor.Mode.TOP_N, List.of(1, 0), List.of("age", "name"), 10);
    assertEquals("TOP_N{keys:[age, name], dirs:[1, 0], n:10}", topN.describe());
  }

  @Test
  void rank_limit_describes_with_partitionKeys_orderKeys_k() {
    CombineDescriptor rankLimit =
        new CombineDescriptor(CombineDescriptor.Mode.RANK_LIMIT, List.of(2), List.of("score"), 5);
    assertEquals("RANK_LIMIT{partitionKeys:[2], orderKeys:[score], k:5}", rankLimit.describe());
  }

  @Test
  void limit_describes_with_n() {
    CombineDescriptor limit =
        new CombineDescriptor(CombineDescriptor.Mode.LIMIT, List.of(), List.of(), 100);
    assertEquals("LIMIT{n:100}", limit.describe());
  }

  @Test
  void merge_agg_empty_params_describes_with_empty_braces() {
    CombineDescriptor mergeAgg =
        new CombineDescriptor(CombineDescriptor.Mode.MERGE_AGG, List.of(), List.of(), 0);
    assertEquals("MERGE_AGG{}", mergeAgg.describe());
  }
}
