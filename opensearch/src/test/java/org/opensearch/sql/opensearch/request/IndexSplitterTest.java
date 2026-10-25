/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.request;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.opensearch.request.OpenSearchRequest.IndexName;

class IndexSplitterTest {

  @Test
  void shouldGroupUntilTheBudgetIsReached() {
    assertGroups("a,b|c,d|e", singleShard("a", "b", "c", "d", "e"), 2);
  }

  @Test
  void shouldCountShardsRatherThanIndices() {
    // Two 6-shard indices open 12 contexts, so a budget of 8 admits only one of them per group.
    assertGroups("a|b", shards(Map.entry("a", 6), Map.entry("b", 6)), 8);
  }

  @Test
  void shouldGiveAnIndexWiderThanTheBudgetAGroupOfItsOwn() {
    assertGroups(
        "small|huge|also-small",
        shards(Map.entry("small", 1), Map.entry("huge", 50), Map.entry("also-small", 1)),
        2);
  }

  @Test
  void shouldReturnOneGroupWhenTheWholeExpressionFits() {
    assertGroups("a,b,c", singleShard("a", "b", "c"), 10);
  }

  @Test
  void shouldClampANonPositiveBudgetToOneShardPerGroup() {
    assertGroups("a|b", singleShard("a", "b"), 0);
  }

  @Test
  void shouldChargeAnUnknownShardCountAsOneShard() {
    assertGroups("a,b", shards(Map.entry("a", 0), Map.entry("b", 1)), 2);
  }

  @Test
  void shouldNameEveryIndexExactlyOnceAndInOrder() {
    String flattened =
        IndexSplitter.group(singleShard("a", "b", "c", "d", "e", "f", "g"), 3).stream()
            .flatMap(group -> Arrays.stream(group.getIndexNames()))
            .collect(Collectors.joining(","));
    assertEquals("a,b,c,d,e,f,g", flattened);
  }

  private static void assertGroups(String expected, Map<String, Integer> shardCounts, int budget) {
    assertEquals(
        expected,
        IndexSplitter.group(shardCounts, budget).stream()
            .map(IndexName::toString)
            .collect(Collectors.joining("|")));
  }

  private static Map<String, Integer> singleShard(String... names) {
    Map<String, Integer> shardCounts = new LinkedHashMap<>();
    Arrays.stream(names).forEach(name -> shardCounts.put(name, 1));
    return shardCounts;
  }

  @SafeVarargs
  private static Map<String, Integer> shards(Map.Entry<String, Integer>... entries) {
    Map<String, Integer> shardCounts = new LinkedHashMap<>();
    Arrays.stream(entries).forEach(entry -> shardCounts.put(entry.getKey(), entry.getValue()));
    return shardCounts;
  }
}
