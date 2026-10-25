/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.request;

import static org.opensearch.action.search.SearchRequest.DEFAULT_INDICES_OPTIONS;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.opensearch.action.admin.indices.resolve.ResolveIndexAction;
import org.opensearch.action.admin.indices.settings.get.GetSettingsResponse;
import org.opensearch.cluster.metadata.IndexMetadata;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.sql.opensearch.request.OpenSearchRequest.IndexName;
import org.opensearch.transport.client.node.NodeClient;

/**
 * Splits an index expression into groups that can be read one at a time, ahead of operations whose
 * cost grows with the number of shards read at once, such as PIT creation.
 */
@Log4j2
@RequiredArgsConstructor
public class IndexSplitter {

  /** Bounds each probe. Generous because a fallback can fail the query, not merely slow it. */
  private static final TimeValue PROBE_TIMEOUT = TimeValue.timeValueSeconds(10);

  /** Both probes are transport actions, so only the node client can issue them. */
  private final NodeClient node;

  /**
   * Returns the groups to read in order, together naming exactly the indices the expression names.
   * A single group means read the expression as one, which is the answer whenever splitting is
   * unsafe, would not narrow the read, or cannot be planned.
   *
   * @param indexName index expression the query named
   * @param shardBudget most primary shards one group may cover
   * @return groups to read in order, never empty
   */
  public List<IndexName> split(IndexName indexName, int shardBudget) {
    try {
      ResolveIndexAction.Response resolved = resolve(indexName);
      // Splitting names concrete indices, which drops an alias filter, so it is refused here for
      // the same reason IndexPruner refuses these.
      if (!resolved.getAliases().isEmpty() || !resolved.getDataStreams().isEmpty()) {
        return List.of(indexName);
      }

      List<IndexName> groups = group(shardCounts(resolved), shardBudget);
      if (groups.size() < 2) {
        return List.of(indexName);
      }
      log.info("Index expression split into {} groups", groups.size());
      return groups;
    } catch (Exception e) {
      log.warn("Index splitting failed; reading the full index expression", e);
      return List.of(indexName);
    }
  }

  /**
   * Groups indices greedily in the order resolved, so one expression always yields the same groups.
   * The budget counts shards rather than indices because a reader context is opened per shard.
   *
   * <p>An index wider than the budget becomes a group of its own, since a group cannot be narrower
   * than one index and refusing would decline a query splitting still helps.
   */
  static List<IndexName> group(Map<String, Integer> shardCounts, int shardBudget) {
    int budget = Math.max(1, shardBudget);
    List<IndexName> groups = new ArrayList<>();
    List<String> current = new ArrayList<>();
    int currentShards = 0;
    for (Map.Entry<String, Integer> index : shardCounts.entrySet()) {
      int shards = Math.max(1, index.getValue());
      if (!current.isEmpty() && currentShards + shards > budget) {
        groups.add(nameOf(current));
        current = new ArrayList<>();
        currentShards = 0;
      }
      current.add(index.getKey());
      currentShards += shards;
    }
    if (!current.isEmpty()) {
      groups.add(nameOf(current));
    }
    return groups;
  }

  private static IndexName nameOf(List<String> indices) {
    return new IndexName(String.join(",", indices));
  }

  private ResolveIndexAction.Response resolve(IndexName indexName) {
    ResolveIndexAction.Request request =
        new ResolveIndexAction.Request(indexName.getIndexNames(), DEFAULT_INDICES_OPTIONS);
    return node.execute(ResolveIndexAction.INSTANCE, request).actionGet(PROBE_TIMEOUT);
  }

  /** Deduplicated, because a comma separated expression can name one index twice. */
  private Map<String, Integer> shardCounts(ResolveIndexAction.Response resolved) {
    List<String> names =
        resolved.getIndices().stream()
            .map(ResolveIndexAction.ResolvedIndex::getName)
            .distinct()
            .toList();
    GetSettingsResponse response =
        node.admin()
            .indices()
            .prepareGetSettings(names.toArray(String[]::new))
            .setLocal(true)
            .execute()
            .actionGet(PROBE_TIMEOUT);

    Map<String, Integer> shardCounts = new LinkedHashMap<>();
    for (String name : names) {
      Settings settings = response.getIndexToSettings().get(name);
      // An index that vanished between the two probes is charged one shard. Reading it then fails,
      // which is what reading the expression unsplit would also have done.
      shardCounts.put(
          name,
          settings == null ? 1 : settings.getAsInt(IndexMetadata.SETTING_NUMBER_OF_SHARDS, 1));
    }
    return shardCounts;
  }
}
