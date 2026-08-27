/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.request;

import java.util.Arrays;
import java.util.List;
import java.util.Optional;
import java.util.stream.Stream;
import lombok.RequiredArgsConstructor;
import lombok.extern.log4j.Log4j2;
import org.opensearch.action.admin.indices.resolve.ResolveIndexAction;
import org.opensearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.opensearch.common.regex.Regex;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.ConstantScoreQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.request.OpenSearchRequest.IndexName;
import org.opensearch.transport.RemoteClusterAware;
import org.opensearch.transport.client.node.NodeClient;

/**
 * Narrows a wildcard index expression to the indices that can match a pushed-down filter before a
 * PIT is created, to avoid exhausting the open PIT reader-context limit. Note that {@code
 * _field_caps} does not surface per-index failures ({@code TransportFieldCapabilitiesAction}
 * swallows them), so an index whose shards all fail is indistinguishable from one legitimately
 * pruned; the guards below reduce but do not eliminate that exposure, which is why the feature is
 * off by default.
 */
@Log4j2
@RequiredArgsConstructor
public class IndexPruner {

  private static final String PROBE_FIELD = "__sql_prune_probe__";

  private final OpenSearchClient client;
  private final Settings settings;

  public IndexName prune(IndexName indexName, QueryBuilder filter) {
    try {
      return isPrunable(indexName, filter) ? doPrune(indexName, filter) : indexName;
    } catch (Exception e) {
      log.warn("Index pruning failed; querying the full index expression", e);
      return indexName;
    }
  }

  private boolean isPrunable(IndexName indexName, QueryBuilder filter) {
    if (!Boolean.TRUE.equals(settings.getSettingValue(Settings.Key.QUERY_PRUNING_ENABLED))) {
      return false;
    }
    if (filter == null) {
      return false;
    }
    if (!hasWildcard(indexName)) {
      return false;
    }
    if (hasClusterQualifier(indexName)) {
      return false;
    }
    // can_match only proves range disjointness via RangeQueryBuilder, so a rangeless filter such
    // as query_string cannot prune.
    return containsRange(filter);
  }

  private IndexName doPrune(IndexName indexName, QueryBuilder filter) {
    Optional<NodeClient> node = client.getNodeClient();
    if (node.isEmpty()) {
      return indexName;
    }
    List<String> matched = resolveConcreteIndices(node.get(), indexName);
    if (matched.size() <= 1) {
      return indexName;
    }
    List<String> survivors = probeMatchingIndices(node.get(), indexName, filter);
    // an empty inclusion list would mean "all indices" to OpenSearch, so the guard is
    // load-bearing.
    if (survivors.isEmpty() || survivors.size() >= matched.size()) {
      return indexName;
    }
    log.info("Index pruning narrowed {} indices to {}", matched.size(), survivors.size());
    return new IndexName(String.join(",", survivors));
  }

  private boolean hasWildcard(IndexName indexName) {
    return Arrays.stream(indexName.getIndexNames()).anyMatch(Regex::isSimpleMatchPattern);
  }

  // A colon does not guarantee a remote cluster (core resolves it against configured cluster
  // names), so treat any qualifier as out of scope rather than trying to decide.
  private boolean hasClusterQualifier(IndexName indexName) {
    return Arrays.stream(indexName.getIndexNames())
        .anyMatch(name -> name.indexOf(RemoteClusterAware.REMOTE_CLUSTER_INDEX_SEPARATOR) >= 0);
  }

  // Resolves against cluster state only (no shard fan-out), so it is the cheap way to learn the
  // full matched index set.
  private List<String> resolveConcreteIndices(NodeClient node, IndexName indexName) {
    ResolveIndexAction.Response response =
        node.execute(
                ResolveIndexAction.INSTANCE,
                new ResolveIndexAction.Request(indexName.getIndexNames()))
            .actionGet();
    return response.getIndices().stream().map(ResolveIndexAction.ResolvedIndex::getName).toList();
  }

  private List<String> probeMatchingIndices(
      NodeClient node, IndexName indexName, QueryBuilder filter) {
    // a nonexistent field keeps the merge cost near zero while the index filter still drives
    // can_match to drop indices whose shards cannot match.
    FieldCapabilitiesRequest request =
        new FieldCapabilitiesRequest().indices(indexName.getIndexNames()).fields(PROBE_FIELD);
    request.indexFilter(filter);
    return List.of(node.fieldCaps(request).actionGet().getIndices());
  }

  private boolean containsRange(QueryBuilder query) {
    if (query instanceof RangeQueryBuilder) {
      return true;
    }
    if (query instanceof BoolQueryBuilder bool) {
      return Stream.of(bool.must(), bool.filter(), bool.should())
          .flatMap(List::stream)
          .anyMatch(this::containsRange);
    }
    if (query instanceof ConstantScoreQueryBuilder constantScore) {
      return containsRange(constantScore.innerQuery());
    }
    return false;
  }
}
