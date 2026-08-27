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
import org.opensearch.action.fieldcaps.FieldCapabilitiesRequest;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.common.regex.Regex;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.index.query.BoolQueryBuilder;
import org.opensearch.index.query.ConstantScoreQueryBuilder;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.index.query.RangeQueryBuilder;
import org.opensearch.sql.calcite.plan.OpenSearchConstants;
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

  private final OpenSearchClient client;

  // No cancellation or circuit breaker on _field_caps, so bound it to avoid hanging the thread.
  private static final TimeValue PROBE_TIMEOUT = TimeValue.timeValueSeconds(3);

  private static final int MAX_PRUNED_INDICES = 50;

  public IndexName prune(IndexName indexName, QueryBuilder filter) {
    try {
      return isPrunable(indexName, filter) ? doPrune(indexName, filter) : indexName;
    } catch (Exception e) {
      log.warn("Index pruning failed; querying the full index expression", e);
      return indexName;
    }
  }

  private boolean isPrunable(IndexName indexName, QueryBuilder filter) {
    if (filter == null) {
      return false;
    }
    if (!hasWildcard(indexName)) {
      return false;
    }
    if (hasClusterQualifier(indexName)) {
      return false;
    }
    // can_match proves disjointness only for date ranges, so any other filter cannot prune.
    return containsTimestampRange(filter);
  }

  private IndexName doPrune(IndexName indexName, QueryBuilder filter) {
    Optional<NodeClient> node = client.getNodeClient();
    if (node.isEmpty()) {
      return indexName;
    }
    // Only the response's index list matters, not its field data, so one field keeps merges cheap.
    FieldCapabilitiesRequest request =
        new FieldCapabilitiesRequest()
            .indices(indexName.getIndexNames())
            .fields(OpenSearchConstants.IMPLICIT_FIELD_TIMESTAMP);
    request.indexFilter(filter);
    // Probe must expand indices like the search, else survivors reflect a different set.
    request.indicesOptions(SearchRequest.DEFAULT_INDICES_OPTIONS);
    List<String> survivors =
        List.of(node.get().fieldCaps(request).actionGet(PROBE_TIMEOUT).getIndices());
    // An empty list would mean "all indices" to OpenSearch, so leave the expression alone.
    // A list past the cap is heavier and more brittle than the wildcard, so keep the wildcard.
    if (survivors.isEmpty() || survivors.size() > MAX_PRUNED_INDICES) {
      return indexName;
    }
    log.info("Index pruning narrowed {} to {} indices", indexName, survivors.size());
    return new IndexName(String.join(",", survivors));
  }

  private boolean hasWildcard(IndexName indexName) {
    return Arrays.stream(indexName.getIndexNames()).anyMatch(Regex::isSimpleMatchPattern);
  }

  private boolean hasClusterQualifier(IndexName indexName) {
    return Arrays.stream(indexName.getIndexNames())
        .anyMatch(name -> name.indexOf(RemoteClusterAware.REMOTE_CLUSTER_INDEX_SEPARATOR) >= 0);
  }

  private boolean containsTimestampRange(QueryBuilder query) {
    if (query instanceof RangeQueryBuilder range) {
      return OpenSearchConstants.IMPLICIT_FIELD_TIMESTAMP.equals(range.fieldName());
    }
    if (query instanceof BoolQueryBuilder bool) {
      return Stream.of(bool.must(), bool.filter(), bool.should())
          .flatMap(List::stream)
          .anyMatch(this::containsTimestampRange);
    }
    if (query instanceof ConstantScoreQueryBuilder constantScore) {
      return containsTimestampRange(constantScore.innerQuery());
    }
    return false;
  }
}
