/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.request;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.index.query.QueryBuilders.boolQuery;
import static org.opensearch.index.query.QueryBuilders.queryStringQuery;
import static org.opensearch.index.query.QueryBuilders.rangeQuery;

import java.util.Collections;
import java.util.Map;
import java.util.Optional;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.action.fieldcaps.FieldCapabilities;
import org.opensearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.request.OpenSearchRequest.IndexName;
import org.opensearch.transport.client.node.NodeClient;

@ExtendWith(MockitoExtension.class)
class IndexPrunerTest {

  @Mock private OpenSearchClient client;
  @Mock private NodeClient node;
  @Mock private ActionFuture<FieldCapabilitiesResponse> future;

  private FieldCapabilitiesResponse response(String... indices) {
    Map<String, Map<String, FieldCapabilities>> empty = Collections.emptyMap();
    return new FieldCapabilitiesResponse(indices, empty);
  }

  private IndexPruner pruner() {
    return new IndexPruner(client);
  }

  private void nodeAvailable() {
    when(client.getNodeClient()).thenReturn(Optional.of(node));
  }

  private void survivors(String... names) {
    when(node.fieldCaps(any())).thenReturn(future);
    when(future.actionGet()).thenReturn(response(names));
  }

  @Test
  void nullFilterReturnsOriginalWithoutProbe() {
    IndexName original = new IndexName("logs-*");
    assertSame(original, pruner().prune(original, null));
    verify(client, never()).getNodeClient();
  }

  @Test
  void noWildcardReturnsOriginalWithoutProbe() {
    IndexName original = new IndexName("logs-2024");
    assertSame(original, pruner().prune(original, rangeQuery("@timestamp").gte("now-1d")));
    verify(client, never()).getNodeClient();
  }

  @Test
  void clusterQualifiedReturnsOriginalWithoutProbe() {
    IndexName original = new IndexName("remote:logs-*");
    assertSame(original, pruner().prune(original, rangeQuery("@timestamp").gte("now-1d")));
    verify(client, never()).getNodeClient();
  }

  @Test
  void filterWithoutRangeReturnsOriginalWithoutProbe() {
    IndexName original = new IndexName("logs-*");
    assertSame(original, pruner().prune(original, queryStringQuery("error")));
    verify(client, never()).getNodeClient();
  }

  @Test
  void rangeOnlyUnderMustNotReturnsOriginalWithoutProbe() {
    IndexName original = new IndexName("logs-*");
    QueryBuilder filter = boolQuery().mustNot(rangeQuery("@timestamp").gte("now-1d"));
    assertSame(original, pruner().prune(original, filter));
    verify(client, never()).getNodeClient();
  }

  @Test
  void restModeReturnsOriginal() {
    when(client.getNodeClient()).thenReturn(Optional.empty());
    IndexName original = new IndexName("logs-*");
    assertSame(original, pruner().prune(original, rangeQuery("@timestamp").gte("now-1d")));
  }

  @Test
  void emptySurvivorsReturnsOriginal() {
    nodeAvailable();
    survivors();
    IndexName original = new IndexName("logs-*");
    assertSame(original, pruner().prune(original, rangeQuery("@timestamp").gte("now-1d")));
  }

  @Test
  void happyPathReturnsSurvivorInclusionList() {
    nodeAvailable();
    survivors("logs-e", "logs-f");
    IndexName result =
        pruner().prune(new IndexName("logs-*"), rangeQuery("@timestamp").gte("now-1d"));
    assertEquals(new IndexName("logs-e,logs-f"), result);
  }

  @Test
  void rangeNestedInBoolIsFound() {
    nodeAvailable();
    survivors("logs-b");
    QueryBuilder filter = boolQuery().filter(rangeQuery("@timestamp").gte("now-1d"));
    IndexName result = pruner().prune(new IndexName("logs-*"), filter);
    assertEquals(new IndexName("logs-b"), result);
  }

  @Test
  void fieldCapsThrowingReturnsOriginal() {
    nodeAvailable();
    when(node.fieldCaps(any())).thenReturn(future);
    when(future.actionGet()).thenThrow(new RuntimeException("boom"));
    IndexName original = new IndexName("logs-*");
    assertSame(original, pruner().prune(original, rangeQuery("@timestamp").gte("now-1d")));
  }
}
