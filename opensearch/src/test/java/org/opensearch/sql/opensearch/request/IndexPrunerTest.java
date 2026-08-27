/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.request;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;
import static org.opensearch.index.query.QueryBuilders.boolQuery;
import static org.opensearch.index.query.QueryBuilders.queryStringQuery;
import static org.opensearch.index.query.QueryBuilders.rangeQuery;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opensearch.action.admin.indices.resolve.ResolveIndexAction;
import org.opensearch.action.fieldcaps.FieldCapabilities;
import org.opensearch.action.fieldcaps.FieldCapabilitiesResponse;
import org.opensearch.common.action.ActionFuture;
import org.opensearch.index.query.QueryBuilder;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.opensearch.client.OpenSearchClient;
import org.opensearch.sql.opensearch.request.OpenSearchRequest.IndexName;
import org.opensearch.transport.client.node.NodeClient;

@ExtendWith(MockitoExtension.class)
class IndexPrunerTest {

  @Mock private OpenSearchClient client;
  @Mock private Settings settings;
  @Mock private NodeClient node;
  @Mock private ActionFuture<FieldCapabilitiesResponse> future;
  @Mock private ActionFuture<ResolveIndexAction.Response> resolveFuture;

  private FieldCapabilitiesResponse response(String... indices) {
    Map<String, Map<String, FieldCapabilities>> empty = Collections.emptyMap();
    return new FieldCapabilitiesResponse(indices, empty);
  }

  private ResolveIndexAction.Response resolved(String... names) {
    List<ResolveIndexAction.ResolvedIndex> indices =
        Arrays.stream(names)
            .map(
                name -> {
                  ResolveIndexAction.ResolvedIndex index =
                      mock(ResolveIndexAction.ResolvedIndex.class);
                  when(index.getName()).thenReturn(name);
                  return index;
                })
            .collect(Collectors.toList());
    return new ResolveIndexAction.Response(indices, List.of(), List.of());
  }

  private IndexPruner pruner() {
    return new IndexPruner(client, settings);
  }

  private void enable() {
    when(settings.getSettingValue(Settings.Key.QUERY_PRUNING_ENABLED)).thenReturn(true);
  }

  private void nodeAvailable() {
    when(client.getNodeClient()).thenReturn(Optional.of(node));
  }

  private void matched(String... names) {
    ResolveIndexAction.Response resolved = resolved(names);
    when(node.execute(eq(ResolveIndexAction.INSTANCE), any())).thenReturn(resolveFuture);
    when(resolveFuture.actionGet()).thenReturn(resolved);
  }

  private void survivors(String... names) {
    when(node.fieldCaps(any())).thenReturn(future);
    when(future.actionGet()).thenReturn(response(names));
  }

  @Test
  void settingDisabledReturnsOriginalWithoutProbe() {
    when(settings.getSettingValue(Settings.Key.QUERY_PRUNING_ENABLED)).thenReturn(false);
    IndexName original = new IndexName("logs-*");
    assertSame(original, pruner().prune(original, rangeQuery("@timestamp").gte("now-1d")));
    verify(client, never()).getNodeClient();
  }

  @Test
  void nullFilterReturnsOriginalWithoutProbe() {
    enable();
    IndexName original = new IndexName("logs-*");
    assertSame(original, pruner().prune(original, null));
    verify(client, never()).getNodeClient();
  }

  @Test
  void noWildcardReturnsOriginalWithoutProbe() {
    enable();
    IndexName original = new IndexName("logs-2024");
    assertSame(original, pruner().prune(original, rangeQuery("@timestamp").gte("now-1d")));
    verify(client, never()).getNodeClient();
  }

  @Test
  void clusterQualifiedReturnsOriginalWithoutProbe() {
    enable();
    IndexName original = new IndexName("remote:logs-*");
    assertSame(original, pruner().prune(original, rangeQuery("@timestamp").gte("now-1d")));
    verify(client, never()).getNodeClient();
  }

  @Test
  void filterWithoutRangeReturnsOriginalWithoutProbe() {
    enable();
    IndexName original = new IndexName("logs-*");
    assertSame(original, pruner().prune(original, queryStringQuery("error")));
    verify(client, never()).getNodeClient();
  }

  @Test
  void rangeOnlyUnderMustNotReturnsOriginalWithoutProbe() {
    enable();
    IndexName original = new IndexName("logs-*");
    QueryBuilder filter = boolQuery().mustNot(rangeQuery("@timestamp").gte("now-1d"));
    assertSame(original, pruner().prune(original, filter));
    verify(client, never()).getNodeClient();
  }

  @Test
  void restModeReturnsOriginal() {
    enable();
    when(client.getNodeClient()).thenReturn(Optional.empty());
    IndexName original = new IndexName("logs-*");
    assertSame(original, pruner().prune(original, rangeQuery("@timestamp").gte("now-1d")));
  }

  @Test
  void singleMatchedReturnsOriginal() {
    enable();
    nodeAvailable();
    matched("logs-a");
    IndexName original = new IndexName("logs-*");
    assertSame(original, pruner().prune(original, rangeQuery("@timestamp").gte("now-1d")));
  }

  @Test
  void emptySurvivorsReturnsOriginal() {
    enable();
    nodeAvailable();
    matched("logs-a", "logs-b");
    survivors();
    IndexName original = new IndexName("logs-*");
    assertSame(original, pruner().prune(original, rangeQuery("@timestamp").gte("now-1d")));
  }

  @Test
  void survivorsEqualMatchedReturnsOriginal() {
    enable();
    nodeAvailable();
    matched("logs-a", "logs-b");
    survivors("logs-a", "logs-b");
    IndexName original = new IndexName("logs-*");
    assertSame(original, pruner().prune(original, rangeQuery("@timestamp").gte("now-1d")));
  }

  @Test
  void happyPathReturnsSurvivorInclusionList() {
    enable();
    nodeAvailable();
    matched("logs-a", "logs-b", "logs-c", "logs-d", "logs-e", "logs-f");
    survivors("logs-e", "logs-f");
    IndexName result =
        pruner().prune(new IndexName("logs-*"), rangeQuery("@timestamp").gte("now-1d"));
    assertEquals(new IndexName("logs-e,logs-f"), result);
  }

  @Test
  void rangeNestedInBoolIsFound() {
    enable();
    nodeAvailable();
    matched("logs-a", "logs-b");
    survivors("logs-b");
    QueryBuilder filter = boolQuery().filter(rangeQuery("@timestamp").gte("now-1d"));
    IndexName result = pruner().prune(new IndexName("logs-*"), filter);
    assertEquals(new IndexName("logs-b"), result);
  }

  @Test
  void fieldCapsThrowingReturnsOriginal() {
    enable();
    nodeAvailable();
    matched("logs-a", "logs-b");
    when(node.fieldCaps(any())).thenReturn(future);
    when(future.actionGet()).thenThrow(new RuntimeException("boom"));
    IndexName original = new IndexName("logs-*");
    assertSame(original, pruner().prune(original, rangeQuery("@timestamp").gte("now-1d")));
  }
}
