/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.rest;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import org.apache.calcite.rel.RelNode;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.analytics.exec.QueryPlanExecutor;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.executor.QueryType;
import org.opensearch.transport.client.node.NodeClient;

/**
 * Tests for analytics index routing in RestUnifiedQueryAction. Uses context parser for AST-based
 * index name extraction.
 */
public class RestUnifiedQueryActionTest {

  private RestUnifiedQueryAction action;
  private Settings pluginSettings;

  @Before
  public void setUp() {
    pluginSettings = mock(Settings.class);
    when(pluginSettings.getSettingValue(Settings.Key.CALCITE_ANALYTICS_FORCE_ROUTING))
        .thenReturn(Boolean.FALSE);
    @SuppressWarnings("unchecked")
    QueryPlanExecutor<RelNode, Iterable<Object[]>> executor = mock(QueryPlanExecutor.class);
    action =
        new RestUnifiedQueryAction(
            mock(NodeClient.class), mock(ClusterService.class), executor, pluginSettings);
  }

  @Test
  public void parquetIndexRoutesToAnalytics() {
    assertTrue(action.isAnalyticsIndex("source = parquet_logs | fields ts", QueryType.PPL));
    assertTrue(
        action.isAnalyticsIndex("source = opensearch.parquet_logs | fields ts", QueryType.PPL));
  }

  @Test
  public void nonParquetIndexRoutesToLucene() {
    assertFalse(action.isAnalyticsIndex("source = my_logs | fields ts", QueryType.PPL));
    assertFalse(action.isAnalyticsIndex(null, QueryType.PPL));
    assertFalse(action.isAnalyticsIndex("", QueryType.PPL));
  }

  @Test
  public void forceRoutingOverridesPrefix() {
    when(pluginSettings.getSettingValue(Settings.Key.CALCITE_ANALYTICS_FORCE_ROUTING))
        .thenReturn(Boolean.TRUE);
    assertTrue(action.isAnalyticsIndex("source = my_logs | fields ts", QueryType.PPL));
    assertFalse(
        "force routing must still skip empty queries — they parse to nothing",
        action.isAnalyticsIndex("", QueryType.PPL));
  }
}
