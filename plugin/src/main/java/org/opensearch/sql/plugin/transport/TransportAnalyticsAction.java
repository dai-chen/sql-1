/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.transport;

import org.opensearch.action.ActionType;
import org.opensearch.action.support.ActionFilters;
import org.opensearch.action.support.HandledTransportAction;
import org.opensearch.common.inject.Inject;
import org.opensearch.core.action.ActionListener;
import org.opensearch.sql.protocol.response.ParquetStubResponse;
import org.opensearch.sql.protocol.response.QueryResult;
import org.opensearch.tasks.Task;
import org.opensearch.transport.TransportService;

public class TransportAnalyticsAction
    extends HandledTransportAction<AnalyticsActionRequest, AnalyticsActionResponse> {

  public static final String NAME = "cluster:admin/opensearch/analytics/execute";
  public static final ActionType<AnalyticsActionResponse> ACTION_TYPE =
      new ActionType<>(NAME, AnalyticsActionResponse::new);

  @Inject
  public TransportAnalyticsAction(TransportService transportService, ActionFilters actionFilters) {
    super(NAME, transportService, actionFilters, AnalyticsActionRequest::new);
  }

  @Override
  protected void doExecute(
      Task task, AnalyticsActionRequest request, ActionListener<AnalyticsActionResponse> listener) {
    try {
      QueryResult stub = ParquetStubResponse.buildStubQueryResult();
      String result =
          "PPL".equals(request.getQueryType())
              ? ParquetStubResponse.formatAsSimpleJson(stub)
              : ParquetStubResponse.formatAsJdbc(stub);
      listener.onResponse(new AnalyticsActionResponse(result));
    } catch (Exception e) {
      listener.onFailure(e);
    }
  }
}
