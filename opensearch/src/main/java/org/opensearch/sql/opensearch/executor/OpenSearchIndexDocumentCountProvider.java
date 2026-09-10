/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor;

import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.opensearch.action.admin.indices.stats.CommonStats;
import org.opensearch.action.admin.indices.stats.IndexStats;
import org.opensearch.action.admin.indices.stats.IndicesStatsRequest;
import org.opensearch.action.admin.indices.stats.IndicesStatsResponse;
import org.opensearch.core.action.ActionListener;
import org.opensearch.index.shard.DocsStats;
import org.opensearch.transport.client.Client;

/**
 * Fetches total primary live-document counts for the indices referenced by a routing assessment.
 */
public final class OpenSearchIndexDocumentCountProvider {

  private static final Logger LOG =
      LogManager.getLogger(OpenSearchIndexDocumentCountProvider.class);

  public record Result(long primaryDocs, boolean complete) {
    public static Result unknown() {
      return new Result(0, false);
    }
  }

  private final Client client;

  public OpenSearchIndexDocumentCountProvider(Client client) {
    this.client = client;
  }

  public void fetch(Set<String> indices, ActionListener<Result> listener) {
    if (indices.isEmpty()) {
      listener.onResponse(Result.unknown());
      return;
    }

    IndicesStatsRequest request = new IndicesStatsRequest();
    request.indices(indices.toArray(String[]::new));
    request.clear();
    request.docs(true);
    client
        .admin()
        .indices()
        .stats(
            request,
            ActionListener.wrap(
                response -> listener.onResponse(toResult(response)),
                e -> {
                  LOG.warn(
                      "Unable to fetch primary document counts for {}; keeping legacy route",
                      indices,
                      e);
                  listener.onResponse(Result.unknown());
                }));
  }

  private static Result toResult(IndicesStatsResponse response) {
    long total = 0;
    int countedIndices = 0;
    for (IndexStats stats : response.getIndices().values()) {
      CommonStats primaries = stats.getPrimaries();
      DocsStats docs = primaries == null ? null : primaries.getDocs();
      if (docs != null) {
        total = saturatingAdd(total, docs.getCount());
        countedIndices++;
      }
    }
    return new Result(total, response.getFailedShards() == 0 && countedIndices > 0);
  }

  private static long saturatingAdd(long left, long right) {
    if (right > 0 && left > Long.MAX_VALUE - right) {
      return Long.MAX_VALUE;
    }
    return left + right;
  }
}
