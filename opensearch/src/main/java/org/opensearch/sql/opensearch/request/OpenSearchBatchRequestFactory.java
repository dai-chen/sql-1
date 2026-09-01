/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.request;

import java.io.IOException;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Optional;
import java.util.function.Consumer;
import lombok.RequiredArgsConstructor;
import org.opensearch.common.xcontent.LoggingDeprecationHandler;
import org.opensearch.common.xcontent.XContentType;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.search.SearchModule;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory;
import org.opensearch.sql.opensearch.request.OpenSearchRequest.IndexName;
import org.opensearch.transport.client.node.NodeClient;

/**
 * Builds a request that reads a wide index expression group by group, so that the resources one
 * group holds are released before the next acquires its own.
 *
 * <p>How a group is read is the caller's business: it passes a {@link ReadOne} and the release it
 * pairs with, so nothing here depends on what a read costs or how it is undone.
 */
@RequiredArgsConstructor
public class OpenSearchBatchRequestFactory {

  /** Parses a source builder back from its own JSON, so each group can be given its own copy. */
  private static final NamedXContentRegistry SOURCE_REGISTRY =
      new NamedXContentRegistry(
          new SearchModule(org.opensearch.common.settings.Settings.EMPTY, Collections.emptyList())
              .getNamedXContents());

  /** Builds the request that reads one index expression with one search source. */
  @FunctionalInterface
  public interface ReadOne {
    OpenSearchRequest apply(IndexName indexName, SearchSourceBuilder source);
  }

  private final Settings settings;
  private final int maxResultWindow;
  private final OpenSearchExprValueFactory exprValueFactory;

  /**
   * Returns a request reading the expression group by group, or empty when it should be read as
   * one: because grouping is off, would change the answer, cannot be planned, or would not narrow
   * anything.
   *
   * @param node issues the probes that resolve the expression; absent for the REST client
   * @param readOne reads one group
   * @param release releases a group once it is exhausted
   */
  public Optional<OpenSearchRequest> create(
      IndexName indexName,
      SearchSourceBuilder sourceBuilder,
      List<String> includes,
      Optional<NodeClient> node,
      ReadOne readOne,
      Consumer<String> release) {
    int shardBudget = settings.getSettingValue(Settings.Key.QUERY_BATCHING_MAX_SHARDS_PER_BATCH);
    if (shardBudget <= 0 || node.isEmpty() || !isBatchable(sourceBuilder)) {
      return Optional.empty();
    }

    List<IndexName> groups = new IndexSplitter(node.get()).split(indexName, shardBudget);
    if (groups.size() < 2) {
      return Optional.empty();
    }
    // Mapped lazily, so a group is read only once the group before it is exhausted.
    Iterator<OpenSearchRequest> requests =
        groups.stream().map(name -> readOne.apply(name, copyOf(sourceBuilder))).iterator();
    return Optional.of(
        new OpenSearchBatchRequest(requests, maxResultWindow, exprValueFactory, includes, release));
  }

  /**
   * A pushed sort orders each group's rows only within that group, so reading the groups in turn
   * would emit them out of order. The rest carry state that means nothing across separate reads.
   */
  private static boolean isBatchable(SearchSourceBuilder sourceBuilder) {
    return sourceBuilder.from() <= 0
        && (sourceBuilder.sorts() == null || sourceBuilder.sorts().isEmpty())
        && sourceBuilder.collapse() == null
        && sourceBuilder.searchAfter() == null
        && !sourceBuilder.trackScores();
  }

  /**
   * A detached copy, because reading a group appends its own tiebreaker sorts and cursor position
   * to the source builder it is given, and {@code shallowCopy} shares both with the original.
   */
  private static SearchSourceBuilder copyOf(SearchSourceBuilder source) {
    try (XContentParser parser =
        XContentType.JSON
            .xContent()
            .createParser(SOURCE_REGISTRY, LoggingDeprecationHandler.INSTANCE, source.toString())) {
      return SearchSourceBuilder.fromXContent(parser);
    } catch (IOException e) {
      throw new IllegalStateException("Failed to copy the search source for a group", e);
    }
  }
}
