/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.request;

import java.io.IOException;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import lombok.extern.log4j.Log4j2;
import org.apache.lucene.search.TotalHits;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchScrollRequest;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory;
import org.opensearch.sql.opensearch.response.OpenSearchResponse;

/**
 * Reads a sequence of requests as if they were one, opening each only once the previous is
 * exhausted and releasing it immediately, so the resources one request holds are never held
 * alongside the next.
 *
 * <p>Pages are returned exactly {@code maxResultWindow} wide until every request is exhausted,
 * because a caller reads a narrower page as the end of the scan. A request whose own final page is
 * short is therefore topped up from the one after it, and the surplus is carried over.
 *
 * <p>Knows nothing about how a request reads: the sequence supplies them and each releases itself
 * through {@link OpenSearchRequest#clean}.
 */
@Log4j2
public class OpenSearchBatchRequest implements OpenSearchRequest {

  /**
   * Supplies the next request only when asked, so its resources are acquired as late as possible.
   */
  private final Iterator<OpenSearchRequest> requests;

  private final int maxResultWindow;
  private final OpenSearchExprValueFactory exprValueFactory;
  private final List<String> includes;

  /** Releases an exhausted request, which cannot wait for {@link #clean} without holding both. */
  private final Consumer<String> releaseAction;

  /** Hits beyond a full page, carried into the next call. Never more than one page. */
  private final Deque<SearchHit> surplus = new ArrayDeque<>();

  private OpenSearchRequest reading;
  private boolean done = false;

  public OpenSearchBatchRequest(
      Iterator<OpenSearchRequest> requests,
      int maxResultWindow,
      OpenSearchExprValueFactory exprValueFactory,
      List<String> includes,
      Consumer<String> releaseAction) {
    this.requests = requests;
    this.maxResultWindow = maxResultWindow;
    this.exprValueFactory = exprValueFactory;
    this.includes = includes;
    this.releaseAction = releaseAction;
  }

  @Override
  public OpenSearchResponse search(
      Function<SearchRequest, SearchResponse> searchAction,
      Function<SearchScrollRequest, SearchResponse> scrollAction) {
    List<SearchHit> page = new ArrayList<>(maxResultWindow);
    while (!surplus.isEmpty() && page.size() < maxResultWindow) {
      page.add(surplus.poll());
    }

    while (page.size() < maxResultWindow && advance()) {
      OpenSearchResponse response = reading.search(searchAction, scrollAction);
      for (SearchHit hit : response.getHits().getHits()) {
        (page.size() < maxResultWindow ? page : surplus).add(hit);
      }
      if (response.getHitsSize() < maxResultWindow) {
        release();
      }
    }
    return pageOf(page);
  }

  @Override
  public boolean hasAnotherBatch() {
    return !surplus.isEmpty()
        || (reading != null && reading.hasAnotherBatch())
        || (!done && requests.hasNext());
  }

  @Override
  public void clean(Consumer<String> cleanAction) {
    forceClean(cleanAction);
  }

  /**
   * Releases the request being read, which is the only one holding anything: the sequence opens a
   * request lazily, so those not yet reached have nothing to release. There is nothing to preserve
   * for a later page either, because a batched read is never resumed from a cursor.
   */
  @Override
  public void forceClean(Consumer<String> cleanAction) {
    done = true;
    surplus.clear();
    if (reading != null) {
      reading.forceClean(cleanAction);
      reading = null;
    }
  }

  @Override
  public OpenSearchExprValueFactory getExprValueFactory() {
    return exprValueFactory;
  }

  /**
   * Unsupported: a cursor carries one request, so resuming would read only part of the sequence.
   * Unreachable because only the v3 scan builds a batched request, and v3 has no pagination.
   */
  @Override
  public void writeTo(StreamOutput out) throws IOException {
    throw new UnsupportedOperationException("A batched request cannot be serialized into a cursor");
  }

  /** Opens the next request when the current one is spent. False once the sequence is spent. */
  private boolean advance() {
    if (reading == null && !done) {
      if (!requests.hasNext()) {
        done = true;
      } else {
        reading = requests.next();
      }
    }
    return reading != null;
  }

  /**
   * Releases before the next is opened, which is what keeps one request's resources held at a time.
   *
   * <p>Forced, because a request whose final page was short but not empty still considers itself
   * worth preserving, and nothing will ever read it again.
   */
  private void release() {
    reading.forceClean(releaseAction);
    reading = null;
  }

  private OpenSearchResponse pageOf(List<SearchHit> hits) {
    return new OpenSearchResponse(
        new SearchHits(
            hits.toArray(new SearchHit[0]),
            new TotalHits(hits.size(), TotalHits.Relation.GREATER_THAN_OR_EQUAL_TO),
            // Scores are per request, so there is no comparable maximum across the sequence. The
            // caller refuses a scored request, so nothing reads this.
            Float.NaN),
        exprValueFactory,
        includes,
        false);
  }
}
