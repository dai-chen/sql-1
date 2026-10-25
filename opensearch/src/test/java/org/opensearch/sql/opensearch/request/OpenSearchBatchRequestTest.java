/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.request;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Deque;
import java.util.Iterator;
import java.util.List;
import java.util.function.Consumer;
import java.util.function.Function;
import org.apache.lucene.search.TotalHits;
import org.junit.jupiter.api.Test;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.action.search.SearchResponse;
import org.opensearch.action.search.SearchScrollRequest;
import org.opensearch.core.common.bytes.BytesArray;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.sql.opensearch.response.OpenSearchResponse;

/**
 * Drives the batched read with fake requests, so the page assembly and the release ordering are
 * covered without a cluster or a PIT.
 */
class OpenSearchBatchRequestTest {

  private static final int PAGE = 3;

  private final List<String> released = new ArrayList<>();

  @Test
  void shouldReadOneRequestThroughUnchanged() {
    OpenSearchBatchRequest request = batchOf(request("r1", 3, 2));

    assertEquals(3, hitCount(request));
    assertEquals(2, hitCount(request));
    assertEquals(0, hitCount(request));
    assertEquals(List.of("r1"), released);
  }

  @Test
  void shouldTopUpAShortPageFromTheNextRequest() {
    // r1's last page is 1 wide. Returning it as is would read as the end of the whole scan.
    OpenSearchBatchRequest request = batchOf(request("r1", 1), request("r2", 3, 3));

    assertEquals(PAGE, hitCount(request));
    assertEquals(PAGE, hitCount(request));
    assertEquals(1, hitCount(request));
  }

  @Test
  void shouldCarrySurplusHitsIntoTheNextPage() {
    OpenSearchBatchRequest request = batchOf(request("r1", 2), request("r2", 3, 1));

    // 2 from r1 plus 3 from r2 is 5 hits: 3 now, and 2 carried over ahead of r2's last page.
    assertEquals(PAGE, hitCount(request));
    assertEquals(PAGE, hitCount(request));
    assertEquals(0, hitCount(request));
  }

  @Test
  void shouldReleaseEachRequestBeforeOpeningTheNext() {
    OpenSearchBatchRequest request = batchOf(request("r1", 1), request("r2", 1), request("r3", 1));

    while (hitCount(request) > 0) {
      // drain
    }
    assertEquals(List.of("r1", "r2", "r3"), released);
  }

  @Test
  void shouldReportAnotherBatchUntilEveryRequestIsRead() {
    OpenSearchBatchRequest request = batchOf(request("r1", 1), request("r2", 1));

    assertTrue(request.hasAnotherBatch());
    hitCount(request);
    assertFalse(request.hasAnotherBatch());
  }

  @Test
  void shouldReleaseOnlyTheRequestBeingReadOnForceClean() {
    // The rest were never opened, so they hold nothing to release.
    OpenSearchBatchRequest request =
        batchOf(request("r1", 3, 3), request("r2", 3), request("r3", 3));

    hitCount(request);
    request.forceClean(released::add);

    assertEquals(List.of("r1"), released);
  }

  @Test
  void shouldForceReleaseARequestWhoseLastPageWasShortButNotEmpty() {
    // Such a request still considers itself worth preserving, so a plain clean would leave it open.
    OpenSearchBatchRequest request = batchOf(request("r1", 1), request("r2", 1));

    hitCount(request);

    assertEquals(List.of("r1", "r2"), released);
  }

  @Test
  void shouldStopReadingOnceCleaned() {
    OpenSearchBatchRequest request = batchOf(request("r1", 3, 3), request("r2", 3));

    request.forceClean(released::add);

    assertEquals(0, hitCount(request));
  }

  @Test
  void shouldRefuseToSerializeIntoACursor() {
    OpenSearchBatchRequest request = batchOf(request("r1", 1));

    assertThrows(UnsupportedOperationException.class, () -> request.writeTo((StreamOutput) null));
  }

  private OpenSearchBatchRequest batchOf(OpenSearchRequest... requests) {
    Iterator<OpenSearchRequest> sequence = Arrays.asList(requests).iterator();
    return new OpenSearchBatchRequest(sequence, PAGE, null, List.of(), released::add);
  }

  private int hitCount(OpenSearchBatchRequest request) {
    return request.search(searchAction(), scrollAction()).getHitsSize();
  }

  private static Function<SearchRequest, SearchResponse> searchAction() {
    return searchRequest -> null;
  }

  private static Function<SearchScrollRequest, SearchResponse> scrollAction() {
    return scrollRequest -> null;
  }

  /** A request that hands out the given page sizes in turn, then empty pages. */
  private static OpenSearchRequest request(String id, int... pageSizes) {
    Deque<Integer> pages = new ArrayDeque<>();
    Arrays.stream(pageSizes).forEach(pages::add);
    return new FakeRequest(id, pages);
  }

  /** Mirrors OpenSearchQueryRequest, including that a plain clean preserves a non-empty request. */
  private static final class FakeRequest implements OpenSearchRequest {

    private final String id;
    private final Deque<Integer> pages;
    private boolean releasable = true;

    private FakeRequest(String id, Deque<Integer> pages) {
      this.id = id;
      this.pages = pages;
    }

    @Override
    public OpenSearchResponse search(
        Function<SearchRequest, SearchResponse> searchAction,
        Function<SearchScrollRequest, SearchResponse> scrollAction) {
      int size = pages.isEmpty() ? 0 : pages.poll();
      releasable = size == 0;
      SearchHit[] hits = new SearchHit[size];
      for (int i = 0; i < size; i++) {
        hits[i] = new SearchHit(i, id + "-" + i, null, null);
        hits[i].sourceRef(new BytesArray("{}"));
      }
      return new OpenSearchResponse(
          new SearchHits(hits, new TotalHits(size, TotalHits.Relation.EQUAL_TO), Float.NaN),
          null,
          List.of(),
          false);
    }

    @Override
    public void clean(Consumer<String> cleanAction) {
      if (releasable) {
        cleanAction.accept(id);
      }
    }

    @Override
    public void forceClean(Consumer<String> cleanAction) {
      cleanAction.accept(id);
    }

    @Override
    public org.opensearch.sql.opensearch.data.value.OpenSearchExprValueFactory
        getExprValueFactory() {
      return null;
    }

    @Override
    public boolean hasAnotherBatch() {
      return !pages.isEmpty();
    }

    @Override
    public void writeTo(StreamOutput out) {
      throw new UnsupportedOperationException();
    }
  }
}
