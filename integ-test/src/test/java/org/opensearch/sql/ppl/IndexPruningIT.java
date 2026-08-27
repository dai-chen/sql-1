/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import java.io.IOException;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;
import org.opensearch.sql.common.setting.Settings.Key;
import org.opensearch.sql.legacy.SQLIntegTestCase;
import org.opensearch.sql.legacy.TestUtils;

/**
 * Index pruning narrows a wildcard source to only the indices whose shards can match a pushed-down
 * {@code @timestamp} range before a PIT opens.
 */
public class IndexPruningIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    enableCalcite();
    disallowCalciteFallback();
  }

  @Before
  public void createIndices() throws IOException {
    MonthlyIndex.JANUARY.create(
        """
        {"@timestamp":"2026-01-15T12:00:00Z","status":1,"body":"january"}\
        """);
    MonthlyIndex.FEBRUARY.create(
        """
        {"@timestamp":"2026-02-15T12:00:00Z","status":2,"body":"february"}\
        """);
    MonthlyIndex.MARCH.create(
        """
        {"@timestamp":"2026-03-15T12:00:00Z","status":3,"body":"march"}\
        """);
    MonthlyIndex.APRIL.create(
        """
        {"@timestamp":"2026-04-15T12:00:00Z","status":4,"body":"april"}\
        """);
    MonthlyIndex.MAY.create(
        """
        {"@timestamp":"2026-05-15T12:00:00Z","status":5,"body":"may"}\
        """);
    MonthlyIndex.JUNE.create(
        """
        {"@timestamp":"2026-06-15T12:00:00Z","status":6,"body":"june"}\
        """);
  }

  @After
  public void cleanUp() throws IOException {
    updateClusterSettings(
        new SQLIntegTestCase.ClusterSetting(
            "transient", Key.QUERY_PRUNING_ENABLED.getKeyValue(), null));
    for (MonthlyIndex month : MonthlyIndex.values()) {
      month.delete();
    }
  }

  @Test
  public void prunesToTheOnlyIndexWithinRange() throws IOException {
    withPruningEnabled(true)
        .run(
            """
            source=pruning-it-*
            | where @timestamp >= '2026-03-01 00:00:00' and @timestamp <= '2026-03-31 23:59:59'
            | head 5\
            """)
        .opensPitOn(MonthlyIndex.MARCH);
  }

  @Test
  public void prunesToBothIndicesWithinRange() throws IOException {
    withPruningEnabled(true)
        .run(
            """
            source=pruning-it-*
            | where @timestamp >= '2026-03-01 00:00:00' and @timestamp <= '2026-04-30 23:59:59'
            | head 5\
            """)
        .opensPitOn(MonthlyIndex.MARCH, MonthlyIndex.APRIL);
  }

  @Test
  public void prunesToEveryIndexAfterAnOpenEndedStart() throws IOException {
    withPruningEnabled(true)
        .run(
            """
            source=pruning-it-*
            | where @timestamp >= '2026-04-01 00:00:00'
            | head 5\
            """)
        .opensPitOn(MonthlyIndex.APRIL, MonthlyIndex.MAY, MonthlyIndex.JUNE);
  }

  @Test
  public void keepsEveryIndexWhenPruningDisabled() throws IOException {
    withPruningEnabled(false)
        .run(
            """
            source=pruning-it-*
            | where @timestamp >= '2026-03-01 00:00:00' and @timestamp <= '2026-03-31 23:59:59'
            | head 5\
            """)
        .opensPitOn(MonthlyIndex.values());
  }

  @Test
  public void keepsEveryIndexWhenFilterHasNoRange() throws IOException {
    withPruningEnabled(true)
        .run(
            """
            source=pruning-it-*
            | where status = 3
            | head 5\
            """)
        .opensPitOn(MonthlyIndex.values());
  }

  @Test
  public void keepsEveryIndexWhenRangeIsNotOnTimestamp() throws IOException {
    withPruningEnabled(true)
        .run(
            """
            source=pruning-it-*
            | where status >= 1 and status <= 100
            | head 5\
            """)
        .opensPitOn(MonthlyIndex.values());
  }

  @Test
  public void returnsBackfilledDocumentStoredOutsideItsMonth() throws IOException {
    MonthlyIndex.JUNE.insert(
        """
        {"@timestamp":"2026-03-20T12:00:00Z","status":99,"body":"backfill"}\
        """);

    assertEquals(
        """
        [["march"],["backfill"]]\
        """,
        withPruningEnabled(true)
            .run(
                """
                source=pruning-it-*
                | where @timestamp >= '2026-03-01 00:00:00' and @timestamp <= '2026-03-31 23:59:59'
                | sort @timestamp
                | fields body
                | head 5\
                """)
            .rows());
  }

  @Test
  public void returnsSameRowsWhetherPruningEnabledOrNot() throws IOException {
    String query =
        """
        source=pruning-it-*
        | where @timestamp >= '2026-03-01 00:00:00' and @timestamp <= '2026-03-31 23:59:59'
        | sort @timestamp
        | fields body
        | head 5\
        """;

    assertEquals(
        withPruningEnabled(false).run(query).rows(), withPruningEnabled(true).run(query).rows());
  }

  private Scenario withPruningEnabled(boolean enabled) throws IOException {
    updateClusterSettings(
        new SQLIntegTestCase.ClusterSetting(
            "transient", Key.QUERY_PRUNING_ENABLED.getKeyValue(), String.valueOf(enabled)));
    return new Scenario();
  }

  private final class Scenario {

    private String query;
    private String rows;
    private Set<String> opened;

    /**
     * Measures {@code point_in_time_total} deltas rather than {@code query_total}: {@code
     * can_match} already skips the query phase on non-matching shards regardless of pruning, so
     * {@code query_total} cannot distinguish the two, whereas PIT creation ignores {@code
     * can_match}. {@code point_in_time_current} is unusable because a non-paginated query
     * force-deletes its PIT before the response returns.
     */
    Scenario run(String pplQuery) throws IOException {
      Map<String, Long> before = pitTotals();
      // The JSON request body cannot carry raw newlines, so flatten the multi-line query first.
      JSONObject result = executeQuery(pplQuery.replace('\n', ' '));
      Map<String, Long> after = pitTotals();
      query = pplQuery;
      rows = result.getJSONArray("datarows").toString();
      opened =
          Arrays.stream(MonthlyIndex.values())
              .map(MonthlyIndex::index)
              .filter(index -> after.get(index) > before.get(index))
              .collect(Collectors.toCollection(LinkedHashSet::new));
      return this;
    }

    Scenario opensPitOn(MonthlyIndex... months) {
      Set<String> expected =
          Arrays.stream(months)
              .map(MonthlyIndex::index)
              .collect(Collectors.toCollection(LinkedHashSet::new));
      assertEquals(query, expected, opened);
      return this;
    }

    String rows() {
      return rows;
    }
  }

  private Map<String, Long> pitTotals() throws IOException {
    Response response =
        client()
            .performRequest(new Request("GET", "/" + MonthlyIndex.wildcard() + "/_stats/search"));
    JSONObject stats = new JSONObject(TestUtils.getResponseBody(response));
    return Arrays.stream(MonthlyIndex.values())
        .collect(
            Collectors.toMap(
                MonthlyIndex::index,
                month ->
                    ((Number)
                            stats.query(
                                "/indices/%s/total/search/point_in_time_total"
                                    .formatted(month.index())))
                        .longValue(),
                (a, b) -> a,
                LinkedHashMap::new));
  }

  @RequiredArgsConstructor
  private enum MonthlyIndex {
    JANUARY("2026.01.01"),
    FEBRUARY("2026.02.01"),
    MARCH("2026.03.01"),
    APRIL("2026.04.01"),
    MAY("2026.05.01"),
    JUNE("2026.06.01");

    private static final String PREFIX = "pruning-it-";

    // head 5 must exceed max_result_window to force the PIT path that pruning runs on.
    private static final int MAX_RESULT_WINDOW = 2;

    private final String suffix;

    static String wildcard() {
      return PREFIX + "*";
    }

    String index() {
      return PREFIX + suffix;
    }

    void create(String doc) throws IOException {
      delete();
      Request create = new Request("PUT", "/" + index());
      create.setJsonEntity(
          """
          {
            "settings": {
              "number_of_shards": 1,
              "number_of_replicas": 0,
              "max_result_window": %d
            },
            "mappings": {
              "properties": {
                "@timestamp": { "type": "date" },
                "status": { "type": "integer" },
                "body": { "type": "text" }
              }
            }
          }
          """
              .formatted(MAX_RESULT_WINDOW));
      client().performRequest(create);
      insert(doc);
    }

    void insert(String doc) throws IOException {
      Request bulk = new Request("POST", "/" + index() + "/_bulk");
      bulk.addParameter("refresh", "true");
      bulk.setJsonEntity(
          """
          {"index":{}}
          %s
          """
              .formatted(doc));
      client().performRequest(bulk);
    }

    void delete() throws IOException {
      try {
        client().performRequest(new Request("DELETE", "/" + index()));
      } catch (ResponseException e) {
        if (e.getResponse().getStatusLine().getStatusCode() != 404) {
          throw e;
        }
      }
    }
  }
}
