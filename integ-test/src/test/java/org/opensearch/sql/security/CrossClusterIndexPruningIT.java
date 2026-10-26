/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.security;

import static org.opensearch.sql.legacy.TestUtils.getResponseBody;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.After;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.ResponseException;
import org.opensearch.sql.common.setting.Settings;

/** Verifies that remote data stream pruning passes concrete backing names to PIT creation. */
public class CrossClusterIndexPruningIT extends CrossClusterTestBase {

  private static final String STREAM_A = "ccs_pruning_ds-a";
  private static final String STREAM_B = "ccs_pruning_ds-b";
  private static final String TEMPLATE = "ccs_pruning_ds_template";
  private static final String FROM = "2026-03-15 12:00:00";
  private static final String TO = "2026-03-15 12:15:00";

  @Override
  protected void init() throws Exception {
    super.init();
    enableCalcite();
    updateClusterSettings(
        new ClusterSetting("persistent", Settings.Key.QUERY_PRUNING_ENABLED.getKeyValue(), "true"));
  }

  @After
  public void restoreDefaultPruningSetting() throws IOException {
    updateClusterSettings(
        new ClusterSetting("persistent", Settings.Key.QUERY_PRUNING_ENABLED.getKeyValue(), null));
  }

  @Test
  public void prunesRemoteDataStreamWildcardAndExactName() throws IOException {
    try {
      seedStreams();

      long before = pitTotal();
      verifyDataRows(
          executeWithBounds(query(REMOTE_CLUSTER + ":ccs_pruning_ds-*")),
          rows("march-a"),
          rows("march-b"));
      releasePits();
      assertEquals(before + 2, pitTotal());

      before = pitTotal();
      verifyDataRows(executeQuery(query(REMOTE_CLUSTER + ":" + STREAM_A)), rows("march-a"));
      releasePits();
      assertEquals(before + 1, pitTotal());
    } finally {
      deleteRemote("/_data_stream/" + STREAM_A + "," + STREAM_B);
      deleteRemote("/_index_template/" + TEMPLATE);
    }
  }

  @Test
  public void prunesRemoteStatsValuesWithRequestBounds() throws IOException {
    try {
      seedStreams();

      long before = pitTotal();
      String query =
          "source="
              + REMOTE_CLUSTER
              + ":ccs_pruning_ds-* | where @timestamp >= '"
              + FROM
              + "' and @timestamp <= '"
              + TO
              + "' | stats values(body) as bodies | eval n = array_length(bodies) | fields n";
      verifyDataRows(executeWithBounds(query), rows(2));
      releasePits();
      assertEquals(before + 2, pitTotal());
    } finally {
      deleteRemote("/_data_stream/" + STREAM_A + "," + STREAM_B);
      deleteRemote("/_index_template/" + TEMPLATE);
    }
  }

  private static String query(String source) {
    return "source="
        + source
        + " | where @timestamp >= '"
        + FROM
        + "' and @timestamp <= '"
        + TO
        + "' | sort body | fields body | head 5";
  }

  private JSONObject executeWithBounds(String query) throws IOException {
    Request request = new Request("POST", "/_plugins/_ppl");
    request.setJsonEntity(
        new JSONObject()
            .put("query", query)
            .put("time_field", "@timestamp")
            .put("start_time", FROM)
            .put("end_time", TO)
            .toString());
    return new JSONObject(getResponseBody(client().performRequest(request), true));
  }

  private void seedStreams() throws IOException {
    putRemote(
        "/_index_template/" + TEMPLATE,
        """
        {
          "index_patterns": ["ccs_pruning_ds-*"],
          "priority": 500,
          "data_stream": {},
          "template": {
            "settings": {
              "number_of_shards": 1,
              "number_of_replicas": 0,
              "max_result_window": 2
            },
            "mappings": {
              "properties": {
                "@timestamp": {"type": "date"},
                "body": {"type": "keyword"}
              }
            }
          }
        }
        """);
    putRemote("/_data_stream/" + STREAM_A, null);
    putRemote("/_data_stream/" + STREAM_B, null);
    createRemoteDoc(STREAM_A, "2026-01-15T12:05:00Z", "january-a");
    postRemote("/" + STREAM_A + "/_rollover");
    createRemoteDoc(STREAM_A, "2026-02-15T12:05:00Z", "february-a");
    createRemoteDoc(STREAM_B, "2026-02-15T12:05:00Z", "february-b");
    postRemote("/" + STREAM_A + "/_rollover");
    postRemote("/" + STREAM_B + "/_rollover");
    createRemoteDoc(STREAM_A, "2026-03-15T12:05:00Z", "march-a");
    createRemoteDoc(STREAM_B, "2026-03-15T12:05:00Z", "march-b");
  }

  private void createRemoteDoc(String stream, String timestamp, String body) throws IOException {
    Request request = new Request("POST", "/" + stream + "/_bulk?refresh=true");
    request.setJsonEntity(
        "{\"create\":{}}\n"
            + new JSONObject().put("@timestamp", timestamp).put("body", body)
            + "\n");
    remoteClient().performRequest(request);
  }

  private long pitTotal() throws IOException {
    JSONObject response =
        new JSONObject(
            getResponseBody(
                remoteClient()
                    .performRequest(new Request("GET", "/ccs_pruning_ds-*/_stats/search")),
                true));
    return response
        .getJSONObject("_all")
        .getJSONObject("total")
        .getJSONObject("search")
        .getLong("point_in_time_total");
  }

  private void releasePits() throws IOException {
    client().performRequest(new Request("DELETE", "/_search/point_in_time/_all"));
    remoteClient().performRequest(new Request("DELETE", "/_search/point_in_time/_all"));
  }

  private void putRemote(String path, String body) throws IOException {
    Request request = new Request("PUT", path);
    if (body != null) {
      request.setJsonEntity(body);
    }
    remoteClient().performRequest(request);
  }

  private void postRemote(String path) throws IOException {
    remoteClient().performRequest(new Request("POST", path));
  }

  private void deleteRemote(String path) throws IOException {
    try {
      remoteClient().performRequest(new Request("DELETE", path));
    } catch (ResponseException e) {
      if (e.getResponse().getStatusLine().getStatusCode() != 404) {
        throw e;
      }
    }
  }
}
