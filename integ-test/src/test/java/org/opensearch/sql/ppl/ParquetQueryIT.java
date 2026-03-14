/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.legacy.TestUtils.getResponseBody;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import java.util.Locale;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;
import org.opensearch.client.ResponseException;

public class ParquetQueryIT extends PPLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    loadIndex(Index.BANK);
  }

  @Test
  public void testPplQueryOnParquetIndex() throws IOException {
    JSONObject response =
        executeQuery("source = parquet_index | fields timestamp, status, message, active");
    assertTrue(response.has("schema"));
    assertTrue(response.has("datarows"));
    assertEquals(2, response.getInt("total"));
    assertEquals(2, response.getInt("size"));
  }

  @Test
  public void testPplFilterProjectAbsorbed() throws IOException {
    JSONObject response =
        executeQuery(
            "source = parquet_index | where status = 200 | fields timestamp, status, message");
    assertTrue(response.has("schema"));
    assertTrue(response.has("datarows"));
    assertEquals(2, response.getInt("total"));
  }

  @Test
  public void testPplAggregateAbsorbed() throws IOException {
    JSONObject response = executeQuery("source = parquet_index | stats count() by status");
    assertTrue(response.has("schema"));
    assertTrue(response.has("datarows"));
    assertEquals(2, response.getInt("total"));
  }

  @Test
  public void testPplSortLimitAbsorbed() throws IOException {
    JSONObject response = executeQuery("source = parquet_index | sort timestamp | head 100");
    assertTrue(response.has("schema"));
    assertTrue(response.has("datarows"));
    assertEquals(2, response.getInt("total"));
  }

  @Test
  public void testSqlQueryOnParquetIndex() throws IOException {
    JSONObject response = executeSqlQuery("SELECT * FROM parquet_index");
    assertTrue(response.has("schema"));
    assertTrue(response.has("datarows"));
    assertEquals(200, response.getInt("status"));
    assertEquals(2, response.getInt("total"));
  }

  @Test
  public void testPplExplainOnParquetIndex() throws IOException {
    JSONObject response =
        executeExplainRequest("source = parquet_index", "/_plugins/_ppl/_explain");
    assertTrue(response.has("Parquet"));
    assertTrue(response.getJSONObject("Parquet").has("description"));
  }

  @Test
  public void testSqlExplainOnParquetIndex() throws IOException {
    JSONObject response =
        executeExplainRequest("SELECT * FROM parquet_index", "/_plugins/_sql/_explain");
    assertTrue(response.has("Parquet"));
    assertTrue(response.getJSONObject("Parquet").has("description"));
  }

  @Test
  public void testSqlFilterProjectOnParquetIndex() throws IOException {
    JSONObject response =
        executeSqlQuery("SELECT `timestamp`, status FROM parquet_index WHERE status = 200");
    assertTrue(response.has("schema"));
    assertTrue(response.has("datarows"));
    assertEquals(200, response.getInt("status"));
    assertEquals(2, response.getInt("total"));
  }

  @Test
  public void testSqlAggregateOnParquetIndex() throws IOException {
    JSONObject response = executeSqlQuery("SELECT count(*) FROM parquet_index GROUP BY status");
    assertTrue(response.has("schema"));
    assertTrue(response.has("datarows"));
    assertEquals(200, response.getInt("status"));
  }

  @Test
  public void testPplMatchOnParquetIndexFailsFast() throws IOException {
    ResponseException ex =
        assertThrows(
            ResponseException.class,
            () -> executeQuery("source = parquet_index | where match(message, 'error')"));
    assertEquals(400, ex.getResponse().getStatusLine().getStatusCode());
    String body = getResponseBody(ex.getResponse(), true);
    assertTrue(body.contains("not supported"));
  }

  @Test
  public void testSqlMatchOnParquetIndexFailsFast() throws IOException {
    ResponseException ex =
        assertThrows(
            ResponseException.class,
            () -> executeSqlQuery("SELECT * FROM parquet_index WHERE match(message, 'error')"));
    assertEquals(400, ex.getResponse().getStatusLine().getStatusCode());
    String body = getResponseBody(ex.getResponse(), true);
    assertTrue(body.contains("not supported"));
  }

  @Test
  public void testPplHourFunctionOnParquetIndex() throws IOException {
    JSONObject response =
        executeQuery(
            "source = parquet_index | eval hour = hour(timestamp) | where timestamp >"
                + " '2024-01-01'");
    assertTrue(response.has("schema"));
    assertTrue(response.has("datarows"));
    assertEquals(2, response.getInt("total"));
  }

  @Test
  public void testPplSpanOnParquetIndex() throws IOException {
    JSONObject response =
        executeQuery("source = parquet_index | stats count() by span(timestamp, 1h)");
    assertTrue(response.has("schema"));
    assertTrue(response.has("datarows"));
    assertEquals(2, response.getInt("total"));
  }

  @Test
  public void testPplDatetimeComparisonWithSpanOnParquetIndex() throws IOException {
    JSONObject response =
        executeQuery(
            "source = parquet_index | where timestamp > '2024-01-01' | stats count() by"
                + " span(timestamp, 1h)");
    assertTrue(response.has("schema"));
    assertTrue(response.has("datarows"));
    assertEquals(2, response.getInt("total"));
  }

  @Test
  public void testLuceneQueryStillWorks() throws IOException {
    JSONObject response =
        executeQuery("source = opensearch-sql_test_index_bank | fields firstname | head 1");
    verifySchema(response, schema("firstname", null, "string"));
  }

  private JSONObject executeSqlQuery(String query) throws IOException {
    Request request = new Request("POST", "/_plugins/_sql");
    request.setJsonEntity(String.format(Locale.ROOT, "{\"query\": \"%s\"}", query));
    RequestOptions.Builder restOptionsBuilder = RequestOptions.DEFAULT.toBuilder();
    restOptionsBuilder.addHeader("Content-Type", "application/json");
    request.setOptions(restOptionsBuilder);
    return new JSONObject(executeRequest(request));
  }

  private JSONObject executeExplainRequest(String query, String endpoint) throws IOException {
    Request request = new Request("POST", endpoint);
    request.setJsonEntity(String.format(Locale.ROOT, "{\"query\": \"%s\"}", query));
    RequestOptions.Builder restOptionsBuilder = RequestOptions.DEFAULT.toBuilder();
    restOptionsBuilder.addHeader("Content-Type", "application/json");
    request.setOptions(restOptionsBuilder);
    return new JSONObject(executeRequest(request));
  }
}
