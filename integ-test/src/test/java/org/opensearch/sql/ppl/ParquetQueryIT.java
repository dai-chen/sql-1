/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl;

import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import java.util.Locale;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.client.RequestOptions;

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
    verifySchema(
        response,
        schema("timestamp", null, "timestamp"),
        schema("status", null, "long"),
        schema("message", null, "string"),
        schema("active", null, "boolean"));
    verifyDataRows(
        response,
        rows("2024-01-15 10:30:00", 200, "Success", true),
        rows("2024-01-15 11:45:00", 404, "Not Found", false));
  }

  @Test
  public void testSqlQueryOnParquetIndex() throws IOException {
    JSONObject response = executeSqlQuery("SELECT * FROM parquet_index");
    // JDBC formatter uses legacyTypeName: STRING -> keyword
    verifySchema(
        response,
        schema("timestamp", null, "timestamp"),
        schema("status", null, "long"),
        schema("message", null, "keyword"),
        schema("active", null, "boolean"));
    verifyDataRows(
        response,
        rows("2024-01-15 10:30:00", 200, "Success", true),
        rows("2024-01-15 11:45:00", 404, "Not Found", false));
    assertEquals(200, response.getInt("status"));
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
