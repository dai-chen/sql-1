/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_ACCOUNT;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.sql.legacy.SQLIntegTestCase;

/**
 * Integration test for the unified SQL REST endpoint. Tests that /_plugins/_sql routes through the
 * unified query API for both OpenSearch SQL (default) and ANSI SQL (mode=ansi).
 */
public class UnifiedSQLRestIT extends SQLIntegTestCase {

  @Override
  protected void init() throws Exception {
    super.init();
    loadIndex(Index.ACCOUNT);
  }

  @Test
  public void testOpenSearchSQL() throws IOException {
    JSONObject response =
        executeQuery(
            String.format(
                "SELECT firstname, age FROM `%s` WHERE lastname = 'Duke' LIMIT 3",
                TEST_INDEX_ACCOUNT));
    verifySchema(response, schema("firstname", null, "keyword"), schema("age", null, "long"));
    verifyDataRows(response, rows("Amber", 32));
  }

  @Test
  public void testAnsiSQL() throws IOException {
    JSONObject response =
        executeAnsiQuery(
            String.format(
                "SELECT \"firstname\", \"age\" FROM \"%s\" WHERE \"lastname\" = 'Duke'",
                TEST_INDEX_ACCOUNT));
    verifySchema(response, schema("firstname", null, "keyword"), schema("age", null, "long"));
    verifyDataRows(response, rows("Amber", 32));
  }

  @Test
  public void testAnsiSQLWithAggregation() throws IOException {
    JSONObject response =
        executeAnsiQuery(
            String.format(
                "SELECT \"state\", COUNT(*) AS \"cnt\" FROM \"%s\""
                    + " WHERE \"state\" = 'IL' GROUP BY \"state\"",
                TEST_INDEX_ACCOUNT));
    verifySchema(response, schema("state", null, "keyword"), schema("cnt", null, "long"));
    verifyDataRows(response, rows("IL", 22));
  }

  private JSONObject executeAnsiQuery(String query) throws IOException {
    Request request = new Request("POST", "/_plugins/_sql?format=jdbc&mode=ansi");
    request.setJsonEntity("{\"query\": \"" + query.replace("\"", "\\\"") + "\"}");
    Response response = client().performRequest(request);
    return new JSONObject(getResponseBody(response));
  }

  private static String getResponseBody(Response response) throws IOException {
    return org.opensearch.sql.util.TestUtils.getResponseBody(response, true);
  }
}
