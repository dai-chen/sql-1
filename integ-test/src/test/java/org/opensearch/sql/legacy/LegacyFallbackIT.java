/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.legacy;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_DOG;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_PEOPLE;

import java.io.IOException;
import java.util.Locale;
import org.json.JSONObject;
import org.junit.Test;

/**
 * Integration tests verifying that SQL queries unsupported by the V2 engine (JOIN, UNION, MINUS,
 * subqueries) correctly fall back to the legacy engine and return valid results.
 *
 * <p>These tests replace coverage lost when legacy ITs (JoinIT, SubqueryIT, MultiQueryIT) were
 * excluded due to JSON response format deprecation in 3.0. They assert only successful execution
 * (HTTP 200 + non-empty results) rather than exact response structure.
 */
public class LegacyFallbackIT extends SQLIntegTestCase {

  @Override
  protected void init() throws Exception {
    loadIndex(Index.DOG);
    loadIndex(Index.PEOPLE);
  }

  // === JOIN fallback ===

  @Test
  public void testInnerJoinFallback() throws IOException {
    String query =
        String.format(
            Locale.ROOT,
            "SELECT a.firstname, d.dog_name FROM %s a JOIN %s d"
                + " ON d.holdersName = a.firstname WHERE a.age > 25",
            TEST_INDEX_PEOPLE,
            TEST_INDEX_DOG);
    assertQuerySucceeds(query);
  }

  @Test
  public void testLeftJoinFallback() throws IOException {
    String query =
        String.format(
            Locale.ROOT,
            "SELECT a.firstname, d.dog_name FROM %s a LEFT JOIN %s d"
                + " ON d.holdersName = a.firstname",
            TEST_INDEX_PEOPLE,
            TEST_INDEX_DOG);
    assertQuerySucceeds(query);
  }

  // === UNION fallback ===
  // Note: UNION and MINUS are not tested here because the legacy engine's UNION/MINUS support
  // has known issues (MultiQueryIT is @Ignored). The V2 fallback works correctly (verified in
  // cluster logs), but the legacy engine itself cannot execute these queries reliably.

  // === Subquery fallback ===

  @Test
  public void testInSubqueryFallback() throws IOException {
    String query =
        String.format(
            Locale.ROOT,
            "SELECT firstname FROM %s WHERE age IN (SELECT age FROM %s WHERE age > 30)",
            TEST_INDEX_PEOPLE,
            TEST_INDEX_PEOPLE);
    assertQuerySucceeds(query);
  }

  private void assertQuerySucceeds(String query) throws IOException {
    JSONObject result = executeQuery(query);
    assertFalse(
        "Query should not return error: " + query,
        result.has("error"));
    assertTrue(
        "Query should return results: " + query,
        getDataRows(result).length() > 0);
  }

  private org.json.JSONArray getDataRows(JSONObject result) {
    if (result.has("datarows")) {
      return result.getJSONArray("datarows");
    }
    if (result.has("hits")) {
      return result.getJSONObject("hits").getJSONArray("hits");
    }
    return new org.json.JSONArray();
  }
}
