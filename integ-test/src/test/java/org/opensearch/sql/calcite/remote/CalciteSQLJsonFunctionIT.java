/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.util.MatcherUtils.rows;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.sql.legacy.SQLIntegTestCase;

/**
 * SQL-specific JSON function integration tests for the Calcite engine. Tests JSON_OBJECT,
 * JSON_ARRAY, and IS JSON with SQL SELECT syntax through the unified Calcite SQL parser path.
 * Table names use double-quote quoting since the Calcite SQL parser uses ANSI SQL quoting.
 */
public class CalciteSQLJsonFunctionIT extends SQLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    loadIndex(Index.BANK);
  }

  /** Properly JSON-encodes the query to handle double-quoted identifiers. */
  @Override
  protected String makeRequest(String query) {
    return new JSONObject().put("query", query).toString();
  }

  @Test
  public void testJsonObject() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "SELECT JSON_OBJECT('name': firstname) FROM \"%s\" WHERE account_number = 1",
                TEST_INDEX_BANK));
    verifyDataRows(result, rows("{\"name\":\"Amber JOHnny\"}"));
  }

  @Test
  public void testJsonObjectMultipleKeys() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "SELECT JSON_OBJECT('name': firstname, 'age': age) FROM \"%s\""
                    + " WHERE account_number = 1",
                TEST_INDEX_BANK));
    verifyDataRows(result, rows("{\"name\":\"Amber JOHnny\",\"age\":32}"));
  }

  @Test
  public void testJsonObjectWithAlias() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "SELECT JSON_OBJECT('name': firstname) AS json_result FROM \"%s\""
                    + " WHERE account_number = 1",
                TEST_INDEX_BANK));
    verifyDataRows(result, rows("{\"name\":\"Amber JOHnny\"}"));
  }

  @Test
  public void testJsonArray() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "SELECT JSON_ARRAY(firstname, age) FROM \"%s\" WHERE account_number = 1",
                TEST_INDEX_BANK));
    verifyDataRows(result, rows("[\"Amber JOHnny\",32]"));
  }

  @Test
  public void testJsonArrayWithAlias() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "SELECT JSON_ARRAY(firstname) AS arr FROM \"%s\" WHERE account_number = 1",
                TEST_INDEX_BANK));
    verifyDataRows(result, rows("[\"Amber JOHnny\"]"));
  }

  @Test
  public void testJsonObjectInSubquery() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "WITH sub AS (SELECT firstname, age FROM \"%s\" WHERE account_number = 1)"
                    + " SELECT JSON_OBJECT('name': firstname, 'age': age) AS info FROM sub",
                TEST_INDEX_BANK));
    verifyDataRows(result, rows("{\"name\":\"Amber JOHnny\",\"age\":32}"));
  }

  @Test
  public void testJsonObjectWithAggregation() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "SELECT JSON_OBJECT('gender': gender, 'count': COUNT(*)) AS json_result"
                    + " FROM \"%s\" GROUP BY gender",
                TEST_INDEX_BANK));
    verifyDataRows(
        result,
        rows("{\"gender\":\"M\",\"count\":4}"),
        rows("{\"gender\":\"F\",\"count\":3}"));
  }

  @Test
  public void testIsJson() throws IOException {
    JSONObject result =
        executeQuery(
            String.format(
                "SELECT firstname, firstname IS JSON AS is_valid FROM \"%s\""
                    + " WHERE account_number = 1",
                TEST_INDEX_BANK));
    verifyDataRows(result, rows("Amber JOHnny", false));
  }
}
