/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.sql.legacy.SQLIntegTestCase;

/**
 * Integration tests verifying that the ANSI SQL schema correctly:
 * 1. Maps date/time/timestamp fields to standard Calcite types (not UDTs)
 * 2. Excludes metadata fields (_id, _index, _score, etc.) from SELECT * results
 */
public class AnsiSQLDateTimeIT extends SQLIntegTestCase {

  private static final String TEST_INDEX = "ansi_sql_datetime_test";

  @Override
  protected void init() throws Exception {
    super.init();
    // Delete index if it already exists (init is called per-test)
    try {
      client().performRequest(new Request("DELETE", "/" + TEST_INDEX));
    } catch (Exception e) {
      // ignore if index doesn't exist
    }
    // Create index with date/time/timestamp fields
    Request createIndex = new Request("PUT", "/" + TEST_INDEX);
    createIndex.setJsonEntity(
        "{\"mappings\":{\"properties\":{"
            + "\"name\":{\"type\":\"keyword\"},"
            + "\"birthday\":{\"type\":\"date\"},"
            + "\"event_time\":{\"type\":\"date\","
            + "\"format\":\"strict_date_optional_time||epoch_millis\"},"
            + "\"age\":{\"type\":\"integer\"},"
            + "\"active\":{\"type\":\"boolean\"}"
            + "}}}");
    client().performRequest(createIndex);

    // Bulk index test data
    Request bulk = new Request("POST", "/" + TEST_INDEX + "/_bulk?refresh=true");
    bulk.setJsonEntity(
        "{\"index\":{}}\n"
            + "{\"name\":\"Alice\",\"birthday\":\"1990-05-15\","
            + "\"event_time\":\"2024-01-15T10:30:00\",\"age\":34,\"active\":true}\n"
            + "{\"index\":{}}\n"
            + "{\"name\":\"Bob\",\"birthday\":\"1985-12-25\","
            + "\"event_time\":\"2024-06-20T14:45:00\",\"age\":39,\"active\":false}\n"
            + "{\"index\":{}}\n"
            + "{\"name\":\"Charlie\",\"birthday\":\"2000-03-01\","
            + "\"event_time\":\"2024-03-01T09:00:00\",\"age\":24,\"active\":true}\n");
    client().performRequest(bulk);
  }

  @Test
  public void testSelectStarExcludesMetadataFields() throws IOException {
    JSONObject response =
        executeAnsiQuery("SELECT * FROM \"" + TEST_INDEX + "\" ORDER BY \"name\"");

    // Verify no metadata fields in schema
    JSONArray schema = response.getJSONArray("schema");
    for (int i = 0; i < schema.length(); i++) {
      String fieldName = schema.getJSONObject(i).getString("name");
      assertFalse("Schema should not contain _id", fieldName.equals("_id"));
      assertFalse("Schema should not contain _index", fieldName.equals("_index"));
      assertFalse("Schema should not contain _score", fieldName.equals("_score"));
      assertFalse("Schema should not contain _routing", fieldName.equals("_routing"));
      assertFalse("Schema should not contain _maxscore", fieldName.equals("_maxscore"));
      assertFalse("Schema should not contain _sort", fieldName.equals("_sort"));
    }

    // Verify data rows are returned
    assertEquals(3, response.getJSONArray("datarows").length());
  }

  @Test
  public void testSelectDateFieldDirectly() throws IOException {
    JSONObject response =
        executeAnsiQuery(
            "SELECT \"name\", \"birthday\" FROM \""
                + TEST_INDEX
                + "\" WHERE \"name\" = 'Alice'");

    JSONArray schema = response.getJSONArray("schema");
    assertEquals(2, schema.length());
    // OpenSearch "date" type maps to TIMESTAMP (OS stores all dates as epoch millis)
    assertEquals("timestamp", schema.getJSONObject(1).getString("type"));

    JSONArray rows = response.getJSONArray("datarows");
    assertEquals(1, rows.length());
  }

  @Test
  public void testSelectTimestampFieldDirectly() throws IOException {
    JSONObject response =
        executeAnsiQuery(
            "SELECT \"name\", \"event_time\" FROM \""
                + TEST_INDEX
                + "\" WHERE \"name\" = 'Bob'");

    JSONArray schema = response.getJSONArray("schema");
    assertEquals(2, schema.length());
    assertEquals("timestamp", schema.getJSONObject(1).getString("type"));

    JSONArray rows = response.getJSONArray("datarows");
    assertEquals(1, rows.length());
  }

  @Test
  public void testExtractYearFromDate() throws IOException {
    JSONObject response =
        executeAnsiQuery(
            "SELECT \"name\", EXTRACT(YEAR FROM \"birthday\") AS \"birth_year\" "
                + "FROM \""
                + TEST_INDEX
                + "\" WHERE \"name\" = 'Alice'");

    JSONArray rows = response.getJSONArray("datarows");
    assertEquals(1, rows.length());
    JSONArray row = rows.getJSONArray(0);
    assertEquals("Alice", row.getString(0));
    assertEquals(1990, row.getInt(1));
  }

  @Test
  public void testExtractMonthFromDate() throws IOException {
    JSONObject response =
        executeAnsiQuery(
            "SELECT \"name\", EXTRACT(MONTH FROM \"birthday\") AS \"birth_month\" "
                + "FROM \""
                + TEST_INDEX
                + "\" WHERE \"name\" = 'Bob'");

    JSONArray rows = response.getJSONArray("datarows");
    assertEquals(1, rows.length());
    JSONArray row = rows.getJSONArray(0);
    assertEquals("Bob", row.getString(0));
    assertEquals(12, row.getInt(1));
  }

  @Test
  public void testExtractDayFromTimestamp() throws IOException {
    JSONObject response =
        executeAnsiQuery(
            "SELECT \"name\", EXTRACT(DAY FROM \"event_time\") AS \"event_day\" "
                + "FROM \""
                + TEST_INDEX
                + "\" WHERE \"name\" = 'Charlie'");

    JSONArray rows = response.getJSONArray("datarows");
    assertEquals(1, rows.length());
    JSONArray row = rows.getJSONArray(0);
    assertEquals("Charlie", row.getString(0));
    assertEquals(1, row.getInt(1));
  }

  @Test
  public void testExtractHourFromTimestamp() throws IOException {
    JSONObject response =
        executeAnsiQuery(
            "SELECT \"name\", EXTRACT(HOUR FROM \"event_time\") AS \"event_hour\" "
                + "FROM \""
                + TEST_INDEX
                + "\" WHERE \"name\" = 'Alice'");

    JSONArray rows = response.getJSONArray("datarows");
    assertEquals(1, rows.length());
    JSONArray row = rows.getJSONArray(0);
    assertEquals("Alice", row.getString(0));
    assertEquals(10, row.getInt(1));
  }

  @Test
  public void testYearFunction() throws IOException {
    JSONObject response =
        executeAnsiQuery(
            "SELECT \"name\", YEAR(\"birthday\") AS \"yr\" "
                + "FROM \""
                + TEST_INDEX
                + "\" ORDER BY \"name\"");

    JSONArray rows = response.getJSONArray("datarows");
    assertEquals(3, rows.length());
    assertEquals(1990, rows.getJSONArray(0).getInt(1)); // Alice
    assertEquals(1985, rows.getJSONArray(1).getInt(1)); // Bob
    assertEquals(2000, rows.getJSONArray(2).getInt(1)); // Charlie
  }

  @Test
  public void testGroupByDateExtract() throws IOException {
    JSONObject response =
        executeAnsiQuery(
            "SELECT EXTRACT(YEAR FROM \"birthday\") AS \"yr\", COUNT(*) AS \"cnt\" "
                + "FROM \""
                + TEST_INDEX
                + "\" GROUP BY EXTRACT(YEAR FROM \"birthday\") "
                + "ORDER BY \"yr\"");

    JSONArray rows = response.getJSONArray("datarows");
    assertEquals(3, rows.length());
  }

  @Test
  public void testDateComparisonInWhere() throws IOException {
    JSONObject response =
        executeAnsiQuery(
            "SELECT \"name\" FROM \""
                + TEST_INDEX
                + "\" WHERE \"birthday\" > DATE '1990-01-01' ORDER BY \"name\"");

    JSONArray rows = response.getJSONArray("datarows");
    assertEquals(2, rows.length());
    assertEquals("Alice", rows.getJSONArray(0).getString(0));
    assertEquals("Charlie", rows.getJSONArray(1).getString(0));
  }

  @Test
  public void testTimestampComparisonInWhere() throws IOException {
    // Alice=2024-01-15T10:30:00, Bob=2024-06-20T14:45:00, Charlie=2024-03-01T09:00:00
    // > 2024-03-01 00:00:00 => Bob and Charlie
    JSONObject response =
        executeAnsiQuery(
            "SELECT \"name\" FROM \""
                + TEST_INDEX
                + "\" WHERE \"event_time\" > TIMESTAMP '2024-03-01 00:00:00' ORDER BY \"name\"");

    JSONArray rows = response.getJSONArray("datarows");
    assertEquals(2, rows.length());
    assertEquals("Bob", rows.getJSONArray(0).getString(0));
    assertEquals("Charlie", rows.getJSONArray(1).getString(0));
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
