/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api;

import static org.junit.Assert.assertTrue;
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
        executeAnsiQuery(
            String.format(
                "SELECT \"firstname\", \"age\" FROM \"%s\" WHERE \"lastname\" = 'Duke' LIMIT 3",
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

  @Test
  public void testAnsiSQLWithMatchPhraseAndJoinAndGroupBy() throws IOException {
    // Combines: match_phrase() push-down + self-JOIN in Calcite + GROUP BY aggregation
    JSONObject response =
        executeAnsiQuery(
            String.format(
                "SELECT a.\"state\", COUNT(*) AS \"cnt\""
                    + " FROM \"%1$s\" a"
                    + " JOIN \"%1$s\" b ON a.\"state\" = b.\"state\""
                    + " WHERE match_phrase(a.\"address\", 'Holmes Lane')"
                    + " AND a.\"account_number\" <> b.\"account_number\""
                    + " GROUP BY a.\"state\""
                    + " ORDER BY \"cnt\" DESC",
                TEST_INDEX_ACCOUNT));
    verifySchema(response, schema("state", null, "keyword"), schema("cnt", null, "long"));
    assertTrue(response.getJSONArray("datarows").length() > 0);
  }

  @Test
  public void testAnsiSQLWithCaseWhenAggregation() throws IOException {
    String index = "oee_events_test";
    // Create index with OEE mapping
    Request createIndex = new Request("PUT", "/" + index);
    createIndex.setJsonEntity(
        "{\"mappings\":{\"properties\":{"
            + "\"OEEClass\":{\"type\":\"integer\"},"
            + "\"mhe_id\":{\"type\":\"long\"},"
            + "\"mp_type\":{\"type\":\"integer\"},"
            + "\"counter_increment\":{\"type\":\"integer\"},"
            + "\"whid\":{\"type\":\"keyword\"},"
            + "\"t_start\":{\"type\":\"date\","
            + "\"format\":\"yyyy-MM-dd HH:mm:ss||strict_date_optional_time||epoch_millis\"}"
            + "}}}");
    client().performRequest(createIndex);

    // Bulk index test data
    Request bulk = new Request("POST", "/" + index + "/_bulk?refresh=true");
    bulk.setJsonEntity(
        "{\"index\":{}}\n"
            + "{\"mhe_id\":1,\"mp_type\":1,\"OEEClass\":1,\"counter_increment\":10,"
            + "\"whid\":\"BOS3\",\"t_start\":\"2026-02-11 08:00:00\"}\n"
            + "{\"index\":{}}\n"
            + "{\"mhe_id\":1,\"mp_type\":4,\"OEEClass\":1,\"counter_increment\":5,"
            + "\"whid\":\"BOS3\",\"t_start\":\"2026-02-11 09:00:00\"}\n"
            + "{\"index\":{}}\n"
            + "{\"mhe_id\":2,\"mp_type\":1,\"OEEClass\":8,\"counter_increment\":20,"
            + "\"whid\":\"BOS3\",\"t_start\":\"2026-02-11 10:00:00\"}\n"
            + "{\"index\":{}}\n"
            + "{\"mhe_id\":2,\"mp_type\":1,\"OEEClass\":3,\"counter_increment\":15,"
            + "\"whid\":\"BOS3\",\"t_start\":\"2026-02-11 11:00:00\"}\n"
            + "{\"index\":{}}\n"
            + "{\"mhe_id\":null,\"mp_type\":1,\"OEEClass\":1,\"counter_increment\":7,"
            + "\"whid\":\"BOS3\",\"t_start\":\"2026-02-11 12:00:00\"}\n");
    client().performRequest(bulk);

    // The query: CASE WHEN + SUM + GROUP BY with ifnull
    JSONObject response =
        executeAnsiQuery(
            "SELECT COALESCE(\"mhe_id\", -99) AS \"ID\","
                + " SUM(CASE WHEN \"mp_type\" = 4 THEN 0"
                + " WHEN \"OEEClass\" = 8 THEN 0"
                + " WHEN \"OEEClass\" = 0 THEN 0"
                + " WHEN \"OEEClass\" IS NULL THEN 0"
                + " ELSE \"counter_increment\" END) AS \"Q_G\""
                + " FROM \"" + index + "\""
                + " WHERE \"whid\" = 'BOS3'"
                + " GROUP BY 1");
    assertTrue(response.getJSONArray("datarows").length() > 0);

    // Cleanup
    client().performRequest(new Request("DELETE", "/" + index));
  }

  @Test
  public void testOpenSearchSQLWithCaseWhenAggregation() throws IOException {
    String index = "oee-events-2026-02-11";
    // Create index with OEE mapping (exact mapping from user)
    Request createIndex = new Request("PUT", "/" + index);
    createIndex.setJsonEntity(
        "{\"mappings\":{\"properties\":{"
            + "\"OEEClass\":{\"type\":\"integer\"},"
            + "\"aat\":{\"type\":\"integer\"},"
            + "\"alarm_list\":{\"type\":\"keyword\"},"
            + "\"area\":{\"type\":\"keyword\"},"
            + "\"capacity\":{\"type\":\"integer\"},"
            + "\"capacity_design_rate\":{\"type\":\"long\"},"
            + "\"capacity_machine_rate\":{\"type\":\"long\"},"
            + "\"capacity_target\":{\"type\":\"integer\"},"
            + "\"counter_increment\":{\"type\":\"integer\"},"
            + "\"description\":{\"type\":\"keyword\"},"
            + "\"duration\":{\"type\":\"integer\"},"
            + "\"eam_id\":{\"type\":\"keyword\"},"
            + "\"em_capacity\":{\"type\":\"float\"},"
            + "\"mhe_id\":{\"type\":\"long\"},"
            + "\"mhe_type\":{\"type\":\"keyword\"},"
            + "\"mp_type\":{\"type\":\"integer\"},"
            + "\"status\":{\"type\":\"integer\"},"
            + "\"subarea\":{\"type\":\"keyword\"},"
            + "\"t_start\":{\"type\":\"date\","
            + "\"format\":\"yyyy-MM-dd HH:mm:ss.SSSSSS||yyyy-MM-dd HH:mm:ss"
            + "||strict_date_optional_time||epoch_millis\"},"
            + "\"t_stop\":{\"type\":\"date\","
            + "\"format\":\"yyyy-MM-dd HH:mm:ss.SSSSSS||yyyy-MM-dd HH:mm:ss"
            + "||strict_date_optional_time||epoch_millis\"},"
            + "\"team\":{\"type\":\"integer\"},"
            + "\"text_code\":{\"type\":\"keyword\"},"
            + "\"whid\":{\"type\":\"keyword\"}"
            + "}}}");
    client().performRequest(createIndex);

    // Bulk index test data
    Request bulk = new Request("POST", "/" + index + "/_bulk?refresh=true");
    bulk.setJsonEntity(
        "{\"index\":{}}\n"
            + "{\"mhe_id\":1,\"mp_type\":1,\"OEEClass\":1,\"counter_increment\":10,"
            + "\"whid\":\"BOS3\",\"t_start\":\"2026-02-11 08:00:00\"}\n"
            + "{\"index\":{}}\n"
            + "{\"mhe_id\":1,\"mp_type\":4,\"OEEClass\":1,\"counter_increment\":5,"
            + "\"whid\":\"BOS3\",\"t_start\":\"2026-02-11 09:00:00\"}\n"
            + "{\"index\":{}}\n"
            + "{\"mhe_id\":2,\"mp_type\":1,\"OEEClass\":8,\"counter_increment\":20,"
            + "\"whid\":\"BOS3\",\"t_start\":\"2026-02-11 10:00:00\"}\n"
            + "{\"index\":{}}\n"
            + "{\"mhe_id\":2,\"mp_type\":1,\"OEEClass\":3,\"counter_increment\":15,"
            + "\"whid\":\"BOS3\",\"t_start\":\"2026-02-11 11:00:00\"}\n"
            + "{\"index\":{}}\n"
            + "{\"mhe_id\":null,\"mp_type\":1,\"OEEClass\":1,\"counter_increment\":7,"
            + "\"whid\":\"BOS3\",\"t_start\":\"2026-02-11 12:00:00\"}\n");
    client().performRequest(bulk);

    // User's exact query pattern via ANSI SQL (Calcite) engine
    // Uses COALESCE (ANSI standard) instead of ifnull (MySQL-specific)
    // Timestamp BETWEEN omitted: EXPR_TIMESTAMP UDT from OpenSearchTypeFactory is incompatible
    // with PlannerImpl's SqlTypeFactoryImpl. Fixing requires making the ANSI SQL planner use
    // OpenSearchTypeFactory, or making table metadata return standard Calcite timestamp types.
    JSONObject response =
        executeAnsiQuery(
            "SELECT COALESCE(\"mhe_id\", -99) AS \"ID\","
                + " SUM(CASE WHEN \"mp_type\" = 4 THEN 0"
                + " WHEN \"OEEClass\" = 8 THEN 0"
                + " WHEN \"OEEClass\" = 0 THEN 0"
                + " WHEN \"OEEClass\" IS NULL THEN 0"
                + " ELSE \"counter_increment\" END) AS \"Q_G\""
                + " FROM \"" + index + "\""
                + " WHERE \"whid\" = 'BOS3'"
                + " GROUP BY 1");
    assertTrue(response.getJSONArray("datarows").length() > 0);

    // Cleanup
    client().performRequest(new Request("DELETE", "/" + index));
  }

  @Test
  public void testExplainShowsFilterAggregation() throws IOException {
    String index = "oee_explain_test";
    Request createIndex = new Request("PUT", "/" + index);
    createIndex.setJsonEntity(
        "{\"mappings\":{\"properties\":{"
            + "\"mhe_id\":{\"type\":\"long\"},"
            + "\"mp_type\":{\"type\":\"integer\"},"
            + "\"OEEClass\":{\"type\":\"integer\"},"
            + "\"counter_increment\":{\"type\":\"integer\"},"
            + "\"whid\":{\"type\":\"keyword\"}"
            + "}}}");
    client().performRequest(createIndex);

    Request bulk = new Request("POST", "/" + index + "/_bulk?refresh=true");
    bulk.setJsonEntity(
        "{\"index\":{}}\n{\"mhe_id\":1,\"mp_type\":1,\"OEEClass\":1,"
            + "\"counter_increment\":10,\"whid\":\"BOS3\"}\n"
            + "{\"index\":{}}\n{\"mhe_id\":1,\"mp_type\":4,\"OEEClass\":1,"
            + "\"counter_increment\":5,\"whid\":\"BOS3\"}\n");
    client().performRequest(bulk);

    // Full multi-condition query with THEN NULL (matches AggregateCaseToFilterRule A1)
    JSONObject response =
        executeAnsiQuery(
            "EXPLAIN SELECT COALESCE(\"mhe_id\", -99) AS \"ID\","
                + " SUM(CASE WHEN \"mp_type\" = 4 THEN NULL"
                + " WHEN \"OEEClass\" = 8 THEN NULL"
                + " WHEN \"OEEClass\" = 0 THEN NULL"
                + " WHEN \"OEEClass\" IS NULL THEN NULL"
                + " ELSE \"counter_increment\" END) AS \"Q_G\""
                + " FROM \"" + index + "\""
                + " WHERE \"whid\" = 'BOS3'"
                + " GROUP BY 1");

    String logical = response.getString("logical");
    assertTrue(
        "Expected FILTER in logical plan but got: " + logical,
        logical.contains("FILTER"));

    String physical = response.getString("physical");
    // Verify native field-based sum with native bool/term filter (not script)
    assertTrue(
        "Expected native sum on field",
        physical.contains("\"sum\":{\"field\":\"counter_increment\"}"));
    assertTrue(
        "Expected native term query for mp_type in filter",
        physical.contains("\"term\":{\"mp_type\""));

    client().performRequest(new Request("DELETE", "/" + index));
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
