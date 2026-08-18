/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
import org.opensearch.sql.legacy.SQLIntegTestCase;

/** Integration test verifying the calcite_exec aggregation collects rows via doc_values/_source. */
public class CalciteExecAggregationIT extends SQLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    loadIndex(Index.BANK);
  }

  @Test
  public void testCalciteExecAggregationCollectsRows() throws IOException {
    String body =
        """
        {
          "size": 0,
          "aggs": {
            "calcite_stage": {
              "calcite_exec": {
                "plan": "dHJpdmlhbA==",
                "fields": [
                  {"name": "account_number", "type": "long"},
                  {"name": "gender", "type": "keyword"}
                ],
                "combine": {"mode": "CONCAT"},
                "row_budget": 200000
              }
            }
          }
        }
        """;

    Request request = new Request("POST", "/" + TEST_INDEX_BANK + "/_search");
    request.setJsonEntity(body);
    String responseStr = executeRequest(request);
    JSONObject response = new JSONObject(responseStr);

    // Verify no errors
    assertFalse("Response should not contain an error field", response.has("error"));
    assertEquals(0, response.getJSONObject("_shards").getInt("failed"));

    // Verify the aggregation result
    JSONObject aggs = response.getJSONObject("aggregations");
    JSONObject calciteStage = aggs.getJSONObject("calcite_stage");
    assertEquals("CONCAT", calciteStage.getJSONObject("combine").getString("mode"));

    // With US-004 collection is active: 7 docs in BANK index
    assertEquals(7, calciteStage.getLong("rowsCollected"));
    assertEquals(7, calciteStage.getLong("rowsEmitted"));
    JSONArray rows = calciteStage.getJSONArray("rows");
    assertEquals(7, rows.length());

    // Verify each row has exactly 2 columns
    for (int i = 0; i < rows.length(); i++) {
      assertEquals(2, rows.getJSONArray(i).length());
    }

    // Sort rows by account_number for deterministic assertion
    List<JSONArray> sortedRows = new ArrayList<>();
    for (int i = 0; i < rows.length(); i++) {
      sortedRows.add(rows.getJSONArray(i));
    }
    sortedRows.sort(Comparator.comparingLong(a -> a.getLong(0)));

    // Expected (account_number, gender) sorted by account_number:
    // gender is a text field with no doc_values for "gender" directly — falls back to _source
    assertEquals(1L, sortedRows.get(0).getLong(0));
    assertEquals("M", sortedRows.get(0).getString(1));
    assertEquals(6L, sortedRows.get(1).getLong(0));
    assertEquals("M", sortedRows.get(1).getString(1));
    assertEquals(13L, sortedRows.get(2).getLong(0));
    assertEquals("F", sortedRows.get(2).getString(1));
    assertEquals(18L, sortedRows.get(3).getLong(0));
    assertEquals("M", sortedRows.get(3).getString(1));
    assertEquals(20L, sortedRows.get(4).getLong(0));
    assertEquals("M", sortedRows.get(4).getString(1));
    assertEquals(25L, sortedRows.get(5).getLong(0));
    assertEquals("F", sortedRows.get(5).getString(1));
    assertEquals(32L, sortedRows.get(6).getLong(0));
    assertEquals("F", sortedRows.get(6).getString(1));
  }

  @Test
  public void testCalciteExecAggregationKeywordLongAndTextField() throws IOException {
    // Tests: city (keyword with doc_values), balance (long with doc_values),
    //        address (bare text field, no keyword subfield — _source only)
    String body =
        """
        {
          "size": 0,
          "aggs": {
            "calcite_stage": {
              "calcite_exec": {
                "plan": "dHJpdmlhbA==",
                "fields": [
                  {"name": "city", "type": "keyword"},
                  {"name": "balance", "type": "long"},
                  {"name": "address", "type": "text"}
                ],
                "combine": {"mode": "CONCAT"},
                "row_budget": 200000
              }
            }
          }
        }
        """;

    Request request = new Request("POST", "/" + TEST_INDEX_BANK + "/_search");
    request.setJsonEntity(body);
    String responseStr = executeRequest(request);
    JSONObject response = new JSONObject(responseStr);

    // Verify no errors
    assertFalse("Response should not contain an error field", response.has("error"));
    assertEquals(0, response.getJSONObject("_shards").getInt("failed"));

    JSONObject aggs = response.getJSONObject("aggregations");
    JSONObject calciteStage = aggs.getJSONObject("calcite_stage");
    assertEquals(7, calciteStage.getLong("rowsCollected"));
    assertEquals(7, calciteStage.getLong("rowsEmitted"));
    assertEquals("CONCAT", calciteStage.getJSONObject("combine").getString("mode"));

    JSONArray rows = calciteStage.getJSONArray("rows");
    assertEquals(7, rows.length());

    // Sort by balance (index 1) for deterministic assertion
    List<JSONArray> sortedRows = new ArrayList<>();
    for (int i = 0; i < rows.length(); i++) {
      sortedRows.add(rows.getJSONArray(i));
    }
    sortedRows.sort(Comparator.comparingLong(a -> a.getLong(1)));

    // Expected sorted by balance ascending:
    // {account_number:18, balance:4180, city:Orick, address:"467 Hutchinson Court"}
    // {account_number:6, balance:5686, city:Dante, address:"671 Bristol Street"}
    // {account_number:20, balance:16418, city:Ribera, address:"282 Kings Place"}
    // {account_number:13, balance:32838, city:Nogal, address:"789 Madison Street"}
    // {account_number:1, balance:39225, city:Brogan, address:"880 Holmes Lane"}
    // {account_number:25, balance:40540, city:Nicholson, address:"171 Putnam Avenue"}
    // {account_number:32, balance:48086, city:Veguita, address:"702 Quentin Street"}

    assertRow(sortedRows.get(0), "Orick", 4180L, "467 Hutchinson Court");
    assertRow(sortedRows.get(1), "Dante", 5686L, "671 Bristol Street");
    assertRow(sortedRows.get(2), "Ribera", 16418L, "282 Kings Place");
    assertRow(sortedRows.get(3), "Nogal", 32838L, "789 Madison Street");
    assertRow(sortedRows.get(4), "Brogan", 39225L, "880 Holmes Lane");
    assertRow(sortedRows.get(5), "Nicholson", 40540L, "171 Putnam Avenue");
    assertRow(sortedRows.get(6), "Veguita", 48086L, "702 Quentin Street");
  }

  private void assertRow(
      JSONArray row, String expectedCity, long expectedBalance, String expectedAddress) {
    assertEquals(expectedCity, row.getString(0));
    assertEquals(expectedBalance, row.getLong(1));
    assertEquals(expectedAddress, row.getString(2));
  }
}
