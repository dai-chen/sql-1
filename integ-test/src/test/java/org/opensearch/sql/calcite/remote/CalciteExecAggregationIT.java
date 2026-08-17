/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
import org.opensearch.sql.legacy.SQLIntegTestCase;

/** Integration test verifying the calcite_exec aggregation skeleton registers and responds. */
public class CalciteExecAggregationIT extends SQLIntegTestCase {

  @Override
  public void init() throws Exception {
    super.init();
    loadIndex(Index.BANK);
  }

  @Test
  public void testCalciteExecAggregationSkeletonReturnsEmptyResult() throws IOException {
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
    assertEquals(0, calciteStage.getLong("rowsCollected"));
    assertEquals(0, calciteStage.getLong("rowsEmitted"));
    assertEquals(0, calciteStage.getJSONArray("rows").length());
    assertEquals("CONCAT", calciteStage.getJSONObject("combine").getString("mode"));
  }
}
