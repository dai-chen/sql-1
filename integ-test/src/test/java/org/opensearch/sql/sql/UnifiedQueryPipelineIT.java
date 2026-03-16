/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql;

import static org.opensearch.sql.legacy.TestUtils.isIndexExist;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.util.MatcherUtils.schema;
import static org.opensearch.sql.util.MatcherUtils.verifyDataRows;
import static org.opensearch.sql.util.MatcherUtils.verifySchema;

import java.io.IOException;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.sql.legacy.SQLIntegTestCase;

/**
 * Integration tests for the unified query pipeline. Queries targeting indices with "parquet_"
 * prefix are routed through UnifiedQueryPlanner → AnalyticsExecutionEngine (stub returning empty
 * results).
 */
public class UnifiedQueryPipelineIT extends SQLIntegTestCase {

  private static final String TEST_INDEX = "parquet_test";

  @Override
  public void init() throws Exception {
    super.init();
    createParquetTestIndex();
    loadIndex(Index.BANK);
  }

  private void createParquetTestIndex() throws IOException {
    if (isIndexExist(client(), TEST_INDEX)) {
      return;
    }
    String mapping =
        "{"
            + "\"mappings\": {"
            + "  \"properties\": {"
            + "    \"timestamp\": {\"type\": \"date\"},"
            + "    \"status\": {\"type\": \"integer\"},"
            + "    \"message\": {\"type\": \"keyword\"},"
            + "    \"department\": {\"type\": \"keyword\"}"
            + "  }"
            + "}"
            + "}";
    Request request = new Request("PUT", "/" + TEST_INDEX);
    request.setJsonEntity(mapping);
    client().performRequest(request);
  }

  @Test
  public void testSqlSelectReturnsEmptyResult() {
    JSONObject result =
        executeJdbcRequest("SELECT status, message FROM " + TEST_INDEX + " WHERE status = 200");
    verifySchema(result, schema("status", "integer"), schema("message", "keyword"));
    verifyDataRows(result);
  }

  @Test
  public void testSqlAggregateReturnsEmptyResult() {
    JSONObject result =
        executeJdbcRequest("SELECT count(*) FROM " + TEST_INDEX + " GROUP BY status");
    verifyDataRows(result);
  }

  @Test
  public void testNonParquetQueryUnaffected() {
    JSONObject result = executeJdbcRequest("SELECT firstname FROM " + TEST_INDEX_BANK);
    assertTrue("Non-parquet query should return results", result.getInt("total") > 0);
  }
}
