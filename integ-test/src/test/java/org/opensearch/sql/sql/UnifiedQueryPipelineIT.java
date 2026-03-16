/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.sql;

import static org.opensearch.sql.legacy.TestUtils.isIndexExist;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;
import static org.opensearch.sql.util.MatcherUtils.schema;

import java.io.IOException;
import org.hamcrest.Matcher;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.Test;
import org.opensearch.client.Request;
import org.opensearch.sql.ppl.PPLIntegTestCase;

/**
 * Integration tests for the unified query pipeline. Queries targeting indices with "parquet_"
 * prefix are routed through UnifiedQueryPlanner → AnalyticsExecutionEngine (stub returning empty
 * results with schema derived from RelNode).
 */
public class UnifiedQueryPipelineIT extends PPLIntegTestCase {

  private static final String TEST_INDEX = "parquet_test";

  @Override
  protected void init() throws Exception {
    super.init();
    createParquetTestIndex();
    loadIndex(Index.BANK);
  }

  private void createParquetTestIndex() throws IOException {
    if (isIndexExist(client(), TEST_INDEX)) {
      return;
    }
    String mapping =
        """
        {
          "mappings": {
            "properties": {
              "timestamp": {"type": "date"},
              "status": {"type": "integer"},
              "message": {"type": "keyword"},
              "department": {"type": "keyword"}
            }
          }
        }\
        """;
    Request request = new Request("PUT", "/" + TEST_INDEX);
    request.setJsonEntity(mapping);
    client().performRequest(request);
  }

  // --- SQL tests ---

  @Test
  public void testSqlSelectWithFilter() throws IOException {
    withSQL(
            """
            SELECT status, message FROM parquet_test WHERE status = 200\
            """)
        .verifySchema(schema("status", "integer"), schema("message", "keyword"))
        .verifyDataRows()
        .verifyExplain(
            """
            LogicalProject(status=[$2], message=[$1])
              LogicalFilter(condition=[=($2, 200)])
                LogicalTableScan(table=[[opensearch, parquet_test]])
            """);
  }

  @Test
  public void testSqlAggregate() throws IOException {
    withSQL(
            """
            SELECT count(*) FROM parquet_test GROUP BY status\
            """)
        .verifyDataRows()
        .verifyExplain(
            """
            LogicalProject(EXPR$0=[$1])
              LogicalAggregate(group=[{0}], EXPR$0=[COUNT()])
                LogicalProject(status=[$2])
                  LogicalTableScan(table=[[opensearch, parquet_test]])
            """);
  }

  // --- PPL tests ---

  @Test
  public void testPplWhereAndProject() throws IOException {
    withPPL(
            """
            source = parquet_test | where status = 200 | fields status, message\
            """)
        .verifySchema(schema("status", "integer"), schema("message", "keyword"))
        .verifyDataRows()
        .verifyExplain(
            """
            LogicalProject(status=[$2], message=[$1])
              LogicalFilter(condition=[=($2, 200)])
                LogicalTableScan(table=[[opensearch, parquet_test]])
            """);
  }

  @Test
  public void testPplStats() throws IOException {
    withPPL(
            """
            source = parquet_test | stats count() by status\
            """)
        .verifyDataRows()
        .verifyExplain(
            """
            LogicalProject(count()=[$1], status=[$0])
              LogicalAggregate(group=[{0}], count()=[COUNT()])
                LogicalProject(status=[$2])
                  LogicalTableScan(table=[[opensearch, parquet_test]])
            """);
  }

  // --- Key Area 1: Datetime UDT ---

  @Test
  public void testPplDatetimeFunction() throws IOException {
    // hour() is a standard Calcite function — no UDT in the plan.
    // Timestamp comparison uses TIMESTAMP('2024-01-01':VARCHAR) cast.
    withPPL(
            """
            source = parquet_test \
            | where timestamp > '2024-01-01' \
            | eval hour = hour(timestamp) \
            | fields hour, status\
            """)
        .verifySchema(schema("hour", "integer"), schema("status", "integer"))
        .verifyDataRows()
        .verifyExplain(
            """
            LogicalProject(hour=[HOUR($3)], status=[$2])
              LogicalFilter(condition=[>($3, TIMESTAMP('2024-01-01':VARCHAR))])
                LogicalTableScan(table=[[opensearch, parquet_test]])
            """);
  }

  // --- Full-text search function ---

  @Test
  public void testPplSearchFunction() throws IOException {
    // match() is a full-text search function — appears in the plan with MAP arguments.
    // On a Parquet-backed index, the Analytics engine would need to handle or reject it.
    withPPL(
            """
            source = parquet_test | where match(message, 'error') | fields message\
            """)
        .verifySchema(schema("message", "keyword"))
        .verifyDataRows()
        .verifyExplain(
            """
            LogicalProject(message=[$1])
              LogicalFilter(condition=[match(MAP('field', $1), MAP('query', 'error':VARCHAR))])
                LogicalTableScan(table=[[opensearch, parquet_test]])
            """);
  }

  // --- Regression test ---

  @Test
  public void testNonParquetQueryUnaffected() {
    JSONObject result = executeJdbcRequest("SELECT firstname FROM " + TEST_INDEX_BANK);
    assertTrue("Non-parquet query should return results", result.getInt("total") > 0);
  }

  // --- Fluent assertion helpers ---

  private QueryAssertion withSQL(String query) {
    return new QueryAssertion(query, true);
  }

  private QueryAssertion withPPL(String query) {
    return new QueryAssertion(query, false);
  }

  private class QueryAssertion {
    private final String query;
    private final boolean isSql;
    private final JSONObject result;

    QueryAssertion(String query, boolean isSql) {
      this.query = query;
      this.isSql = isSql;
      try {
        this.result = isSql ? executeJdbcRequest(query) : executeQuery(query);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    }

    @SafeVarargs
    final QueryAssertion verifySchema(Matcher<JSONObject>... matchers) {
      org.opensearch.sql.util.MatcherUtils.verifySchema(result, matchers);
      return this;
    }

    @SafeVarargs
    final QueryAssertion verifyDataRows(Matcher<JSONArray>... matchers) {
      org.opensearch.sql.util.MatcherUtils.verifyDataRows(result, matchers);
      return this;
    }

    QueryAssertion verifyExplain(String expectedPlan) throws IOException {
      String explain = isSql ? explainQuery(query).strip() : explainQueryToString(query).strip();
      String logical = new JSONObject(explain).getJSONObject("calcite").getString("logical");
      assertEquals(expectedPlan, logical);
      return this;
    }
  }
}
