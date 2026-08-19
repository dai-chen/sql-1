/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.remote;

import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.TYPE_FACTORY;
import static org.opensearch.sql.legacy.TestsConstants.TEST_INDEX_BANK;

import com.google.common.collect.ImmutableList;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Properties;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexWindowBounds;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.Frameworks;
import org.json.JSONArray;
import org.json.JSONObject;
import org.junit.jupiter.api.Test;
import org.opensearch.client.Request;
import org.opensearch.client.ResponseException;
import org.opensearch.sql.legacy.SQLIntegTestCase;
import org.opensearch.sql.opensearch.stage.RelFragmentCodec;

/** Integration test verifying the calcite_exec aggregation collects rows via doc_values/_source. */
public class CalciteExecAggregationIT extends SQLIntegTestCase {

  private static final String INDEX_NAME = "opensearch-sql_test_index_bank";
  private static final RelDataTypeFactory TYPE_FAC = TYPE_FACTORY;
  private static final RexBuilder REX_BUILDER = new RexBuilder(TYPE_FAC);
  private static final RelOptCluster CLUSTER =
      RelOptCluster.create(new VolcanoPlanner(), REX_BUILDER);

  @Override
  public void init() throws Exception {
    super.init();
    loadIndex(Index.BANK);
  }

  @Test
  public void testCalciteExecAggregationCollectsRows() throws IOException {
    // Build a real identity-projection fragment that passes all fields through unchanged.
    String identityPlan =
        buildIdentityPlan(
            List.of("account_number", "gender"), List.of(SqlTypeName.BIGINT, SqlTypeName.VARCHAR));

    String body =
        """
        {
          "size": 0,
          "aggs": {
            "calcite_stage": {
              "calcite_exec": {
                "plan": "%s",
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
        """
            .formatted(identityPlan);

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
    String identityPlan =
        buildIdentityPlan(
            List.of("city", "balance", "address"),
            List.of(SqlTypeName.VARCHAR, SqlTypeName.BIGINT, SqlTypeName.VARCHAR));

    String body =
        """
        {
          "size": 0,
          "aggs": {
            "calcite_stage": {
              "calcite_exec": {
                "plan": "%s",
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
        """
            .formatted(identityPlan);

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

  /**
   * US-005: Ships a Filter+Project fragment to the shard. The fragment filters rows where
   * account_number > 20 and projects only (account_number, gender). Asserts that only filtered,
   * projected rows are returned.
   */
  @Test
  public void testCalciteExecWithFilterProjectFragment() throws IOException {
    // Build a Filter(account_number > 20) + Project(account_number, gender) fragment
    RelNode tableScan = buildTableScan();

    // Filter: account_number (index 0) > 20
    RexNode condition =
        REX_BUILDER.makeCall(
            SqlStdOperatorTable.GREATER_THAN,
            REX_BUILDER.makeInputRef(tableScan.getRowType().getFieldList().get(0).getType(), 0),
            REX_BUILDER.makeLiteral(20L, TYPE_FAC.createSqlType(SqlTypeName.BIGINT), false));
    RelNode filter = LogicalFilter.create(tableScan, condition);

    // Project: account_number (index 0), gender (index 1)
    RelNode project =
        LogicalProject.create(
            filter,
            List.of(),
            List.of(
                REX_BUILDER.makeInputRef(filter.getRowType().getFieldList().get(0).getType(), 0),
                REX_BUILDER.makeInputRef(filter.getRowType().getFieldList().get(1).getType(), 1)),
            List.of("account_number", "gender"));

    String base64Plan = RelFragmentCodec.serialize(project);

    String body =
        """
        {
          "size": 0,
          "aggs": {
            "calcite_stage": {
              "calcite_exec": {
                "plan": "%s",
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
        """
            .formatted(base64Plan);

    Request request = new Request("POST", "/" + TEST_INDEX_BANK + "/_search");
    request.setJsonEntity(body);
    String responseStr = executeRequest(request);
    JSONObject response = new JSONObject(responseStr);

    assertFalse("Response should not contain an error field", response.has("error"));
    assertEquals(0, response.getJSONObject("_shards").getInt("failed"));

    JSONObject calciteStage = response.getJSONObject("aggregations").getJSONObject("calcite_stage");

    // All 7 docs are collected, but only those with account_number > 20 are emitted
    assertEquals(7, calciteStage.getLong("rowsCollected"));

    JSONArray rows = calciteStage.getJSONArray("rows");
    // account_numbers > 20 in the BANK index: 25, 32 → 2 rows
    long rowsEmitted = calciteStage.getLong("rowsEmitted");
    assertEquals(2, rowsEmitted);
    assertEquals(2, rows.length());

    // Each row should have exactly 2 columns (account_number, gender)
    for (int i = 0; i < rows.length(); i++) {
      assertEquals(2, rows.getJSONArray(i).length());
    }

    // Sort by account_number for deterministic verification
    List<JSONArray> sortedRows = new ArrayList<>();
    for (int i = 0; i < rows.length(); i++) {
      sortedRows.add(rows.getJSONArray(i));
    }
    sortedRows.sort(Comparator.comparingLong(a -> a.getLong(0)));

    assertEquals(25L, sortedRows.get(0).getLong(0));
    assertEquals("F", sortedRows.get(0).getString(1));
    assertEquals(32L, sortedRows.get(1).getLong(0));
    assertEquals("F", sortedRows.get(1).getString(1));
  }

  /**
   * US-005: Ships a ROW_NUMBER() OVER (PARTITION BY gender) fragment to the shard. Asserts that
   * per-shard ranked rows are returned with correct rank assignments per partition.
   */
  @Test
  public void testCalciteExecWithRowNumberWindowFragment() throws IOException {
    // Build ROW_NUMBER() OVER (PARTITION BY gender) + all fields
    RelNode tableScan = buildTableScan();

    RelDataType bigintType = TYPE_FAC.createSqlType(SqlTypeName.BIGINT);
    RexNode partitionKey =
        REX_BUILDER.makeInputRef(tableScan.getRowType().getFieldList().get(1).getType(), 1);
    RexNode rowNumber =
        REX_BUILDER.makeOver(
            bigintType,
            SqlStdOperatorTable.ROW_NUMBER,
            List.of(),
            List.of(partitionKey),
            ImmutableList.of(),
            RexWindowBounds.UNBOUNDED_PRECEDING,
            RexWindowBounds.CURRENT_ROW,
            true, // physical (ROWS)
            true, // allowPartial
            false, // nullWhenCountZero
            false // distinct
            );

    List<RexNode> projects =
        List.of(
            REX_BUILDER.makeInputRef(tableScan.getRowType().getFieldList().get(0).getType(), 0),
            REX_BUILDER.makeInputRef(tableScan.getRowType().getFieldList().get(1).getType(), 1),
            rowNumber);
    List<String> fieldNames = List.of("account_number", "gender", "rn");
    RelNode project = LogicalProject.create(tableScan, List.of(), projects, fieldNames);

    String base64Plan = RelFragmentCodec.serialize(project);

    String body =
        """
        {
          "size": 0,
          "aggs": {
            "calcite_stage": {
              "calcite_exec": {
                "plan": "%s",
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
        """
            .formatted(base64Plan);

    Request request = new Request("POST", "/" + TEST_INDEX_BANK + "/_search");
    request.setJsonEntity(body);
    String responseStr = executeRequest(request);
    JSONObject response = new JSONObject(responseStr);

    assertFalse("Response should not contain an error field", response.has("error"));
    assertEquals(0, response.getJSONObject("_shards").getInt("failed"));

    JSONObject calciteStage = response.getJSONObject("aggregations").getJSONObject("calcite_stage");

    // All 7 docs are collected and all 7 are emitted (window adds a column but doesn't filter)
    assertEquals(7, calciteStage.getLong("rowsCollected"));
    assertEquals(7, calciteStage.getLong("rowsEmitted"));

    JSONArray rows = calciteStage.getJSONArray("rows");
    assertEquals(7, rows.length());

    // Each row should have 3 columns (account_number, gender, rn)
    for (int i = 0; i < rows.length(); i++) {
      assertEquals(3, rows.getJSONArray(i).length());
    }

    // Verify per-partition rank assignment: "M" has 4 members → ranks 1-4, "F" has 3 → ranks 1-3
    List<JSONArray> mRows = new ArrayList<>();
    List<JSONArray> fRows = new ArrayList<>();
    for (int i = 0; i < rows.length(); i++) {
      JSONArray row = rows.getJSONArray(i);
      if ("M".equals(row.getString(1))) {
        mRows.add(row);
      } else {
        fRows.add(row);
      }
    }

    assertEquals(4, mRows.size());
    assertEquals(3, fRows.size());

    // Ranks within "M" partition should be {1, 2, 3, 4}
    List<Long> mRanks = mRows.stream().map(r -> r.getLong(2)).sorted().toList();
    assertEquals(List.of(1L, 2L, 3L, 4L), mRanks);

    // Ranks within "F" partition should be {1, 2, 3}
    List<Long> fRanks = fRows.stream().map(r -> r.getLong(2)).sorted().toList();
    assertEquals(List.of(1L, 2L, 3L), fRanks);
  }

  /**
   * US-009: Verifies that a very small row_budget causes the request to fail with a descriptive
   * error naming both the row count and the forcing operator. Uses
   * allow_partial_search_results=false to ensure the shard-level exception propagates.
   */
  @Test
  public void testRowBudgetBreachFailsFast() throws IOException {
    String identityPlan =
        buildIdentityPlan(
            List.of("account_number", "gender"), List.of(SqlTypeName.BIGINT, SqlTypeName.VARCHAR));

    String body =
        """
        {
          "size": 0,
          "aggs": {
            "calcite_stage": {
              "calcite_exec": {
                "plan": "%s",
                "fields": [
                  {"name": "account_number", "type": "long"},
                  {"name": "gender", "type": "keyword"}
                ],
                "combine": {"mode": "CONCAT"},
                "row_budget": 2,
                "forcing_operator": "LogicalWindow"
              }
            }
          }
        }
        """
            .formatted(identityPlan);

    // With allow_partial_search_results=false, the shard failure propagates as a top-level error
    Request request =
        new Request("POST", "/" + TEST_INDEX_BANK + "/_search?allow_partial_search_results=false");
    request.setJsonEntity(body);

    ResponseException exception =
        assertThrows(ResponseException.class, () -> executeRequest(request));

    String errorMessage = exception.getMessage();
    // The error must contain the exact formatted message produced by RowBudgetExceededException.
    // BANK has 7 docs on a single shard; the 3rd collected row breaches budget=2.
    assertTrue(
        "Error should contain the exact budget breach message, got: " + errorMessage,
        errorMessage.contains(
            "LogicalWindow requires all rows on one node; gathered 3 rows, budget 2"));
  }

  /**
   * US-009: Verifies that the existing tests with row_budget=200000 are unaffected — 7 docs in BANK
   * index is well below the budget.
   */
  @Test
  public void testDefaultRowBudgetDoesNotAffectSmallIndex() throws IOException {
    // This is effectively a regression guard: the other tests in this class use row_budget=200000
    // and collect 7 rows. If the budget logic is broken, they would all fail. This test makes
    // the assertion explicit with a slightly different budget to confirm the path.
    String identityPlan = buildIdentityPlan(List.of("account_number"), List.of(SqlTypeName.BIGINT));

    String body =
        """
        {
          "size": 0,
          "aggs": {
            "calcite_stage": {
              "calcite_exec": {
                "plan": "%s",
                "fields": [
                  {"name": "account_number", "type": "long"}
                ],
                "combine": {"mode": "CONCAT"},
                "row_budget": 100
              }
            }
          }
        }
        """
            .formatted(identityPlan);

    Request request = new Request("POST", "/" + TEST_INDEX_BANK + "/_search");
    request.setJsonEntity(body);
    String responseStr = executeRequest(request);
    JSONObject response = new JSONObject(responseStr);

    assertFalse("Response should not contain an error field", response.has("error"));
    assertEquals(0, response.getJSONObject("_shards").getInt("failed"));
    assertEquals(
        7,
        response
            .getJSONObject("aggregations")
            .getJSONObject("calcite_stage")
            .getLong("rowsCollected"));
  }

  /**
   * US-013: Verifies that early_termination_limit bounds the number of rows collected on the shard.
   * Uses TEST_INDEX_BANK (7 docs, single shard) with an early_termination_limit of 3, well below
   * the doc count, proving that the shard-level collection terminates early.
   */
  @Test
  public void testEarlyTerminationLimitBoundsRowsCollected() throws IOException {
    String identityPlan =
        buildIdentityPlan(
            List.of("account_number", "gender"), List.of(SqlTypeName.BIGINT, SqlTypeName.VARCHAR));

    // early_termination_limit = 3 means the shard must stop collecting at 3 rows
    String body =
        """
        {
          "size": 0,
          "aggs": {
            "calcite_stage": {
              "calcite_exec": {
                "plan": "%s",
                "fields": [
                  {"name": "account_number", "type": "long"},
                  {"name": "gender", "type": "keyword"}
                ],
                "combine": {"mode": "LIMIT", "n": 3},
                "row_budget": 200000,
                "early_termination_limit": 3
              }
            }
          }
        }
        """
            .formatted(identityPlan);

    // allow_partial_search_results=false ensures shard exceptions propagate (not hidden as 200)
    Request request =
        new Request("POST", "/" + TEST_INDEX_BANK + "/_search?allow_partial_search_results=false");
    request.setJsonEntity(body);
    String responseStr = executeRequest(request);
    JSONObject response = new JSONObject(responseStr);

    assertFalse("Response should not contain an error field", response.has("error"));
    assertEquals(0, response.getJSONObject("_shards").getInt("failed"));

    JSONObject calciteStage = response.getJSONObject("aggregations").getJSONObject("calcite_stage");

    // rowsCollected must be bounded by the early_termination_limit (3) — NOT 7
    long rowsCollected = calciteStage.getLong("rowsCollected");
    assertTrue(
        "rowsCollected (" + rowsCollected + ") must be <= early_termination_limit (3)",
        rowsCollected <= 3);

    // rows must be non-empty (at least some data was collected)
    JSONArray rows = calciteStage.getJSONArray("rows");
    assertTrue("rows must be non-empty", rows.length() > 0);

    // rows.length() <= 3 (bounded by the limit)
    assertTrue("rows.length (" + rows.length() + ") must be <= 3", rows.length() <= 3);
  }

  private void assertRow(
      JSONArray row, String expectedCity, long expectedBalance, String expectedAddress) {
    assertEquals(expectedCity, row.getString(0));
    assertEquals(expectedBalance, row.getLong(1));
    assertEquals(expectedAddress, row.getString(2));
  }

  /**
   * Build a table scan over the schema [OpenSearch, opensearch-sql_test_index_bank] with fields
   * matching the IT request's "fields" declaration: (account_number: BIGINT, gender: VARCHAR).
   */
  private RelNode buildTableScan() {
    RelDataType rowType =
        TYPE_FAC
            .builder()
            .add(
                "account_number",
                TYPE_FAC.createTypeWithNullability(
                    TYPE_FAC.createSqlType(SqlTypeName.BIGINT), true))
            .add(
                "gender",
                TYPE_FAC.createTypeWithNullability(
                    TYPE_FAC.createSqlType(SqlTypeName.VARCHAR), true))
            .build();

    SchemaPlus rootSchema = Frameworks.createRootSchema(false);
    SchemaPlus osSchema = rootSchema.add(RelFragmentCodec.SCHEMA_NAME, new AbstractSchema() {});
    osSchema.add(INDEX_NAME, new RelFragmentCodec.ShardRowSourceTable(rowType));

    CalciteSchema calciteSchema = CalciteSchema.from(rootSchema);
    CalciteCatalogReader catalogReader =
        new CalciteCatalogReader(
            calciteSchema, List.of(), TYPE_FAC, new CalciteConnectionConfigImpl(new Properties()));

    RelOptTable table =
        catalogReader.getTableForMember(List.of(RelFragmentCodec.SCHEMA_NAME, INDEX_NAME));
    return LogicalTableScan.create(CLUSTER, table, List.of());
  }

  /**
   * Build a serialized identity-projection plan (projects every field straight through in order)
   * over the shard row-source table. Reuses the same schema/table path convention as {@link
   * #buildTableScan()} so that deserialization resolves the table correctly.
   */
  private String buildIdentityPlan(List<String> fieldNames, List<SqlTypeName> fieldTypes) {
    // Build the row type for the table
    RelDataTypeFactory.Builder rtBuilder = TYPE_FAC.builder();
    for (int i = 0; i < fieldNames.size(); i++) {
      rtBuilder.add(
          fieldNames.get(i),
          TYPE_FAC.createTypeWithNullability(TYPE_FAC.createSqlType(fieldTypes.get(i)), true));
    }
    RelDataType rowType = rtBuilder.build();

    // Register the table under the same [OpenSearch, <indexName>] path
    SchemaPlus rootSchema = Frameworks.createRootSchema(false);
    SchemaPlus osSchema = rootSchema.add(RelFragmentCodec.SCHEMA_NAME, new AbstractSchema() {});
    osSchema.add(INDEX_NAME, new RelFragmentCodec.ShardRowSourceTable(rowType));

    CalciteSchema calciteSchema = CalciteSchema.from(rootSchema);
    CalciteCatalogReader catalogReader =
        new CalciteCatalogReader(
            calciteSchema, List.of(), TYPE_FAC, new CalciteConnectionConfigImpl(new Properties()));

    RelOptTable table =
        catalogReader.getTableForMember(List.of(RelFragmentCodec.SCHEMA_NAME, INDEX_NAME));
    RelNode tableScan = LogicalTableScan.create(CLUSTER, table, List.of());

    // Build identity projection: project every field by index
    List<RexNode> projects = new ArrayList<>();
    for (int i = 0; i < fieldNames.size(); i++) {
      projects.add(
          REX_BUILDER.makeInputRef(tableScan.getRowType().getFieldList().get(i).getType(), i));
    }
    RelNode project = LogicalProject.create(tableScan, List.of(), projects, fieldNames);

    return RelFragmentCodec.serialize(project);
  }
}
