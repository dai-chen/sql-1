/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.stage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.TYPE_FACTORY;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.Frameworks;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.opensearch.action.search.SearchRequest;
import org.opensearch.index.query.QueryBuilders;
import org.opensearch.search.builder.SearchSourceBuilder;
import org.opensearch.sql.common.setting.Settings;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.executor.OpenSearchExecutionEngine;
import org.opensearch.sql.opensearch.request.OpenSearchRequest;
import org.opensearch.sql.opensearch.storage.OpenSearchIndex;
import org.opensearch.sql.opensearch.storage.scan.AbstractCalciteIndexScan;
import org.opensearch.sql.opensearch.storage.scan.CalciteLogicalIndexScan;
import org.opensearch.sql.opensearch.storage.scan.context.PushDownContext;

/**
 * Unit tests for the staged search request shape. Verifies that {@link
 * OpenSearchExecutionEngine#buildStagedSearchRequest(StagePlan)} produces a request with:
 *
 * <ul>
 *   <li>size == 0
 *   <li>exactly one aggregation named "calcite_stage" of type "calcite_exec"
 *   <li>allowPartialSearchResults == false
 *   <li>no PIT, scroll, or search_after
 *   <li>the pushed-down QueryBuilder from PredicateAnalyzer (when present)
 * </ul>
 */
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
public class StagedRequestShapeTest {

  private static final String INDEX_NAME = "accounts";
  private static final RelDataTypeFactory TYPE_FAC = TYPE_FACTORY;
  private static final RexBuilder REX_BUILDER = new RexBuilder(TYPE_FAC);
  private static final RelOptCluster CLUSTER;

  static {
    VolcanoPlanner planner = new VolcanoPlanner();
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);
    CLUSTER = RelOptCluster.create(planner, REX_BUILDER);
  }

  @Test
  void staged_request_has_correct_shape() {
    // Build a plan: LogicalSort(LIMIT 10000) -> Filter(age > 30) -> Scan
    AbstractCalciteIndexScan scan = buildIndexScanWithPushedFilter();

    RexNode condition =
        REX_BUILDER.makeCall(
            SqlStdOperatorTable.GREATER_THAN,
            REX_BUILDER.makeInputRef(scan.getRowType().getFieldList().get(1).getType(), 1),
            REX_BUILDER.makeLiteral(30, TYPE_FAC.createSqlType(SqlTypeName.INTEGER), false));
    RelNode filter = LogicalFilter.create(scan, condition);

    RexNode fetchLiteral =
        REX_BUILDER.makeLiteral(10000, TYPE_FAC.createSqlType(SqlTypeName.INTEGER), false);
    RelNode limit = LogicalSort.create(filter, RelCollations.EMPTY, null, fetchLiteral);

    StagePlan stagePlan = StagePlanner.split(limit);
    assertTrue(stagePlan.staged());

    // Build the request
    SearchRequest request = OpenSearchExecutionEngine.buildStagedSearchRequest(stagePlan);
    SearchSourceBuilder source = request.source();

    // (a) size == 0
    assertEquals(0, source.size());

    // (b) exactly one aggregation of type calcite_exec
    assertNotNull(source.aggregations());
    var aggs = new java.util.ArrayList<>(source.aggregations().getAggregatorFactories());
    assertEquals(1, aggs.size());
    assertEquals("calcite_stage", aggs.get(0).getName());
    assertEquals(CalciteExecAggregationBuilder.NAME, aggs.get(0).getType());

    // (c) allowPartialSearchResults == false
    assertFalse(request.allowPartialSearchResults());

    // (d) no PIT, scroll, or search_after
    assertNull(source.pointInTimeBuilder());
    assertNull(source.searchAfter());
    // No scroll: SearchRequest has no scrollId or keepAlive set (default is null)

    // (e) pushed-down query clause is present (from the mock PushDownContext)
    assertNotNull(source.query());
    // Verify it contains the exists filter we set up in the mock
    assertTrue(source.query().toString().contains("exists"));
  }

  @Test
  void staged_request_targets_correct_index() {
    AbstractCalciteIndexScan scan = buildIndexScanWithPushedFilter();
    StagePlan stagePlan = StagePlanner.split(scan);
    assertTrue(stagePlan.staged());

    SearchRequest request = OpenSearchExecutionEngine.buildStagedSearchRequest(stagePlan);
    assertEquals(1, request.indices().length);
    assertEquals(INDEX_NAME, request.indices()[0]);
  }

  /**
   * Builds a CalciteLogicalIndexScan with a mocked OpenSearchIndex that provides: - field OS types
   * (account_number->long, age->integer, gender->keyword) - a PushDownContext whose
   * createRequestBuilder returns a source with a query clause
   */
  @SuppressWarnings("unchecked")
  private AbstractCalciteIndexScan buildIndexScanWithPushedFilter() {
    RelDataType rowType =
        TYPE_FAC
            .builder()
            .add(
                "account_number",
                TYPE_FAC.createTypeWithNullability(
                    TYPE_FAC.createSqlType(SqlTypeName.BIGINT), true))
            .add(
                "age",
                TYPE_FAC.createTypeWithNullability(
                    TYPE_FAC.createSqlType(SqlTypeName.INTEGER), true))
            .add(
                "gender",
                TYPE_FAC.createTypeWithNullability(
                    TYPE_FAC.createSqlType(SqlTypeName.VARCHAR), true))
            .build();

    OpenSearchIndex osIndex = mock(OpenSearchIndex.class);
    OpenSearchRequest.IndexName indexNameObj = mock(OpenSearchRequest.IndexName.class);
    when(indexNameObj.toString()).thenReturn(INDEX_NAME);
    when(indexNameObj.getIndexNames()).thenReturn(new String[] {INDEX_NAME});
    when(osIndex.getIndexName()).thenReturn(indexNameObj);

    // Mock field types
    OpenSearchDataType longType = mock(OpenSearchDataType.class);
    OpenSearchDataType intType = mock(OpenSearchDataType.class);
    OpenSearchDataType kwType = mock(OpenSearchDataType.class);
    when(longType.getMappingType()).thenReturn(OpenSearchDataType.MappingType.Long);
    when(intType.getMappingType()).thenReturn(OpenSearchDataType.MappingType.Integer);
    when(kwType.getMappingType()).thenReturn(OpenSearchDataType.MappingType.Keyword);
    Map<String, OpenSearchDataType> fieldTypes =
        Map.of(
            "account_number", longType,
            "age", intType,
            "gender", kwType);
    when(osIndex.getFieldOpenSearchTypes()).thenReturn(fieldTypes);

    // Mock settings (pushdown disabled)
    Settings settings = mock(Settings.class);
    when(settings.getSettingValue(Settings.Key.CALCITE_PUSHDOWN_ENABLED)).thenReturn(false);
    when(osIndex.getSettings()).thenReturn(settings);

    // Mock createRequestBuilder: returns a builder whose source has a query clause
    org.opensearch.sql.opensearch.request.OpenSearchRequestBuilder requestBuilder =
        mock(org.opensearch.sql.opensearch.request.OpenSearchRequestBuilder.class);
    SearchSourceBuilder mockSource = new SearchSourceBuilder();
    mockSource.query(QueryBuilders.existsQuery("gender"));
    when(requestBuilder.getSourceBuilder()).thenReturn(mockSource);

    // Mock the PushDownContext to return our request builder
    PushDownContext pushDownContext = mock(PushDownContext.class);
    when(pushDownContext.getOsIndex()).thenReturn(osIndex);
    when(pushDownContext.createRequestBuilder()).thenReturn(requestBuilder);

    // Max result window
    when(osIndex.getMaxResultWindow()).thenReturn(10000);
    when(osIndex.createRequestBuilder()).thenReturn(requestBuilder);

    SchemaPlus rootSchema = Frameworks.createRootSchema(false);
    SchemaPlus osSchema = rootSchema.add(RelFragmentCodec.SCHEMA_NAME, new AbstractSchema() {});
    osSchema.add(INDEX_NAME, new RelFragmentCodec.ShardRowSourceTable(rowType));
    CalciteSchema calciteSchema = CalciteSchema.from(rootSchema);
    CalciteCatalogReader catalogReader =
        new CalciteCatalogReader(
            calciteSchema, List.of(), TYPE_FAC, new CalciteConnectionConfigImpl(new Properties()));
    RelOptTable table =
        catalogReader.getTableForMember(List.of(RelFragmentCodec.SCHEMA_NAME, INDEX_NAME));

    // Use the package-private full constructor with the mocked PushDownContext
    return new TestableCalciteLogicalIndexScan(CLUSTER, table, osIndex, rowType, pushDownContext);
  }

  /**
   * Test-only subclass that allows injecting a mock PushDownContext. This is necessary because
   * AbstractCalciteIndexScan.pushDownContext is final and set in the constructor.
   */
  static class TestableCalciteLogicalIndexScan extends CalciteLogicalIndexScan {
    TestableCalciteLogicalIndexScan(
        RelOptCluster cluster,
        RelOptTable table,
        OpenSearchIndex osIndex,
        RelDataType schema,
        PushDownContext pushDownContext) {
      super(
          cluster,
          cluster.traitSetOf(Convention.NONE),
          List.of(),
          table,
          osIndex,
          schema,
          pushDownContext);
    }
  }
}
