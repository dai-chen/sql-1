/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.TYPE_FACTORY;

import java.util.List;
import java.util.Properties;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.plan.RelTraitSet;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
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
import org.opensearch.sql.opensearch.request.OpenSearchRequest;
import org.opensearch.sql.opensearch.stage.RelFragmentCodec;
import org.opensearch.sql.opensearch.stage.StagePlan;
import org.opensearch.sql.opensearch.stage.StagePlanner;
import org.opensearch.sql.opensearch.storage.OpenSearchIndex;
import org.opensearch.sql.opensearch.storage.scan.CalciteLogicalIndexScan;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
public class StagedExplainLeafSpliceTest {

  private static final String INDEX_NAME = "test_index";
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
  void spliced_fragment_leaf_names_real_scan_not_logical_table_scan() {
    CalciteLogicalIndexScan scan = buildIndexScan();
    RexNode condition =
        REX_BUILDER.makeCall(
            SqlStdOperatorTable.GREATER_THAN,
            REX_BUILDER.makeInputRef(scan.getRowType().getFieldList().get(1).getType(), 1),
            REX_BUILDER.makeLiteral(10, TYPE_FAC.createSqlType(SqlTypeName.INTEGER), false));
    RelNode filter = LogicalFilter.create(scan, condition);
    RelNode project =
        LogicalProject.create(
            filter,
            List.of(),
            List.of(
                REX_BUILDER.makeInputRef(filter.getRowType().getFieldList().get(0).getType(), 0)),
            List.of("account_number"));

    StagePlan stagePlan = StagePlanner.split(project);
    assertTrue(stagePlan.staged());

    // Before splice: the fragment's leaf is a LogicalTableScan (serializable form)
    String beforeSplice = RelOptUtil.toString(stagePlan.shardFragment());
    assertTrue(
        beforeSplice.contains("LogicalTableScan"),
        "Fragment before splice should have LogicalTableScan leaf");

    // After splice: the leaf should be CalciteLogicalIndexScan
    RelNode spliced =
        OpenSearchExecutionEngine.replaceSerializableLeafWithRealScan(
            stagePlan.shardFragment(), scan);
    String afterSplice = RelOptUtil.toString(spliced);
    assertTrue(
        afterSplice.contains("CalciteLogicalIndexScan"),
        "Fragment after splice should have CalciteLogicalIndexScan leaf");
    assertFalse(
        afterSplice.contains("LogicalTableScan"),
        "Fragment after splice should NOT have LogicalTableScan leaf");
  }

  private CalciteLogicalIndexScan buildIndexScan() {
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
    when(osIndex.getIndexName()).thenReturn(indexNameObj);

    RelTraitSet traitSet = CLUSTER.traitSetOf(Convention.NONE);

    SchemaPlus rootSchema = Frameworks.createRootSchema(false);
    SchemaPlus osSchema = rootSchema.add(RelFragmentCodec.SCHEMA_NAME, new AbstractSchema() {});
    osSchema.add(INDEX_NAME, new RelFragmentCodec.ShardRowSourceTable(rowType));
    CalciteSchema calciteSchema = CalciteSchema.from(rootSchema);
    CalciteCatalogReader catalogReader =
        new CalciteCatalogReader(
            calciteSchema, List.of(), TYPE_FAC, new CalciteConnectionConfigImpl(new Properties()));
    RelOptTable table =
        catalogReader.getTableForMember(List.of(RelFragmentCodec.SCHEMA_NAME, INDEX_NAME));

    return new CalciteLogicalIndexScan(CLUSTER, table, osIndex);
  }
}
