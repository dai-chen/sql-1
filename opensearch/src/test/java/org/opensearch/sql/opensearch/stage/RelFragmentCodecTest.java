/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.stage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.TYPE_FACTORY;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Properties;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.RelOptUtil;
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
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.opensearch.stage.CalciteExecAggregationBuilder.FieldDescriptor;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
public class RelFragmentCodecTest {

  private static final String INDEX_NAME = "test_index";
  private static final RelDataTypeFactory TYPE_FAC = TYPE_FACTORY;
  private static final RexBuilder REX_BUILDER = new RexBuilder(TYPE_FAC);
  private static final RelOptCluster CLUSTER =
      RelOptCluster.create(new VolcanoPlanner(), REX_BUILDER);

  @Test
  void roundTrip_project_over_filter_over_tableScan() {
    RelNode tableScan = buildTableScan();

    // Filter: field0 > 10
    RexNode condition =
        REX_BUILDER.makeCall(
            SqlStdOperatorTable.GREATER_THAN,
            REX_BUILDER.makeInputRef(tableScan.getRowType().getFieldList().get(1).getType(), 1),
            REX_BUILDER.makeLiteral(10, TYPE_FAC.createSqlType(SqlTypeName.INTEGER), false));
    RelNode filter = LogicalFilter.create(tableScan, condition);

    // Project: keep only field0 (account_number)
    RelNode project =
        LogicalProject.create(
            filter,
            List.of(),
            List.of(
                REX_BUILDER.makeInputRef(filter.getRowType().getFieldList().get(0).getType(), 0)),
            List.of("account_number"));

    String before = RelOptUtil.toString(project);

    String encoded = RelFragmentCodec.serialize(project);
    RelNode deserialized =
        RelFragmentCodec.deserialize(
            encoded,
            INDEX_NAME,
            List.of(
                new FieldDescriptor("account_number", "long"),
                new FieldDescriptor("age", "integer"),
                new FieldDescriptor("gender", "keyword")));

    String after = RelOptUtil.toString(deserialized);
    assertEquals(before, after);
  }

  @Test
  void roundTrip_window_rowNumber() {
    RelNode tableScan = buildTableScan();

    // Build ROW_NUMBER() OVER (PARTITION BY gender) as a RexOver
    RelDataType bigintType = TYPE_FAC.createSqlType(SqlTypeName.BIGINT);
    RexNode partitionKey =
        REX_BUILDER.makeInputRef(tableScan.getRowType().getFieldList().get(2).getType(), 2);
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

    // Project all original fields plus the row_number column
    List<RexNode> projects =
        List.of(
            REX_BUILDER.makeInputRef(tableScan.getRowType().getFieldList().get(0).getType(), 0),
            REX_BUILDER.makeInputRef(tableScan.getRowType().getFieldList().get(1).getType(), 1),
            REX_BUILDER.makeInputRef(tableScan.getRowType().getFieldList().get(2).getType(), 2),
            rowNumber);
    List<String> fieldNames = List.of("account_number", "age", "gender", "rn");

    RelNode project = LogicalProject.create(tableScan, List.of(), projects, fieldNames);

    String before = RelOptUtil.toString(project);

    String encoded = RelFragmentCodec.serialize(project);
    RelNode deserialized =
        RelFragmentCodec.deserialize(
            encoded,
            INDEX_NAME,
            List.of(
                new FieldDescriptor("account_number", "long"),
                new FieldDescriptor("age", "integer"),
                new FieldDescriptor("gender", "keyword")));

    String after = RelOptUtil.toString(deserialized);
    assertEquals(before, after);
  }

  @Test
  void deserialize_unrecognized_field_type_throws_with_field_name_and_type() {
    RelNode tableScan = buildTableScan();
    String encoded = RelFragmentCodec.serialize(tableScan);

    IllegalArgumentException ex =
        org.junit.jupiter.api.Assertions.assertThrows(
            IllegalArgumentException.class,
            () ->
                RelFragmentCodec.deserialize(
                    encoded,
                    INDEX_NAME,
                    List.of(
                        new FieldDescriptor("account_number", "long"),
                        new FieldDescriptor("status", "flattened"),
                        new FieldDescriptor("gender", "keyword"))));

    assertEquals(
        "Unrecognized OpenSearch field type 'flattened' for field 'status'", ex.getMessage());
  }

  private RelNode buildTableScan() {
    // Build a schema matching [OpenSearch, test_index] with fields: account_number, age, gender
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
}
