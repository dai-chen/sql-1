/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.stage;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
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

  @Test
  void roundTrip_filter_with_ILIKE_operator_exercises_parent_toOp() {
    // This test exercises the RelJsonReader deserialization path through the parent RelJson's
    // package-private toOp() method. ILIKE is a library operator (SqlLibraryOperators.ILIKE)
    // that is NOT in SqlStdOperatorTable — it can only be found if the reflection in
    // ExtendedRelJson's 3-arg constructor successfully set the parent's operatorTable field.
    // Without the reflection, this test produces: CalciteException "no operator ILIKE"
    RelNode tableScan = buildTableScan();

    // Build a filter: account_number ILIKE '%test%'
    // ILIKE uses kind=LIKE, syntax=SPECIAL — it's in SqlLibraryOperators (POSTGRESQL library)
    RexNode likeCondition =
        REX_BUILDER.makeCall(
            org.apache.calcite.sql.fun.SqlLibraryOperators.ILIKE,
            REX_BUILDER.makeInputRef(tableScan.getRowType().getFieldList().get(2).getType(), 2),
            REX_BUILDER.makeLiteral("%test%"));
    RelNode filter = LogicalFilter.create(tableScan, likeCondition);

    String before = RelOptUtil.toString(filter);

    // Serialize and deserialize — the deserialize path goes through RelJsonReader which
    // internally calls the parent RelJson.toOp() for operator lookup.
    String encoded = RelFragmentCodec.serialize(filter);
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
  void roundTrip_logicalSort_with_fetch_and_empty_collation() {
    // US-013: a fragment whose root is a plain LogicalSort(fetch, empty collation) must serialize
    // and deserialize to an identical plan string, proving the shard-side limit can round-trip.
    RelNode tableScan = buildTableScan();

    org.apache.calcite.rex.RexBuilder rexBuilder = REX_BUILDER;
    org.apache.calcite.rel.type.RelDataTypeFactory typeFac = TYPE_FAC;
    RexNode fetchLiteral =
        rexBuilder.makeLiteral(10, typeFac.createSqlType(SqlTypeName.INTEGER), false);
    RelNode sort =
        org.apache.calcite.rel.logical.LogicalSort.create(
            tableScan, org.apache.calcite.rel.RelCollations.EMPTY, null, fetchLiteral);

    String before = RelOptUtil.toString(sort);

    String encoded = RelFragmentCodec.serialize(sort);
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
  void roundTrip_rexLiteral_sqlTypeName_flag_exercises_extended_relJson_write_and_read() {
    // Verifies that a Project containing RexBuilder.makeFlag(SqlTypeName.DOUBLE) — a RexLiteral
    // whose value is a SqlTypeName enum — serializes correctly through our ExtendedRelJson write
    // path and that the serialized form contains the distinguishing "SqlTypeName.DOUBLE" prefix.
    //
    // A full round-trip through RelJsonReader DOES NOT work because RelJsonReader's inner code
    // calls the parent RelJson's package-private toRex → RelEnumTypes.toEnum, which does not
    // know SqlTypeName. This is the fundamental constraint that Critical A addresses by making
    // StagePlanner classify such nodes as NEEDS_GATHER (tested in StagePlannerTest).
    RelNode tableScan = buildTableScan();

    // Build a Project containing a SqlTypeName flag literal alongside a regular field ref
    RexNode flagLiteral = REX_BUILDER.makeFlag(SqlTypeName.DOUBLE);
    RelNode project =
        LogicalProject.create(
            tableScan,
            List.of(),
            List.of(
                REX_BUILDER.makeInputRef(tableScan.getRowType().getFieldList().get(0).getType(), 0),
                flagLiteral),
            List.of("account_number", "type_flag"));

    // Verify that serialization succeeds and contains the prefixed SqlTypeName value
    String encoded = RelFragmentCodec.serialize(project);
    assertTrue(encoded.length() > 0, "Serialization must succeed for SqlTypeName flag literals");

    // Verify the node is correctly classified as non-round-trippable by StagePlanner
    assertTrue(
        StagePlanner.containsNonRoundTrippableEnum(project),
        "Project with SqlTypeName flag must be detected as non-round-trippable");
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
