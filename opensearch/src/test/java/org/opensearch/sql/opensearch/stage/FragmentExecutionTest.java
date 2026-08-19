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
import org.opensearch.sql.calcite.utils.CalciteClassLoaderHelper;
import org.opensearch.sql.opensearch.stage.CalciteExecAggregationBuilder.FieldDescriptor;

/**
 * Unit tests for the US-005 fragment execution path: serialize a logical RelNode fragment,
 * deserialize it, compile to a Bindable via Janino, and verify the output rows. Mirrors the style
 * of {@link RelFragmentCodecTest}.
 */
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
public class FragmentExecutionTest {

  private static final String INDEX_NAME = "test_index";
  private static final RelDataTypeFactory TYPE_FAC = TYPE_FACTORY;
  private static final RexBuilder REX_BUILDER = new RexBuilder(TYPE_FAC);
  private static final RelOptCluster CLUSTER =
      RelOptCluster.create(new VolcanoPlanner(), REX_BUILDER);

  private static final List<FieldDescriptor> FIELD_DESCRIPTORS =
      List.of(
          new FieldDescriptor("account_number", "long"),
          new FieldDescriptor("age", "integer"),
          new FieldDescriptor("gender", "keyword"));

  @Test
  void execute_filter_and_project_fragment() {
    // Build a Filter(age > 30) + Project(account_number) over the shard row-source table
    RelNode tableScan = buildTableScan();

    // Filter: age (index 1) > 30
    RexNode condition =
        REX_BUILDER.makeCall(
            SqlStdOperatorTable.GREATER_THAN,
            REX_BUILDER.makeInputRef(tableScan.getRowType().getFieldList().get(1).getType(), 1),
            REX_BUILDER.makeLiteral(30, TYPE_FAC.createSqlType(SqlTypeName.INTEGER), false));
    RelNode filter = LogicalFilter.create(tableScan, condition);

    // Project: keep only account_number (index 0)
    RelNode project =
        LogicalProject.create(
            filter,
            List.of(),
            List.of(
                REX_BUILDER.makeInputRef(filter.getRowType().getFieldList().get(0).getType(), 0)),
            List.of("account_number"));

    // Serialize to base64
    String base64Plan = RelFragmentCodec.serialize(project);

    // Prepare input rows: (1, 25, "M"), (2, 35, "F"), (3, 40, "M"), (4, 28, "F")
    List<Object[]> inputRows =
        List.of(
            new Object[] {1L, 25, "M"},
            new Object[] {2L, 35, "F"},
            new Object[] {3L, 40, "M"},
            new Object[] {4L, 28, "F"});

    // Execute via the production code path, wrapped in the classloader helper
    List<List<Object>> result =
        CalciteClassLoaderHelper.withCalciteClassLoader(
            () ->
                CalciteExecAggregator.executeFragment(
                    base64Plan, FIELD_DESCRIPTORS, inputRows, INDEX_NAME),
            FragmentExecutionTest.class);

    // Only rows with age > 30 survive, projected to account_number only
    assertEquals(2, result.size());
    assertEquals(List.of((Object) 2L), result.get(0));
    assertEquals(List.of((Object) 3L), result.get(1));
  }

  @Test
  void execute_fetch_fragment_truncates_rows() {
    // US-013: a fragment containing a LogicalSort(fetch=3, empty collation) over the table scan
    // must truncate the output to 3 rows even though more input rows exist.
    RelNode tableScan = buildTableScan();

    org.apache.calcite.rex.RexBuilder rexBuilder = REX_BUILDER;
    org.apache.calcite.rel.type.RelDataTypeFactory typeFac = TYPE_FAC;
    RexNode fetchLiteral =
        rexBuilder.makeLiteral(3, typeFac.createSqlType(SqlTypeName.INTEGER), false);
    RelNode sort =
        org.apache.calcite.rel.logical.LogicalSort.create(
            tableScan, org.apache.calcite.rel.RelCollations.EMPTY, null, fetchLiteral);

    String base64Plan = RelFragmentCodec.serialize(sort);

    // 5 input rows — more than the fetch limit of 3
    List<Object[]> inputRows =
        List.of(
            new Object[] {1L, 25, "M"},
            new Object[] {2L, 35, "F"},
            new Object[] {3L, 40, "M"},
            new Object[] {4L, 28, "F"},
            new Object[] {5L, 50, "M"});

    List<List<Object>> result =
        CalciteClassLoaderHelper.withCalciteClassLoader(
            () ->
                CalciteExecAggregator.executeFragment(
                    base64Plan, FIELD_DESCRIPTORS, inputRows, INDEX_NAME),
            FragmentExecutionTest.class);

    // Output must be truncated to 3 rows
    assertEquals(3, result.size());
  }

  @Test
  void execute_window_rowNumber_fragment() {
    // Build ROW_NUMBER() OVER (PARTITION BY gender) plus all columns
    RelNode tableScan = buildTableScan();

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

    List<RexNode> projects =
        List.of(
            REX_BUILDER.makeInputRef(tableScan.getRowType().getFieldList().get(0).getType(), 0),
            REX_BUILDER.makeInputRef(tableScan.getRowType().getFieldList().get(1).getType(), 1),
            REX_BUILDER.makeInputRef(tableScan.getRowType().getFieldList().get(2).getType(), 2),
            rowNumber);
    List<String> fieldNames = List.of("account_number", "age", "gender", "rn");
    RelNode project = LogicalProject.create(tableScan, List.of(), projects, fieldNames);

    String base64Plan = RelFragmentCodec.serialize(project);

    // Input rows: two "M" rows, one "F" row
    List<Object[]> inputRows =
        List.of(new Object[] {1L, 25, "M"}, new Object[] {2L, 35, "F"}, new Object[] {3L, 40, "M"});

    List<List<Object>> result =
        CalciteClassLoaderHelper.withCalciteClassLoader(
            () ->
                CalciteExecAggregator.executeFragment(
                    base64Plan, FIELD_DESCRIPTORS, inputRows, INDEX_NAME),
            FragmentExecutionTest.class);

    // 3 rows: all input rows with ROW_NUMBER added. Within each partition (gender),
    // rows receive sequential numbers starting at 1.
    assertEquals(3, result.size());

    // Verify that each partition has correct rank assignments
    long mCount = result.stream().filter(r -> "M".equals(r.get(2))).count();
    long fCount = result.stream().filter(r -> "F".equals(r.get(2))).count();
    assertEquals(2, mCount);
    assertEquals(1, fCount);

    // The single "F" row must have rn=1
    List<Object> fRow = result.stream().filter(r -> "F".equals(r.get(2))).findFirst().orElseThrow();
    assertEquals(1L, ((Number) fRow.get(3)).longValue());

    // The "M" rows must have rn=1 and rn=2 (in some order)
    List<Long> mRanks =
        result.stream()
            .filter(r -> "M".equals(r.get(2)))
            .map(r -> ((Number) r.get(3)).longValue())
            .sorted()
            .toList();
    assertEquals(List.of(1L, 2L), mRanks);
  }

  /**
   * Regression test for US-013: a fragment shaped Sort(fetch) → Calc(query_string) → scan triggers
   * IllegalStateException because relevance functions have no enumerable implementor. The fix
   * classifies nodes containing relevance functions as NEEDS_GATHER in placement, which prevents
   * this fragment shape from being produced. This test directly verifies the compilation failure to
   * guard against regression if the placement guard is ever removed.
   */
  @Test
  void execute_limit_over_calc_with_relevance_function_fails_with_suppressed_cause() {
    // Build the table scan
    RelNode tableScan = buildTableScan();

    // Build a Filter containing query_string (simulated via the actual PPLBuiltinOperators UDF)
    org.apache.calcite.sql.SqlOperator queryStringOp =
        org.opensearch.sql.expression.function.PPLBuiltinOperators.QUERY_STRING;

    // query_string takes a MAP argument. Build MAP('query', 'firstname:Amber').
    RexNode queryKey =
        REX_BUILDER.makeLiteral("query", TYPE_FAC.createSqlType(SqlTypeName.VARCHAR), true);
    RexNode queryValue =
        REX_BUILDER.makeLiteral(
            "firstname:Amber", TYPE_FAC.createSqlType(SqlTypeName.VARCHAR), true);
    RexNode mapExpr =
        REX_BUILDER.makeCall(
            TYPE_FAC.createMapType(
                TYPE_FAC.createSqlType(SqlTypeName.VARCHAR),
                TYPE_FAC.createSqlType(SqlTypeName.VARCHAR)),
            SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR,
            List.of(queryKey, queryValue));

    // Build query_string(MAP('query','firstname:Amber')) → BOOLEAN
    RexNode queryStringCall = REX_BUILDER.makeCall(queryStringOp, mapExpr);

    // Wrap in a LogicalFilter
    RelNode filter = LogicalFilter.create(tableScan, queryStringCall);

    // Project all fields (simulating proj#0..2)
    RelNode project =
        LogicalProject.create(
            filter,
            List.of(),
            List.of(
                REX_BUILDER.makeInputRef(filter.getRowType().getFieldList().get(0).getType(), 0),
                REX_BUILDER.makeInputRef(filter.getRowType().getFieldList().get(1).getType(), 1),
                REX_BUILDER.makeInputRef(filter.getRowType().getFieldList().get(2).getType(), 2)),
            List.of("account_number", "age", "gender"));

    // Add LogicalSort with fetch=3 and empty collation (the limit promotion shape)
    RexNode fetchLiteral =
        REX_BUILDER.makeLiteral(3, TYPE_FAC.createSqlType(SqlTypeName.INTEGER), false);
    RelNode sort =
        org.apache.calcite.rel.logical.LogicalSort.create(
            project, org.apache.calcite.rel.RelCollations.EMPTY, null, fetchLiteral);

    // Serialize and attempt to execute — this MUST fail with the known root cause
    String base64Plan = RelFragmentCodec.serialize(sort);

    List<Object[]> inputRows =
        List.of(
            new Object[] {1L, 25, "M"},
            new Object[] {2L, 35, "F"},
            new Object[] {3L, 40, "M"},
            new Object[] {4L, 28, "F"},
            new Object[] {5L, 50, "M"});

    IllegalStateException thrown =
        org.junit.jupiter.api.Assertions.assertThrows(
            IllegalStateException.class,
            () ->
                CalciteClassLoaderHelper.withCalciteClassLoader(
                    () ->
                        CalciteExecAggregator.executeFragment(
                            base64Plan, FIELD_DESCRIPTORS, inputRows, INDEX_NAME),
                    FragmentExecutionTest.class));

    // Verify the suppressed root cause is the relevance function's UnsupportedOperationException
    Throwable[] suppressed = thrown.getSuppressed();
    assertEquals(1, suppressed.length, "Expected exactly one suppressed exception");
    org.junit.jupiter.api.Assertions.assertInstanceOf(
        UnsupportedOperationException.class, suppressed[0]);
    org.junit.jupiter.api.Assertions.assertTrue(
        suppressed[0].getMessage().contains("Relevance search query functions"),
        "Suppressed message should reference relevance functions, got: "
            + suppressed[0].getMessage());
  }

  private RelNode buildTableScan() {
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
