/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.stage;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.TYPE_FACTORY;

import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.ConventionTraitDef;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.plan.volcano.VolcanoPlanner;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelCollationTraitDef;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexWindowBounds;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.Frameworks;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;

/**
 * Unit tests for {@link CoordinatorTreeExecutor}: verifies that the coordinator-side tier-2
 * execution correctly compiles a coordinator tree (post-optimize Enumerable nodes +
 * LogicalTableScan leaf) over gathered rows.
 */
@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
public class CoordinatorTreeExecutorTest {

  private static final RelDataTypeFactory TYPE_FAC = TYPE_FACTORY;
  private static final RexBuilder REX_BUILDER = new RexBuilder(TYPE_FAC);

  /** Build a VolcanoPlanner with ConventionTraitDef registered. */
  private static RelOptCluster createCluster() {
    VolcanoPlanner planner = new VolcanoPlanner();
    planner.addRelTraitDef(ConventionTraitDef.INSTANCE);
    planner.addRelTraitDef(RelCollationTraitDef.INSTANCE);
    return RelOptCluster.create(planner, REX_BUILDER);
  }

  /**
   * Multi-column test: coordinator tree is a LogicalSort (LIMIT 2) over a gathered-rows scan with 3
   * columns. Asserts that column ORDER is preserved.
   */
  @Test
  void execute_multi_column_limit() {
    RelDataType rowType =
        TYPE_FAC
            .builder()
            .add(
                "firstname",
                TYPE_FAC.createTypeWithNullability(
                    TYPE_FAC.createSqlType(SqlTypeName.VARCHAR), true))
            .add(
                "age",
                TYPE_FAC.createTypeWithNullability(
                    TYPE_FAC.createSqlType(SqlTypeName.BIGINT), true))
            .add(
                "city",
                TYPE_FAC.createTypeWithNullability(
                    TYPE_FAC.createSqlType(SqlTypeName.VARCHAR), true))
            .build();

    RelOptCluster cluster = createCluster();
    LogicalTableScan tableScan = buildGatheredRowsScan(cluster, rowType);

    // Coordinator tree: LIMIT 2 (a Sort with no collation, just fetch)
    RexNode fetchLiteral =
        REX_BUILDER.makeLiteral(2, TYPE_FAC.createSqlType(SqlTypeName.INTEGER), false);
    RelNode coordinatorTree =
        LogicalSort.create(tableScan, RelCollations.EMPTY, null, fetchLiteral);

    // Gathered rows: 4 rows of (firstname, age, city)
    List<Object[]> gatheredRows =
        List.of(
            new Object[] {"Alice", 30L, "Seattle"},
            new Object[] {"Bob", 25L, "Portland"},
            new Object[] {"Charlie", 40L, "Denver"},
            new Object[] {"Dana", 35L, "Austin"});

    List<Object[]> result = CoordinatorTreeExecutor.execute(coordinatorTree, gatheredRows, rowType);

    // LIMIT 2: only first 2 rows
    assertEquals(2, result.size());
    // Verify column order is preserved: [firstname, age, city]
    assertArrayEquals(new Object[] {"Alice", 30L, "Seattle"}, result.get(0));
    assertArrayEquals(new Object[] {"Bob", 25L, "Portland"}, result.get(1));
  }

  /**
   * Single-column test: coordinator tree projects to one field. This exercises the Prefer.ARRAY
   * bare-scalar guard — Calcite returns a bare scalar (not Object[]) when output has exactly one
   * column.
   */
  @Test
  void execute_single_column_project() {
    RelDataType inputRowType =
        TYPE_FAC
            .builder()
            .add(
                "name",
                TYPE_FAC.createTypeWithNullability(
                    TYPE_FAC.createSqlType(SqlTypeName.VARCHAR), true))
            .add(
                "value",
                TYPE_FAC.createTypeWithNullability(
                    TYPE_FAC.createSqlType(SqlTypeName.BIGINT), true))
            .build();

    RelOptCluster cluster = createCluster();
    LogicalTableScan tableScan = buildGatheredRowsScan(cluster, inputRowType);

    // Project: keep only "value" (index 1) — single column output
    RelNode coordinatorTree =
        LogicalProject.create(
            tableScan,
            List.of(),
            List.of(
                REX_BUILDER.makeInputRef(
                    tableScan.getRowType().getFieldList().get(1).getType(), 1)),
            List.of("value"));

    // Gathered rows: 3 rows of (name, value)
    List<Object[]> gatheredRows =
        List.of(new Object[] {"a", 10L}, new Object[] {"b", 20L}, new Object[] {"c", 30L});

    List<Object[]> result =
        CoordinatorTreeExecutor.execute(coordinatorTree, gatheredRows, inputRowType);

    // 3 rows, each with 1 column
    assertEquals(3, result.size());
    assertArrayEquals(new Object[] {10L}, result.get(0));
    assertArrayEquals(new Object[] {20L}, result.get(1));
    assertArrayEquals(new Object[] {30L}, result.get(2));
  }

  /**
   * Regression test for the {@code top} command's coordinator shape. The placement floor makes a
   * Project holding a {@code RexOver} NEEDS_GATHER, so a rank-filter window lands on the
   * coordinator as {@code Project(ROW_NUMBER)} + {@code Filter(rn <= k)} + {@code Project}. The
   * coordinator's HEP pass merges those into ONE {@code LogicalCalc} carrying the {@code RexOver},
   * and nothing can convert such a Calc to EnumerableConvention — Volcano fails with {@code
   * CannotPlanException} ("Missing conversion is LogicalCalc[convention: NONE -&gt; ENUMERABLE]").
   * The window must therefore be extracted into a LogicalWindow BEFORE any Calc merging, exactly as
   * the shard path does in {@link CalciteExecAggregator#optimizeFragment}.
   *
   * <p>Asserts the RESULT, not just that it does not throw: with 3 gathered rows ordered by count
   * descending, {@code rn <= 1} must yield the single highest-count row.
   */
  @Test
  void execute_window_rank_filter_keeps_only_the_top_ranked_row() {
    RelDataType rowType =
        TYPE_FAC
            .builder()
            .add(
                "gender",
                TYPE_FAC.createTypeWithNullability(
                    TYPE_FAC.createSqlType(SqlTypeName.VARCHAR), true))
            .add(
                "count",
                TYPE_FAC.createTypeWithNullability(
                    TYPE_FAC.createSqlType(SqlTypeName.BIGINT), true))
            .build();

    RelOptCluster cluster = createCluster();
    LogicalTableScan tableScan = buildGatheredRowsScan(cluster, rowType);

    RelDataType genderType = tableScan.getRowType().getFieldList().get(0).getType();
    RelDataType countType = tableScan.getRowType().getFieldList().get(1).getType();

    // ROW_NUMBER() OVER (ORDER BY count DESC) — no partition, matching `top N <field>`.
    RexNode rowNumber =
        REX_BUILDER.makeOver(
            TYPE_FAC.createSqlType(SqlTypeName.BIGINT),
            SqlStdOperatorTable.ROW_NUMBER,
            List.of(),
            List.of(),
            ImmutableList.of(
                new RexFieldCollation(
                    REX_BUILDER.makeInputRef(countType, 1), Set.of(SqlKind.DESCENDING))),
            RexWindowBounds.UNBOUNDED_PRECEDING,
            RexWindowBounds.CURRENT_ROW,
            true, // physical (ROWS)
            true, // allowPartial
            false, // nullWhenCountZero
            false // distinct
            );

    RelNode withRank =
        LogicalProject.create(
            tableScan,
            List.of(),
            List.of(
                REX_BUILDER.makeInputRef(genderType, 0),
                REX_BUILDER.makeInputRef(countType, 1),
                rowNumber),
            List.of("gender", "count", "rn"));

    RelNode rankFilter =
        LogicalFilter.create(
            withRank,
            REX_BUILDER.makeCall(
                SqlStdOperatorTable.LESS_THAN_OR_EQUAL,
                REX_BUILDER.makeInputRef(withRank.getRowType().getFieldList().get(2).getType(), 2),
                REX_BUILDER.makeLiteral(1, TYPE_FAC.createSqlType(SqlTypeName.INTEGER), false)));

    // Drop the rank column, as the PPL lowering does.
    RelNode coordinatorTree =
        LogicalProject.create(
            rankFilter,
            List.of(),
            List.of(
                REX_BUILDER.makeInputRef(genderType, 0), REX_BUILDER.makeInputRef(countType, 1)),
            List.of("gender", "count"));

    List<Object[]> gatheredRows =
        List.of(new Object[] {"M", 3L}, new Object[] {"F", 7L}, new Object[] {"U", 1L});

    List<Object[]> result = CoordinatorTreeExecutor.execute(coordinatorTree, gatheredRows, rowType);

    assertEquals(1, result.size());
    assertArrayEquals(new Object[] {"F", 7L}, result.get(0));
  }

  /**
   * Builds a LogicalTableScan over the gathered-rows table (matching what StagePlanner produces).
   * Uses GATHERED_ROWS_STASH_KEY so CoordinatorTreeExecutor's DataContext will find the rows.
   */
  private static LogicalTableScan buildGatheredRowsScan(
      RelOptCluster cluster, RelDataType rowType) {
    SchemaPlus rootSchema = Frameworks.createRootSchema(false);
    SchemaPlus osSchema = rootSchema.add(RelFragmentCodec.SCHEMA_NAME, new AbstractSchema() {});
    osSchema.add(
        "_gathered_rows",
        new RelFragmentCodec.ShardRowSourceTable(rowType, StagePlanner.GATHERED_ROWS_STASH_KEY));

    CalciteSchema calciteSchema = CalciteSchema.from(rootSchema);
    CalciteCatalogReader catalogReader =
        new CalciteCatalogReader(
            calciteSchema, List.of(), TYPE_FAC, new CalciteConnectionConfigImpl(new Properties()));
    RelOptTable table =
        catalogReader.getTableForMember(List.of(RelFragmentCodec.SCHEMA_NAME, "_gathered_rows"));
    return LogicalTableScan.create(cluster, table, List.of());
  }

  /**
   * Defect 1 regression test: when the shard fragment is a Project over the scan (narrower row type
   * than the scan), the gathered-rows row type must be the FRAGMENT's output type, not the scan's.
   * If the wrong row type is used, the coordinator reads wrong column indices and produces wrong
   * results.
   *
   * <p>Scenario: scan has (firstname, age, city) but the fragment projects (firstname, age) only.
   * Gathered rows are 2-column. Coordinator tree applies LIMIT 1. Asserts 2 output columns, not 3.
   */
  @Test
  void execute_with_narrower_fragment_row_type_uses_fragment_not_scan() {
    // The fragment's OUTPUT row type (2 columns only — what gathered rows actually have)
    RelDataType fragmentOutputRowType =
        TYPE_FAC
            .builder()
            .add(
                "firstname",
                TYPE_FAC.createTypeWithNullability(
                    TYPE_FAC.createSqlType(SqlTypeName.VARCHAR), true))
            .add(
                "age",
                TYPE_FAC.createTypeWithNullability(
                    TYPE_FAC.createSqlType(SqlTypeName.BIGINT), true))
            .build();

    RelOptCluster cluster = createCluster();
    LogicalTableScan tableScan = buildGatheredRowsScan(cluster, fragmentOutputRowType);

    // Coordinator tree: LIMIT 1 (a Sort with no collation, just fetch)
    RexNode fetchLiteral =
        REX_BUILDER.makeLiteral(1, TYPE_FAC.createSqlType(SqlTypeName.INTEGER), false);
    RelNode coordinatorTree =
        LogicalSort.create(tableScan, RelCollations.EMPTY, null, fetchLiteral);

    // Gathered rows are 2-column (fragment output), NOT 3-column (scan output)
    List<Object[]> gatheredRows = List.of(new Object[] {"Alice", 30L}, new Object[] {"Bob", 25L});

    // Execute with the FRAGMENT row type (2 columns)
    List<Object[]> result =
        CoordinatorTreeExecutor.execute(coordinatorTree, gatheredRows, fragmentOutputRowType);

    // LIMIT 1 → 1 row, 2 columns
    assertEquals(1, result.size());
    assertEquals(2, result.get(0).length);
    assertArrayEquals(new Object[] {"Alice", 30L}, result.get(0));
  }
}
