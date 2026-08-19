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

import com.google.common.collect.ImmutableList;
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
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.core.Sort;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalSort;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.logical.LogicalUnion;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexProgram;
import org.apache.calcite.rex.RexProgramBuilder;
import org.apache.calcite.rex.RexWindowBounds;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.util.ImmutableBitSet;
import org.junit.jupiter.api.DisplayNameGeneration;
import org.junit.jupiter.api.DisplayNameGenerator;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.calcite.plan.rel.LogicalGraphLookup;
import org.opensearch.sql.calcite.plan.rel.LogicalSystemLimit;
import org.opensearch.sql.opensearch.request.OpenSearchRequest;
import org.opensearch.sql.opensearch.storage.OpenSearchIndex;
import org.opensearch.sql.opensearch.storage.scan.AbstractCalciteIndexScan;
import org.opensearch.sql.opensearch.storage.scan.CalciteLogicalIndexScan;

@DisplayNameGeneration(DisplayNameGenerator.ReplaceUnderscores.class)
public class StagePlannerTest {

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

  // --- Placement tests: SHARD_LOCAL ---

  @Test
  void scan_is_shard_local() {
    AbstractCalciteIndexScan scan = buildIndexScan();
    StagePlan result = StagePlanner.split(scan);

    assertTrue(result.staged());
    assertNotNull(result.shardFragment());
    assertEquals(CombineDescriptor.concat(), result.combine());
    assertEquals(scan, result.shardScan());
  }

  @Test
  void project_over_scan_is_shard_local() {
    AbstractCalciteIndexScan scan = buildIndexScan();
    RelNode project =
        LogicalProject.create(
            scan,
            List.of(),
            List.of(REX_BUILDER.makeInputRef(scan.getRowType().getFieldList().get(0).getType(), 0)),
            List.of("account_number"));

    StagePlan result = StagePlanner.split(project);

    assertTrue(result.staged());
    // The cut is the project (highest SHARD_LOCAL)
    assertEquals(project.getRowType(), result.shardFragment().getRowType());
  }

  @Test
  void filter_over_scan_is_shard_local() {
    AbstractCalciteIndexScan scan = buildIndexScan();
    RexNode condition =
        REX_BUILDER.makeCall(
            SqlStdOperatorTable.GREATER_THAN,
            REX_BUILDER.makeInputRef(scan.getRowType().getFieldList().get(1).getType(), 1),
            REX_BUILDER.makeLiteral(10, TYPE_FAC.createSqlType(SqlTypeName.INTEGER), false));
    RelNode filter = LogicalFilter.create(scan, condition);

    StagePlan result = StagePlanner.split(filter);

    assertTrue(result.staged());
    assertEquals(filter.getRowType(), result.shardFragment().getRowType());
  }

  @Test
  void calc_over_scan_is_shard_local() {
    AbstractCalciteIndexScan scan = buildIndexScan();
    RexProgramBuilder programBuilder = new RexProgramBuilder(scan.getRowType(), REX_BUILDER);
    programBuilder.addProject(
        REX_BUILDER.makeInputRef(scan.getRowType().getFieldList().get(0).getType(), 0),
        "account_number");
    RexProgram program = programBuilder.getProgram();
    RelNode calc = LogicalCalc.create(scan, program);

    StagePlan result = StagePlanner.split(calc);

    assertTrue(result.staged());
    assertEquals(calc.getRowType(), result.shardFragment().getRowType());
  }

  // --- Placement tests: NEEDS_GATHER defaults ---

  @Test
  void aggregate_defaults_to_needs_gather() {
    AbstractCalciteIndexScan scan = buildIndexScan();
    RelNode agg = LogicalAggregate.create(scan, List.of(), ImmutableBitSet.of(0), null, List.of());

    StagePlan result = StagePlanner.split(agg);

    assertTrue(result.staged());
    // The cut is NOT the aggregate — it falls to the scan
    assertEquals(scan.getRowType(), result.shardFragment().getRowType());
  }

  @Test
  void project_with_rexOver_over_scan_needs_gather() {
    // A Project containing a RexOver (e.g. ROW_NUMBER() from dedup/eventstats) is NOT
    // distribution-preserving — per-shard window numbering restarts independently.
    AbstractCalciteIndexScan scan = buildIndexScan();
    RelDataType bigintType = TYPE_FAC.createSqlType(SqlTypeName.BIGINT);
    RexNode rowNumber =
        REX_BUILDER.makeOver(
            bigintType,
            SqlStdOperatorTable.ROW_NUMBER,
            List.of(),
            List.of(REX_BUILDER.makeInputRef(scan.getRowType().getFieldList().get(0).getType(), 0)),
            ImmutableList.of(),
            RexWindowBounds.UNBOUNDED_PRECEDING,
            RexWindowBounds.CURRENT_ROW,
            true,
            true,
            false,
            false);
    RelNode windowProject =
        LogicalProject.create(
            scan,
            List.of(),
            List.of(
                REX_BUILDER.makeInputRef(scan.getRowType().getFieldList().get(0).getType(), 0),
                rowNumber),
            List.of("account_number", "rn"));

    StagePlan result = StagePlanner.split(windowProject);

    assertTrue(result.staged());
    // The cut falls to the scan — the RexOver-bearing Project is NEEDS_GATHER
    assertEquals(scan.getRowType(), result.shardFragment().getRowType());
  }

  @Test
  void calc_with_rexOver_over_scan_needs_gather() {
    // A Calc whose RexProgram contains a RexOver is NOT distribution-preserving.
    AbstractCalciteIndexScan scan = buildIndexScan();
    RelDataType bigintType = TYPE_FAC.createSqlType(SqlTypeName.BIGINT);
    RexNode rowNumber =
        REX_BUILDER.makeOver(
            bigintType,
            SqlStdOperatorTable.ROW_NUMBER,
            List.of(),
            List.of(REX_BUILDER.makeInputRef(scan.getRowType().getFieldList().get(0).getType(), 0)),
            ImmutableList.of(),
            RexWindowBounds.UNBOUNDED_PRECEDING,
            RexWindowBounds.CURRENT_ROW,
            true,
            true,
            false,
            false);
    RexProgramBuilder programBuilder = new RexProgramBuilder(scan.getRowType(), REX_BUILDER);
    programBuilder.addProject(
        REX_BUILDER.makeInputRef(scan.getRowType().getFieldList().get(0).getType(), 0),
        "account_number");
    programBuilder.addProject(rowNumber, "rn");
    RexProgram program = programBuilder.getProgram();
    RelNode calc = LogicalCalc.create(scan, program);

    StagePlan result = StagePlanner.split(calc);

    assertTrue(result.staged());
    // The cut falls to the scan — the RexOver-bearing Calc is NEEDS_GATHER
    assertEquals(scan.getRowType(), result.shardFragment().getRowType());
  }

  @Test
  void join_defaults_to_needs_gather() {
    AbstractCalciteIndexScan scan1 = buildIndexScan();
    RelNode plainScan = buildTableScan();
    RelNode join =
        LogicalJoin.create(
            scan1,
            plainScan,
            List.of(),
            REX_BUILDER.makeLiteral(true),
            java.util.Set.of(),
            JoinRelType.INNER);

    // Two scans (one AbstractCalciteIndexScan + one plain) → but collectScans finds only 1
    // AbstractCalciteIndexScan, so it IS staged with cut at scan1
    StagePlan result = StagePlanner.split(join);
    assertTrue(result.staged());
    // The join is NEEDS_GATHER, so the cut is scan1
    assertEquals(scan1.getRowType(), result.shardFragment().getRowType());
  }

  @Test
  void sort_defaults_to_needs_gather() {
    AbstractCalciteIndexScan scan = buildIndexScan();
    RelNode project =
        LogicalProject.create(
            scan,
            List.of(),
            List.of(REX_BUILDER.makeInputRef(scan.getRowType().getFieldList().get(0).getType(), 0)),
            List.of("account_number"));
    RelNode sort =
        LogicalSort.create(
            project,
            RelCollations.EMPTY,
            null,
            REX_BUILDER.makeLiteral(10, TYPE_FAC.createSqlType(SqlTypeName.INTEGER), false));

    StagePlan result = StagePlanner.split(sort);

    assertTrue(result.staged());
    // Cut is the project (highest SHARD_LOCAL); sort stays on coordinator
    assertEquals(project.getRowType(), result.shardFragment().getRowType());
  }

  // --- Cut position and LogicalSystemLimit ---

  @Test
  void logicalSystemLimit_project_filter_scan_cuts_at_project() {
    AbstractCalciteIndexScan scan = buildIndexScan();
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
    RelNode limit =
        LogicalSystemLimit.create(
            LogicalSystemLimit.SystemLimitType.QUERY_SIZE_LIMIT,
            project,
            REX_BUILDER.makeLiteral(10000, TYPE_FAC.createSqlType(SqlTypeName.INTEGER), false));

    StagePlan result = StagePlanner.split(limit);

    assertTrue(result.staged());
    // Cut at project (highest SHARD_LOCAL); LogicalSystemLimit on coordinator
    assertEquals(project.getRowType(), result.shardFragment().getRowType());
    // Coordinator tree's root is a Sort (LogicalSystemLimit)
    assertTrue(result.coordinatorTree() instanceof Sort);
  }

  // --- CONCAT descriptor ---

  @Test
  void split_produces_concat_descriptor() {
    AbstractCalciteIndexScan scan = buildIndexScan();
    RelNode project =
        LogicalProject.create(
            scan,
            List.of(),
            List.of(REX_BUILDER.makeInputRef(scan.getRowType().getFieldList().get(0).getType(), 0)),
            List.of("account_number"));

    StagePlan result = StagePlanner.split(project);

    assertEquals(CombineDescriptor.concat(), result.combine());
    assertEquals(CombineDescriptor.Mode.CONCAT, result.combine().getMode());
  }

  // --- Coordinator tree gathered-rows leaf row type ---

  @Test
  void coordinator_tree_gathered_rows_leaf_has_correct_row_type() {
    AbstractCalciteIndexScan scan = buildIndexScan();
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
    RelNode limit =
        LogicalSystemLimit.create(
            LogicalSystemLimit.SystemLimitType.QUERY_SIZE_LIMIT,
            project,
            REX_BUILDER.makeLiteral(10000, TYPE_FAC.createSqlType(SqlTypeName.INTEGER), false));

    StagePlan result = StagePlanner.split(limit);

    // Find the leaf of the coordinator tree — it should be a LogicalTableScan
    RelNode leaf = findLeaf(result.coordinatorTree());
    assertTrue(leaf instanceof LogicalTableScan);
    // Its row type must match the cut node's (project's) row type
    assertEquals(project.getRowType(), leaf.getRowType());
  }

  // --- ShardFragment RelJson round-trip ---

  @Test
  void shardFragment_round_trips_through_relFragmentCodec() {
    AbstractCalciteIndexScan scan = buildIndexScan();
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

    StagePlan result = StagePlanner.split(project);
    String beforePlan = RelOptUtil.toString(result.shardFragment());

    // Serialize and deserialize
    String encoded = RelFragmentCodec.serialize(result.shardFragment());
    RelNode deserialized =
        RelFragmentCodec.deserialize(
            encoded,
            INDEX_NAME,
            List.of(
                new CalciteExecAggregationBuilder.FieldDescriptor("account_number", "long"),
                new CalciteExecAggregationBuilder.FieldDescriptor("age", "integer"),
                new CalciteExecAggregationBuilder.FieldDescriptor("gender", "keyword")));

    String afterPlan = RelOptUtil.toString(deserialized);
    assertEquals(beforePlan, afterPlan);
  }

  // --- Zero-scan and two-scan coordinator-only cases ---

  @Test
  void zero_scans_returns_coordinator_only() {
    // A plan with no AbstractCalciteIndexScan (plain LogicalTableScan)
    RelNode plainScan = buildTableScan();
    RelNode project =
        LogicalProject.create(
            plainScan,
            List.of(),
            List.of(
                REX_BUILDER.makeInputRef(
                    plainScan.getRowType().getFieldList().get(0).getType(), 0)),
            List.of("account_number"));

    StagePlan result = StagePlanner.split(project);

    assertFalse(result.staged());
    assertNull(result.shardFragment());
    assertNull(result.shardScan());
    assertNull(result.combine());
    assertEquals(project, result.coordinatorTree());
  }

  @Test
  void two_scans_returns_coordinator_only() {
    AbstractCalciteIndexScan scan1 = buildIndexScan();
    AbstractCalciteIndexScan scan2 = buildIndexScan();
    RelNode join =
        LogicalJoin.create(
            scan1,
            scan2,
            List.of(),
            REX_BUILDER.makeLiteral(true),
            java.util.Set.of(),
            JoinRelType.INNER);

    StagePlan result = StagePlanner.split(join);

    assertFalse(result.staged());
    assertNull(result.shardFragment());
    assertNull(result.shardScan());
    assertEquals(join, result.coordinatorTree());
  }

  // --- Totality test ---

  @Test
  void split_is_total_for_all_operator_types() {
    // Build a plan containing: Aggregate, Window (RexOver in Project), Join, Sort,
    // Union, Filter, Project, Calc, LogicalSystemLimit, LogicalGraphLookup
    AbstractCalciteIndexScan scan = buildIndexScan();

    // Filter
    RexNode condition =
        REX_BUILDER.makeCall(
            SqlStdOperatorTable.GREATER_THAN,
            REX_BUILDER.makeInputRef(scan.getRowType().getFieldList().get(1).getType(), 1),
            REX_BUILDER.makeLiteral(10, TYPE_FAC.createSqlType(SqlTypeName.INTEGER), false));
    RelNode filter = LogicalFilter.create(scan, condition);

    // Project
    RelNode project =
        LogicalProject.create(
            filter,
            List.of(),
            List.of(
                REX_BUILDER.makeInputRef(filter.getRowType().getFieldList().get(0).getType(), 0),
                REX_BUILDER.makeInputRef(filter.getRowType().getFieldList().get(1).getType(), 1),
                REX_BUILDER.makeInputRef(filter.getRowType().getFieldList().get(2).getType(), 2)),
            List.of("account_number", "age", "gender"));

    // Calc
    RexProgramBuilder programBuilder = new RexProgramBuilder(project.getRowType(), REX_BUILDER);
    programBuilder.addProject(
        REX_BUILDER.makeInputRef(project.getRowType().getFieldList().get(0).getType(), 0),
        "account_number");
    programBuilder.addProject(
        REX_BUILDER.makeInputRef(project.getRowType().getFieldList().get(2).getType(), 2),
        "gender");
    RexProgram program = programBuilder.getProgram();
    RelNode calc = LogicalCalc.create(project, program);

    // Aggregate (NEEDS_GATHER)
    RelNode agg = LogicalAggregate.create(calc, List.of(), ImmutableBitSet.of(1), null, List.of());

    // Sort (NEEDS_GATHER)
    RelNode sort =
        LogicalSort.create(
            agg,
            RelCollations.EMPTY,
            null,
            REX_BUILDER.makeLiteral(10, TYPE_FAC.createSqlType(SqlTypeName.INTEGER), false));

    // Union (NEEDS_GATHER) — needs two inputs with same row type
    RelNode union = LogicalUnion.create(List.of(sort, sort), true);

    // Window-like project with RexOver (acts like a window)
    RelDataType bigintType = TYPE_FAC.createSqlType(SqlTypeName.BIGINT);
    RexNode rowNumber =
        REX_BUILDER.makeOver(
            bigintType,
            SqlStdOperatorTable.ROW_NUMBER,
            List.of(),
            List.of(
                REX_BUILDER.makeInputRef(union.getRowType().getFieldList().get(0).getType(), 0)),
            ImmutableList.of(),
            RexWindowBounds.UNBOUNDED_PRECEDING,
            RexWindowBounds.CURRENT_ROW,
            true,
            true,
            false,
            false);
    RelNode windowProject =
        LogicalProject.create(
            union,
            List.of(),
            List.of(
                REX_BUILDER.makeInputRef(union.getRowType().getFieldList().get(0).getType(), 0),
                rowNumber),
            List.of("gender", "rn"));

    // Join (NEEDS_GATHER) — join with a plain scan
    RelNode plainScan = buildTableScanWithType(windowProject.getRowType());
    RelNode join =
        LogicalJoin.create(
            windowProject,
            plainScan,
            List.of(),
            REX_BUILDER.makeLiteral(true),
            java.util.Set.of(),
            JoinRelType.INNER);

    // LogicalGraphLookup (NEEDS_GATHER) — needs two inputs
    RelNode graphLookupScan = buildTableScan();
    RelNode graphLookup =
        LogicalGraphLookup.create(
            join,
            graphLookupScan,
            "account_number",
            null,
            "from_field",
            "to_field",
            "output_field",
            null,
            3,
            false,
            false,
            false,
            false,
            null);

    // LogicalSystemLimit at the top (Sort subclass → NEEDS_GATHER)
    RelNode limit =
        LogicalSystemLimit.create(
            LogicalSystemLimit.SystemLimitType.QUERY_SIZE_LIMIT,
            graphLookup,
            REX_BUILDER.makeLiteral(10000, TYPE_FAC.createSqlType(SqlTypeName.INTEGER), false));

    // The plan has only ONE AbstractCalciteIndexScan, so split() must succeed
    StagePlan result = StagePlanner.split(limit);
    assertNotNull(result);
    assertTrue(result.staged());
    assertNotNull(result.shardFragment());
    assertNotNull(result.coordinatorTree());
    assertNotNull(result.combine());
    assertNotNull(result.shardScan());
  }

  // --- forcingOperator tests (US-009) ---

  @Test
  void window_project_over_scan_records_window_as_forcing_operator() {
    // Build a REAL window: Project(account_number, ROW_NUMBER() OVER(PARTITION BY account_number))
    // over scan. The Project with RexOver is NEEDS_GATHER, so cut falls to scan, and the
    // forcing operator is the Project with RexOver (its immediate parent of cut=scan).
    AbstractCalciteIndexScan scan = buildIndexScan();
    RelDataType bigintType = TYPE_FAC.createSqlType(SqlTypeName.BIGINT);
    RexNode rowNumber =
        REX_BUILDER.makeOver(
            bigintType,
            SqlStdOperatorTable.ROW_NUMBER,
            List.of(),
            List.of(REX_BUILDER.makeInputRef(scan.getRowType().getFieldList().get(0).getType(), 0)),
            ImmutableList.of(),
            RexWindowBounds.UNBOUNDED_PRECEDING,
            RexWindowBounds.CURRENT_ROW,
            true,
            true,
            false,
            false);
    RelNode windowProject =
        LogicalProject.create(
            scan,
            List.of(),
            List.of(
                REX_BUILDER.makeInputRef(scan.getRowType().getFieldList().get(0).getType(), 0),
                rowNumber),
            List.of("account_number", "rn"));

    StagePlan result = StagePlanner.split(windowProject);

    assertTrue(result.staged());
    // Cut is scan; forcing operator is the parent of scan = the RexOver-bearing LogicalProject
    assertEquals("LogicalProject[window]", result.forcingOperator());
  }

  @Test
  void window_project_forcing_operator_name_includes_window_qualifier() {
    // Explicit test: a window-bearing Project's forcingOperator name includes [window]
    AbstractCalciteIndexScan scan = buildIndexScan();
    RelDataType bigintType = TYPE_FAC.createSqlType(SqlTypeName.BIGINT);
    RexNode rowNumber =
        REX_BUILDER.makeOver(
            bigintType,
            SqlStdOperatorTable.ROW_NUMBER,
            List.of(),
            List.of(REX_BUILDER.makeInputRef(scan.getRowType().getFieldList().get(0).getType(), 0)),
            ImmutableList.of(),
            RexWindowBounds.UNBOUNDED_PRECEDING,
            RexWindowBounds.CURRENT_ROW,
            true,
            true,
            false,
            false);
    RelNode windowProject =
        LogicalProject.create(
            scan,
            List.of(),
            List.of(
                REX_BUILDER.makeInputRef(scan.getRowType().getFieldList().get(0).getType(), 0),
                rowNumber),
            List.of("account_number", "rn"));

    StagePlan result = StagePlanner.split(windowProject);

    assertTrue(result.staged());
    assertEquals("LogicalProject[window]", result.forcingOperator());
  }

  @Test
  void aggregate_over_scan_records_aggregate_as_forcing_operator() {
    AbstractCalciteIndexScan scan = buildIndexScan();
    RelNode agg = LogicalAggregate.create(scan, List.of(), ImmutableBitSet.of(0), null, List.of());

    StagePlan result = StagePlanner.split(agg);

    assertTrue(result.staged());
    // Cut is scan; forcing operator is the aggregate (immediate parent of scan)
    assertEquals("LogicalAggregate", result.forcingOperator());
  }

  @Test
  void fully_shard_local_plan_has_null_forcing_operator() {
    // A plan that is fully shard-local: just a Project over scan — the cut IS the root
    AbstractCalciteIndexScan scan = buildIndexScan();
    RelNode project =
        LogicalProject.create(
            scan,
            List.of(),
            List.of(REX_BUILDER.makeInputRef(scan.getRowType().getFieldList().get(0).getType(), 0)),
            List.of("account_number"));

    StagePlan result = StagePlanner.split(project);

    assertTrue(result.staged());
    // Cut is the project which IS the root → forcingOperator is null
    assertNull(result.forcingOperator());
  }

  @Test
  void sort_above_project_scan_records_sort_as_forcing_operator() {
    AbstractCalciteIndexScan scan = buildIndexScan();
    RelNode project =
        LogicalProject.create(
            scan,
            List.of(),
            List.of(REX_BUILDER.makeInputRef(scan.getRowType().getFieldList().get(0).getType(), 0)),
            List.of("account_number"));
    RelNode sort =
        LogicalSort.create(
            project,
            RelCollations.EMPTY,
            null,
            REX_BUILDER.makeLiteral(10, TYPE_FAC.createSqlType(SqlTypeName.INTEGER), false));

    StagePlan result = StagePlanner.split(sort);

    assertTrue(result.staged());
    // Cut is project (highest SHARD_LOCAL); parent of project is the sort
    assertEquals("LogicalSort", result.forcingOperator());
  }

  // --- Helper methods ---

  private AbstractCalciteIndexScan buildIndexScan() {
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
    osSchema.add("plain_table", new RelFragmentCodec.ShardRowSourceTable(rowType));

    CalciteSchema calciteSchema = CalciteSchema.from(rootSchema);
    CalciteCatalogReader catalogReader =
        new CalciteCatalogReader(
            calciteSchema, List.of(), TYPE_FAC, new CalciteConnectionConfigImpl(new Properties()));
    RelOptTable table =
        catalogReader.getTableForMember(List.of(RelFragmentCodec.SCHEMA_NAME, "plain_table"));
    return LogicalTableScan.create(CLUSTER, table, List.of());
  }

  private RelNode buildTableScanWithType(RelDataType rowType) {
    SchemaPlus rootSchema = Frameworks.createRootSchema(false);
    SchemaPlus osSchema = rootSchema.add(RelFragmentCodec.SCHEMA_NAME, new AbstractSchema() {});
    osSchema.add("typed_table", new RelFragmentCodec.ShardRowSourceTable(rowType));

    CalciteSchema calciteSchema = CalciteSchema.from(rootSchema);
    CalciteCatalogReader catalogReader =
        new CalciteCatalogReader(
            calciteSchema, List.of(), TYPE_FAC, new CalciteConnectionConfigImpl(new Properties()));
    RelOptTable table =
        catalogReader.getTableForMember(List.of(RelFragmentCodec.SCHEMA_NAME, "typed_table"));
    return LogicalTableScan.create(CLUSTER, table, List.of());
  }

  private RelNode findLeaf(RelNode node) {
    if (node.getInputs().isEmpty()) {
      return node;
    }
    return findLeaf(node.getInputs().get(0));
  }
}
