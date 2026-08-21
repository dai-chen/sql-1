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
import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
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
import org.apache.calcite.rel.rules.CoreRules;
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
    // After US-012: a pure GROUP BY (no agg calls) is splittable — MERGE_AGG fires, and the
    // Aggregate becomes the shardFragment root. Its row type is the Aggregate's output type.
    assertEquals(CombineDescriptor.Mode.MERGE_AGG, result.combine().getMode());
    assertEquals(agg.getRowType(), result.shardFragment().getRowType());
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

  /**
   * Proves the generic placement floor still classifies a Sort as NEEDS_GATHER independently of the
   * LIMIT promotion path. This shape cannot be promoted to LIMIT because it has a non-empty
   * collation (an ordered sort). Without this test, nothing standalone guards the invariant that
   * Sort → NEEDS_GATHER in the generic floor — the repurposed tests above only exercise the LIMIT
   * promotion shortcut. If this test is ever "repurposed", the placement floor is unproven.
   */
  @Test
  void ordered_sort_above_project_scan_is_needs_gather_with_concat() {
    AbstractCalciteIndexScan scan = buildIndexScan();
    RelNode project =
        LogicalProject.create(
            scan,
            List.of(),
            List.of(REX_BUILDER.makeInputRef(scan.getRowType().getFieldList().get(0).getType(), 0)),
            List.of("account_number"));
    // Non-empty collation: ORDER BY account_number — this cannot be promoted to LIMIT
    RelFieldCollation fieldCollation = new RelFieldCollation(0);
    RelNode sort = LogicalSort.create(project, RelCollations.of(fieldCollation), null, null);

    StagePlan result = StagePlanner.split(sort);

    assertTrue(result.staged());
    // The Sort is NEEDS_GATHER → cut falls to project (the highest SHARD_LOCAL)
    assertEquals(CombineDescriptor.Mode.CONCAT, result.combine().getMode());
    // The shardFragment's row type is the project's (below the Sort)
    assertEquals(project.getRowType(), result.shardFragment().getRowType());
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
  void unordered_fetch_above_project_is_promoted_to_limit() {
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
    // US-013: unordered Sort with fetch directly above a shard-local node → LIMIT promotion
    assertEquals(CombineDescriptor.Mode.LIMIT, result.combine().getMode());
    // Cut is the project (highest SHARD_LOCAL); sort's row type == project's row type
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
    // US-013: LogicalSystemLimit is a Sort with empty collation and fetch, directly above cut
    // (project). Limit promotion fires → LIMIT mode, coordinator tree is just gathered-rows scan.
    assertEquals(CombineDescriptor.Mode.LIMIT, result.combine().getMode());
    assertEquals(10000, result.combine().getIntParam());
    // Coordinator tree is the gathered-rows LogicalTableScan (no Sort wrapper)
    assertTrue(result.coordinatorTree() instanceof LogicalTableScan);
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

  // --- DAG-sharing hazard: shared scan instance must NOT be staged ---

  @Test
  void shared_scan_instance_in_union_returns_coordinator_only() {
    // When the SAME AbstractCalciteIndexScan instance is used as input to two parents with
    // DIFFERENT operators above it (e.g. different filters in a union), the plan cannot be
    // cleanly staged: only one branch's subtree matches the cut node, leaving a residual live
    // scan in the coordinator tree. The post-split validation catches this.
    AbstractCalciteIndexScan scan = buildIndexScan();
    RexNode condYoung =
        REX_BUILDER.makeCall(
            SqlStdOperatorTable.LESS_THAN,
            REX_BUILDER.makeInputRef(scan.getRowType().getFieldList().get(1).getType(), 1),
            REX_BUILDER.makeLiteral(30, TYPE_FAC.createSqlType(SqlTypeName.INTEGER), false));
    RexNode condOld =
        REX_BUILDER.makeCall(
            SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
            REX_BUILDER.makeInputRef(scan.getRowType().getFieldList().get(1).getType(), 1),
            REX_BUILDER.makeLiteral(30, TYPE_FAC.createSqlType(SqlTypeName.INTEGER), false));
    RelNode branch1 = LogicalFilter.create(scan, condYoung);
    RelNode branch2 = LogicalFilter.create(scan, condOld);
    RelNode union = LogicalUnion.create(List.of(branch1, branch2), true);

    StagePlan result = StagePlanner.split(union);

    assertFalse(result.staged());
    assertNull(result.shardFragment());
    assertNull(result.shardScan());
    assertEquals(union, result.coordinatorTree());
  }

  @Test
  void union_over_identical_scans_after_hep_pass_returns_coordinator_only() {
    // Regression test for US-012: HepPlanner's AGGREGATE_REDUCE_FUNCTIONS pass can intern
    // structurally identical subtrees into a single shared RelNode instance (DAG). When a UNION
    // has two branches scanning the same index, the HEP pass may collapse them into one shared
    // scan. The single-gather-boundary guard must still detect two occurrences and refuse to
    // stage. This test mirrors the exact sequence splitIfStaged() uses.
    AbstractCalciteIndexScan scan1 = buildIndexScan();
    AbstractCalciteIndexScan scan2 = buildIndexScan();
    // Build: Agg(COUNT()) over Union(Filter(<30, scan1), Filter(>=30, scan2))
    RexNode condYoung =
        REX_BUILDER.makeCall(
            SqlStdOperatorTable.LESS_THAN,
            REX_BUILDER.makeInputRef(scan1.getRowType().getFieldList().get(1).getType(), 1),
            REX_BUILDER.makeLiteral(30, TYPE_FAC.createSqlType(SqlTypeName.INTEGER), false));
    RexNode condOld =
        REX_BUILDER.makeCall(
            SqlStdOperatorTable.GREATER_THAN_OR_EQUAL,
            REX_BUILDER.makeInputRef(scan2.getRowType().getFieldList().get(1).getType(), 1),
            REX_BUILDER.makeLiteral(30, TYPE_FAC.createSqlType(SqlTypeName.INTEGER), false));
    RelNode branch1 = LogicalFilter.create(scan1, condYoung);
    RelNode branch2 = LogicalFilter.create(scan2, condOld);
    RelNode union = LogicalUnion.create(List.of(branch1, branch2), true);
    RelNode agg =
        LogicalAggregate.create(
            union,
            List.of(),
            ImmutableBitSet.of(2),
            null,
            List.of(
                AggregateCall.create(
                    SqlStdOperatorTable.COUNT,
                    false,
                    false,
                    false,
                    List.of(),
                    List.of(),
                    -1,
                    null,
                    RelCollations.EMPTY,
                    TYPE_FAC.createSqlType(SqlTypeName.BIGINT),
                    "cnt")));

    // Apply the same HEP pass that splitIfStaged uses
    org.apache.calcite.plan.hep.HepProgramBuilder hepBuilder =
        new org.apache.calcite.plan.hep.HepProgramBuilder();
    hepBuilder.addRuleInstance(CoreRules.AGGREGATE_REDUCE_FUNCTIONS);
    org.apache.calcite.plan.hep.HepPlanner hepPlanner =
        new org.apache.calcite.plan.hep.HepPlanner(hepBuilder.build());
    hepPlanner.setRoot(agg);
    RelNode reduced = hepPlanner.findBestExp();

    StagePlan result = StagePlanner.split(reduced);

    assertFalse(
        result.staged(),
        "A UNION with two scan occurrences (even if HEP-shared) must NOT be staged");
    assertNull(result.shardFragment());
    assertNull(result.shardScan());
  }

  @Test
  void shared_cut_subtree_in_self_join_returns_coordinator_only() {
    // A self-join where HEP interning causes both branches to share the SAME scan instance.
    // collectScans counts it once (identity dedup), so scans.size()==1 passes, but the cut node
    // is reachable from two parents — staging would conflate independent join inputs.
    AbstractCalciteIndexScan scan = buildIndexScan();
    RelNode join =
        LogicalJoin.create(
            scan,
            scan,
            List.of(),
            REX_BUILDER.makeCall(
                SqlStdOperatorTable.EQUALS,
                REX_BUILDER.makeInputRef(scan.getRowType().getFieldList().get(0).getType(), 0),
                REX_BUILDER.makeInputRef(
                    scan.getRowType().getFieldList().get(0).getType(),
                    scan.getRowType().getFieldCount())),
            java.util.Set.of(),
            JoinRelType.INNER);

    StagePlan result = StagePlanner.split(join);

    assertFalse(result.staged(), "A shared cut subtree (self-join) must NOT be staged");
    assertNull(result.shardFragment());
    assertNull(result.shardScan());
    assertEquals(join, result.coordinatorTree());
  }

  @Test
  void shared_unordered_fetch_under_join_dag_is_not_staged_as_limit() {
    // Regression test for defect 1a (US-013 DAG shared-subtree gap): when HEP interning makes
    // two structurally identical "head N" subtrees share ONE LogicalSort instance that feeds
    // BOTH inputs of a LogicalJoin, the limit promotion must NOT fire. Before the fix, the guard
    // checked cutNode (which has exactly one parent — the sortNode), missed that sortNode itself
    // was shared, and replaceSubtree collapsed both join inputs into one gathered-rows scan.
    AbstractCalciteIndexScan scan = buildIndexScan();
    RelNode project =
        LogicalProject.create(
            scan,
            List.of(),
            List.of(REX_BUILDER.makeInputRef(scan.getRowType().getFieldList().get(0).getType(), 0)),
            List.of("account_number"));
    // ONE Sort instance — this is what HEP interning produces
    RelNode sortNode =
        LogicalSort.create(
            project,
            RelCollations.EMPTY,
            null,
            REX_BUILDER.makeLiteral(5, TYPE_FAC.createSqlType(SqlTypeName.INTEGER), false));
    // Pass the SAME sortNode as BOTH inputs of a join → DAG (shared subtree)
    RelNode join =
        LogicalJoin.create(
            sortNode,
            sortNode,
            List.of(),
            REX_BUILDER.makeCall(
                SqlStdOperatorTable.EQUALS,
                REX_BUILDER.makeInputRef(sortNode.getRowType().getFieldList().get(0).getType(), 0),
                REX_BUILDER.makeInputRef(
                    sortNode.getRowType().getFieldList().get(0).getType(),
                    sortNode.getRowType().getFieldCount())),
            java.util.Set.of(),
            JoinRelType.INNER);

    StagePlan result = StagePlanner.split(join);

    // The plan must NOT be staged with LIMIT. Either it's coordinator-only or it's CONCAT
    // at the scan level — never LIMIT with the shared Sort promoted.
    if (result.staged()) {
      assertFalse(
          result.combine().getMode() == CombineDescriptor.Mode.LIMIT,
          "A shared Sort (unordered fetch) under a join must NOT produce LIMIT combine — "
              + "staging conflates independent join inputs");
    } else {
      // If not staged at all, coordinator-only is also acceptable
      assertEquals(join, result.coordinatorTree());
    }
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

    // Union (NEEDS_GATHER) — needs two inputs with same row type.
    // Use a plain table scan for the second branch so the plan has only ONE
    // AbstractCalciteIndexScan (the totality test's intent is "never throws for any plan shape",
    // not testing multi-scan behaviour — that's covered by the DAG-sharing tests above).
    RelNode sortCopy = buildTableScanWithType(sort.getRowType());
    RelNode union = LogicalUnion.create(List.of(sort, sortCopy), true);

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
    // After US-012: MERGE_AGG fires for the splittable aggregate. The Aggregate IS the root, so
    // forcingOperator is null (nothing above the promoted cut).
    assertNull(result.forcingOperator());
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
  void unordered_fetch_at_root_has_null_forcing_operator() {
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
    // US-013: unordered fetch directly above shard-local Project → LIMIT promotion fires.
    // The Sort IS the promoted cut and is the root → forcingOperator is null.
    assertEquals(CombineDescriptor.Mode.LIMIT, result.combine().getMode());
    assertNull(result.forcingOperator());
  }

  // --- MERGE_AGG split rule tests (US-012) ---

  @Test
  void splittable_stats_by_produces_merge_agg_descriptor() {
    // stats count() by gender → Aggregate(group={2}, COUNT()) over scan
    AbstractCalciteIndexScan scan = buildIndexScan();
    RelNode agg =
        LogicalAggregate.create(
            scan,
            List.of(),
            ImmutableBitSet.of(2),
            null,
            List.of(
                AggregateCall.create(
                    SqlStdOperatorTable.COUNT,
                    false,
                    false,
                    false,
                    List.of(),
                    List.of(),
                    -1,
                    null,
                    RelCollations.EMPTY,
                    TYPE_FAC.createSqlType(SqlTypeName.BIGINT),
                    "cnt")));

    StagePlan result = StagePlanner.split(agg);

    assertTrue(result.staged());
    assertEquals(CombineDescriptor.Mode.MERGE_AGG, result.combine().getMode());
    assertEquals(List.of(0), result.combine().getIntListParam());
    assertEquals(List.of("SUM(1)"), result.combine().getStringListParam());
    // Aggregate is in shardFragment (its row type matches agg's)
    assertEquals(agg.getRowType(), result.shardFragment().getRowType());
  }

  @Test
  void distinct_agg_call_does_not_fire_merge_agg() {
    // COUNT(DISTINCT gender) → should NOT fire, plan returned with Aggregate on coordinator
    AbstractCalciteIndexScan scan = buildIndexScan();
    RelNode agg =
        LogicalAggregate.create(
            scan,
            List.of(),
            ImmutableBitSet.of(0),
            null,
            List.of(
                AggregateCall.create(
                    SqlStdOperatorTable.COUNT,
                    true, // distinct
                    false,
                    false,
                    List.of(),
                    List.of(2),
                    -1,
                    null,
                    RelCollations.EMPTY,
                    TYPE_FAC.createSqlType(SqlTypeName.BIGINT),
                    "cnt_distinct")));

    StagePlan result = StagePlanner.split(agg);

    assertTrue(result.staged());
    // Falls back to CONCAT — Aggregate is NOT in shardFragment
    assertEquals(CombineDescriptor.Mode.CONCAT, result.combine().getMode());
    assertEquals(scan.getRowType(), result.shardFragment().getRowType());
  }

  @Test
  void grouping_sets_does_not_fire_merge_agg() {
    // Aggregate with multiple grouping sets → should NOT fire
    AbstractCalciteIndexScan scan = buildIndexScan();
    RelNode agg =
        LogicalAggregate.create(
            scan,
            List.of(),
            ImmutableBitSet.of(0, 2),
            ImmutableList.of(ImmutableBitSet.of(0), ImmutableBitSet.of(2)),
            List.of(
                AggregateCall.create(
                    SqlStdOperatorTable.COUNT,
                    false,
                    false,
                    false,
                    List.of(),
                    List.of(),
                    -1,
                    null,
                    RelCollations.EMPTY,
                    TYPE_FAC.createSqlType(SqlTypeName.BIGINT),
                    "cnt")));

    StagePlan result = StagePlanner.split(agg);

    assertTrue(result.staged());
    assertEquals(CombineDescriptor.Mode.CONCAT, result.combine().getMode());
  }

  @Test
  void unsplittable_sqlkind_does_not_fire_merge_agg() {
    // An aggregate with an unsplittable kind (e.g., AVG — after AggregateReduceFunctionsRule it
    // would be decomposed, but here we test that the raw AVG is correctly refused)
    AbstractCalciteIndexScan scan = buildIndexScan();
    RelNode agg =
        LogicalAggregate.create(
            scan,
            List.of(),
            ImmutableBitSet.of(2),
            null,
            List.of(
                AggregateCall.create(
                    SqlStdOperatorTable.AVG,
                    false,
                    false,
                    false,
                    List.of(),
                    List.of(1),
                    -1,
                    null,
                    RelCollations.EMPTY,
                    TYPE_FAC.createSqlType(SqlTypeName.DOUBLE),
                    "avg_age")));

    StagePlan result = StagePlanner.split(agg);

    assertTrue(result.staged());
    // AVG is not in SPLITTABLE_KINDS → falls back to CONCAT
    assertEquals(CombineDescriptor.Mode.CONCAT, result.combine().getMode());
  }

  @Test
  void avg_query_after_reduction_ships_sum_and_count_in_shard_fragment() {
    // Simulate what AggregateReduceFunctionsRule produces for AVG: SUM + COUNT with a Project
    // above for division. The Project (div) stays on the coordinator.
    AbstractCalciteIndexScan scan = buildIndexScan();
    // Partial aggregate: group={2(gender)}, SUM(age), COUNT(age)
    RelNode partialAgg =
        LogicalAggregate.create(
            scan,
            List.of(),
            ImmutableBitSet.of(2),
            null,
            List.of(
                AggregateCall.create(
                    SqlStdOperatorTable.SUM,
                    false,
                    false,
                    false,
                    List.of(),
                    List.of(1),
                    -1,
                    null,
                    RelCollations.EMPTY,
                    TYPE_FAC.createTypeWithNullability(
                        TYPE_FAC.createSqlType(SqlTypeName.BIGINT), true),
                    "sum_age"),
                AggregateCall.create(
                    SqlStdOperatorTable.COUNT,
                    false,
                    false,
                    false,
                    List.of(),
                    List.of(1),
                    -1,
                    null,
                    RelCollations.EMPTY,
                    TYPE_FAC.createSqlType(SqlTypeName.BIGINT),
                    "count_age")));

    // Division project (finalization): gender, SUM(age) / COUNT(age)
    RelNode divProject =
        LogicalProject.create(
            partialAgg,
            List.of(),
            List.of(
                REX_BUILDER.makeInputRef(
                    partialAgg.getRowType().getFieldList().get(0).getType(), 0),
                REX_BUILDER.makeCall(
                    SqlStdOperatorTable.DIVIDE,
                    REX_BUILDER.makeInputRef(
                        partialAgg.getRowType().getFieldList().get(1).getType(), 1),
                    REX_BUILDER.makeInputRef(
                        partialAgg.getRowType().getFieldList().get(2).getType(), 2))),
            List.of("gender", "avg_age"));

    StagePlan result = StagePlanner.split(divProject);

    assertTrue(result.staged());
    // MERGE_AGG fires for the splittable aggregate (SUM+COUNT)
    assertEquals(CombineDescriptor.Mode.MERGE_AGG, result.combine().getMode());
    assertEquals(List.of(0), result.combine().getIntListParam());
    assertEquals(List.of("SUM(1)", "SUM(2)"), result.combine().getStringListParam());
    // The division project is in coordinatorTree (not in shardFragment)
    assertEquals(partialAgg.getRowType(), result.shardFragment().getRowType());
    // coordinatorTree root is the division project containing the DIVIDE finalization
    String coordinatorPlan = RelOptUtil.toString(result.coordinatorTree());
    assertTrue(
        coordinatorPlan.contains("/"),
        "coordinatorTree must contain the division (/) produced by AGGREGATE_REDUCE_FUNCTIONS");
  }

  // --- TOP_N split rule tests (US-014, rule 2) ---

  @Test
  void ordered_sort_with_fetch_ships_shard_local_top_n_and_keeps_coordinator_sort() {
    // sort - age | head 5 shape: LogicalSort(age DESC-nulls-last, fetch=5) over Project over scan.
    AbstractCalciteIndexScan scan = buildIndexScan();
    RelNode project =
        LogicalProject.create(
            scan,
            List.of(),
            List.of(
                REX_BUILDER.makeInputRef(scan.getRowType().getFieldList().get(0).getType(), 0),
                REX_BUILDER.makeInputRef(scan.getRowType().getFieldList().get(1).getType(), 1)),
            List.of("account_number", "age"));
    RelFieldCollation ageDesc =
        new RelFieldCollation(
            1, RelFieldCollation.Direction.DESCENDING, RelFieldCollation.NullDirection.LAST);
    RelNode sortFetch =
        LogicalSort.create(
            project,
            RelCollations.of(ageDesc),
            null,
            REX_BUILDER.makeLiteral(5, TYPE_FAC.createSqlType(SqlTypeName.INTEGER), false));

    StagePlan result = StagePlanner.split(sortFetch);

    assertTrue(result.staged());
    assertEquals(CombineDescriptor.Mode.TOP_N, result.combine().getMode());
    assertEquals("TOP_N{keys:[1], dirs:[DESCENDING:LAST], n:5}", result.combine().describe());

    // The shard fragment root is a standard LogicalSort carrying the SAME collation and fetch, so
    // each shard ships at most 5 rows already in order.
    assertEquals(LogicalSort.class, result.shardFragment().getClass());
    Sort shardSort = (Sort) result.shardFragment();
    assertEquals(List.of(ageDesc), shardSort.getCollation().getFieldCollations());
    assertNotNull(shardSort.fetch);
    assertNull(shardSort.offset);

    // Unlike LIMIT and MERGE_AGG, the coordinator KEEPS the original Sort — it is the
    // authoritative final merge over the gathered per-shard runs.
    assertTrue(result.coordinatorTree() instanceof Sort);
    assertEquals(
        List.of(ageDesc), ((Sort) result.coordinatorTree()).getCollation().getFieldCollations());
    assertTrue(
        RelOptUtil.toString(result.coordinatorTree()).contains("_gathered_rows"),
        "coordinatorTree leaf must be the gathered-rows scan");
  }

  @Test
  void top_n_resolves_unspecified_null_direction_from_the_direction_default() {
    // A collation built without an explicit null direction carries UNSPECIFIED. The wire spec must
    // never contain UNSPECIFIED — the reduce side has no way to resolve it.
    AbstractCalciteIndexScan scan = buildIndexScan();
    RelFieldCollation ascUnspecified =
        new RelFieldCollation(
            0, RelFieldCollation.Direction.ASCENDING, RelFieldCollation.NullDirection.UNSPECIFIED);
    RelNode sortFetch =
        LogicalSort.create(
            scan,
            RelCollations.of(ascUnspecified),
            null,
            REX_BUILDER.makeLiteral(3, TYPE_FAC.createSqlType(SqlTypeName.INTEGER), false));

    StagePlan result = StagePlanner.split(sortFetch);

    assertEquals(CombineDescriptor.Mode.TOP_N, result.combine().getMode());
    assertEquals(
        List.of(RelFieldCollation.Direction.ASCENDING.defaultNullDirection().name()),
        result.combine().getStringListParam().stream().map(s -> s.split(":")[1]).toList());
  }

  @Test
  void ordered_sort_without_fetch_is_not_promoted_to_top_n() {
    // A bare sort reduces nothing per shard — the coordinator must sort the whole union anyway.
    AbstractCalciteIndexScan scan = buildIndexScan();
    RelNode sort = LogicalSort.create(scan, RelCollations.of(new RelFieldCollation(0)), null, null);

    StagePlan result = StagePlanner.split(sort);

    assertTrue(result.staged());
    assertEquals(CombineDescriptor.Mode.CONCAT, result.combine().getMode());
  }

  @Test
  void ordered_sort_with_non_zero_offset_is_not_promoted_to_top_n() {
    // Shipping a shard-local Fetch(n) while the coordinator applies OFFSET m would drop rows that
    // belong in the global window.
    AbstractCalciteIndexScan scan = buildIndexScan();
    RelNode sortFetch =
        LogicalSort.create(
            scan,
            RelCollations.of(new RelFieldCollation(0)),
            REX_BUILDER.makeLiteral(2, TYPE_FAC.createSqlType(SqlTypeName.INTEGER), false),
            REX_BUILDER.makeLiteral(5, TYPE_FAC.createSqlType(SqlTypeName.INTEGER), false));

    StagePlan result = StagePlanner.split(sortFetch);

    assertTrue(result.staged());
    assertEquals(CombineDescriptor.Mode.CONCAT, result.combine().getMode());
  }

  @Test
  void top_n_is_not_promoted_below_an_aggregate() {
    // The Sort sits above an Aggregate, so the Aggregate — not the Sort — is directly above the
    // cut. MERGE_AGG must win and the Sort must stay on the coordinator.
    AbstractCalciteIndexScan scan = buildIndexScan();
    AggregateCall countCall =
        AggregateCall.create(
            SqlStdOperatorTable.COUNT,
            false,
            false,
            false,
            List.of(),
            List.of(),
            -1,
            null,
            RelCollations.EMPTY,
            TYPE_FAC.createSqlType(SqlTypeName.BIGINT),
            "cnt");
    RelNode agg =
        LogicalAggregate.create(
            scan, List.of(), ImmutableBitSet.of(2), null, ImmutableList.of(countCall));
    RelNode sortFetch =
        LogicalSort.create(
            agg,
            RelCollations.of(new RelFieldCollation(1, RelFieldCollation.Direction.DESCENDING)),
            null,
            REX_BUILDER.makeLiteral(5, TYPE_FAC.createSqlType(SqlTypeName.INTEGER), false));

    StagePlan result = StagePlanner.split(sortFetch);

    assertTrue(result.staged());
    assertEquals(CombineDescriptor.Mode.MERGE_AGG, result.combine().getMode());
  }

  @Test
  void shared_ordered_sort_fetch_under_join_dag_is_not_staged_as_top_n() {
    // A self-join over one HEP-interned subtree: the cut is reachable from both join inputs, so
    // replacing it with a single gathered-rows table would conflate two independent streams.
    AbstractCalciteIndexScan scan = buildIndexScan();
    RelNode join =
        LogicalJoin.create(
            scan,
            scan,
            List.of(),
            REX_BUILDER.makeLiteral(true),
            java.util.Set.of(),
            JoinRelType.INNER);
    RelNode sortFetch =
        LogicalSort.create(
            join,
            RelCollations.of(new RelFieldCollation(0)),
            null,
            REX_BUILDER.makeLiteral(5, TYPE_FAC.createSqlType(SqlTypeName.INTEGER), false));

    StagePlan result = StagePlanner.split(sortFetch);

    assertFalse(result.staged());
  }

  // --- LIMIT split rule tests (US-013) ---

  @Test
  void unordered_fetch_over_project_over_scan_is_promoted_to_limit() {
    // head N → LogicalSort(fetch=N, collation=EMPTY) over Project over scan
    AbstractCalciteIndexScan scan = buildIndexScan();
    RelNode project =
        LogicalProject.create(
            scan,
            List.of(),
            List.of(REX_BUILDER.makeInputRef(scan.getRowType().getFieldList().get(0).getType(), 0)),
            List.of("account_number"));
    RelNode fetch =
        LogicalSort.create(
            project,
            RelCollations.EMPTY,
            null,
            REX_BUILDER.makeLiteral(5, TYPE_FAC.createSqlType(SqlTypeName.INTEGER), false));

    StagePlan result = StagePlanner.split(fetch);

    assertTrue(result.staged());
    assertEquals(CombineDescriptor.Mode.LIMIT, result.combine().getMode());
    assertEquals(5, result.combine().getIntParam());
    // shardFragment root is a Sort with that fetch and empty collation
    assertTrue(result.shardFragment() instanceof Sort);
    Sort shardSort = (Sort) result.shardFragment();
    assertTrue(shardSort.getCollation().getFieldCollations().isEmpty());
    assertNotNull(shardSort.fetch);
    // coordinatorTree contains no Sort over the gathered-rows scan
    assertFalse(result.coordinatorTree() instanceof Sort);
  }

  @Test
  void ordered_sort_fetch_is_promoted_to_top_n_not_limit() {
    // sort X | head N → Sort with non-empty collation → US-014's TOP_N, never LIMIT. The empty-vs-
    // non-empty collation test is the whole boundary between rule 4 and rule 2.
    AbstractCalciteIndexScan scan = buildIndexScan();
    RelNode project =
        LogicalProject.create(
            scan,
            List.of(),
            List.of(REX_BUILDER.makeInputRef(scan.getRowType().getFieldList().get(0).getType(), 0)),
            List.of("account_number"));
    RelFieldCollation fieldCollation = new RelFieldCollation(0);
    RelNode sortFetch =
        LogicalSort.create(
            project,
            RelCollations.of(fieldCollation),
            null,
            REX_BUILDER.makeLiteral(10, TYPE_FAC.createSqlType(SqlTypeName.INTEGER), false));

    StagePlan result = StagePlanner.split(sortFetch);

    assertTrue(result.staged());
    assertEquals(CombineDescriptor.Mode.TOP_N, result.combine().getMode());
    assertEquals(10, result.combine().getIntParam());
    // Rule 2 never sets an early termination limit: the n-th smallest key is unknown until every
    // matching document has been examined, so stopping early would silently truncate.
    assertNull(result.earlyTerminationLimit());
  }

  @Test
  void non_zero_offset_is_not_promoted_to_limit() {
    AbstractCalciteIndexScan scan = buildIndexScan();
    RelNode project =
        LogicalProject.create(
            scan,
            List.of(),
            List.of(REX_BUILDER.makeInputRef(scan.getRowType().getFieldList().get(0).getType(), 0)),
            List.of("account_number"));
    RelNode fetch =
        LogicalSort.create(
            project,
            RelCollations.EMPTY,
            REX_BUILDER.makeLiteral(5, TYPE_FAC.createSqlType(SqlTypeName.INTEGER), false),
            REX_BUILDER.makeLiteral(10, TYPE_FAC.createSqlType(SqlTypeName.INTEGER), false));

    StagePlan result = StagePlanner.split(fetch);

    assertTrue(result.staged());
    // Non-zero offset → must NOT be promoted
    assertEquals(CombineDescriptor.Mode.CONCAT, result.combine().getMode());
  }

  @Test
  void fetch_directly_above_aggregate_is_not_promoted_to_limit() {
    // When the node above the cut is an Aggregate, aggregate promotion wins — LIMIT must not fire
    AbstractCalciteIndexScan scan = buildIndexScan();
    RelNode agg =
        LogicalAggregate.create(
            scan,
            List.of(),
            ImmutableBitSet.of(0),
            null,
            List.of(
                AggregateCall.create(
                    SqlStdOperatorTable.COUNT,
                    false,
                    false,
                    false,
                    List.of(),
                    List.of(),
                    -1,
                    null,
                    RelCollations.EMPTY,
                    TYPE_FAC.createSqlType(SqlTypeName.BIGINT),
                    "cnt")));
    RelNode fetch =
        LogicalSort.create(
            agg,
            RelCollations.EMPTY,
            null,
            REX_BUILDER.makeLiteral(5, TYPE_FAC.createSqlType(SqlTypeName.INTEGER), false));

    StagePlan result = StagePlanner.split(fetch);

    assertTrue(result.staged());
    // Aggregate promotion must win — MERGE_AGG
    assertEquals(CombineDescriptor.Mode.MERGE_AGG, result.combine().getMode());
  }

  @Test
  void fetch_above_project_with_rexOver_is_not_promoted_to_limit() {
    // A RexOver-bearing Project is NEEDS_GATHER → cut falls to scan. The fetch is
    // NOT directly above the cut, so LIMIT does not fire.
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
    RelNode fetch =
        LogicalSort.create(
            windowProject,
            RelCollations.EMPTY,
            null,
            REX_BUILDER.makeLiteral(5, TYPE_FAC.createSqlType(SqlTypeName.INTEGER), false));

    StagePlan result = StagePlanner.split(fetch);

    assertTrue(result.staged());
    // RexOver-bearing Project is NEEDS_GATHER → cut falls to scan, and the fetch is NOT
    // directly above the scan — the window project is between them. So LIMIT does not fire.
    assertEquals(CombineDescriptor.Mode.CONCAT, result.combine().getMode());
  }

  @Test
  void fetch_above_join_is_not_promoted_to_limit() {
    // Join is NEEDS_GATHER. If the cut is below the join, the fetch can't be directly above cut.
    AbstractCalciteIndexScan scan = buildIndexScan();
    RelNode plainScan = buildTableScan();
    RelNode join =
        LogicalJoin.create(
            scan,
            plainScan,
            List.of(),
            REX_BUILDER.makeLiteral(true),
            java.util.Set.of(),
            JoinRelType.INNER);
    RelNode fetch =
        LogicalSort.create(
            join,
            RelCollations.EMPTY,
            null,
            REX_BUILDER.makeLiteral(5, TYPE_FAC.createSqlType(SqlTypeName.INTEGER), false));

    StagePlan result = StagePlanner.split(fetch);

    assertTrue(result.staged());
    // Join makes the cut fall to scan; fetch is not directly above cut → CONCAT
    assertEquals(CombineDescriptor.Mode.CONCAT, result.combine().getMode());
  }

  @Test
  void earlyTerminationLimit_is_non_null_for_fetch_over_project_over_scan() {
    AbstractCalciteIndexScan scan = buildIndexScan();
    RelNode project =
        LogicalProject.create(
            scan,
            List.of(),
            List.of(REX_BUILDER.makeInputRef(scan.getRowType().getFieldList().get(0).getType(), 0)),
            List.of("account_number"));
    RelNode fetch =
        LogicalSort.create(
            project,
            RelCollations.EMPTY,
            null,
            REX_BUILDER.makeLiteral(7, TYPE_FAC.createSqlType(SqlTypeName.INTEGER), false));

    StagePlan result = StagePlanner.split(fetch);

    assertTrue(result.staged());
    assertEquals(CombineDescriptor.Mode.LIMIT, result.combine().getMode());
    // Project (no window) over scan is cardinality-preserving → earlyTerminationLimit is set
    assertNotNull(result.earlyTerminationLimit());
    assertEquals(7, result.earlyTerminationLimit());
  }

  @Test
  void earlyTerminationLimit_is_null_when_filter_sits_between_fetch_and_scan() {
    AbstractCalciteIndexScan scan = buildIndexScan();
    RexNode condition =
        REX_BUILDER.makeCall(
            SqlStdOperatorTable.GREATER_THAN,
            REX_BUILDER.makeInputRef(scan.getRowType().getFieldList().get(1).getType(), 1),
            REX_BUILDER.makeLiteral(30, TYPE_FAC.createSqlType(SqlTypeName.INTEGER), false));
    RelNode filter = LogicalFilter.create(scan, condition);
    RelNode fetch =
        LogicalSort.create(
            filter,
            RelCollations.EMPTY,
            null,
            REX_BUILDER.makeLiteral(5, TYPE_FAC.createSqlType(SqlTypeName.INTEGER), false));

    StagePlan result = StagePlanner.split(fetch);

    assertTrue(result.staged());
    assertEquals(CombineDescriptor.Mode.LIMIT, result.combine().getMode());
    // Filter is NOT cardinality-preserving → earlyTerminationLimit is null
    assertNull(result.earlyTerminationLimit());
  }

  // --- Non-round-trippable enum literal placement (Critical A fix) ---

  @Test
  void project_with_non_round_trippable_enum_literal_is_needs_gather() {
    // A Project containing a RexLiteral whose SYMBOL value is SqlTypeName (non-round-trippable
    // through the RelJson codec) must be placed as NEEDS_GATHER. This exercises the Critical A
    // fix: instead of mutating upstream statics with Unsafe, StagePlanner prevents such nodes
    // from reaching the shard codec. The plan still executes (coordinator-side) per Design
    // Invariant 1 — never reject a plan.
    AbstractCalciteIndexScan scan = buildIndexScan();
    RexNode flagLiteral = REX_BUILDER.makeFlag(SqlTypeName.DOUBLE);
    RelNode project =
        LogicalProject.create(
            scan,
            List.of(),
            List.of(
                REX_BUILDER.makeInputRef(scan.getRowType().getFieldList().get(0).getType(), 0),
                flagLiteral),
            List.of("account_number", "type_flag"));

    StagePlan result = StagePlanner.split(project);

    assertTrue(result.staged(), "Plan must still be staged — Design Invariant 1: never reject");
    // The Project with the enum flag is NEEDS_GATHER, so the cut falls to the scan
    assertEquals(scan.getRowType(), result.shardFragment().getRowType());
    assertEquals(CombineDescriptor.Mode.CONCAT, result.combine().getMode());
    // The coordinatorTree must contain the Project with the flag literal
    String coordinatorPlan = RelOptUtil.toString(result.coordinatorTree());
    assertTrue(
        coordinatorPlan.contains("type_flag"),
        "coordinatorTree must contain the non-round-trippable expression");
  }

  @Test
  void aggregate_above_project_with_non_round_trippable_enum_falls_to_concat() {
    // When a Project below an Aggregate contains a non-round-trippable enum literal, the
    // Project is NOT shard-local. This means the Aggregate's input (the Project) is
    // NEEDS_GATHER, so the MERGE_AGG split rule cannot fire (it requires its input to be
    // shard-local). The plan falls through to CONCAT with the cut at the scan.
    AbstractCalciteIndexScan scan = buildIndexScan();
    RexNode flagLiteral = REX_BUILDER.makeFlag(SqlTypeName.DOUBLE);
    RelNode projectWithFlag =
        LogicalProject.create(
            scan,
            List.of(),
            List.of(
                REX_BUILDER.makeInputRef(scan.getRowType().getFieldList().get(0).getType(), 0),
                REX_BUILDER.makeInputRef(scan.getRowType().getFieldList().get(1).getType(), 1),
                flagLiteral),
            List.of("account_number", "age", "type_flag"));
    RelNode agg =
        LogicalAggregate.create(
            projectWithFlag,
            List.of(),
            ImmutableBitSet.of(0),
            null,
            List.of(
                AggregateCall.create(
                    SqlStdOperatorTable.COUNT,
                    false,
                    false,
                    false,
                    List.of(),
                    List.of(),
                    -1,
                    null,
                    RelCollations.EMPTY,
                    TYPE_FAC.createSqlType(SqlTypeName.BIGINT),
                    "cnt")));

    StagePlan result = StagePlanner.split(agg);

    assertTrue(result.staged());
    // Falls to CONCAT because projectWithFlag is NEEDS_GATHER (non-round-trippable enum)
    assertEquals(CombineDescriptor.Mode.CONCAT, result.combine().getMode());
    assertEquals(scan.getRowType(), result.shardFragment().getRowType());
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

  @Test
  void filter_with_relevance_function_is_needs_gather() {
    // A Filter whose condition calls a relevance function (query_string, match, etc.) has no
    // enumerable implementor on the shard — compilation throws UnsupportedOperationException.
    // Such nodes must be classified NEEDS_GATHER per Design Invariant 1: the plan falls through
    // to the JDBC path (coordinatorOnly) which handles relevance functions natively via
    // OpenSearch's query DSL.
    AbstractCalciteIndexScan scan = buildIndexScan();

    // Build query_string(MAP('query','firstname:Amber')) → BOOLEAN condition
    org.apache.calcite.sql.SqlOperator queryStringOp =
        org.opensearch.sql.expression.function.PPLBuiltinOperators.QUERY_STRING;
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
    RexNode queryStringCall = REX_BUILDER.makeCall(queryStringOp, mapExpr);

    RelNode filter = LogicalFilter.create(scan, queryStringCall);

    StagePlan result = StagePlanner.split(filter);

    // The coordinator tree would contain the relevance function → coordinatorOnly.
    // Design Invariant 1: never reject, the JDBC path handles it.
    assertFalse(
        result.staged(), "Plan must be coordinatorOnly — relevance functions need JDBC path");
  }

  @Test
  void relevance_filter_above_unordered_fetch_is_not_staged() {
    // Regression: trySplitLimit must not return a staged plan when the coordinatorTree retains
    // a relevance-function call (query_string). Shape: Filter(query_string) → Sort(fetch=N,
    // empty collation) → Project → IndexScan. The Sort is directly above the cut (Project),
    // so LIMIT promotion fires. The coordinatorTree = Filter(query_string) → gathered_rows.
    // Without the relevance-function guard in trySplitLimit, CoordinatorTreeExecutor would fail
    // with UnsupportedOperationException from RelevanceQueryImplementor.
    AbstractCalciteIndexScan scan = buildIndexScan();
    RelNode project =
        LogicalProject.create(
            scan,
            List.of(),
            List.of(REX_BUILDER.makeInputRef(scan.getRowType().getFieldList().get(0).getType(), 0)),
            List.of("account_number"));

    // Unordered fetch (Sort with empty collation and fetch literal)
    RelNode sort =
        LogicalSort.create(
            project,
            RelCollations.EMPTY,
            null,
            REX_BUILDER.makeLiteral(10, TYPE_FAC.createSqlType(SqlTypeName.INTEGER), false));

    // Relevance filter ABOVE the fetch
    org.apache.calcite.sql.SqlOperator queryStringOp =
        org.opensearch.sql.expression.function.PPLBuiltinOperators.QUERY_STRING;
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
    RexNode queryStringCall = REX_BUILDER.makeCall(queryStringOp, mapExpr);
    RelNode filter = LogicalFilter.create(sort, queryStringCall);

    StagePlan result = StagePlanner.split(filter);

    // Must NOT be staged — relevance function in coordinatorTree has no enumerable implementor
    assertFalse(
        result.staged(),
        "Plan must be coordinatorOnly — relevance function above promoted limit needs JDBC path");
  }

  @Test
  void sort_above_filter_with_relevance_function_does_not_promote_limit() {
    // When a Filter below a Sort (limit) contains a relevance function, the Filter is NOT
    // shard-local. The coordinatorTree would contain the relevance function, so the plan
    // must be coordinatorOnly — limit promotion must NOT fire.
    AbstractCalciteIndexScan scan = buildIndexScan();

    org.apache.calcite.sql.SqlOperator queryStringOp =
        org.opensearch.sql.expression.function.PPLBuiltinOperators.QUERY_STRING;
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
    RexNode queryStringCall = REX_BUILDER.makeCall(queryStringOp, mapExpr);
    RelNode filter = LogicalFilter.create(scan, queryStringCall);

    // Sort(fetch=10000, empty collation) above the filter — the US-013 regression shape
    RexNode fetchLiteral =
        REX_BUILDER.makeLiteral(10000, TYPE_FAC.createSqlType(SqlTypeName.INTEGER), false);
    RelNode sort =
        org.apache.calcite.rel.logical.LogicalSort.create(
            filter, org.apache.calcite.rel.RelCollations.EMPTY, null, fetchLiteral);

    StagePlan result = StagePlanner.split(sort);

    // The plan MUST be coordinatorOnly — the coordinator tree would need to compile
    // query_string which has no enumerable implementor.
    assertFalse(
        result.staged(), "Plan must be coordinatorOnly — relevance functions need JDBC path");
  }
}
