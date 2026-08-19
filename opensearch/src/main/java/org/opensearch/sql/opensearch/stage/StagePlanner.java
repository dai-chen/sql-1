/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.stage;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Properties;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.tools.Frameworks;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.opensearch.storage.scan.AbstractCalciteIndexScan;

/**
 * Computes a single shard-to-coordinator gather boundary for a Calcite plan. The generic placement
 * floor assigns SHARD_LOCAL or NEEDS_GATHER bottom-up, then cuts at the highest SHARD_LOCAL node.
 *
 * <p><b>Module deviation:</b> The design doc assigns StagePlanner to module {@code core}, but that
 * module's {@code build.gradle} has no dependency on {@code :opensearch}. Since this class must
 * reference {@link CombineDescriptor}, {@link RelFragmentCodec}, and {@link
 * AbstractCalciteIndexScan} (all in the {@code opensearch} module), it lives here instead.
 *
 * <p><b>Zero-or-many scans:</b> When the plan contains zero or more than one {@link
 * AbstractCalciteIndexScan}, there is no single shard-to-coordinator boundary. In that case a
 * coordinator-only {@link StagePlan} is returned (the plan runs entirely on the coordinator). This
 * is a PoC scope limit, not a bug — multi-scan staging is deferred.
 */
public final class StagePlanner {

  private StagePlanner() {}

  /**
   * Splits the plan at the single shard-to-coordinator gather boundary.
   *
   * @param root the top-level RelNode (typically wrapped in LogicalSystemLimit)
   * @return a {@link StagePlan} — never null, never throws for any valid RelNode tree
   */
  public static StagePlan split(RelNode root) {
    // 1. Find all AbstractCalciteIndexScan instances in the tree
    List<AbstractCalciteIndexScan> scans = new ArrayList<>();
    collectScans(root, scans);

    // Zero or multiple scans → coordinator-only
    if (scans.size() != 1) {
      return StagePlan.coordinatorOnly(root);
    }

    AbstractCalciteIndexScan shardScan = scans.get(0);

    // 2. Compute placement bottom-up in a single memoized pass, then find the highest
    //    SHARD_LOCAL node (the cut) with a top-down walk that reads the memo.
    IdentityHashMap<RelNode, Boolean> memo = new IdentityHashMap<>();
    computePlacement(root, memo);
    RelNode cutNode = findCut(root, memo);
    if (cutNode == null) {
      // The scan itself is the only SHARD_LOCAL node — the cut is the scan
      cutNode = shardScan;
    }

    // 3. Identify the forcing operator: the immediate parent of the cut in the original tree.
    //    This is the LOWEST NEEDS_GATHER node above the cut — i.e. the node whose input list
    //    contains cutNode. Null when the cut IS the root (fully shard-local plan).
    String forcingOperator = (cutNode == root) ? null : findParentOf(root, cutNode);

    // 4. Build shardFragment: replace the AbstractCalciteIndexScan leaf with a serializable
    //    LogicalTableScan over [RelFragmentCodec.SCHEMA_NAME, <indexName>] carrying the same
    //    row type so that RelFragmentCodec.serialize(shardFragment) round-trips.
    String indexName = shardScan.getOsIndex().getIndexName().toString();
    RelNode shardFragment = replaceLeafWithSerializable(cutNode, shardScan, indexName);

    // 5. Build coordinatorTree: replace the cut subtree with a gathered-rows scan
    RelNode coordinatorTree = replaceSubtree(root, cutNode, buildGatheredRowsScan(cutNode));

    // 6. Return the staged result with CONCAT descriptor and the forcing operator name
    return StagePlan.staged(
        shardFragment, CombineDescriptor.concat(), coordinatorTree, shardScan, forcingOperator);
  }

  /** Package-private predicate: true iff the node is the shard-local OpenSearch scan. */
  static boolean isShardScan(RelNode node) {
    return node instanceof AbstractCalciteIndexScan;
  }

  /**
   * Computes placement bottom-up in a single pass, memoizing results in an identity-based map.
   * Returns SHARD_LOCAL only for AbstractCalciteIndexScan and for Project/Filter/Calc whose single
   * input is SHARD_LOCAL and whose expressions contain no window function.
   *
   * <p>A Project/Calc containing a {@link RexOver} (e.g. ROW_NUMBER() from dedup or eventstats) is
   * NOT distribution-preserving: per-shard window numbering restarts independently, producing
   * silently wrong results. Partial window evaluation is US-015's RANK_LIMIT split rule — the
   * generic floor must classify these as NEEDS_GATHER.
   *
   * <p>Filter cannot legally contain a RexOver on Calcite 1.42.0 — the validator rejects it — so no
   * window check is needed for Filter.
   */
  private static void computePlacement(RelNode node, IdentityHashMap<RelNode, Boolean> memo) {
    if (memo.containsKey(node)) {
      return;
    }
    // Bottom-up: compute children first
    for (RelNode input : node.getInputs()) {
      computePlacement(input, memo);
    }
    memo.put(node, isShardLocalNode(node, memo));
  }

  private static boolean isShardLocalNode(RelNode node, IdentityHashMap<RelNode, Boolean> memo) {
    if (isShardScan(node)) {
      return true;
    }
    if (node instanceof Project) {
      List<RelNode> inputs = node.getInputs();
      if (inputs.size() != 1 || !Boolean.TRUE.equals(memo.get(inputs.get(0)))) {
        return false;
      }
      // Guard: a Project containing a RexOver (window function) is NOT distribution-preserving.
      // Per-shard window numbering restarts → silently wrong results. Partial windows are
      // US-015's RANK_LIMIT rule; the generic floor must classify these as NEEDS_GATHER.
      return !containsWindowFunction(node);
    }
    if (node instanceof Filter) {
      List<RelNode> inputs = node.getInputs();
      // Filter: SHARD_LOCAL iff single input is SHARD_LOCAL.
      // RexOver cannot appear in a filter condition on Calcite 1.42.0 (validator rejects it).
      return inputs.size() == 1 && Boolean.TRUE.equals(memo.get(inputs.get(0)));
    }
    if (node instanceof Calc) {
      List<RelNode> inputs = node.getInputs();
      if (inputs.size() != 1 || !Boolean.TRUE.equals(memo.get(inputs.get(0)))) {
        return false;
      }
      // Guard: a Calc whose RexProgram contains a RexOver is NOT distribution-preserving.
      return !containsWindowFunction(node);
    }
    // Everything else (Aggregate, Sort, Join, Window, Union, etc.): NEEDS_GATHER
    return false;
  }

  /**
   * Finds the highest SHARD_LOCAL node in the tree (the cut point). Walks top-down using the
   * pre-computed memo: the first node whose placement is SHARD_LOCAL is the cut.
   */
  private static RelNode findCut(RelNode node, IdentityHashMap<RelNode, Boolean> memo) {
    if (Boolean.TRUE.equals(memo.get(node))) {
      return node;
    }
    // Look in inputs — for a linear plan (single-input chain), the cut is in the input
    for (RelNode input : node.getInputs()) {
      RelNode found = findCut(input, memo);
      if (found != null) {
        return found;
      }
    }
    return null;
  }

  /**
   * Finds the immediate parent of the target node in the tree and returns its relTypeName, with a
   * {@code [window]} qualifier appended when the parent is a Project/Calc containing a window
   * function. Returns null if the target is not found as a direct child of any node (should not
   * happen for valid input since target is guaranteed to be reachable from root).
   */
  private static String findParentOf(RelNode node, RelNode target) {
    for (RelNode input : node.getInputs()) {
      if (input == target) {
        return qualifiedOperatorName(node);
      }
      String found = findParentOf(input, target);
      if (found != null) {
        return found;
      }
    }
    return null;
  }

  /**
   * Returns the operator name with a {@code [window]} qualifier when the node is a Project or Calc
   * containing a window function ({@link RexOver}). All other nodes return the bare relTypeName.
   */
  private static String qualifiedOperatorName(RelNode node) {
    if (containsWindowFunction(node)) {
      return node.getRelTypeName() + "[window]";
    }
    return node.getRelTypeName();
  }

  /**
   * Returns true iff the node is a Project containing a {@link RexOver} or a Calc whose program
   * contains a {@link RexOver}. Used by both the placement logic ({@link #isShardLocalNode}) and
   * the operator-name qualifier ({@link #qualifiedOperatorName}) so the two can never disagree.
   */
  private static boolean containsWindowFunction(RelNode node) {
    if (node instanceof Project project) {
      return RexOver.containsOver(project.getProjects(), null);
    }
    if (node instanceof Calc calc) {
      return RexOver.containsOver(calc.getProgram());
    }
    return false;
  }

  /** Collects all distinct AbstractCalciteIndexScan instances in the tree. */
  private static void collectScans(RelNode node, List<AbstractCalciteIndexScan> scans) {
    if (node instanceof AbstractCalciteIndexScan scan) {
      // Dedup: AbstractRelNode.equals is final reference equality, so List.contains is
      // identity-based. A DAG may share the same scan node in multiple branches.
      if (!scans.contains(scan)) {
        scans.add(scan);
      }
      return;
    }
    for (RelNode input : node.getInputs()) {
      collectScans(input, scans);
    }
  }

  /**
   * Replaces the AbstractCalciteIndexScan leaf in a subtree with a plain LogicalTableScan over
   * [SCHEMA_NAME, indexName] carrying the original scan's row type, making the fragment
   * serializable via RelFragmentCodec.
   */
  private static RelNode replaceLeafWithSerializable(
      RelNode node, AbstractCalciteIndexScan originalScan, String indexName) {
    if (node == originalScan) {
      return buildSerializableTableScan(originalScan, indexName);
    }
    // Rebuild the node with replaced inputs
    List<RelNode> newInputs = new ArrayList<>();
    boolean changed = false;
    for (RelNode input : node.getInputs()) {
      RelNode replaced = replaceLeafWithSerializable(input, originalScan, indexName);
      newInputs.add(replaced);
      if (replaced != input) {
        changed = true;
      }
    }
    if (!changed) {
      return node;
    }
    return node.copy(node.getTraitSet(), newInputs);
  }

  /**
   * Builds a serializable LogicalTableScan over [SCHEMA_NAME, indexName] with the scan's row type.
   */
  private static LogicalTableScan buildSerializableTableScan(
      AbstractCalciteIndexScan scan, String indexName) {
    RelDataType rowType = scan.getRowType();
    RelDataTypeFactory typeFactory = OpenSearchTypeFactory.TYPE_FACTORY;

    SchemaPlus rootSchema = Frameworks.createRootSchema(false);
    SchemaPlus osSchema = rootSchema.add(RelFragmentCodec.SCHEMA_NAME, new AbstractSchema() {});
    osSchema.add(indexName, new RelFragmentCodec.ShardRowSourceTable(rowType));

    CalciteSchema calciteSchema = CalciteSchema.from(rootSchema);
    CalciteCatalogReader catalogReader =
        new CalciteCatalogReader(
            calciteSchema,
            List.of(),
            typeFactory,
            new CalciteConnectionConfigImpl(new Properties()));
    RelOptTable table =
        catalogReader.getTableForMember(List.of(RelFragmentCodec.SCHEMA_NAME, indexName));
    return LogicalTableScan.create(scan.getCluster(), table, List.of());
  }

  /**
   * Builds a gathered-rows scan for the coordinator tree: a LogicalTableScan over a
   * ShardRowSourceTable parameterized with GATHERED_ROWS_STASH_KEY, carrying the cut node's row
   * type so the coordinator's operators see the correct schema.
   */
  private static LogicalTableScan buildGatheredRowsScan(RelNode cutNode) {
    RelDataType rowType = cutNode.getRowType();
    RelDataTypeFactory typeFactory = OpenSearchTypeFactory.TYPE_FACTORY;

    SchemaPlus rootSchema = Frameworks.createRootSchema(false);
    SchemaPlus osSchema = rootSchema.add(RelFragmentCodec.SCHEMA_NAME, new AbstractSchema() {});
    osSchema.add(
        GATHERED_ROWS_TABLE_NAME,
        new RelFragmentCodec.ShardRowSourceTable(rowType, GATHERED_ROWS_STASH_KEY));

    CalciteSchema calciteSchema = CalciteSchema.from(rootSchema);
    CalciteCatalogReader catalogReader =
        new CalciteCatalogReader(
            calciteSchema,
            List.of(),
            typeFactory,
            new CalciteConnectionConfigImpl(new Properties()));
    RelOptTable table =
        catalogReader.getTableForMember(
            List.of(RelFragmentCodec.SCHEMA_NAME, GATHERED_ROWS_TABLE_NAME));
    return LogicalTableScan.create(cutNode.getCluster(), table, List.of());
  }

  /**
   * Replaces a subtree (identified by reference equality with cutNode) with a replacement node
   * inside the root tree. Returns a rebuilt tree where the cut is substituted.
   */
  private static RelNode replaceSubtree(RelNode node, RelNode cutNode, RelNode replacement) {
    if (node == cutNode) {
      return replacement;
    }
    List<RelNode> newInputs = new ArrayList<>();
    boolean changed = false;
    for (RelNode input : node.getInputs()) {
      RelNode replaced = replaceSubtree(input, cutNode, replacement);
      newInputs.add(replaced);
      if (replaced != input) {
        changed = true;
      }
    }
    if (!changed) {
      return node;
    }
    return node.copy(node.getTraitSet(), newInputs);
  }

  /** DataContext stash key for gathered rows on the coordinator side. */
  public static final String GATHERED_ROWS_STASH_KEY = "calcite_exec.gathered_rows";

  /** Table name used in the coordinator's gathered-rows schema. */
  private static final String GATHERED_ROWS_TABLE_NAME = "_gathered_rows";
}
