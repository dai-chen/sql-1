/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.stage;

import java.util.ArrayList;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import org.apache.calcite.config.CalciteConnectionConfigImpl;
import org.apache.calcite.jdbc.CalciteSchema;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.CalciteCatalogReader;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.core.Calc;
import org.apache.calcite.rel.core.Filter;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.Frameworks;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.opensearch.storage.scan.AbstractCalciteIndexScan;
import org.opensearch.sql.opensearch.storage.serde.ExtendedRelJson;

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

    // 3a. Post-placement aggregate promotion: if the node directly above the cut is a splittable
    // Aggregate, promote the cut to include that Aggregate and emit MERGE_AGG instead of CONCAT.
    StagePlan aggPlan = trySplitAggregate(root, cutNode, shardScan);
    if (aggPlan != null) {
      return aggPlan;
    }

    // 3b. Shared-subtree guard (CONCAT path only): if the cut node is reachable from more than
    //     one parent in the DAG AND its direct parent is a join, the plan cannot be staged via
    //     CONCAT. In self-join DAGs produced by HEP interning, both join inputs reference the
    //     same subtree. Staging conflates independent data streams and some shapes throw
    //     UnsupportedOperationException at runtime. Chart/timechart/streamstats plans with HEP-
    //     shared subtrees work correctly when the WHOLE shared chain is the cut (both positions
    //     get _gathered_rows), but self-join plans where the join's parent expects independent
    //     streams do not. Guard specifically for the join case per Design Invariant 1.
    //     findDirectParent returns the first parent in DFS order, so a shared node with both a
    //     join and a non-join parent may skip this guard; the containsIndexScan validation in
    //     step 5a is the backstop that catches the residual live scan in that case.
    if (cutNode != root && isSharedNode(root, cutNode)) {
      RelNode parent = findDirectParent(root, cutNode);
      if (parent instanceof LogicalJoin) {
        return StagePlan.coordinatorOnly(root);
      }
    }

    // 4. Build shardFragment: replace the AbstractCalciteIndexScan leaf with a serializable
    //    LogicalTableScan over [RelFragmentCodec.SCHEMA_NAME, <indexName>] carrying the same
    //    row type so that RelFragmentCodec.serialize(shardFragment) round-trips.
    String indexName = shardScan.getOsIndex().getIndexName().toString();
    RelNode shardFragment = replaceLeafWithSerializable(cutNode, shardScan, indexName);

    // 5. Build coordinatorTree: replace the cut subtree with a gathered-rows scan
    RelNode coordinatorTree = replaceSubtree(root, cutNode, buildGatheredRowsScan(cutNode));

    // 5a. Post-split validation: if the coordinatorTree still contains a live index scan, the
    //     split was incomplete (e.g. a union/join where only one branch's scan was replaced).
    //     Fall back to coordinator-only. This catches the DAG-sharing hazard introduced by
    //     HepPlanner when structurally identical scans in different branches become one instance.
    if (containsIndexScan(coordinatorTree)) {
      return StagePlan.coordinatorOnly(root);
    }

    // 6. Return the staged result with CONCAT descriptor and the forcing operator name
    return StagePlan.staged(
        shardFragment, CombineDescriptor.concat(), coordinatorTree, shardScan, forcingOperator);
  }

  /** Package-private predicate: true iff the node is the shard-local OpenSearch scan. */
  static boolean isShardScan(RelNode node) {
    return node instanceof AbstractCalciteIndexScan;
  }

  /**
   * Returns true if the tree rooted at {@code node} contains any {@link AbstractCalciteIndexScan}.
   * Used as a post-split validation: if the coordinator tree still has a live index scan after
   * substitution, the split was incomplete (a branch was not replaced) and the plan must not be
   * staged. This catches the DAG-sharing hazard where HepPlanner makes structurally identical scans
   * share one instance — replaceSubtree only replaces the cut node, leaving unreplaced branches
   * with a live scan that CoordinatorTreeExecutor cannot execute.
   */
  private static boolean containsIndexScan(RelNode node) {
    if (node instanceof AbstractCalciteIndexScan) {
      return true;
    }
    for (RelNode input : node.getInputs()) {
      if (containsIndexScan(input)) {
        return true;
      }
    }
    return false;
  }

  /**
   * Returns true iff {@code target} is reachable from more than one parent in the tree rooted at
   * {@code root}. A shared node means the cut subtree appears in multiple positions (e.g. both
   * sides of a self-join after HEP interning) — staging would conflate independent data streams.
   */
  private static boolean isSharedNode(RelNode root, RelNode target) {
    int[] count = {0};
    countParentReferences(root, target, count);
    return count[0] > 1;
  }

  private static void countParentReferences(RelNode node, RelNode target, int[] count) {
    for (RelNode input : node.getInputs()) {
      if (input == target) {
        count[0]++;
        if (count[0] > 1) {
          return;
        }
      } else {
        countParentReferences(input, target, count);
        if (count[0] > 1) {
          return;
        }
      }
    }
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
      if (containsWindowFunction(node)) {
        return false;
      }
      // Guard: a Project containing a non-round-trippable enum literal (e.g. SqlTypeName from
      // percentile_approx's makeFlag) cannot survive the RelJson codec. Such nodes execute on
      // the coordinator per Design Invariant 1 — never reject, never silently fail. Same shape
      // as the RexOver guard; both use a shared predicate for placement and diagnostics.
      return !containsNonRoundTrippableEnum(node);
    }
    if (node instanceof Filter) {
      List<RelNode> inputs = node.getInputs();
      // Filter: SHARD_LOCAL iff single input is SHARD_LOCAL.
      // RexOver cannot appear in a filter condition on Calcite 1.42.0 (validator rejects it).
      if (inputs.size() != 1 || !Boolean.TRUE.equals(memo.get(inputs.get(0)))) {
        return false;
      }
      return !containsNonRoundTrippableEnum(node);
    }
    if (node instanceof Calc) {
      List<RelNode> inputs = node.getInputs();
      if (inputs.size() != 1 || !Boolean.TRUE.equals(memo.get(inputs.get(0)))) {
        return false;
      }
      // Guard: a Calc whose RexProgram contains a RexOver is NOT distribution-preserving.
      if (containsWindowFunction(node)) {
        return false;
      }
      return !containsNonRoundTrippableEnum(node);
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

  /**
   * Returns true iff the node's expressions contain a RexLiteral whose SYMBOL value is an enum that
   * cannot round-trip through the RelJson shard codec (e.g. SqlTypeName). Used by placement ({@link
   * #isShardLocalNode}) and will be reused for any diagnostic message that names the forcing
   * reason, so classification and messages cannot drift. Same shape as {@link
   * #containsWindowFunction}.
   *
   * <p>Design Invariant 1: absence of codec support means NEEDS_GATHER, never reject a plan.
   */
  static boolean containsNonRoundTrippableEnum(RelNode node) {
    List<RexNode> expressions;
    if (node instanceof Project project) {
      expressions = project.getProjects();
    } else if (node instanceof Filter filter) {
      expressions = List.of(filter.getCondition());
    } else if (node instanceof Calc calc) {
      expressions = calc.getProgram().getExprList();
    } else {
      return false;
    }
    return expressions.stream().anyMatch(StagePlanner::exprContainsNonRoundTrippableEnum);
  }

  /**
   * Walks a single RexNode tree looking for a RexLiteral whose value is a non-round-trippable enum.
   */
  private static boolean exprContainsNonRoundTrippableEnum(RexNode expr) {
    boolean[] found = {false};
    expr.accept(
        new RexVisitorImpl<Void>(true) {
          @Override
          public Void visitLiteral(RexLiteral literal) {
            if (literal.getTypeName() == SqlTypeName.SYMBOL) {
              Object value = literal.getValue();
              if (value instanceof Enum<?> e && !ExtendedRelJson.isRoundTrippableEnum(e)) {
                found[0] = true;
              }
            }
            return null;
          }
        });
    return found[0];
  }

  /** SqlKinds that can be split across shards: partial runs on shard, merge on coordinator. */
  private static final Set<SqlKind> SPLITTABLE_KINDS =
      Set.of(SqlKind.COUNT, SqlKind.SUM, SqlKind.SUM0, SqlKind.MIN, SqlKind.MAX);

  /**
   * Returns the merge function name for a given partial aggregate SqlKind. COUNT partial outputs
   * are summed to produce the global count.
   */
  static String mergeFunctionForKind(SqlKind kind) {
    return switch (kind) {
      case COUNT -> "SUM";
      case SUM -> "SUM";
      case SUM0 -> "SUM0";
      case MIN -> "MIN";
      case MAX -> "MAX";
      default -> throw new IllegalArgumentException("Not splittable: " + kind);
    };
  }

  /**
   * Post-placement rewrite: if the node directly above the cut is a LogicalAggregate whose every
   * call is splittable, promotes the cut to that Aggregate (it becomes the shard fragment's root)
   * and emits a MERGE_AGG descriptor. Everything above the Aggregate stays in coordinatorTree,
   * which is what keeps AVG finalization off the shards.
   *
   * <p>Returns null when the rule does not fire (Aggregate not directly above cut, or has
   * unsplittable calls). In that case the caller falls through to the default CONCAT path. Design
   * Invariant 1: never reject a plan.
   */
  private static StagePlan trySplitAggregate(
      RelNode root, RelNode cutNode, AbstractCalciteIndexScan shardScan) {
    // Walk the tree to find the immediate parent of the cutNode
    RelNode parent = findDirectParent(root, cutNode);
    if (parent == null || !(parent instanceof LogicalAggregate agg)) {
      return null;
    }
    if (!isSplittableAggregate(agg)) {
      return null;
    }

    // The Aggregate is splittable — promote the cut to include it.
    // shardFragment = the Aggregate and everything below
    String indexName = shardScan.getOsIndex().getIndexName().toString();
    RelNode shardFragment = replaceLeafWithSerializable(agg, shardScan, indexName);

    // Build MERGE_AGG descriptor:
    // groupKeys = positional indices of group-by columns in the Aggregate's output row.
    // In Calcite's Aggregate output, group keys come first (positions 0..groupCount-1).
    List<Integer> groupKeys = new ArrayList<>();
    for (int i = 0; i < agg.getGroupCount(); i++) {
      groupKeys.add(i);
    }
    // aggs = per-agg-column merge spec as "FUNCTION(colIdx)"
    List<String> aggs = new ArrayList<>();
    int colIdx = agg.getGroupCount();
    for (AggregateCall call : agg.getAggCallList()) {
      String mergeFunc = mergeFunctionForKind(call.getAggregation().getKind());
      aggs.add(mergeFunc + "(" + colIdx + ")");
      colIdx++;
    }

    CombineDescriptor combine = CombineDescriptor.mergeAgg(groupKeys, aggs);

    // coordinatorTree = everything above the Aggregate, with the Aggregate replaced by gathered
    // rows scan (whose row type matches the Aggregate's output row type)
    RelNode coordinatorTree = replaceSubtree(root, agg, buildGatheredRowsScan(agg));

    // Post-split validation: if the coordinatorTree still contains a live index scan, the split
    // was incomplete (DAG-sharing hazard). Return null to fall through to the CONCAT path, which
    // has its own validation and will ultimately return coordinator-only.
    if (containsIndexScan(coordinatorTree)) {
      return null;
    }

    // forcingOperator is null when the Aggregate IS the root; otherwise it's the parent of the agg
    String forcingOperator = (agg == root) ? null : findParentOf(root, agg);

    return StagePlan.staged(shardFragment, combine, coordinatorTree, shardScan, forcingOperator);
  }

  /**
   * Returns true iff the aggregate can be split: every call is in SPLITTABLE_KINDS, no call is
   * DISTINCT, and there is at most one grouping set (no GROUPING SETS / CUBE / ROLLUP).
   */
  static boolean isSplittableAggregate(LogicalAggregate agg) {
    // Reject grouping sets (more than one groupSet means GROUPING SETS/CUBE/ROLLUP)
    if (agg.getGroupSets().size() > 1) {
      return false;
    }
    for (AggregateCall call : agg.getAggCallList()) {
      if (call.isDistinct()) {
        return false;
      }
      if (!SPLITTABLE_KINDS.contains(call.getAggregation().getKind())) {
        return false;
      }
    }
    return true;
  }

  /**
   * Finds the direct parent of target in the tree. Returns null if target is the root or not found.
   */
  private static RelNode findDirectParent(RelNode node, RelNode target) {
    for (RelNode input : node.getInputs()) {
      if (input == target) {
        return node;
      }
      RelNode found = findDirectParent(input, target);
      if (found != null) {
        return found;
      }
    }
    return null;
  }

  /**
   * Collects all distinct AbstractCalciteIndexScan instances in the tree (identity-based). A DAG
   * produced by HepPlanner may share a single RelNode instance across multiple parents — e.g. a
   * self-join where both branches reference the same subtree. Identity-based dedup means such a
   * shared scan is counted ONCE, which is correct: replaceSubtree uses identity equality and will
   * replace ALL references to the cut node, so both join branches receive _gathered_rows.
   *
   * <p>The hazardous case (union with two DIFFERENT subtrees above a shared scan instance) is
   * caught by the post-split validation ({@link #containsIndexScan}) which detects any residual
   * live scan in the coordinator tree after substitution.
   */
  private static void collectScans(RelNode node, List<AbstractCalciteIndexScan> scans) {
    if (node instanceof AbstractCalciteIndexScan scan) {
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
