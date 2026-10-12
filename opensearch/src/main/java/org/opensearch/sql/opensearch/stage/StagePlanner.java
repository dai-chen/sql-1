/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.stage;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.HashMap;
import java.util.IdentityHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.core.AggregateCall;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.sql.SqlAggFunction;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ImmutableBitSet;
import org.opensearch.sql.expression.function.PPLBuiltinOperators;
import org.opensearch.sql.opensearch.storage.scan.AbstractCalciteIndexScan;

/**
 * Splits independent scan branches into shard fragments and a coordinator continuation.
 *
 * <p>Filters, projections, calculations, and one decomposable aggregate may run on each shard.
 * Operators that need a global view, including aggregate merge, windows, sorts, and joins, remain
 * in the coordinator tree.
 */
public final class StagePlanner {

  private static final String GATHERED_ROWS_PREFIX = "_gathered_rows_";
  private static final String MATCHING_ROWS_PREFIX = "_matching_rows_";
  private static final String STASH_PREFIX = "calcite_exec.gathered_rows.";

  private static final Set<SqlKind> SPLITTABLE_AGGREGATES =
      Set.of(SqlKind.COUNT, SqlKind.SUM, SqlKind.SUM0, SqlKind.MIN, SqlKind.MAX);
  private static final HepProgram AGGREGATE_REDUCTION =
      new HepProgramBuilder().addRuleInstance(CoreRules.AGGREGATE_REDUCE_FUNCTIONS).build();

  private StagePlanner() {}

  public static Optional<StagePlan> split(RelNode root) {
    RelNode distributableRoot = reduceAggregates(root);
    List<Candidate> candidates = findCandidates(distributableRoot);
    if (candidates.isEmpty()
        || candidates.stream()
            .anyMatch(
                candidate ->
                    !StageExecutionSupport.supportsScan(
                        candidate.scan(), candidate.aggregate() != null))) {
      return Optional.empty();
    }
    boolean residualCoordinatorWork = hasGlobalWork(distributableRoot, candidates);

    Map<RelNode, RelNode> replacements = new IdentityHashMap<>();
    List<StagePlan.GatherExchange> exchanges = new ArrayList<>(candidates.size());
    for (int index = 0; index < candidates.size(); index++) {
      Candidate candidate = candidates.get(index);
      String coordinatorTableName = GATHERED_ROWS_PREFIX + index;
      String fragmentTableName = MATCHING_ROWS_PREFIX + index;
      String stashKey = STASH_PREFIX + index;

      LogicalTableScan matchingRowsScan =
          CalciteFragmentSerde.tableScan(
              candidate.scan().getCluster(),
              candidate.scan().getRowType(),
              fragmentTableName,
              CalciteFragmentSerde.MATCHING_ROWS_KEY);
      RelNode fragment = replace(candidate.root(), Map.of(candidate.scan(), matchingRowsScan));
      BitSet requiredInputFields =
          CalciteFragmentSerde.requiredInputFields(
              fragment, candidate.scan().getRowType().getFieldCount());
      if (!StageExecutionSupport.supportsRequiredMappings(candidate.scan(), requiredInputFields)) {
        return Optional.empty();
      }
      String fragmentJson;
      try {
        fragmentJson = CalciteFragmentSerde.serialize(fragment);
      } catch (RuntimeException | AssertionError unsupportedFragment) {
        // Calcite codecs use both exceptions and assertions for unsupported custom plan shapes.
        return Optional.empty();
      }
      LogicalTableScan gatheredRowsScan =
          CalciteFragmentSerde.tableScan(
              candidate.root().getCluster(),
              candidate.root().getRowType(),
              coordinatorTableName,
              stashKey);
      RelNode coordinatorInput =
          candidate.aggregate() == null
              ? gatheredRowsScan
              : finalAggregate(candidate.aggregate(), gatheredRowsScan);

      replacements.put(candidate.root(), coordinatorInput);
      exchanges.add(
          new StagePlan.GatherExchange(
              new StagePlan.ShardFragment(
                  fragmentTableName,
                  fragmentJson,
                  CalciteFragmentSerde.serializeType(candidate.scan().getRowType()),
                  candidate.root().getRowType(),
                  candidate.scan(),
                  candidate.aggregate() != null),
              new StagePlan.CoordinatorInput(coordinatorTableName, stashKey)));
    }

    RelNode coordinatorTree = replace(distributableRoot, replacements);
    if (containsScan(coordinatorTree)
        || !StageExecutionSupport.supportsCoordinator(coordinatorTree)) {
      return Optional.empty();
    }
    return Optional.of(new StagePlan(exchanges, coordinatorTree, residualCoordinatorWork));
  }

  private static RelNode reduceAggregates(RelNode root) {
    HepPlanner planner = new HepPlanner(AGGREGATE_REDUCTION);
    planner.setRoot(normalizeBigintAvg(root));
    return planner.findBestExp();
  }

  /**
   * Preserves BIGINT_AVG's double accumulator when Calcite decomposes AVG into SUM and COUNT.
   * Without the cast, the generic rule introduces a BIGINT SUM that can overflow before division.
   */
  private static RelNode normalizeBigintAvg(RelNode root) {
    return root.accept(
        new RelShuttleImpl() {
          @Override
          public RelNode visit(LogicalAggregate aggregate) {
            RelNode input = aggregate.getInput().accept(this);
            List<AggregateCall> calls = aggregate.getAggCallList();
            if (calls.stream()
                .noneMatch(call -> call.getAggregation() == PPLBuiltinOperators.BIGINT_AVG)) {
              return aggregate.copy(
                  aggregate.getTraitSet(),
                  input,
                  aggregate.getGroupSet(),
                  aggregate.getGroupSets(),
                  calls);
            }

            var rexBuilder = input.getCluster().getRexBuilder();
            RelDataType nullableDouble =
                input
                    .getCluster()
                    .getTypeFactory()
                    .createTypeWithNullability(
                        input.getCluster().getTypeFactory().createSqlType(SqlTypeName.DOUBLE),
                        true);
            List<RexNode> projects = new ArrayList<>(input.getRowType().getFieldCount());
            List<String> fieldNames = new ArrayList<>(input.getRowType().getFieldNames());
            for (int field = 0; field < input.getRowType().getFieldCount(); field++) {
              projects.add(rexBuilder.makeInputRef(input, field));
            }

            Map<Integer, Integer> doubleFields = new HashMap<>();
            for (AggregateCall call : calls) {
              if (call.getAggregation() != PPLBuiltinOperators.BIGINT_AVG
                  || call.getArgList().size() != 1) {
                continue;
              }
              int argument = call.getArgList().getFirst();
              if (!doubleFields.containsKey(argument)) {
                projects.add(
                    rexBuilder.makeCast(nullableDouble, rexBuilder.makeInputRef(input, argument)));
                fieldNames.add("_staged_avg_" + doubleFields.size());
                doubleFields.put(argument, projects.size() - 1);
              }
            }

            LogicalProject project = LogicalProject.create(input, List.of(), projects, fieldNames);
            List<AggregateCall> normalizedCalls = new ArrayList<>(calls.size());
            for (AggregateCall call : calls) {
              if (call.getAggregation() != PPLBuiltinOperators.BIGINT_AVG
                  || call.getArgList().size() != 1) {
                normalizedCalls.add(call);
                continue;
              }
              normalizedCalls.add(
                  AggregateCall.create(
                      SqlStdOperatorTable.AVG,
                      call.isDistinct(),
                      call.isApproximate(),
                      call.ignoreNulls(),
                      call.rexList,
                      List.of(doubleFields.get(call.getArgList().getFirst())),
                      call.filterArg,
                      call.distinctKeys,
                      call.getCollation(),
                      aggregate.getGroupCount(),
                      project,
                      call.getType(),
                      call.getName()));
            }
            return LogicalAggregate.create(
                project,
                aggregate.getHints(),
                aggregate.getGroupSet(),
                aggregate.getGroupSets(),
                normalizedCalls);
          }
        });
  }

  private static List<Candidate> findCandidates(RelNode root) {
    List<Candidate> candidates = new ArrayList<>();
    Candidate rootCandidate = analyze(root, candidates);
    if (rootCandidate != null) {
      candidates.add(rootCandidate);
    }
    return distinctCandidates(candidates);
  }

  private static List<Candidate> distinctCandidates(List<Candidate> candidates) {
    IdentityHashMap<RelNode, Boolean> seen = new IdentityHashMap<>();
    List<Candidate> distinct = new ArrayList<>(candidates.size());
    for (Candidate candidate : candidates) {
      if (seen.put(candidate.root(), Boolean.TRUE) == null) {
        distinct.add(candidate);
      }
    }
    return distinct;
  }

  private static Candidate analyze(RelNode node, List<Candidate> candidates) {
    if (node instanceof AbstractCalciteIndexScan scan) {
      return new Candidate(scan, scan, null);
    }

    if (node.getInputs().size() == 1) {
      Candidate child = analyze(node.getInput(0), candidates);
      if (child == null) {
        return null;
      }
      if (canAppend(node, child)) {
        return new Candidate(
            node,
            child.scan(),
            node instanceof LogicalAggregate aggregate ? aggregate : child.aggregate());
      }
      candidates.add(child);
      return null;
    }

    for (RelNode input : node.getInputs()) {
      Candidate child = analyze(input, candidates);
      if (child != null) {
        candidates.add(child);
      }
    }
    return null;
  }

  private static boolean canAppend(RelNode node, Candidate candidate) {
    if (candidate.aggregate() != null) {
      return false;
    }
    if (node instanceof LogicalProject project) {
      return !RexOver.containsOver(project.getProjects(), null)
          && StageExecutionSupport.supportsShardExpressions(project.getProjects());
    }
    if (node instanceof LogicalFilter filter) {
      return !RexOver.containsOver(filter.getCondition())
          && StageExecutionSupport.supportsShardExpressions(List.of(filter.getCondition()));
    }
    if (node instanceof LogicalCalc calc) {
      return !RexOver.containsOver(calc.getProgram())
          && StageExecutionSupport.supportsShardExpressions(calc.getProgram().getExprList());
    }
    if (node instanceof LogicalAggregate aggregate) {
      return isSplittable(aggregate);
    }
    return false;
  }

  private static boolean isSplittable(LogicalAggregate aggregate) {
    if (aggregate.getGroupSets().size() != 1) {
      return false;
    }
    for (AggregateCall call : aggregate.getAggCallList()) {
      if (call.isDistinct()
          || call.hasFilter()
          || !SPLITTABLE_AGGREGATES.contains(call.getAggregation().getKind())) {
        return false;
      }
    }
    return true;
  }

  private static RelNode finalAggregate(
      LogicalAggregate partialAggregate, LogicalTableScan gatheredRows) {
    int groupCount = partialAggregate.getGroupCount();
    ImmutableBitSet groupSet = ImmutableBitSet.range(groupCount);
    List<AggregateCall> finalCalls = new ArrayList<>(partialAggregate.getAggCallList().size());
    for (int i = 0; i < partialAggregate.getAggCallList().size(); i++) {
      AggregateCall partialCall = partialAggregate.getAggCallList().get(i);
      SqlAggFunction operation =
          switch (partialCall.getAggregation().getKind()) {
            case COUNT -> SqlStdOperatorTable.SUM0;
            case SUM -> partialCall.getAggregation();
            case SUM0 -> SqlStdOperatorTable.SUM0;
            case MIN -> SqlStdOperatorTable.MIN;
            case MAX -> SqlStdOperatorTable.MAX;
            default -> throw new IllegalStateException("Unsupported aggregate " + partialCall);
          };
      finalCalls.add(
          AggregateCall.create(
              operation,
              false,
              false,
              false,
              List.of(),
              List.of(groupCount + i),
              -1,
              null,
              RelCollations.EMPTY,
              groupCount,
              gatheredRows,
              null,
              partialCall.getName()));
    }
    LogicalAggregate merged =
        LogicalAggregate.create(gatheredRows, groupSet, List.of(groupSet), finalCalls);
    List<RexNode> projects = new ArrayList<>(merged.getRowType().getFieldCount());
    for (int i = 0; i < groupCount; i++) {
      projects.add(merged.getCluster().getRexBuilder().makeInputRef(merged, i));
    }
    for (int i = 0; i < partialAggregate.getAggCallList().size(); i++) {
      AggregateCall partialCall = partialAggregate.getAggCallList().get(i);
      RexNode value = merged.getCluster().getRexBuilder().makeInputRef(merged, groupCount + i);
      if (partialCall.getAggregation().getKind() == SqlKind.SUM) {
        RexNode zero = merged.getCluster().getRexBuilder().makeZeroLiteral(partialCall.getType());
        value =
            merged.getCluster().getRexBuilder().makeCall(SqlStdOperatorTable.COALESCE, value, zero);
      }
      projects.add(
          merged.getCluster().getRexBuilder().ensureType(partialCall.getType(), value, true));
    }
    return LogicalProject.create(
        merged, List.of(), projects, partialAggregate.getRowType().getFieldNames());
  }

  private static RelNode replace(RelNode node, Map<RelNode, RelNode> replacements) {
    RelNode replacement = replacements.get(node);
    if (replacement != null) {
      return replacement;
    }
    List<RelNode> inputs = node.getInputs();
    List<RelNode> replacedInputs = new ArrayList<>(inputs.size());
    boolean changed = false;
    for (RelNode input : inputs) {
      RelNode replaced = replace(input, replacements);
      replacedInputs.add(replaced);
      changed |= replaced != input;
    }
    return changed ? node.copy(node.getTraitSet(), replacedInputs) : node;
  }

  private static boolean containsScan(RelNode node) {
    if (node instanceof AbstractCalciteIndexScan) {
      return true;
    }
    for (RelNode input : node.getInputs()) {
      if (containsScan(input)) {
        return true;
      }
    }
    return false;
  }

  private static boolean hasGlobalWork(RelNode root, List<Candidate> candidates) {
    IdentityHashMap<RelNode, Boolean> shardRoots = new IdentityHashMap<>();
    for (Candidate candidate : candidates) {
      shardRoots.put(candidate.root(), Boolean.TRUE);
    }
    return hasGlobalWork(root, shardRoots);
  }

  private static boolean hasGlobalWork(RelNode node, IdentityHashMap<RelNode, Boolean> shardRoots) {
    if (shardRoots.containsKey(node)) {
      return false;
    }
    if (node instanceof org.opensearch.sql.calcite.plan.rel.LogicalSystemLimit) {
      return hasGlobalWork(node.getInput(0), shardRoots);
    }
    if (node instanceof LogicalProject project) {
      if (RexOver.containsOver(project.getProjects(), null)) {
        return true;
      }
      return hasGlobalWork(project.getInput(), shardRoots);
    }
    return true;
  }

  private record Candidate(
      RelNode root, AbstractCalciteIndexScan scan, LogicalAggregate aggregate) {}
}
