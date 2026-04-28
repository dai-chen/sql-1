/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api;

import static org.opensearch.sql.monitor.profile.MetricName.OPTIMIZE;

import java.util.List;
import org.apache.calcite.plan.hep.HepMatchOrder;
import org.apache.calcite.plan.hep.HepPlanner;
import org.apache.calcite.plan.hep.HepProgram;
import org.apache.calcite.plan.hep.HepProgramBuilder;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.RelFactories;
import org.apache.calcite.rel.rules.CoreRules;
import org.apache.calcite.sql2rel.RelDecorrelator;
import org.apache.calcite.sql2rel.RelFieldTrimmer;

/**
 * {@code UnifiedQueryOptimizer} applies logical optimizations to a Calcite {@link RelNode} plan. It
 * performs decorrelation, field trimming, and two phases of heuristic (HEP) rule-based
 * optimization: filter pushdown/constant folding, followed by project cleanup/sort removal.
 *
 * <p>The output remains in {@code Convention.NONE} — no physical conversion is performed.
 */
public class UnifiedQueryOptimizer {

  /**
   * Two-phase HEP program: filter pushdown/constant folding first, then project cleanup/sort
   * removal. Each phase runs to fixpoint before the next begins.
   */
  private static final HepProgram HEP_PROGRAM =
      new HepProgramBuilder()
          // Phase 1: filter pushdown and constant folding
          .addMatchOrder(HepMatchOrder.BOTTOM_UP)
          .addRuleCollection(
              List.of(
                  CoreRules.FILTER_PROJECT_TRANSPOSE,
                  CoreRules.FILTER_AGGREGATE_TRANSPOSE,
                  CoreRules.FILTER_SET_OP_TRANSPOSE,
                  CoreRules.FILTER_MERGE,
                  CoreRules.FILTER_REDUCE_EXPRESSIONS,
                  CoreRules.PROJECT_REDUCE_EXPRESSIONS,
                  CoreRules.JOIN_REDUCE_EXPRESSIONS))
          // Phase 2: project cleanup and sort removal
          .addMatchOrder(HepMatchOrder.BOTTOM_UP)
          .addRuleCollection(
              List.of(CoreRules.PROJECT_MERGE, CoreRules.PROJECT_REMOVE, CoreRules.SORT_REMOVE))
          .build();

  /** Unified query context for profiling support. */
  private final UnifiedQueryContext context;

  /**
   * Constructs a UnifiedQueryOptimizer with a unified query context.
   *
   * @param context the unified query context for profiling
   */
  public UnifiedQueryOptimizer(UnifiedQueryContext context) {
    this.context = context;
  }

  /**
   * Optimizes a logical plan by decorrelating, trimming unused fields, and applying heuristic
   * rewrite rules. The result remains in {@code Convention.NONE}.
   *
   * @param logical the input logical plan
   * @return the optimized logical plan
   */
  public RelNode optimize(RelNode logical) {
    try {
      return context.measure(
          OPTIMIZE,
          () -> {
            // Step 1: Decorrelate correlated subqueries
            RelNode decorrelated = RelDecorrelator.decorrelateQuery(logical);

            // Step 2: Trim unused fields
            RelNode trimmed =
                new RelFieldTrimmer(
                        null, RelFactories.LOGICAL_BUILDER.create(decorrelated.getCluster(), null))
                    .trim(decorrelated);

            // Step 3: HEP — filter pushdown, constant folding, project cleanup, sort removal
            return hep(trimmed, HEP_PROGRAM);
          });
    } catch (UnsupportedOperationException e) {
      throw e;
    } catch (Exception e) {
      throw new IllegalStateException("Failed to optimize query plan", e);
    }
  }

  /** Runs a HEP optimization phase with the given pre-built program. */
  private RelNode hep(RelNode node, HepProgram program) {
    HepPlanner planner = new HepPlanner(program);
    planner.setRoot(node);
    return planner.findBestExp();
  }
}
