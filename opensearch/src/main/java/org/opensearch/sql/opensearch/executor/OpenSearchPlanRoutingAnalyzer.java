/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor;

import java.util.HashSet;
import java.util.Set;
import org.apache.calcite.adapter.enumerable.EnumerableLimit;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.opensearch.sql.opensearch.storage.scan.AbstractCalciteIndexScan;

/**
 * Classifies the fully optimized legacy physical plan for engine routing.
 *
 * <p>A single OpenSearch scan with only row-preserving projections or result-limit wrappers is
 * fully pushed down. Every other operator is treated conservatively as residual coordinator work.
 */
public final class OpenSearchPlanRoutingAnalyzer {

  public enum LegacyExecutionShape {
    FULLY_PUSHDOWN,
    RESIDUAL_COORDINATOR_WORK
  }

  public record Assessment(LegacyExecutionShape legacyShape, Set<String> indices) {
    public Assessment {
      indices = Set.copyOf(indices);
    }
  }

  private OpenSearchPlanRoutingAnalyzer() {}

  public static Assessment analyze(RelNode physicalPlan) {
    Set<String> indices = new HashSet<>();
    int[] scanCount = {0};
    boolean[] residual = {false};

    walk(physicalPlan, indices, scanCount, residual);

    LegacyExecutionShape shape =
        residual[0] || scanCount[0] > 1
            ? LegacyExecutionShape.RESIDUAL_COORDINATOR_WORK
            : LegacyExecutionShape.FULLY_PUSHDOWN;
    return new Assessment(shape, indices);
  }

  private static void walk(RelNode node, Set<String> indices, int[] scanCount, boolean[] residual) {
    if (node instanceof AbstractCalciteIndexScan scan) {
      scanCount[0]++;
      var qualifiedName = scan.getTable().getQualifiedName();
      if (!qualifiedName.isEmpty()) {
        indices.add(qualifiedName.get(qualifiedName.size() - 1));
      }
    } else if (!isLightweightWrapper(node)) {
      residual[0] = true;
    }
    for (RelNode input : node.getInputs()) {
      walk(input, indices, scanCount, residual);
    }
  }

  private static boolean isLightweightWrapper(RelNode node) {
    if (node instanceof EnumerableLimit) {
      return true;
    }
    if (node instanceof Project project) {
      for (RexNode expression : project.getProjects()) {
        if (!(expression instanceof RexInputRef) && !(expression instanceof RexLiteral)) {
          return false;
        }
      }
      return true;
    }
    return false;
  }
}
