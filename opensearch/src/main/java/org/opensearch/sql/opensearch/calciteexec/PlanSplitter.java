/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.calciteexec;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.rel.RelCollation;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.core.Project;
import org.apache.calcite.rel.core.Window;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.apache.calcite.rel.externalize.RelJsonWriter;
import org.opensearch.sql.calcite.plan.rel.Dedup;
import org.opensearch.sql.opensearch.storage.scan.AbstractCalciteIndexScan;

/**
 * Splits an optimized RelNode into a shard fragment (to execute per shard via calcite_exec
 * aggregation) and extracts parameters for the coordinator reduce tree.
 *
 * <p>Phase 1 strategy (single-shard PoC):
 * <ul>
 *   <li>Shard fragment: a TableScan on "shard_source" that collects doc_values rows.
 *       The plan is serializable via RelJsonWriter.</li>
 *   <li>Reduce: described by parameters (dedup keys, window groups, output projection)
 *       which the engine uses to build a fresh plan via RelBuilder.</li>
 * </ul>
 */
public final class PlanSplitter {

  private PlanSplitter() {}

  public record SplitResult(
      RelNode shardFragment,
      List<String> shardFieldNames,
      List<SqlTypeName> shardFieldTypes,
      ReduceSpec reduceSpec) {}

  public sealed interface ReduceSpec permits DedupReduce, WindowReduce {}

  public record DedupReduce(
      List<Integer> dedupFieldIndices,
      int allowedDuplication,
      boolean keepEmpty,
      RelCollation inputCollation,
      List<Integer> outputProjection
  ) implements ReduceSpec {}

  public record WindowReduce(
      Window window,
      List<Integer> outputProjection
  ) implements ReduceSpec {}

  /**
   * Phase 1: shard fragment is a plain scan (collect all rows). Reduce spec describes what
   * operations to apply on the coordinator.
   */
  public static SplitResult split(RelNode plan) {
    AbstractCalciteIndexScan osScan = findScan(plan);
    if (osScan == null) {
      throw new IllegalStateException("No OpenSearch index scan found in plan");
    }

    // Extract schema from the scan's row type
    List<String> fieldNames = osScan.getRowType().getFieldNames();
    List<SqlTypeName> fieldTypes = osScan.getRowType().getFieldList().stream()
        .map(f -> f.getType().getSqlTypeName())
        .toList();

    // Build shard fragment: just a TableScan on "shard_source" (trivially serializable)
    SchemaPlus shardSchema = FragmentDeserializer.buildSchema(fieldNames, fieldTypes);
    RelBuilder shardBuilder = RelBuilder.create(
        Frameworks.newConfigBuilder().defaultSchema(shardSchema).build());
    RelNode shardFragment = shardBuilder.scan(FragmentDeserializer.TABLE_NAME).build();

    // Analyze the original plan to extract reduce parameters
    ReduceSpec reduceSpec = analyzeForReduce(plan);

    return new SplitResult(shardFragment, fieldNames, fieldTypes, reduceSpec);
  }

  private static ReduceSpec analyzeForReduce(RelNode plan) {
    Dedup[] dedup = {null};
    Window[] window = {null};
    List<Integer>[] outputProj = new List[]{null};

    new RelVisitor() {
      @Override
      public void visit(RelNode node, int ordinal, RelNode parent) {
        if (node instanceof Dedup d && dedup[0] == null) {
          dedup[0] = d;
        } else if (node instanceof Window w && window[0] == null) {
          window[0] = w;
        }
        // Capture the first Project above the Dedup/Window as the output projection.
        // The previous `parent == null` check missed it when a SystemLimit wraps the plan.
        if (node instanceof Project p && outputProj[0] == null
            && dedup[0] == null && window[0] == null) {
          List<Integer> proj = new ArrayList<>();
          for (RexNode expr : p.getProjects()) {
            if (expr instanceof RexInputRef ref) {
              proj.add(ref.getIndex());
            } else {
              proj.add(-1);
            }
          }
          outputProj[0] = proj;
        }
        super.visit(node, ordinal, parent);
      }
    }.go(plan);

    if (dedup[0] != null) {
      List<Integer> dedupFieldIndices = new ArrayList<>();
      for (RexNode field : dedup[0].getDedupeFields()) {
        if (field instanceof RexInputRef ref) {
          dedupFieldIndices.add(ref.getIndex());
        }
      }
      return new DedupReduce(
          dedupFieldIndices,
          dedup[0].getAllowedDuplication(),
          dedup[0].getKeepEmpty(),
          dedup[0].getInputCollation(),
          outputProj[0]);
    } else if (window[0] != null) {
      return new WindowReduce(window[0], outputProj[0]);
    }

    throw new IllegalStateException("Plan has neither Dedup nor Window");
  }

  /**
   * Extracts the scan fields (columns needed from doc_values) by finding the AbstractCalciteIndexScan.
   */
  public static AbstractCalciteIndexScan findScan(RelNode plan) {
    AbstractCalciteIndexScan[] scan = {null};
    new RelVisitor() {
      @Override
      public void visit(RelNode node, int ordinal, RelNode parent) {
        if (scan[0] != null) {
          return;
        }
        if (node instanceof AbstractCalciteIndexScan s) {
          scan[0] = s;
          return;
        }
        super.visit(node, ordinal, parent);
      }
    }.go(plan);
    return scan[0];
  }
}
