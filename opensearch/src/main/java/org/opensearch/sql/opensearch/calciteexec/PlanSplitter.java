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
import org.opensearch.sql.calcite.plan.rel.Dedup;
import org.opensearch.sql.calcite.plan.rule.PPLDedupConvertRule;
import org.opensearch.sql.opensearch.storage.scan.AbstractCalciteIndexScan;

/**
 * Splits an optimized RelNode into a shard fragment (to execute per shard via calcite_exec
 * aggregation) and extracts parameters for the coordinator reduce tree.
 *
 * <p>Phase 1 strategy (single-shard PoC):
 *
 * <ul>
 *   <li>Shard fragment: a TableScan on "shard_source" that collects doc_values rows. The plan is
 *       serializable via RelJsonWriter.
 *   <li>Reduce: described by parameters (dedup keys, window groups, output projection) which the
 *       engine uses to build a fresh plan via RelBuilder.
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
      List<Integer> outputProjection)
      implements ReduceSpec {}

  public record WindowReduce(Window window, List<Integer> outputProjection) implements ReduceSpec {}

  private static final java.util.Set<String> METADATA_FIELDS =
      java.util.Set.of("_id", "_index", "_score", "_maxscore", "_sort", "_routing");

  /**
   * Phase 1: shard fragment executes dedup locally (idempotent — re-dedup on coordinator is safe).
   * For window/eventstats, the shard ships raw rows because a window needs the full partition.
   */
  public static SplitResult split(RelNode plan) {
    AbstractCalciteIndexScan osScan = findScan(plan);
    if (osScan == null) {
      throw new IllegalStateException("No OpenSearch index scan found in plan");
    }

    // Extract schema from the scan's row type, excluding metadata fields (no doc_values)
    List<String> allFieldNames = osScan.getRowType().getFieldNames();
    List<SqlTypeName> allFieldTypes =
        osScan.getRowType().getFieldList().stream().map(f -> f.getType().getSqlTypeName()).toList();

    List<String> fieldNames = new ArrayList<>();
    List<SqlTypeName> fieldTypes = new ArrayList<>();
    for (int i = 0; i < allFieldNames.size(); i++) {
      if (!METADATA_FIELDS.contains(allFieldNames.get(i))) {
        fieldNames.add(allFieldNames.get(i));
        fieldTypes.add(allFieldTypes.get(i));
      }
    }

    // Analyze the original plan to extract reduce parameters (using filtered field indices)
    ReduceSpec reduceSpec = analyzeForReduce(plan, allFieldNames);

    // Build shard fragment with dedup pushed down for the dedup case
    SchemaPlus shardSchema = FragmentDeserializer.buildSchema(fieldNames, fieldTypes);
    RelBuilder shardBuilder =
        RelBuilder.create(Frameworks.newConfigBuilder().defaultSchema(shardSchema).build());
    shardBuilder.scan(FragmentDeserializer.TABLE_NAME);

    if (reduceSpec instanceof DedupReduce dedupSpec) {
      // Dedup is idempotent: shard-local dedup reduces rows shipped to coordinator
      List<RexNode> dedupFields = new ArrayList<>();
      for (int idx : dedupSpec.dedupFieldIndices()) {
        dedupFields.add(shardBuilder.field(idx));
      }
      if (dedupSpec.keepEmpty()) {
        PPLDedupConvertRule.buildDedupOrNull(
            shardBuilder, dedupFields, dedupSpec.allowedDuplication(), dedupSpec.inputCollation());
      } else {
        PPLDedupConvertRule.buildDedupNotNull(
            shardBuilder, dedupFields, dedupSpec.allowedDuplication(), dedupSpec.inputCollation());
      }
    }
    // Window/eventstats: do NOT push to shard — window needs the full partition across all shards

    RelNode shardFragment = shardBuilder.build();
    return new SplitResult(shardFragment, fieldNames, fieldTypes, reduceSpec);
  }

  private static ReduceSpec analyzeForReduce(RelNode plan, List<String> allFieldNames) {
    Dedup[] dedup = {null};
    Window[] window = {null};
    List<Integer>[] outputProj = new List[] {null};

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
        if (node instanceof Project p
            && outputProj[0] == null
            && dedup[0] == null
            && window[0] == null) {
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

    // Build a remapping from original field indices to filtered (no-metadata) indices
    int[] remapIndex = new int[allFieldNames.size()];
    int filteredIdx = 0;
    for (int i = 0; i < allFieldNames.size(); i++) {
      if (!METADATA_FIELDS.contains(allFieldNames.get(i))) {
        remapIndex[i] = filteredIdx++;
      } else {
        remapIndex[i] = -1;
      }
    }

    if (dedup[0] != null) {
      List<Integer> dedupFieldIndices = new ArrayList<>();
      for (RexNode field : dedup[0].getDedupeFields()) {
        if (field instanceof RexInputRef ref) {
          int mapped = remapIndex[ref.getIndex()];
          if (mapped >= 0) {
            dedupFieldIndices.add(mapped);
          }
        }
      }
      List<Integer> remappedOutputProj = remapProjection(outputProj[0], remapIndex);
      RelCollation remappedCollation = remapCollation(dedup[0].getInputCollation(), remapIndex);
      return new DedupReduce(
          dedupFieldIndices,
          dedup[0].getAllowedDuplication(),
          dedup[0].getKeepEmpty(),
          remappedCollation,
          remappedOutputProj);
    } else if (window[0] != null) {
      List<Integer> remappedOutputProj = remapProjection(outputProj[0], remapIndex);
      return new WindowReduce(window[0], remappedOutputProj);
    }

    throw new IllegalStateException("Plan has neither Dedup nor Window");
  }

  private static List<Integer> remapProjection(List<Integer> proj, int[] remapIndex) {
    if (proj == null) return null;
    List<Integer> remapped = new ArrayList<>();
    for (int idx : proj) {
      if (idx < 0 || idx >= remapIndex.length) {
        remapped.add(idx);
      } else {
        remapped.add(remapIndex[idx]);
      }
    }
    return remapped;
  }

  private static RelCollation remapCollation(RelCollation collation, int[] remapIndex) {
    if (collation == null || collation.getFieldCollations().isEmpty()) {
      return collation;
    }
    List<org.apache.calcite.rel.RelFieldCollation> remapped = new ArrayList<>();
    for (org.apache.calcite.rel.RelFieldCollation fc : collation.getFieldCollations()) {
      int oldIdx = fc.getFieldIndex();
      if (oldIdx >= 0 && oldIdx < remapIndex.length && remapIndex[oldIdx] >= 0) {
        remapped.add(fc.withFieldIndex(remapIndex[oldIdx]));
      }
    }
    if (remapped.isEmpty()) {
      return org.apache.calcite.rel.RelCollations.EMPTY;
    }
    return org.apache.calcite.rel.RelCollations.of(remapped);
  }

  /**
   * Extracts the scan fields (columns needed from doc_values) by finding the
   * AbstractCalciteIndexScan.
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
