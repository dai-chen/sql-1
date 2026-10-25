/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.stage;

import javax.annotation.Nullable;
import org.apache.calcite.rel.RelNode;
import org.opensearch.sql.opensearch.storage.scan.AbstractCalciteIndexScan;

/**
 * Immutable result of {@link StagePlanner#split(RelNode)}. Represents a plan split into a
 * shard-local fragment, a combine descriptor, and a coordinator tree.
 *
 * <p>When the plan cannot be staged (zero or multiple scans), {@link #staged()} returns false, the
 * coordinator tree is the untouched input root, and the shard-side accessors return null. This is a
 * PoC scope limit — multi-scan staging is deferred to a later story.
 */
public final class StagePlan {

  private final boolean staged;
  private final @Nullable RelNode shardFragment;
  private final @Nullable CombineDescriptor combine;
  private final RelNode coordinatorTree;
  private final @Nullable AbstractCalciteIndexScan shardScan;
  private final @Nullable String forcingOperator;
  private final @Nullable Integer earlyTerminationLimit;

  private StagePlan(
      boolean staged,
      @Nullable RelNode shardFragment,
      @Nullable CombineDescriptor combine,
      RelNode coordinatorTree,
      @Nullable AbstractCalciteIndexScan shardScan,
      @Nullable String forcingOperator,
      @Nullable Integer earlyTerminationLimit) {
    this.staged = staged;
    this.shardFragment = shardFragment;
    this.combine = combine;
    this.coordinatorTree = coordinatorTree;
    this.shardScan = shardScan;
    this.forcingOperator = forcingOperator;
    this.earlyTerminationLimit = earlyTerminationLimit;
  }

  /** Creates a fully staged result with a shard fragment, combine descriptor, and coordinator. */
  static StagePlan staged(
      RelNode shardFragment,
      CombineDescriptor combine,
      RelNode coordinatorTree,
      AbstractCalciteIndexScan shardScan,
      @Nullable String forcingOperator) {
    return new StagePlan(
        true, shardFragment, combine, coordinatorTree, shardScan, forcingOperator, null);
  }

  /** Creates a fully staged result with an early termination limit for the shard. */
  static StagePlan staged(
      RelNode shardFragment,
      CombineDescriptor combine,
      RelNode coordinatorTree,
      AbstractCalciteIndexScan shardScan,
      @Nullable String forcingOperator,
      @Nullable Integer earlyTerminationLimit) {
    return new StagePlan(
        true,
        shardFragment,
        combine,
        coordinatorTree,
        shardScan,
        forcingOperator,
        earlyTerminationLimit);
  }

  /**
   * Creates a coordinator-only (non-staged) result. Used when the plan has zero or more than one
   * {@link AbstractCalciteIndexScan}, making a single shard-to-coordinator boundary impossible.
   */
  static StagePlan coordinatorOnly(RelNode root) {
    return new StagePlan(false, null, null, root, null, null, null);
  }

  /** Whether this plan was successfully split into shard + coordinator stages. */
  public boolean staged() {
    return staged;
  }

  /** The shard-local fragment (serializable via RelFragmentCodec). Null if not staged. */
  @Nullable
  public RelNode shardFragment() {
    return shardFragment;
  }

  /** The combine descriptor (CONCAT for the generic floor). Null if not staged. */
  @Nullable
  public CombineDescriptor combine() {
    return combine;
  }

  /** The coordinator-side tree. If staged, its leaf is a gathered-rows scan. If not, the root. */
  public RelNode coordinatorTree() {
    return coordinatorTree;
  }

  /**
   * The original AbstractCalciteIndexScan that was cut, so downstream (US-008) can extract the
   * index name, pushed-down query DSL, and field list without re-walking the tree. Null if not
   * staged.
   */
  @Nullable
  public AbstractCalciteIndexScan shardScan() {
    return shardScan;
  }

  /**
   * The Calcite RelNode type name of the operator that forced the gather (the immediate parent of
   * the cut in the original tree). Null when the cut IS the root, i.e. nothing forced a gather —
   * which happens when the entire plan is shard-local. Used in the row budget error message so the
   * user knows which operation caused the full-shard gather.
   */
  @Nullable
  public String forcingOperator() {
    return forcingOperator;
  }

  /**
   * The early termination limit for shard-level collection. Non-null only when rule 4 (limit
   * pushdown) fires AND the entire fragment between the promoted Sort and the scan is
   * cardinality-preserving (no Filter, Aggregate, Window, Join, etc.). When non-null, the shard may
   * stop collecting documents at this count via CollectionTerminatedException. When null, the shard
   * must collect all matching documents even though a LIMIT combine may be in effect — because a
   * shard-local Filter could reduce the output below N if collection is stopped early.
   */
  @Nullable
  public Integer earlyTerminationLimit() {
    return earlyTerminationLimit;
  }
}
