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

  private StagePlan(
      boolean staged,
      @Nullable RelNode shardFragment,
      @Nullable CombineDescriptor combine,
      RelNode coordinatorTree,
      @Nullable AbstractCalciteIndexScan shardScan) {
    this.staged = staged;
    this.shardFragment = shardFragment;
    this.combine = combine;
    this.coordinatorTree = coordinatorTree;
    this.shardScan = shardScan;
  }

  /** Creates a fully staged result with a shard fragment, combine descriptor, and coordinator. */
  static StagePlan staged(
      RelNode shardFragment,
      CombineDescriptor combine,
      RelNode coordinatorTree,
      AbstractCalciteIndexScan shardScan) {
    return new StagePlan(true, shardFragment, combine, coordinatorTree, shardScan);
  }

  /**
   * Creates a coordinator-only (non-staged) result. Used when the plan has zero or more than one
   * {@link AbstractCalciteIndexScan}, making a single shard-to-coordinator boundary impossible.
   */
  static StagePlan coordinatorOnly(RelNode root) {
    return new StagePlan(false, null, null, root, null);
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
}
