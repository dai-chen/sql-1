/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.stage;

import java.util.List;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.opensearch.sql.opensearch.storage.scan.AbstractCalciteIndexScan;

/** Shard fragments, gather exchanges, and the coordinator continuation that consumes them. */
public record StagePlan(
    List<GatherExchange> exchanges, RelNode coordinatorTree, boolean residualCoordinatorWork) {

  public StagePlan {
    exchanges = List.copyOf(exchanges);
  }

  public boolean allShardFragmentsReduce() {
    return exchanges.stream().allMatch(exchange -> exchange.source().partialAggregate());
  }

  /**
   * Explicit gather boundary from an independently executable shard fragment to one coordinator.
   */
  public record GatherExchange(ShardFragment source, CoordinatorInput target) {}

  /** Plan and scan contract executed independently on every selected shard. */
  public record ShardFragment(
      String tableName,
      String planJson,
      String inputRowTypeJson,
      RelDataType outputRowType,
      AbstractCalciteIndexScan scan,
      boolean partialAggregate) {}

  /** Coordinator table bound to the rows gathered from one shard fragment. */
  public record CoordinatorInput(String tableName, String dataContextKey) {}
}
