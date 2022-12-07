/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical.join.legacy;

import lombok.RequiredArgsConstructor;
import org.opensearch.sql.planner.logical.LogicalRelation;
import org.opensearch.sql.planner.physical.PhysicalPlan;

@RequiredArgsConstructor
public class V2ToLegacyOperatorAdaptor<T> implements PhysicalOperator<T> {

  private final PhysicalPlan v2Operator;

  private final String alias;

  @Override
  public Cost estimate() {
    return new Cost();
  }

  @Override
  public void open(ExecuteParams params) throws Exception {
    v2Operator.open();
  }

  @Override
  public void close() {
    v2Operator.close();
  }

  @Override
  public boolean hasNext() {
    return v2Operator.hasNext();
  }

  @Override
  public Row<T> next() {
    return (Row<T>) new ExprValueRow(v2Operator.next(), alias);
  }

  @Override
  public PlanNode[] children() {
    return new PlanNode[0];
  }
}
