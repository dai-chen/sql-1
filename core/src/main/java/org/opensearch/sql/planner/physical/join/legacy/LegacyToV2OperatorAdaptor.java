/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.physical.join.legacy;

import java.util.List;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.data.model.ExprValue;
import org.opensearch.sql.planner.physical.PhysicalPlan;
import org.opensearch.sql.planner.physical.PhysicalPlanNodeVisitor;

@RequiredArgsConstructor
public class LegacyToV2OperatorAdaptor extends PhysicalPlan {

  private final PhysicalOperator legacyOperator;

  @Override
  public <R, C> R accept(PhysicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitLegacy(this, context);
  }

  @Override
  public void open() {
    try {
      legacyOperator.open(null);
    } catch (Exception e) {
      e.printStackTrace();
    }
  }

  @Override
  public void close() {
    legacyOperator.close();
  }

  @Override
  public boolean hasNext() {
    return legacyOperator.hasNext();
  }

  @Override
  public ExprValue next() {
    return ((ExprValueRow) legacyOperator.next()).data();
  }

  @Override
  public List<PhysicalPlan> getChild() {
    return null;
  }
}
