/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.planner.logical;

import java.util.Arrays;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.ToString;
import org.opensearch.sql.expression.Expression;

/**
 * Logical join operator.
 */
@EqualsAndHashCode(callSuper = false)
@Getter
@ToString
public class LogicalJoin extends LogicalPlan {

  private final String joinType;

  private final Expression joinCondition;

  public LogicalJoin(LogicalPlan left,
                     LogicalPlan right,
                     String joinType,
                     Expression joinCondition) {
    super(Arrays.asList(left, right));

    this.joinType = joinType;
    this.joinCondition = joinCondition;
  }

  @Override
  public <R, C> R accept(LogicalPlanNodeVisitor<R, C> visitor, C context) {
    return visitor.visitJoin(this, context);
  }
}
