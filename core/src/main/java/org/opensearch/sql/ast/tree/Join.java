/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import java.util.Arrays;
import java.util.List;
import lombok.Data;
import lombok.EqualsAndHashCode;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.Node;
import org.opensearch.sql.ast.expression.UnresolvedExpression;

/**
 * Join AST node.
 */
@Data
@EqualsAndHashCode(callSuper = false)
public class Join extends UnresolvedPlan {

  private UnresolvedPlan left;

  private UnresolvedPlan right;

  private final String joinType;

  private UnresolvedExpression joinCondition;

  @Override
  public List<? extends Node> getChild() {
    return Arrays.asList(left, right);
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitJoin(this, context);
  }

  @Override
  public UnresolvedPlan attach(UnresolvedPlan child) {
    if (left == null) {
      left = child;
    } else if (right == null) {
      right = child;
    }
    return this;
  }
}
