/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import com.google.common.collect.ImmutableList;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;

/** Logical plan node for Except (MINUS) operation. Returns rows in left that are not in right. */
@Getter
@ToString
@EqualsAndHashCode(callSuper = false)
@RequiredArgsConstructor
public class Except extends UnresolvedPlan {
  private final UnresolvedPlan left;
  private final UnresolvedPlan right;

  @Override
  public UnresolvedPlan attach(UnresolvedPlan child) {
    return new Except(child, right);
  }

  @Override
  public List<? extends UnresolvedPlan> getChild() {
    return ImmutableList.of(left, right);
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitExcept(this, context);
  }
}
