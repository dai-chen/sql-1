/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import java.util.Collections;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.Node;
import org.opensearch.sql.ast.expression.UnresolvedExpression;

/** Abstract unresolved plan. */
@EqualsAndHashCode(callSuper = false)
@ToString
public abstract class UnresolvedPlan extends Node {
  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitChildren(this, context);
  }

  public abstract UnresolvedPlan attach(UnresolvedPlan child);

  /**
   * Returns the field expressions this command operates on by name (not via expression resolution).
   * Commands that match field names by string against the schema should override this to declare
   * their operand fields, enabling auto-materialization of MAP dotted paths before the command
   * runs.
   *
   * @return list of field expressions this command needs in the schema, empty by default
   */
  public List<UnresolvedExpression> getOperands() {
    return Collections.emptyList();
  }
}
