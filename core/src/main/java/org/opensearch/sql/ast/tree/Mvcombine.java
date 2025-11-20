/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import com.google.common.collect.ImmutableList;
import java.util.List;
import javax.annotation.Nullable;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.RequiredArgsConstructor;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.UnresolvedExpression;

/** AST node representing a {@code mvcombine [delim="..."] <field>} operation. */
@ToString
@RequiredArgsConstructor
@EqualsAndHashCode(callSuper = false)
public class Mvcombine extends UnresolvedPlan {

  private UnresolvedPlan child;
  @Getter private final UnresolvedExpression field;
  @Getter @Nullable private final String delimiter;

  @Override
  public Mvcombine attach(UnresolvedPlan child) {
    this.child = child;
    return this;
  }

  @Override
  public List<UnresolvedPlan> getChild() {
    return this.child == null ? ImmutableList.of() : ImmutableList.of(this.child);
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> nodeVisitor, C context) {
    return nodeVisitor.visitMvcombine(this, context);
  }
}
