/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ast.tree;

import com.google.common.collect.ImmutableList;
import java.util.List;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;
import org.jetbrains.annotations.Nullable;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.expression.UnresolvedExpression;

/**
 * AST node for the mvcombine command.
 * Combines multiple events with identical field values (except for the specified field)
 * into a single event with the specified field as a multivalue array or delimited string.
 */
@Getter
@Setter
@ToString
@EqualsAndHashCode(callSuper = false)
public class Mvcombine extends UnresolvedPlan {
  private final UnresolvedExpression field;
  @Nullable private final String delimiter;
  @Nullable private UnresolvedPlan child;

  /**
   * Constructor for mvcombine command.
   *
   * @param field The field expression to combine into multivalue field
   * @param delimiter Optional delimiter string for combining values (null means array output)
   */
  public Mvcombine(UnresolvedExpression field, @Nullable String delimiter) {
    this.field = field;
    this.delimiter = delimiter;
  }

  @Override
  public Mvcombine attach(UnresolvedPlan child) {
    if (null == this.child) {
      this.child = child;
    } else {
      this.child.attach(child);
    }
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
