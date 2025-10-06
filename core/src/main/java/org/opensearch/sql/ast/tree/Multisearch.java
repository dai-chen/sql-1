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
import lombok.Setter;
import lombok.ToString;
import org.opensearch.sql.ast.AbstractNodeVisitor;
import org.opensearch.sql.ast.Node;

/** Logical plan node of Multisearch, the interface for UNION ALL + ORDER BY @timestamp DESC for multiple subsearches. */
@Getter
@Setter
@ToString
@EqualsAndHashCode(callSuper = false)
@RequiredArgsConstructor
public class Multisearch extends UnresolvedPlan {

  private final List<UnresolvedPlan> subSearches;

  private UnresolvedPlan child;

  @Override
  public UnresolvedPlan attach(UnresolvedPlan child) {
    this.child = child;
    return this;
  }

  @Override
  public List<? extends Node> getChild() {
    return this.child == null ? ImmutableList.of() : ImmutableList.of(this.child);
  }

  @Override
  public <T, C> T accept(AbstractNodeVisitor<T, C> visitor, C context) {
    return visitor.visitMultisearch(this, context);
  }
}
