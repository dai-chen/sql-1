/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite;

import java.util.HashMap;
import java.util.Map;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.tools.RelBuilder;
import org.opensearch.sql.ast.expression.UnresolvedExpression;

/**
 * The GROUP BY columns one {@code Aggregate} exposes to the expressions above it.
 *
 * <p>Above an Aggregate the base columns a computed group key was built from are gone, so a
 * reference to that key can only mean the group-key column the Aggregate already produces. This
 * records, per group-by expression, which output column that is.
 *
 * <p>Modelled on Calcite's {@code org.apache.calcite.sql2rel.AggConverter}, which does the same job
 * for {@code SqlToRelConverter}: it keeps the group expressions it has registered and offers {@code
 * lookupGroupExpr(SqlNode)} (ordinal, or -1) alongside {@code lookupAggregates(SqlCall)}, and is
 * held as the single {@code Blackboard.agg} field for the query block being converted. We key the
 * lookup on a hash map rather than Calcite's {@code SqlUtil.indexOfDeep} list scan because our AST
 * nodes implement structural {@code equals}/{@code hashCode}, which {@code SqlNode} does not.
 *
 * <p><b>Lifetime.</b> The recorded ordinals address one Aggregate's output row type and mean
 * nothing against any other, so exactly one scope is live at a time: {@link #enter()} discards the
 * previous one and must be called before an aggregation resolves its own expressions -- that is,
 * after its children are built, since an inner Aggregate (a subquery, or the other side of a join)
 * registers its own keys while it is being planned. Calcite gets the same guarantee by clearing
 * {@code Blackboard.agg} in a {@code finally}; here the scope has to outlive the {@code
 * visitAggregation} call that fills it, because the Project, HAVING and ORDER BY above the
 * Aggregate are resolved by the caller afterwards.
 */
public class AggregateOutputScope {

  private final Map<UnresolvedExpression, Integer> groupKeyOrdinals = new HashMap<>();

  /** Starts a fresh scope for the Aggregate about to be built, discarding any previous one. */
  public void enter() {
    groupKeyOrdinals.clear();
  }

  /**
   * Records that {@code key} is emitted as output column {@code ordinal} of the Aggregate this
   * scope describes. Registering every group-by expression, not only {@code Function} nodes, is
   * what makes {@code CASE}/{@code CAST}/{@code IN}/{@code NOT}/{@code AND}/{@code OR}/{@code
   * BETWEEN} keys resolvable -- those extend {@link UnresolvedExpression} directly.
   */
  public void registerGroupKey(UnresolvedExpression key, int ordinal) {
    groupKeyOrdinals.put(key, ordinal);
  }

  /**
   * Resolves a reference to a registered group-by expression to the Aggregate's group-key column.
   *
   * @return that column, or {@code null} when {@code node} is not a registered group key, in which
   *     case the caller resolves it the ordinary way
   * @throws IllegalStateException if the recorded ordinal is out of range for what {@code
   *     relBuilder} currently holds, which means a scope outlived its Aggregate -- a planner bug
   *     that would otherwise surface as an {@code IndexOutOfBoundsException} or, when the ordinal
   *     happens to be in range, as a silently wrong column
   */
  public RexNode lookupGroupKey(UnresolvedExpression node, RelBuilder relBuilder) {
    Integer ordinal = groupKeyOrdinals.get(node);
    if (ordinal == null) {
      return null;
    }
    int available = relBuilder.peek().getRowType().getFieldCount();
    if (ordinal >= available) {
      throw new IllegalStateException(
          String.format(
              "Group-key scope outlived its Aggregate: %s registered at output column %d but the"
                  + " current row type has only %d column(s)",
              node, ordinal, available));
    }
    return relBuilder.field(ordinal);
  }
}
