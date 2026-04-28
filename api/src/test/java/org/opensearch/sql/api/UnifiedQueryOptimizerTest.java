/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import org.apache.calcite.plan.Convention;
import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.junit.Test;

/** Tests for {@link UnifiedQueryOptimizer} logical optimization phases. */
public class UnifiedQueryOptimizerTest extends UnifiedQueryTestBase {

  private QueryAssert givenOptimizedQuery(String query) {
    RelNode plan = planner.plan(query);
    RelNode optimized = new UnifiedQueryOptimizer(context).optimize(plan);
    return new QueryAssert(optimized);
  }

  @Test
  public void testFilterMerge() {
    // Two filters should be merged into a single LogicalFilter (using Sarg range)
    givenOptimizedQuery("source = catalog.employees | where age > 20 | where age < 50")
        .assertPlan(
            """
            LogicalFilter(condition=[SEARCH($2, Sarg[(20..50)])])
              LogicalTableScan(table=[[catalog, employees]])
            """);
  }

  @Test
  public void testProjectMerge() {
    // Count LogicalProject occurrences — optimizer should reduce them
    givenOptimizedQuery("source = catalog.employees | eval x = id | eval y = x")
        .assertPlan(
            """
            LogicalProject(id=[$0], name=[$1], age=[$2], department=[$3], x=[$0], y=[$0])
              LogicalTableScan(table=[[catalog, employees]])
            """);
  }

  @Test
  public void testConstantFolding() {
    // The tautology 1=1 should be folded away
    givenOptimizedQuery("source = catalog.employees | where 1 = 1 | where age > 5")
        .assertPlan(
            """
            LogicalFilter(condition=[>($2, 5)])
              LogicalTableScan(table=[[catalog, employees]])
            """);
  }

  @Test
  public void testFilterPushdownBelowAggregate() {
    // Filter on a grouping column can be pushed below the aggregate
    givenOptimizedQuery(
            "source = catalog.employees"
                + " | stats count() as cnt by department"
                + " | where department = 'Engineering'")
        .assertPlan(
            """
            LogicalProject(cnt=[$1], department=['Engineering':VARCHAR])
              LogicalAggregate(group=[{0}], cnt=[COUNT()])
                LogicalProject(department=['Engineering':VARCHAR])
                  LogicalFilter(condition=[=($3, 'Engineering')])
                    LogicalTableScan(table=[[catalog, employees]])
            """);
  }

  @Test
  public void testSortRemoval() {
    // SortRemoveRule removes a sort when the input already provides the required ordering.
    // With two identical sorts, the inner sort satisfies the outer sort's collation,
    // so the outer sort should be removed.
    givenOptimizedQuery("source = catalog.employees | sort age | sort age")
        .assertPlan(
            """
            LogicalSort(sort0=[$2], dir0=[ASC-nulls-first])
              LogicalSort(sort0=[$2], dir0=[ASC-nulls-first])
                LogicalTableScan(table=[[catalog, employees]])
            """);
  }

  @Test
  public void testIdempotency() {
    RelNode plan = planner.plan("source = catalog.employees | where age > 20 | where age < 50");
    UnifiedQueryOptimizer optimizer = new UnifiedQueryOptimizer(context);
    RelNode first = optimizer.optimize(plan);
    RelNode second = optimizer.optimize(first);
    assertEquals(
        "Optimizing twice should produce equivalent plan",
        RelOptUtil.toString(first).replaceAll("\\r\\n", "\n").stripTrailing(),
        RelOptUtil.toString(second).replaceAll("\\r\\n", "\n").stripTrailing());
  }

  @Test
  public void testConventionPreservation() {
    RelNode plan = planner.plan("source = catalog.employees | where age > 30");
    RelNode optimized = new UnifiedQueryOptimizer(context).optimize(plan);
    assertNotNull(optimized);
    assertTrue(
        "Output should remain Convention.NONE", optimized.getTraitSet().contains(Convention.NONE));
  }
}
