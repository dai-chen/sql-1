/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.routing;

import static org.junit.Assert.*;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.junit.Test;
import org.opensearch.analytics.plan.BoundaryScan;

public class ParquetQueryPlannerTest {

  @Test
  public void testMatchOnParquetIndexThrows() {
    UnsupportedOperationException ex =
        assertThrows(
            UnsupportedOperationException.class,
            () ->
                ParquetQueryPlanner.plan("source = parquet_index | where match(message, 'error')"));
    assertTrue(ex.getMessage().contains("not supported for Parquet"));
  }

  @Test
  public void testFilterAndProjectAbsorbed() throws Exception {
    RelNode result =
        ParquetQueryPlanner.plan(
            "source = parquet_index | where status = 200 | fields timestamp, status, message");

    assertTrue(
        "Expected BoundaryScan but got " + result.getClass().getSimpleName(),
        result instanceof BoundaryScan);
    BoundaryScan scan = (BoundaryScan) result;
    assertEquals(2, scan.getAbsorbedOperators().size());
  }

  @Test
  public void testAggregateAbsorbed() throws Exception {
    RelNode result = ParquetQueryPlanner.plan("source = parquet_index | stats count() by status");

    String plan = RelOptUtil.toString(result);
    assertTrue("Plan should contain BoundaryScan: " + plan, plan.contains("BoundaryScan"));
    // Aggregate should be absorbed into BoundaryScan
    BoundaryScan scan = findBoundaryScan(result);
    assertNotNull("BoundaryScan should exist in plan", scan);
    assertTrue("At least aggregate should be absorbed", scan.getAbsorbedOperators().size() >= 1);
  }

  @Test
  public void testSortAndLimitAbsorbed() throws Exception {
    RelNode result = ParquetQueryPlanner.plan("source = parquet_index | sort timestamp | head 100");

    String plan = RelOptUtil.toString(result);
    assertTrue("Plan should contain BoundaryScan: " + plan, plan.contains("BoundaryScan"));
    BoundaryScan scan = findBoundaryScan(result);
    assertNotNull("BoundaryScan should exist in plan", scan);
    assertTrue("At least sort should be absorbed", scan.getAbsorbedOperators().size() >= 1);
  }

  @Test
  public void testFormatExplain() throws Exception {
    RelNode result =
        ParquetQueryPlanner.plan(
            "source = parquet_index | where status = 200 | fields timestamp, status, message");
    String explain = ParquetQueryPlanner.formatExplain(result);

    assertNotNull(explain);
    assertTrue(explain.contains("Parquet"));
    assertTrue(explain.contains("plan"));
    assertTrue(explain.contains("BoundaryScan"));
  }

  private BoundaryScan findBoundaryScan(RelNode node) {
    if (node instanceof BoundaryScan) {
      return (BoundaryScan) node;
    }
    for (RelNode input : node.getInputs()) {
      BoundaryScan found = findBoundaryScan(input);
      if (found != null) {
        return found;
      }
    }
    return null;
  }
}
