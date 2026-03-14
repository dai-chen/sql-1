/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.plugin.routing;

import static org.junit.Assert.*;

import org.apache.calcite.plan.RelOptUtil;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalAggregate;
import org.apache.calcite.rel.logical.LogicalFilter;
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

  @Test
  public void testHourFunctionOnTimestamp() throws Exception {
    RelNode result =
        ParquetQueryPlanner.plan(
            "source = parquet_index | eval hour = hour(timestamp) | where timestamp >"
                + " '2024-01-01'");
    String plan = RelOptUtil.toString(result);
    assertTrue("Plan should contain BoundaryScan: " + plan, plan.contains("BoundaryScan"));
    // Validate timestamp filter is present in the top-level plan (no UDT type mismatch)
    assertTrue("Plan should contain timestamp filter: " + plan, plan.contains("2024"));
    // Validate BoundaryScan absorbed the project with HOUR
    BoundaryScan scan = findBoundaryScan(result);
    assertNotNull("BoundaryScan should exist in plan", scan);
    assertTrue("Operators should be absorbed", scan.getAbsorbedOperators().size() >= 1);
    // Validate row type includes the 'hour' field from eval (proves HOUR function worked)
    String rowType = scan.getRowType().toString();
    assertTrue("Row type should contain hour field: " + rowType, rowType.contains("hour"));
  }

  @Test
  public void testSpanOnTimestamp() throws Exception {
    RelNode result =
        ParquetQueryPlanner.plan("source = parquet_index | stats count() by span(timestamp, 1h)");
    String plan = RelOptUtil.toString(result);
    assertTrue("Plan should contain BoundaryScan: " + plan, plan.contains("BoundaryScan"));
    // Validate SPAN function is present in the plan (no UDT type mismatch)
    assertTrue("Plan should contain span: " + plan, plan.contains("span"));
    // Validate aggregate structure is absorbed
    BoundaryScan scan = findBoundaryScan(result);
    assertNotNull("BoundaryScan should exist in plan", scan);
    boolean hasAggregate =
        scan.getAbsorbedOperators().stream().anyMatch(op -> op instanceof LogicalAggregate);
    assertTrue("Absorbed operators should include aggregate", hasAggregate);
  }

  @Test
  public void testDatetimeComparisonWithSpan() throws Exception {
    RelNode result =
        ParquetQueryPlanner.plan(
            "source = parquet_index | where timestamp > '2024-01-01' | stats count() by"
                + " span(timestamp, 1h)");
    String plan = RelOptUtil.toString(result);
    assertTrue("Plan should contain BoundaryScan: " + plan, plan.contains("BoundaryScan"));
    // Validate span function appears in the plan output
    assertTrue("Plan should contain span: " + plan, plan.contains("span"));
    // Validate both filter and aggregate are absorbed
    BoundaryScan scan = findBoundaryScan(result);
    assertNotNull("BoundaryScan should exist in plan", scan);
    assertTrue(
        "At least filter + aggregate should be absorbed", scan.getAbsorbedOperators().size() >= 2);
    boolean hasFilter =
        scan.getAbsorbedOperators().stream().anyMatch(op -> op instanceof LogicalFilter);
    boolean hasAggregate =
        scan.getAbsorbedOperators().stream().anyMatch(op -> op instanceof LogicalAggregate);
    assertTrue("Absorbed operators should include filter", hasFilter);
    assertTrue("Absorbed operators should include aggregate", hasAggregate);
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

  @Test
  public void testSqlFilterProject() throws Exception {
    RelNode result =
        ParquetQueryPlanner.planSql(
            "SELECT `timestamp`, status, message FROM parquet_index WHERE status = 200");

    String plan = RelOptUtil.toString(result);
    assertTrue("Plan should contain BoundaryScan: " + plan, plan.contains("BoundaryScan"));
    BoundaryScan scan = findBoundaryScan(result);
    assertNotNull("BoundaryScan should exist in plan", scan);
    assertEquals("Filter + Project should be absorbed", 2, scan.getAbsorbedOperators().size());
  }

  @Test
  public void testSqlAggregate() throws Exception {
    RelNode result =
        ParquetQueryPlanner.planSql("SELECT count(*) FROM parquet_index GROUP BY status");

    String plan = RelOptUtil.toString(result);
    assertTrue("Plan should contain BoundaryScan: " + plan, plan.contains("BoundaryScan"));
    BoundaryScan scan = findBoundaryScan(result);
    assertNotNull("BoundaryScan should exist in plan", scan);
    assertTrue("At least aggregate should be absorbed", scan.getAbsorbedOperators().size() >= 1);
  }

  @Test
  public void testSqlDatetime() throws Exception {
    RelNode result =
        ParquetQueryPlanner.planSql(
            "SELECT * FROM parquet_index WHERE `timestamp` > TIMESTAMP '2024-01-01 00:00:00'");

    String plan = RelOptUtil.toString(result);
    assertTrue("Plan should contain BoundaryScan: " + plan, plan.contains("BoundaryScan"));
    BoundaryScan scan = findBoundaryScan(result);
    assertNotNull("BoundaryScan should exist in plan", scan);
    assertTrue("At least filter should be absorbed", scan.getAbsorbedOperators().size() >= 1);
    boolean hasFilter =
        scan.getAbsorbedOperators().stream().anyMatch(op -> op instanceof LogicalFilter);
    assertTrue("Absorbed operators should include filter", hasFilter);
  }

  @Test
  public void testSqlFormatExplain() throws Exception {
    RelNode result =
        ParquetQueryPlanner.planSql(
            "SELECT `timestamp`, status FROM parquet_index WHERE status = 200");
    String explain = ParquetQueryPlanner.formatExplain(result);

    assertNotNull(explain);
    assertTrue(explain.contains("Parquet"));
    assertTrue(explain.contains("plan"));
    assertTrue(explain.contains("BoundaryScan"));
  }
}
