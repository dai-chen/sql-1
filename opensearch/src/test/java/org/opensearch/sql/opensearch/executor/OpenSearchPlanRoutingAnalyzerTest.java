/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.executor;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.List;
import org.apache.calcite.adapter.enumerable.EnumerableLimit;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.opensearch.executor.OpenSearchPlanRoutingAnalyzer.LegacyExecutionShape;
import org.opensearch.sql.opensearch.planner.physical.CalciteEnumerableTopK;
import org.opensearch.sql.opensearch.storage.scan.AbstractCalciteIndexScan;

class OpenSearchPlanRoutingAnalyzerTest {

  @Test
  void singleScanIsFullyPushedDown() {
    AbstractCalciteIndexScan scan = scan("logs");

    var assessment = OpenSearchPlanRoutingAnalyzer.analyze(scan);

    assertEquals(LegacyExecutionShape.FULLY_PUSHDOWN, assessment.legacyShape());
    assertEquals(java.util.Set.of("logs"), assessment.indices());
  }

  @Test
  void lightweightLimitAboveScanIsFullyPushedDown() {
    AbstractCalciteIndexScan scan = scan("logs");
    EnumerableLimit limit = mock(EnumerableLimit.class);
    when(limit.getInputs()).thenReturn(List.of(scan));

    var assessment = OpenSearchPlanRoutingAnalyzer.analyze(limit);

    assertEquals(LegacyExecutionShape.FULLY_PUSHDOWN, assessment.legacyShape());
  }

  @Test
  void topKAboveScanIsResidualCoordinatorWork() {
    AbstractCalciteIndexScan scan = scan("logs");
    CalciteEnumerableTopK topK = mock(CalciteEnumerableTopK.class);
    when(topK.getInputs()).thenReturn(List.of(scan));

    var assessment = OpenSearchPlanRoutingAnalyzer.analyze(topK);

    assertEquals(LegacyExecutionShape.RESIDUAL_COORDINATOR_WORK, assessment.legacyShape());
  }

  @Test
  void multipleScansAreResidualCoordinatorWork() {
    RelNode root = mock(RelNode.class);
    AbstractCalciteIndexScan left = scan("left");
    AbstractCalciteIndexScan right = scan("right");
    when(root.getInputs()).thenReturn(List.of(left, right));

    var assessment = OpenSearchPlanRoutingAnalyzer.analyze(root);

    assertEquals(LegacyExecutionShape.RESIDUAL_COORDINATOR_WORK, assessment.legacyShape());
    assertEquals(java.util.Set.of("left", "right"), assessment.indices());
  }

  private static AbstractCalciteIndexScan scan(String index) {
    AbstractCalciteIndexScan scan = mock(AbstractCalciteIndexScan.class);
    RelOptTable table = mock(RelOptTable.class);
    when(scan.getTable()).thenReturn(table);
    when(table.getQualifiedName()).thenReturn(List.of("OpenSearch", index));
    when(scan.getInputs()).thenReturn(List.of());
    return scan;
  }
}
