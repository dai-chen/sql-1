/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Test;

public class UnifiedQueryEngineTest extends CalciteAbstractTest {

  public UnifiedQueryEngineTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Test
  public void test() {
    String ppl = "source=EMP | stats count() as c";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalAggregate(group=[{}], c=[COUNT()])\n"
            + "  LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
    String expectedResult = "c=14\n";
    verifyResult(root, expectedResult);

    String expectedSparkSql = "" + "SELECT COUNT(*) `c`\n" + "FROM `scott`.`EMP`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }
}
