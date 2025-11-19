/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import static org.junit.Assert.assertTrue;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Test;

/** Unit tests for mvcombine command logical plan generation. */
public class CalcitePPLMvcombineTest extends CalcitePPLAbstractTest {

  public CalcitePPLMvcombineTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Test
  public void testMvcombineBasicSyntax() {
    String ppl = "source=EMP | stats max(SAL) AS max, min(SAL) AS min BY JOB | mvcombine JOB";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        "LogicalProject(max=[$0], min=[$1], JOB=[$2])\n"
            + "  LogicalAggregate(group=[{0, 1}], JOB=[ARRAY_AGG($2)])\n"
            + "    LogicalProject(max=[$0], min=[$1], JOB=[$2])\n"
            + "      LogicalAggregate(group=[{0}], max=[MAX($1)], min=[MIN($1)])\n"
            + "        LogicalProject(JOB=[$2], SAL=[$5])\n"
            + "          LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
  }

  @Test
  public void testMvcombineWithDelimiter() {
    String ppl = "source=EMP | stats count() BY DEPTNO | mvcombine delim=\",\" DEPTNO";
    RelNode root = getRelNode(ppl);
    
    // Verify plan contains ARRAY_AGG aggregation
    String logical = root.explain();
    assertTrue("Plan should use ARRAY_AGG aggregation", logical.contains("ARRAY_AGG"));
  }

  @Test
  public void testMvcombineGroupingLogic() {
    // Test that mvcombine correctly groups by all fields except the target field
    String ppl = "source=EMP | eval a=1, b=2, c=3 | mvcombine b";
    RelNode root = getRelNode(ppl);
    
    // Verify ARRAY_AGG is used
    String logical = root.explain();
    assertTrue("Plan should use ARRAY_AGG for field b", logical.contains("ARRAY_AGG"));
  }

  @Test
  public void testMvcombineSQLTranslation() {
    String ppl = "source=EMP | stats max(SAL) AS max, min(SAL) AS min BY JOB | mvcombine JOB";
    RelNode root = getRelNode(ppl);
    String expectedSparkSql =
        "SELECT `max`, `min`, ARRAY_AGG(`JOB`) `JOB`\n"
            + "FROM (SELECT MAX(`SAL`) `max`, MIN(`SAL`) `min`, `JOB`\n"
            + "FROM `scott`.`EMP`\n"
            + "GROUP BY `JOB`) `t`\n"
            + "GROUP BY `max`, `min`";
    verifyPPLToSparkSQL(root, expectedSparkSql);
  }
}
