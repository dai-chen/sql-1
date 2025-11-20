/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Test;

public class CalcitePPLMvcombineTest extends CalcitePPLAbstractTest {

  public CalcitePPLMvcombineTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Test
  public void testMvcombineWithoutDelimiter() {
    String ppl = "source=EMP | stats max(SAL) as max, min(SAL) as min by JOB | mvcombine JOB";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalAggregate(group=[{0, 1}], JOB=[LIST($2)])\n"
            + "  LogicalProject(max=[$1], min=[$2], JOB=[$0])\n"
            + "    LogicalAggregate(group=[{0}], max=[MAX($1)], min=[MIN($1)])\n"
            + "      LogicalProject(JOB=[$2], SAL=[$5])\n"
            + "        LogicalTableScan(table=[[scott, EMP]])\n";

    verifyLogical(root, expectedLogical);
  }

  @Test
  public void testMvcombineWithDelimiter() {
    String ppl =
        "source=EMP | stats max(SAL) as max, min(SAL) as min by JOB | mvcombine delim=\",\" JOB";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(max=[$0], min=[$1], JOB=[ARRAY_JOIN($2, ',')])\n"
            + "  LogicalAggregate(group=[{0, 1}], __temp_list__=[LIST($2)])\n"
            + "    LogicalProject(max=[$1], min=[$2], JOB=[$0])\n"
            + "      LogicalAggregate(group=[{0}], max=[MAX($1)], min=[MIN($1)])\n"
            + "        LogicalProject(JOB=[$2], SAL=[$5])\n"
            + "          LogicalTableScan(table=[[scott, EMP]])\n";

    verifyLogical(root, expectedLogical);
  }

  @Test
  public void testMvcombineWithCustomDelimiter() {
    String ppl =
        "source=EMP | stats count() as cnt by DEPTNO | mvcombine delim=\" | \" DEPTNO";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalProject(cnt=[$0], DEPTNO=[ARRAY_JOIN($1, ' | ')])\n"
            + "  LogicalAggregate(group=[{0}], __temp_list__=[LIST($1)])\n"
            + "    LogicalProject(cnt=[$1], DEPTNO=[$0])\n"
            + "      LogicalAggregate(group=[{0}], cnt=[COUNT()])\n"
            + "        LogicalProject(DEPTNO=[$7])\n"
            + "          LogicalTableScan(table=[[scott, EMP]])\n";

    verifyLogical(root, expectedLogical);
  }

  @Test
  public void testMvcombineWithMultipleGroupByFields() {
    String ppl =
        "source=EMP | stats count() as cnt by DEPTNO, MGR | mvcombine DEPTNO";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalAggregate(group=[{0, 2}], DEPTNO=[LIST($1)])\n"
            + "  LogicalProject(cnt=[$2], DEPTNO=[$0], MGR=[$1])\n"
            + "    LogicalAggregate(group=[{0, 1}], cnt=[COUNT()])\n"
            + "      LogicalProject(DEPTNO=[$7], MGR=[$3])\n"
            + "        LogicalTableScan(table=[[scott, EMP]])\n";

    verifyLogical(root, expectedLogical);
  }

  @Test
  public void testMvcombineAfterFilter() {
    String ppl =
        "source=EMP | where SAL > 1000 | stats max(SAL) as max by JOB | mvcombine JOB";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalAggregate(group=[{0}], JOB=[LIST($1)])\n"
            + "  LogicalProject(max=[$1], JOB=[$0])\n"
            + "    LogicalAggregate(group=[{0}], max=[MAX($1)])\n"
            + "      LogicalProject(JOB=[$2], SAL=[$5])\n"
            + "        LogicalFilter(condition=[>($5, 1000)])\n"
            + "          LogicalTableScan(table=[[scott, EMP]])\n";

    verifyLogical(root, expectedLogical);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMvcombineWithNonexistentField() {
    String ppl = "source=EMP | stats count() by JOB | mvcombine INVALID_FIELD";
    getRelNode(ppl);
  }

  @Test
  public void testMvcombineWithNumericField() {
    String ppl = "source=EMP | stats avg(SAL) as avg by DEPTNO | mvcombine DEPTNO";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalAggregate(group=[{0}], DEPTNO=[LIST($1)])\n"
            + "  LogicalProject(avg=[$1], DEPTNO=[$0])\n"
            + "    LogicalAggregate(group=[{0}], avg=[AVG($1)])\n"
            + "      LogicalProject(DEPTNO=[$7], SAL=[$5])\n"
            + "        LogicalTableScan(table=[[scott, EMP]])\n";

    verifyLogical(root, expectedLogical);
  }
}
