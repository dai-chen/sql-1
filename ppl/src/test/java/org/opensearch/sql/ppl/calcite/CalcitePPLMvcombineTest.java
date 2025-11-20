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
            + "  LogicalProject(max=[$2], min=[$3], JOB=[$0])\n"
            + "    LogicalAggregate(group=[{2}], max=[MAX($5)], min=[MIN($5)])\n"
            + "      LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5])\n"
            + "        LogicalFilter(condition=[IS NOT NULL($2)])\n"
            + "          LogicalTableScan(table=[[scott, EMP]])\n";

    verifyLogical(root, expectedLogical);
  }

  @Test
  public void testMvcombineWithDelimiter() {
    String ppl =
        "source=EMP | stats max(SAL) as max, min(SAL) as min by JOB | mvcombine delim=\",\" JOB";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalAggregate(group=[{0, 1}], JOB=[MVJOIN(LIST($2), ',':VARCHAR)])\n"
            + "  LogicalProject(max=[$2], min=[$3], JOB=[$0])\n"
            + "    LogicalAggregate(group=[{2}], max=[MAX($5)], min=[MIN($5)])\n"
            + "      LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5])\n"
            + "        LogicalFilter(condition=[IS NOT NULL($2)])\n"
            + "          LogicalTableScan(table=[[scott, EMP]])\n";

    verifyLogical(root, expectedLogical);
  }

  @Test
  public void testMvcombineWithCustomDelimiter() {
    String ppl =
        "source=EMP | stats count() as cnt by DEPTNO | mvcombine delim=\" | \" DEPTNO";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalAggregate(group=[{0}], DEPTNO=[MVJOIN(LIST($1), ' | ':VARCHAR)])\n"
            + "  LogicalProject(cnt=[$1], DEPTNO=[$0])\n"
            + "    LogicalAggregate(group=[{7}], cnt=[COUNT()])\n"
            + "      LogicalFilter(condition=[IS NOT NULL($7)])\n"
            + "        LogicalTableScan(table=[[scott, EMP]])\n";

    verifyLogical(root, expectedLogical);
  }

  @Test
  public void testMvcombineWithMultipleGroupByFields() {
    String ppl =
        "source=EMP | stats count() as cnt by DEPTNO, MGR | mvcombine DEPTNO";
    RelNode root = getRelNode(ppl);

    String expectedLogical =
        "LogicalAggregate(group=[{0, 1}], DEPTNO=[LIST($2)])\n"
            + "  LogicalProject(cnt=[$2], MGR=[$1], DEPTNO=[$0])\n"
            + "    LogicalAggregate(group=[{7, 3}], cnt=[COUNT()])\n"
            + "      LogicalFilter(condition=[AND(IS NOT NULL($7), IS NOT NULL($3))])\n"
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
            + "    LogicalAggregate(group=[{2}], max=[MAX($5)])\n"
            + "      LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5])\n"
            + "        LogicalFilter(condition=[AND(IS NOT NULL($2), >($5, 1000))])\n"
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
            + "    LogicalAggregate(group=[{7}], avg=[AVG($5)])\n"
            + "      LogicalProject(EMPNO=[$0], ENAME=[$1], JOB=[$2], MGR=[$3], HIREDATE=[$4],"
            + " SAL=[$5], COMM=[$6], DEPTNO=[$7])\n"
            + "        LogicalFilter(condition=[IS NOT NULL($7)])\n"
            + "          LogicalTableScan(table=[[scott, EMP]])\n";

    verifyLogical(root, expectedLogical);
  }
}
