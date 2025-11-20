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
  public void testMvcombineBasic() {
    String ppl = "source=EMP | stats max(SAL) by DEPTNO | mvcombine DEPTNO";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalAggregate(group=[{0}], DEPTNO=[LIST($1)])\n"
            + "  LogicalProject(max(SAL)=[$1], DEPTNO=[$0])\n"
            + "    LogicalAggregate(group=[{0}], max(SAL)=[MAX($1)])\n"
            + "      LogicalProject(DEPTNO=[$7], SAL=[$5])\n"
            + "        LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
  }

  @Test
  public void testMvcombineWithDelimiter() {
    String ppl = "source=EMP | stats max(SAL) by DEPTNO | mvcombine delim=\",\" DEPTNO";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalAggregate(group=[{0}], DEPTNO=[LISTAGG($1, $2)])\n"
            + "  LogicalProject(max(SAL)=[$1], $f2=[SAFE_CAST($0)], $f3=[','])\n"
            + "    LogicalAggregate(group=[{0}], max(SAL)=[MAX($1)])\n"
            + "      LogicalProject(DEPTNO=[$7], SAL=[$5])\n"
            + "        LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
  }

  @Test
  public void testMvcombineWithEmptyDelimiter() {
    String ppl = "source=EMP | stats max(SAL) by DEPTNO | mvcombine delim=\"\" DEPTNO";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalAggregate(group=[{0}], DEPTNO=[LISTAGG($1, $2)])\n"
            + "  LogicalProject(max(SAL)=[$1], $f2=[SAFE_CAST($0)], $f3=[''])\n"
            + "    LogicalAggregate(group=[{0}], max(SAL)=[MAX($1)])\n"
            + "      LogicalProject(DEPTNO=[$7], SAL=[$5])\n"
            + "        LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
  }

  @Test
  public void testMvcombineWithMultipleGroupByFields() {
    String ppl = "source=EMP | stats count() by JOB, DEPTNO | mvcombine JOB";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalAggregate(group=[{0, 2}], JOB=[LIST($1)])\n"
            + "  LogicalProject(count()=[$2], JOB=[$0], DEPTNO=[$1])\n"
            + "    LogicalAggregate(group=[{0, 1}], count()=[COUNT()])\n"
            + "      LogicalProject(JOB=[$2], DEPTNO=[$7])\n"
            + "        LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
  }

  @Test
  public void testMvcombineNumericField() {
    String ppl = "source=EMP | fields DEPTNO, EMPNO | mvcombine EMPNO";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalAggregate(group=[{0}], EMPNO=[LIST($1)])\n"
            + "  LogicalProject(DEPTNO=[$7], EMPNO=[$0])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
  }

  @Test
  public void testMvcombineStringFieldWithDelimiter() {
    String ppl = "source=EMP | fields JOB, ENAME | mvcombine delim=\" | \" ENAME";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalAggregate(group=[{0}], ENAME=[LISTAGG($1, $2)])\n"
            + "  LogicalProject(JOB=[$2], ENAME=[$1], $f2=[' | '])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
  }

  @Test
  public void testMvcombineAllRowsIntoOne() {
    String ppl = "source=EMP | fields ENAME | mvcombine ENAME";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalAggregate(group=[{}], ENAME=[LIST($0)])\n"
            + "  LogicalProject(ENAME=[$1])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
  }

  @Test
  public void testMvcombineExcludesMetadataFields() {
    // Metadata fields like _id, _index should be automatically excluded from grouping
    String ppl = "source=EMP | fields DEPTNO, JOB | mvcombine JOB";
    RelNode root = getRelNode(ppl);
    String expectedLogical =
        ""
            + "LogicalAggregate(group=[{0}], JOB=[LIST($1)])\n"
            + "  LogicalProject(DEPTNO=[$7], JOB=[$2])\n"
            + "    LogicalTableScan(table=[[scott, EMP]])\n";
    verifyLogical(root, expectedLogical);
  }

  @Test(expected = Exception.class)
  public void testMvcombineNonExistentField() {
    // Should throw exception when field doesn't exist
    String ppl = "source=EMP | mvcombine nonexistent_field";
    getRelNode(ppl);
  }
}
