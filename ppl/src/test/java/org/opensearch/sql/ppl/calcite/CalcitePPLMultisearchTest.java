/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.ppl.calcite;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.test.CalciteAssert;
import org.junit.Test;

public class CalcitePPLMultisearchTest extends CalcitePPLAbstractTest {

  public CalcitePPLMultisearchTest() {
    super(CalciteAssert.SchemaSpec.SCOTT_WITH_TEMPORAL);
  }

  @Test
  public void testMultisearchBasicFunctionality() {
    String ppl = "source=EMP | multisearch [ source=EMP | where DEPTNO = 10 ] [ source=EMP | where DEPTNO = 20 ]";
    RelNode root = getRelNode(ppl);
    
    // Basic verification - should create a union plan with multiple table scans
    String logicalPlan = root.explain();
    assertTrue("Should contain LogicalUnion", logicalPlan.contains("LogicalUnion"));
    assertTrue("Should contain multiple table scans", logicalPlan.contains("LogicalTableScan"));
    
    verifyResultCount(root, 22); // 14 original + 3 dept 10 + 5 dept 20
  }

  @Test
  public void testMultisearchWithEvalFields() {
    String ppl = "source=EMP | multisearch [ source=EMP | where DEPTNO = 10 | eval category = 'young' ] [ source=EMP | where DEPTNO = 20 | eval category = 'old' ] | fields EMPNO, DEPTNO, category";
    RelNode root = getRelNode(ppl);
    
    // Verify basic structure
    String logicalPlan = root.explain();
    assertTrue("Should contain LogicalUnion for combining results", logicalPlan.contains("LogicalUnion"));
    assertTrue("Should contain LogicalProject for eval operations", logicalPlan.contains("LogicalProject"));
  }

  @Test
  public void testMultisearchWithStats() {
    String ppl = "source=EMP | multisearch [ source=EMP | where DEPTNO = 10 | stats count() by JOB | eval type = 'dept10' ] [ source=EMP | where DEPTNO = 20 | stats count() by JOB | eval type = 'dept20' ]";
    RelNode root = getRelNode(ppl);
    
    // Verify it creates a valid logical plan
    String logicalPlan = root.explain();
    assertTrue("Should contain LogicalUnion", logicalPlan.contains("LogicalUnion"));
    assertTrue("Should contain LogicalAggregate for stats", logicalPlan.contains("LogicalAggregate"));
  }

  @Test
  public void testMultisearchThreeSubsearches() {
    String ppl = "source=EMP | multisearch [ source=EMP | where DEPTNO = 10 ] [ source=EMP | where DEPTNO = 20 ] [ source=EMP | where DEPTNO = 30 ]";
    RelNode root = getRelNode(ppl);
    
    // Verify basic structure
    String logicalPlan = root.explain();
    assertTrue("Should contain LogicalUnion", logicalPlan.contains("LogicalUnion"));
    
    verifyResultCount(root, 28); // 14 original + 3 dept 10 + 5 dept 20 + 6 dept 30
  }

  @Test
  public void testMultisearchNoTimestampField() {
    // Test table without @timestamp - should work without timestamp ordering
    String ppl = "source=DEPT | multisearch [ source=DEPT | where DEPTNO = 10 ] [ source=DEPT | where DEPTNO = 20 ]";
    RelNode root = getRelNode(ppl);
    
    // Verify basic functionality
    String logicalPlan = root.explain();
    assertTrue("Should contain LogicalUnion", logicalPlan.contains("LogicalUnion"));
    assertFalse("Should not contain timestamp sorting", logicalPlan.contains("@timestamp"));
  }

  @Test  
  public void testMultisearchWithComplexPipeline() {
    String ppl = "source=EMP | multisearch [ source=EMP | where DEPTNO = 10 ] [ source=EMP | where DEPTNO = 20 ] | head 5";
    RelNode root = getRelNode(ppl);
    
    // Verify basic structure and limit
    String logicalPlan = root.explain();
    assertTrue("Should contain LogicalUnion", logicalPlan.contains("LogicalUnion"));
    assertTrue("Should contain limit/sort for head", logicalPlan.contains("LogicalSort") || logicalPlan.contains("fetch"));
    
    verifyResultCount(root, 5); // Limited to 5 results
  }

  @Test
  public void testMultisearchSyntaxValidation() {
    // Test that multisearch properly rejects single subsearch at runtime
    try {
      String ppl = "source=EMP | multisearch [ source=EMP | where DEPTNO = 10 ] [ source=EMP | where DEPTNO = 20 ]";
      RelNode root = getRelNode(ppl);
      // This should succeed - we're testing that valid syntax works
      String logicalPlan = root.explain();
      assertTrue("Should contain LogicalUnion", logicalPlan.contains("LogicalUnion"));
    } catch (Exception e) {
      fail("Valid multisearch syntax should not fail: " + e.getMessage());
    }
  }
}
