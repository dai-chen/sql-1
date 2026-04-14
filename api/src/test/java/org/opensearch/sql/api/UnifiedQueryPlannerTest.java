/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertThrows;

import java.util.Map;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.schema.Schema;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.junit.Test;
import org.opensearch.sql.calcite.type.AbstractExprRelDataType;
import org.opensearch.sql.common.antlr.SyntaxCheckException;
import org.opensearch.sql.executor.QueryType;

public class UnifiedQueryPlannerTest extends UnifiedQueryTestBase {

  /** Test catalog consists of test schema above */
  private final AbstractSchema testDeepSchema =
      new AbstractSchema() {
        @Override
        protected Map<String, Schema> getSubSchemaMap() {
          return Map.of("opensearch", testSchema);
        }
      };

  @Test
  public void testPPLQueryPlanning() {
    RelNode plan = planner.plan("source = catalog.employees | eval f = abs(id)");
    assertNotNull("Plan should be created", plan);
  }

  @Test
  public void testPPLJoinQueryPlanning() {
    RelNode plan =
        planner.plan(
            "source = catalog.employees | join left = l right = r on l.id = r.age"
                + " catalog.employees");
    assertNotNull("Join query should be created", plan);
  }

  @Test
  public void testPPLQueryPlanningWithDefaultNamespace() {
    UnifiedQueryContext context =
        UnifiedQueryContext.builder()
            .language(QueryType.PPL)
            .catalog("opensearch", testSchema)
            .defaultNamespace("opensearch")
            .build();
    UnifiedQueryPlanner planner = new UnifiedQueryPlanner(context);

    assertNotNull("Plan should be created", planner.plan("source = opensearch.employees"));
    assertNotNull("Plan should be created", planner.plan("source = employees"));
  }

  @Test
  public void testPPLQueryPlanningWithDefaultNamespaceMultiLevel() {
    UnifiedQueryContext context =
        UnifiedQueryContext.builder()
            .language(QueryType.PPL)
            .catalog("catalog", testDeepSchema)
            .defaultNamespace("catalog.opensearch")
            .build();
    UnifiedQueryPlanner planner = new UnifiedQueryPlanner(context);

    assertNotNull("Plan should be created", planner.plan("source = catalog.opensearch.employees"));
    assertNotNull("Plan should be created", planner.plan("source = employees"));

    // This is valid in SparkSQL, but Calcite requires "catalog" as the default root schema to
    // resolve it
    assertThrows(IllegalStateException.class, () -> planner.plan("source = opensearch.employees"));
  }

  @Test
  public void testPPLQueryPlanningWithMultipleCatalogs() {
    UnifiedQueryContext context =
        UnifiedQueryContext.builder()
            .language(QueryType.PPL)
            .catalog("catalog1", testSchema)
            .catalog("catalog2", testSchema)
            .build();
    UnifiedQueryPlanner planner = new UnifiedQueryPlanner(context);

    RelNode plan =
        planner.plan(
            "source = catalog1.employees | lookup catalog2.employees id | eval f = abs(id)");
    assertNotNull("Plan should be created with multiple catalogs", plan);
  }

  @Test
  public void testPPLQueryPlanningWithMultipleCatalogsAndDefaultNamespace() {
    UnifiedQueryContext context =
        UnifiedQueryContext.builder()
            .language(QueryType.PPL)
            .catalog("catalog1", testSchema)
            .catalog("catalog2", testSchema)
            .defaultNamespace("catalog2")
            .build();
    UnifiedQueryPlanner planner = new UnifiedQueryPlanner(context);

    RelNode plan =
        planner.plan("source = catalog1.employees | lookup employees id | eval f = abs(id)");
    assertNotNull("Plan should be created with multiple catalogs", plan);
  }

  @Test
  public void testDateTimeUdfsProduceOnlyStandardCalciteTypesInPlanAndRexNodes() {
    RelNode plan =
        planner.plan(
            "source = catalog.employees"
                + " | eval d = DATE('2020-06-01'), t = TIME('09:30:00'),"
                + " ts = TIMESTAMP('2020-06-01 09:30:00')"
                + " | where d > DATE('2020-05-01') and t > TIME('09:00:00')"
                + " and ts > TIMESTAMP('2020-06-01 09:00:00')"
                + " | fields d, t, ts");

    assertEquals(
        SqlTypeName.DATE, plan.getRowType().getFieldList().get(0).getType().getSqlTypeName());
    assertEquals(
        SqlTypeName.TIME, plan.getRowType().getFieldList().get(1).getType().getSqlTypeName());
    assertEquals(
        SqlTypeName.TIMESTAMP, plan.getRowType().getFieldList().get(2).getType().getSqlTypeName());

    assertPlanHasNoExprUdtTypes(plan);
  }

  @Test
  public void testMultipleDatetimeUdfsHaveNoExprUdtInPlan() {
    RelNode plan =
        planner.plan(
            "source = catalog.employees"
                + " | eval d = DATE('2020-06-01'),"
                + " t = TIME('09:30:00'),"
                + " ts = TIMESTAMP('2020-06-01 09:30:00'),"
                + " md = MAKEDATE(2020, 15),"
                + " s2d = STR_TO_DATE('01,5,2013', '%d,%m,%Y'),"
                + " ad = ADDDATE(DATE('2020-06-01'), 1),"
                + " sd = SUBDATE(DATE('2020-06-02'), 1),"
                + " cd = CURRENT_DATE(),"
                + " ud = UTC_DATE(),"
                + " st = SYSDATE()"
                + " | fields d, t, ts, md, s2d, ad, sd, cd, ud, st");

    assertPlanHasNoExprUdtTypes(plan);
  }

  @Test
  public void testDatetimeUdfSignaturesSanitizedForBroadDatetimeFunctionSet() {
    RelNode plan =
        planner.plan(
            "source = catalog.employees"
                + " | eval d = DATE('2020-06-01'),"
                + " t = TIME('09:30:00'),"
                + " ts = TIMESTAMP('2020-06-01 09:30:00'),"
                + " at = ADDTIME(TIME('09:30:00'), TIME('00:10:00')),"
                + " st2 = SUBTIME(TIME('09:30:00'), TIME('00:10:00')),"
                + " ad = ADDDATE(DATE('2020-06-01'), 1),"
                + " sd = SUBDATE(DATE('2020-06-02'), 1),"
                + " da = DATE_ADD(TIMESTAMP('2020-06-01 09:30:00'), INTERVAL 1 DAY),"
                + " ds = DATE_SUB(TIMESTAMP('2020-06-02 09:30:00'), INTERVAL 1 DAY),"
                + " n = NOW(),"
                + " ct = CURRENT_TIME(),"
                + " cd = CURRENT_DATE(),"
                + " cz = CONVERT_TZ('2020-06-01 10:00:00', '+00:00', '+08:00'),"
                + " ld = LAST_DAY('2020-06-15'),"
                + " fd = FROM_DAYS(738000),"
                + " md = MAKEDATE(2020, 15),"
                + " mt = MAKETIME(10, 20, 30),"
                + " s2d = STR_TO_DATE('01,5,2013', '%d,%m,%Y'),"
                + " sd2 = SYSDATE(),"
                + " s2t = SEC_TO_TIME(3600)"
                + " | fields d, t, ts, at, st2, ad, sd, da, ds, n, ct, cd, cz, ld, fd, md, mt, s2d, sd2, s2t");

    assertPlanHasNoExprUdtTypes(plan);
  }


  private void assertPlanHasNoExprUdtTypes(RelNode plan) {
    RexVisitorImpl<Void> rexTypeVerifier =
        new RexVisitorImpl<>(true) {
          @Override
          public Void visitCall(RexCall call) {
            assertNoExprUdt(call.getType());
            return super.visitCall(call);
          }

          @Override
          public Void visitInputRef(RexInputRef inputRef) {
            assertNoExprUdt(inputRef.getType());
            return super.visitInputRef(inputRef);
          }

          @Override
          public Void visitLiteral(RexLiteral literal) {
            assertNoExprUdt(literal.getType());
            return super.visitLiteral(literal);
          }
        };

    new RelVisitor() {
      @Override
      public void visit(RelNode node, int ordinal, RelNode parent) {
        assertNoExprUdt(node.getRowType());
        for (RexNode childExpr : node.getChildExps()) {
          childExpr.accept(rexTypeVerifier);
        }
        super.visit(node, ordinal, parent);
      }
    }.go(plan);
  }

  private void assertNoExprUdt(RelDataType type) {
    assertFalse(type instanceof AbstractExprRelDataType);

    for (RelDataTypeField field : type.getFieldList()) {
      assertNoExprUdt(field.getType());
    }

    if (type.getComponentType() != null) {
      assertNoExprUdt(type.getComponentType());
    }

    if (type.getKeyType() != null) {
      assertNoExprUdt(type.getKeyType());
    }

    if (type.getValueType() != null) {
      assertNoExprUdt(type.getValueType());
    }
  }

  @Test(expected = UnsupportedOperationException.class)
  public void testUnsupportedStatementType() {
    planner.plan("explain source = catalog.employees"); // explain statement
  }

  @Test(expected = SyntaxCheckException.class)
  public void testPlanPropagatingSyntaxCheckException() {
    planner.plan("source = catalog.employees | eval"); // Trigger syntax error from parser
  }
}
