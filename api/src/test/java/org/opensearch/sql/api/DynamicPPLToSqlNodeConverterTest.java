/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api;

import static org.junit.Assert.assertEquals;

import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.impl.AbstractTable;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.dialect.CalciteSqlDialect;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.Frameworks;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.sql.ast.tree.UnresolvedPlan;

/**
 * Schema-dependent PPL-to-SQL conversion tests.
 *
 * <p>Tests use an in-memory schema with table "t" (a:INTEGER, b:VARCHAR, c:DOUBLE)
 * to verify commands that require column resolution: fields -, fillnull all-fields,
 * eval column override.
 */
public class DynamicPPLToSqlNodeConverterTest {

  private SchemaPlus schema;

  @Before
  public void setUp() {
    schema = Frameworks.createRootSchema(false);
    schema.add("t", new AbstractTable() {
      @Override
      public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return typeFactory.builder()
            .add("a", SqlTypeName.INTEGER)
            .add("b", SqlTypeName.VARCHAR)
            .add("c", SqlTypeName.DOUBLE)
            .build();
      }
    });
  }

  private static String toSql(SqlNode node) {
    return node.toSqlString(CalciteSqlDialect.DEFAULT).getSql();
  }

  /** Fluent assertion: {@code ppl("source=t | fields - a").shouldTranslateTo("SELECT ...")} */
  private PplAssertion ppl(String ppl) {
    return new PplAssertion(ppl);
  }

  private class PplAssertion {
    private final String ppl;
    private final String sql;

    PplAssertion(String ppl) {
      this.ppl = ppl;
      UnresolvedPlan plan = PPLToSqlNodeConverter.parse(ppl);
      DynamicPPLToSqlNodeConverter converter = new DynamicPPLToSqlNodeConverter(schema);
      this.sql = toSql(converter.convert(plan));
    }

    void shouldTranslateTo(String expectedSql) {
      assertEquals("PPL: " + ppl, expectedSql, sql);
    }
  }

  @Test
  public void testFieldsExclude() {
    // fields - a → SELECT only non-excluded columns (b, c) resolved from schema
    ppl("source=t | fields - a").shouldTranslateTo("""
        SELECT "b", "c"
        FROM (SELECT *
        FROM "t") AS "_t1\"""");
  }

  @Test
  public void testFillNullAllFields() {
    // fillnull without field list → COALESCE every column from schema
    ppl("source=t | fillnull value=0").shouldTranslateTo("""
        SELECT COALESCE("a", 0) AS "a", COALESCE("b", 0) AS "b", COALESCE("c", 0) AS "c"
        FROM (SELECT *
        FROM "t") AS "_t1\"""");
  }

  @Test
  public void testEvalColumnOverride() {
    // eval a = b → explicit column list with override instead of SELECT *, expr
    ppl("source=t | eval a = b").shouldTranslateTo("""
        SELECT "b" AS "a", "b", "c"
        FROM (SELECT *
        FROM "t") AS "_t1\"""");
  }
}
