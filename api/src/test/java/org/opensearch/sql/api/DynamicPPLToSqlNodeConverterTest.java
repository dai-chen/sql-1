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
    // Table with nested struct fields (mapping order: message, comment, myNum, someField)
    schema.add("nested_tbl", new AbstractTable() {
      @Override
      public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return typeFactory.builder()
            .add("message", typeFactory.createArrayType(
                typeFactory.createStructType(
                    java.util.List.of(
                        typeFactory.createSqlType(SqlTypeName.VARCHAR),
                        typeFactory.createSqlType(SqlTypeName.VARCHAR),
                        typeFactory.createSqlType(SqlTypeName.BIGINT)),
                    java.util.List.of("author", "info", "dayOfWeek")), -1))
            .add("message.author", SqlTypeName.VARCHAR)
            .add("message.dayOfWeek", SqlTypeName.BIGINT)
            .add("message.info", SqlTypeName.VARCHAR)
            .add("myNum", SqlTypeName.BIGINT)
            .add("comment", typeFactory.createArrayType(
                typeFactory.createStructType(
                    java.util.List.of(
                        typeFactory.createSqlType(SqlTypeName.VARCHAR),
                        typeFactory.createSqlType(SqlTypeName.BIGINT)),
                    java.util.List.of("data", "likes")), -1))
            .add("someField", SqlTypeName.VARCHAR)
            .build();
      }
    });
    // Table with array field for expand tests
    schema.add("arr_tbl", new AbstractTable() {
      @Override
      public RelDataType getRowType(RelDataTypeFactory typeFactory) {
        return typeFactory.builder()
            .add("name", SqlTypeName.VARCHAR)
            .add("age", SqlTypeName.BIGINT)
            .add("address", typeFactory.createArrayType(
                typeFactory.createSqlType(SqlTypeName.VARCHAR), -1))
            .add("id", SqlTypeName.BIGINT)
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
    // fillnull without field list → COALESCE every column from schema via * REPLACE
    ppl("source=t | fillnull value=0").shouldTranslateTo("""
        SELECT * REPLACE (COALESCE("a", 0) AS "a", COALESCE("b", 0) AS "b", COALESCE("c", 0) AS "c")
        FROM (SELECT *
        FROM "t") AS "_t1\"""");
  }

  @Test
  public void testEvalColumnOverride() {
    // eval a = b → SELECT * REPLACE to avoid column ambiguity
    ppl("source=t | eval a = b").shouldTranslateTo("""
        SELECT * REPLACE ("b" AS "a")
        FROM (SELECT *
        FROM "t") AS "_t1\"""");
  }

  @Test
  public void testFlattenColumnOrdering() {
    // Flatten should produce top-level columns in alphabetical order:
    // comment, myNum, someField (not myNum, comment, someField)
    ppl("source=nested_tbl | flatten message").shouldTranslateTo("""
        SELECT "comment", "myNum", "someField", "message", "message.author" AS "author", "message.dayOfWeek" AS "dayOfWeek", "message.info" AS "info"
        FROM (SELECT *
        FROM "nested_tbl") AS "_t1\"""");
  }

  @Test
  public void testExpandWithEvalAlias() {
    // eval addr=address creates a new column; expand addr should include all 5 columns
    ppl("source=arr_tbl | eval addr=address | expand addr").shouldTranslateTo(
        "SELECT \"_t2\".\"name\", \"_t2\".\"age\", \"_t2\".\"address\", \"_t2\".\"id\", \"_u\".\"addr\" AS \"addr\"\n"
            + "FROM (SELECT *, \"address\" AS \"addr\"\n"
            + "FROM (SELECT *\n"
            + "FROM \"arr_tbl\") AS \"_t1\") AS \"_t2\"\n"
            + "CROSS JOIN UNNEST(\"_t2\".\"addr\") AS \"_u\" (\"addr\")");
  }
}
