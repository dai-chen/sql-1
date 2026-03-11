/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

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

  private String convert(String ppl) {
    UnresolvedPlan plan = PPLToSqlNodeConverter.parse(ppl);
    DynamicPPLToSqlNodeConverter converter = new DynamicPPLToSqlNodeConverter(schema);
    SqlNode result = converter.convert(plan);
    return toSql(result);
  }

  @Test
  public void testFieldsExclude() {
    String sql = convert("source=t | fields - a");
    assertTrue(sql, sql.contains("\"b\""));
    assertTrue(sql, sql.contains("\"c\""));
    assertFalse(sql, sql.contains("\"a\""));
  }

  @Test
  public void testFillNullAllFields() {
    String sql = convert("source=t | fillnull value=0");
    assertTrue(sql, sql.contains("COALESCE"));
    assertTrue(sql, sql.contains("\"a\""));
    assertTrue(sql, sql.contains("\"b\""));
    assertTrue(sql, sql.contains("\"c\""));
  }

  @Test
  public void testEvalColumnOverride() {
    String sql = convert("source=t | eval a = b");
    assertTrue(sql, sql.contains("\"b\" AS \"a\""));
    assertTrue(sql, sql.contains("\"b\""));
    assertTrue(sql, sql.contains("\"c\""));
  }
}
