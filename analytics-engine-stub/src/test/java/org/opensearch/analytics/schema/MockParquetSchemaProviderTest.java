/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.analytics.schema;

import static org.junit.jupiter.api.Assertions.*;

import java.util.List;
import org.apache.calcite.config.Lex;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.Frameworks;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class MockParquetSchemaProviderTest {

  private SchemaPlus schema;
  private final RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();

  @BeforeEach
  void setUp() {
    schema = new MockParquetSchemaProvider().buildSchema(null);
  }

  @Test
  void testTableHasFourColumnsWithCorrectNamesAndTypes() {
    Table table = schema.getTable("parquet_index");
    assertNotNull(table);

    RelDataType rowType = table.getRowType(typeFactory);
    List<RelDataTypeField> fields = rowType.getFieldList();

    assertEquals(4, fields.size());
    assertEquals("message", fields.get(0).getName());
    assertEquals(SqlTypeName.VARCHAR, fields.get(0).getType().getSqlTypeName());
    assertEquals("status", fields.get(1).getName());
    assertEquals(SqlTypeName.BIGINT, fields.get(1).getType().getSqlTypeName());
    assertEquals("timestamp", fields.get(2).getName());
    assertEquals(SqlTypeName.TIMESTAMP, fields.get(2).getType().getSqlTypeName());
    assertEquals("active", fields.get(3).getName());
    assertEquals(SqlTypeName.BOOLEAN, fields.get(3).getType().getSqlTypeName());
  }

  @Test
  void testAllColumnsAreNullable() {
    Table table = schema.getTable("parquet_index");
    RelDataType rowType = table.getRowType(typeFactory);

    for (RelDataTypeField field : rowType.getFieldList()) {
      assertTrue(field.getType().isNullable(), field.getName() + " should be nullable");
    }
  }

  @Test
  void testSchemaRegistersInFrameworkConfig() {
    assertDoesNotThrow(
        () ->
            Frameworks.newConfigBuilder()
                .defaultSchema(schema)
                .parserConfig(SqlParser.config().withLex(Lex.MYSQL))
                .build());
  }
}
