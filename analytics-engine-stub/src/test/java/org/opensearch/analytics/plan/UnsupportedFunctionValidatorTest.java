/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.analytics.plan;

import static org.junit.jupiter.api.Assertions.*;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.tools.Frameworks;
import org.apache.calcite.tools.RelBuilder;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opensearch.analytics.schema.MockParquetSchemaProvider;
import org.opensearch.analytics.spi.ParquetEngineCapabilities;

class UnsupportedFunctionValidatorTest {

  private RelBuilder builder;
  private ParquetEngineCapabilities capabilities;

  @BeforeEach
  void setUp() {
    SchemaPlus schema = new MockParquetSchemaProvider().buildSchema(null);
    builder = RelBuilder.create(Frameworks.newConfigBuilder().defaultSchema(schema).build());
    capabilities = new ParquetEngineCapabilities();
  }

  @Test
  void testUnsupportedFunctionThrows() {
    // Build a filter with an unsupported OTHER_FUNCTION call (simulating match())
    RelDataTypeFactory typeFactory = builder.getTypeFactory();
    RelDataType boolType = typeFactory.createSqlType(SqlTypeName.BOOLEAN);
    SqlOperator matchOp =
        new org.apache.calcite.sql.SqlFunction(
            "match",
            org.apache.calcite.sql.SqlKind.OTHER_FUNCTION,
            opBinding -> boolType,
            null,
            null,
            org.apache.calcite.sql.SqlFunctionCategory.USER_DEFINED_FUNCTION);

    RelNode tree =
        builder
            .scan("parquet_index")
            .filter(builder.call(matchOp, builder.field("message"), builder.literal("error")))
            .build();

    UnsupportedOperationException ex =
        assertThrows(
            UnsupportedOperationException.class,
            () -> UnsupportedFunctionValidator.validate(tree, capabilities));
    assertTrue(ex.getMessage().contains("match"));
    assertTrue(ex.getMessage().contains("not supported for Parquet"));
  }

  @Test
  void testSupportedOperationsPass() {
    RelNode tree =
        builder
            .scan("parquet_index")
            .filter(builder.equals(builder.field("status"), builder.literal(200)))
            .project(builder.field("message"), builder.field("status"))
            .build();

    assertDoesNotThrow(() -> UnsupportedFunctionValidator.validate(tree, capabilities));
  }
}
