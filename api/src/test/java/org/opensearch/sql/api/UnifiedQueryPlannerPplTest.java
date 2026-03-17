/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api;

import static org.apache.calcite.sql.type.SqlTypeName.DATE;
import static org.apache.calcite.sql.type.SqlTypeName.INTEGER;
import static org.apache.calcite.sql.type.SqlTypeName.TIME;
import static org.apache.calcite.sql.type.SqlTypeName.TIMESTAMP;
import static org.apache.calcite.sql.type.SqlTypeName.VARBINARY;
import static org.apache.calcite.sql.type.SqlTypeName.VARCHAR;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;

import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.Table;
import org.apache.calcite.schema.impl.AbstractSchema;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.Before;
import org.junit.Test;
import org.opensearch.sql.calcite.type.AbstractExprRelDataType;
import org.opensearch.sql.executor.QueryType;

/**
 * Phase 2 PoC: Validates PPL RelNode generation against a schema with standard Calcite types.
 *
 * <p>Key Area 1 — UDT interference. PPL V3 defines 5 custom UDTs (EXPR_DATE, EXPR_TIME,
 * EXPR_TIMESTAMP, EXPR_BINARY, EXPR_IP) backed by VARCHAR. This test validates that when the schema
 * uses standard Calcite types (DATE, TIME, TIMESTAMP, VARBINARY, VARCHAR), the RelNode output
 * preserves those standard types without UDT contamination.
 */
public class UnifiedQueryPlannerPplTest extends UnifiedQueryTestBase {

  @Override
  @Before
  public void setUp() {
    testSchema =
        new AbstractSchema() {
          @Override
          protected Map<String, Table> getTableMap() {
            return Map.of("parquet_index", createParquetIndexTable());
          }
        };

    context =
        UnifiedQueryContext.builder()
            .language(QueryType.PPL)
            .catalog(DEFAULT_CATALOG, testSchema)
            .build();
    planner = new UnifiedQueryPlanner(context);
  }

  /** Schema covers all 5 UDT-corresponding standard Calcite types. */
  private Table createParquetIndexTable() {
    return SimpleTable.builder()
        .col("ts", TIMESTAMP)
        .col("dt", DATE)
        .col("tm", TIME)
        .col("data", VARBINARY)
        .col("ip_addr", VARCHAR)
        .col("status", INTEGER)
        .col("message", VARCHAR)
        .build();
  }

  @Test
  public void testFilterAndProject() {
    RelNode plan =
        planner.plan(
            "source = catalog.parquet_index | where status = 200 | fields ts, status, message");
    assertNotNull(plan);
  }

  @Test
  public void testAggregate() {
    RelNode plan = planner.plan("source = catalog.parquet_index | stats count() by status");
    assertNotNull(plan);
  }

  @Test
  public void testSortAndLimit() {
    RelNode plan = planner.plan("source = catalog.parquet_index | sort ts | head 100");
    assertNotNull(plan);
  }

  // --- Key Area 1: Verify standard types are preserved, no UDT contamination ---

  @Test
  public void testTimestampColumnPreservesStandardType() {
    RelNode plan = planner.plan("source = catalog.parquet_index | fields ts");
    assertStandardType(plan, "ts", SqlTypeName.TIMESTAMP);
  }

  @Test
  public void testDateColumnPreservesStandardType() {
    RelNode plan = planner.plan("source = catalog.parquet_index | fields dt");
    assertStandardType(plan, "dt", SqlTypeName.DATE);
  }

  @Test
  public void testTimeColumnPreservesStandardType() {
    RelNode plan = planner.plan("source = catalog.parquet_index | fields tm");
    assertStandardType(plan, "tm", SqlTypeName.TIME);
  }

  @Test
  public void testVarbinaryColumnPreservesStandardType() {
    RelNode plan = planner.plan("source = catalog.parquet_index | fields data");
    assertStandardType(plan, "data", SqlTypeName.VARBINARY);
  }

  @Test
  public void testVarcharColumnPreservesStandardType() {
    RelNode plan = planner.plan("source = catalog.parquet_index | fields ip_addr");
    assertStandardType(plan, "ip_addr", SqlTypeName.VARCHAR);
  }

  @Test
  public void testHourFunctionReturnsInteger() {
    RelNode plan = planner.plan("source = catalog.parquet_index | eval hour = hour(ts)");
    assertStandardType(plan, "hour", SqlTypeName.INTEGER);
  }

  @Test
  public void testDayFunctionReturnsInteger() {
    RelNode plan = planner.plan("source = catalog.parquet_index | eval day = day(dt)");
    assertStandardType(plan, "day", SqlTypeName.INTEGER);
  }

  @Test
  public void testTimestampFilter() {
    RelNode plan = planner.plan("source = catalog.parquet_index | where ts > '2024-01-01'");
    assertStandardType(plan, "ts", SqlTypeName.TIMESTAMP);
  }

  /**
   * Asserts the named field in the RelNode output has the expected standard SqlTypeName and is NOT
   * a UDT.
   */
  private void assertStandardType(RelNode plan, String fieldName, SqlTypeName expectedType) {
    assertNotNull("Plan should not be null", plan);
    RelDataTypeField field = findField(plan.getRowType(), fieldName);
    assertNotNull("Field '" + fieldName + "' should exist in output", field);
    RelDataType type = field.getType();
    assertFalse(
        "Field '"
            + fieldName
            + "' should NOT be a UDT (got "
            + type.getClass().getSimpleName()
            + ")",
        type instanceof AbstractExprRelDataType<?>);
    assertEquals(
        "Field '" + fieldName + "' should have standard type " + expectedType,
        expectedType,
        type.getSqlTypeName());
  }

  private RelDataTypeField findField(RelDataType rowType, String name) {
    List<RelDataTypeField> fields = rowType.getFieldList();
    for (RelDataTypeField f : fields) {
      if (f.getName().equalsIgnoreCase(name)) {
        return f;
      }
    }
    return null;
  }
}
