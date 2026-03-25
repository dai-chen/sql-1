/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.TYPE_FACTORY;

import java.util.List;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.junit.jupiter.api.Test;
import org.opensearch.sql.calcite.type.AbstractExprRelDataType;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory.ExprUDT;

public class OpenSearchTypeFactoryTest {

  @Test
  public void testLeastRestrictivePreservesUdtWhenAllInputsSameUdt() {
    RelDataType ip1 = TYPE_FACTORY.createUDT(ExprUDT.EXPR_IP);
    RelDataType ip2 = TYPE_FACTORY.createUDT(ExprUDT.EXPR_IP);

    RelDataType result = TYPE_FACTORY.leastRestrictive(List.of(ip1, ip2));

    assertNotNull(result);
    assertInstanceOf(AbstractExprRelDataType.class, result);
    assertEquals(ExprUDT.EXPR_IP, ((AbstractExprRelDataType<?>) result).getUdt());
  }

  @Test
  public void testLeastRestrictivePreservesUdtForBinaryType() {
    RelDataType b1 = TYPE_FACTORY.createUDT(ExprUDT.EXPR_BINARY);
    RelDataType b2 = TYPE_FACTORY.createUDT(ExprUDT.EXPR_BINARY);

    RelDataType result = TYPE_FACTORY.leastRestrictive(List.of(b1, b2));

    assertNotNull(result);
    assertInstanceOf(AbstractExprRelDataType.class, result);
    assertEquals(ExprUDT.EXPR_BINARY, ((AbstractExprRelDataType<?>) result).getUdt());
  }

  @Test
  public void testLeastRestrictivePreservesUdtForThreeInputs() {
    RelDataType ip1 = TYPE_FACTORY.createUDT(ExprUDT.EXPR_IP);
    RelDataType ip2 = TYPE_FACTORY.createUDT(ExprUDT.EXPR_IP);
    RelDataType ip3 = TYPE_FACTORY.createUDT(ExprUDT.EXPR_IP);

    RelDataType result = TYPE_FACTORY.leastRestrictive(List.of(ip1, ip2, ip3));

    assertNotNull(result);
    assertInstanceOf(AbstractExprRelDataType.class, result);
    assertEquals(ExprUDT.EXPR_IP, ((AbstractExprRelDataType<?>) result).getUdt());
  }

  @Test
  public void testLeastRestrictiveReturnsNullableWhenAnyInputIsNullable() {
    RelDataType nonNullable = TYPE_FACTORY.createUDT(ExprUDT.EXPR_IP, false);
    RelDataType nullable = TYPE_FACTORY.createUDT(ExprUDT.EXPR_IP, true);

    RelDataType result = TYPE_FACTORY.leastRestrictive(List.of(nonNullable, nullable));

    assertNotNull(result);
    assertInstanceOf(AbstractExprRelDataType.class, result);
    assertEquals(ExprUDT.EXPR_IP, ((AbstractExprRelDataType<?>) result).getUdt());
    assertTrue(result.isNullable());
  }

  @Test
  public void testLeastRestrictiveReturnsNullableWhenFirstNullableSecondNot() {
    RelDataType nullable = TYPE_FACTORY.createUDT(ExprUDT.EXPR_IP, true);
    RelDataType nonNullable = TYPE_FACTORY.createUDT(ExprUDT.EXPR_IP, false);

    RelDataType result = TYPE_FACTORY.leastRestrictive(List.of(nullable, nonNullable));

    assertNotNull(result);
    assertInstanceOf(AbstractExprRelDataType.class, result);
    assertTrue(result.isNullable());
  }

  @Test
  public void testLeastRestrictiveFallsBackForMixedUdtAndNonUdt() {
    RelDataType udt = TYPE_FACTORY.createUDT(ExprUDT.EXPR_BINARY);
    RelDataType plain = TYPE_FACTORY.createSqlType(SqlTypeName.VARCHAR);

    // Different types — falls back to super.leastRestrictive, incompatible types return null
    RelDataType result = TYPE_FACTORY.leastRestrictive(List.of(udt, plain));
    // EXPR_BINARY (VARBINARY) and VARCHAR are incompatible
  }

  @Test
  public void testLeastRestrictiveFallsBackForDifferentUdts() {
    RelDataType ip1 = TYPE_FACTORY.createUDT(ExprUDT.EXPR_IP);
    RelDataType ip2 = TYPE_FACTORY.createUDT(ExprUDT.EXPR_IP);

    // Same UDTs — preserves UDT
    RelDataType result = TYPE_FACTORY.leastRestrictive(List.of(ip1, ip2));
    assertNotNull(result);
    assertInstanceOf(AbstractExprRelDataType.class, result);
    assertEquals(ExprUDT.EXPR_IP, ((AbstractExprRelDataType<?>) result).getUdt());
  }

  @Test
  public void testLeastRestrictiveDelegatesToSuperForSingleType() {
    RelDataType single = TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER);

    RelDataType result = TYPE_FACTORY.leastRestrictive(List.of(single));

    assertNotNull(result);
    assertEquals(SqlTypeName.INTEGER, result.getSqlTypeName());
  }

  @Test
  public void testLeastRestrictiveDelegatesToSuperForPlainTypes() {
    RelDataType int1 = TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER);
    RelDataType int2 = TYPE_FACTORY.createSqlType(SqlTypeName.INTEGER);

    RelDataType result = TYPE_FACTORY.leastRestrictive(List.of(int1, int2));

    assertNotNull(result);
    assertEquals(SqlTypeName.INTEGER, result.getSqlTypeName());
  }
}
