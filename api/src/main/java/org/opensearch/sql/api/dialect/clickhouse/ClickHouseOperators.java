/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.dialect.clickhouse;

import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.type.SqlOperandCountRanges;
import org.apache.calcite.sql.type.SqlTypeFamily;
import org.apache.calcite.sql.type.SqlTypeTransforms;
import org.apache.calcite.sql.util.SqlOperatorTables;

/**
 * ClickHouse functions registered only in the ClickHouse dialect's operator table. They are
 * validated with ClickHouse names (case-sensitive) and are expanded into operators the Analytics
 * Engine already executes by {@link ClickHouseFunctionExpansionRule} after analysis, so the
 * distributed plan never contains them.
 */
public final class ClickHouseOperators {

  private ClickHouseOperators() {}

  /** (B) Alias of an existing function: {@code lengthUTF8(s)} = {@code CHAR_LENGTH(s)}. */
  public static final SqlFunction LENGTH_UTF8 =
      new SqlFunction(
          "lengthUTF8",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.INTEGER_NULLABLE,
          null,
          OperandTypes.CHARACTER,
          SqlFunctionCategory.STRING);

  /**
   * (C) New function with no OpenSearch SQL equivalent: {@code toYYYYMM(ts)} returns year*100 +
   * month, for example 202401. Implemented by expansion into EXTRACT arithmetic.
   */
  public static final SqlFunction TO_YYYYMM =
      new SqlFunction(
          "toYYYYMM",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.INTEGER_NULLABLE,
          null,
          OperandTypes.family(SqlTypeFamily.DATETIME),
          SqlFunctionCategory.TIMEDATE);

  /**
   * (C) Dialect-local semantic override: ClickHouse {@code concat(a, b, ...)} returns NULL when any
   * argument is NULL, whereas the OpenSearch SQL CONCAT on the Analytics Engine skips NULLs. Only
   * this dialect's operator table resolves {@code concat} to this operator; default SQL and PPL
   * keep their CONCAT.
   */
  public static final SqlFunction CONCAT_NULL_PROPAGATING =
      new SqlFunction(
          "concat",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.MULTIVALENT_STRING_SUM_PRECISION.andThen(SqlTypeTransforms.TO_NULLABLE),
          null,
          OperandTypes.repeat(SqlOperandCountRanges.from(2), OperandTypes.STRING),
          SqlFunctionCategory.STRING);

  public static SqlOperatorTable table() {
    return SqlOperatorTables.of(LENGTH_UTF8, TO_YYYYMM, CONCAT_NULL_PROPAGATING);
  }
}
