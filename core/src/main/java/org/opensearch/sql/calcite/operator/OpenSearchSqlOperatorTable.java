/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.operator;

import com.google.common.base.Suppliers;
import java.util.function.Supplier;
import org.apache.calcite.sql.SqlFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.util.ReflectiveSqlOperatorTable;

/**
 * Operator table for OpenSearch SQL-specific functions that are not part of standard Calcite or PPL.
 * Includes relevance search functions and datetime aliases.
 */
public class OpenSearchSqlOperatorTable extends ReflectiveSqlOperatorTable {

  private static final Supplier<OpenSearchSqlOperatorTable> INSTANCE =
      Suppliers.memoize(() -> (OpenSearchSqlOperatorTable) new OpenSearchSqlOperatorTable().init());

  // Relevance functions — variadic to accept named params (field, query, option=value ...)
  public static final SqlFunction MATCH =
      new SqlFunction(
          "MATCH",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.BOOLEAN_NULLABLE,
          null,
          OperandTypes.VARIADIC,
          SqlFunctionCategory.USER_DEFINED_FUNCTION);

  public static final SqlFunction MATCH_PHRASE =
      new SqlFunction(
          "MATCH_PHRASE",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.BOOLEAN_NULLABLE,
          null,
          OperandTypes.VARIADIC,
          SqlFunctionCategory.USER_DEFINED_FUNCTION);

  public static final SqlFunction MULTI_MATCH =
      new SqlFunction(
          "MULTI_MATCH",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.BOOLEAN_NULLABLE,
          null,
          OperandTypes.VARIADIC,
          SqlFunctionCategory.USER_DEFINED_FUNCTION);

  // Datetime aliases
  public static final SqlFunction NOW =
      new SqlFunction(
          "NOW",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.TIMESTAMP,
          null,
          OperandTypes.NILADIC,
          SqlFunctionCategory.TIMEDATE);

  public static final SqlFunction CURDATE =
      new SqlFunction(
          "CURDATE",
          SqlKind.OTHER_FUNCTION,
          ReturnTypes.DATE,
          null,
          OperandTypes.NILADIC,
          SqlFunctionCategory.TIMEDATE);

  /** Returns the singleton instance, creating it if necessary. */
  public static OpenSearchSqlOperatorTable instance() {
    return INSTANCE.get();
  }
}
