/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.spec.library;

import java.util.ArrayList;
import java.util.List;
import org.apache.calcite.sql.SqlBasicFunction;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.type.OperandTypes;
import org.apache.calcite.sql.type.ReturnTypes;
import org.apache.calcite.sql.util.SqlOperatorTables;
import org.opensearch.sql.api.spec.LanguageSpec;

/**
 * Language extension that registers permissive stub UDFs for OpenSearch SQL functions that aren't
 * in any Calcite operator table. These stubs accept any number of arguments of any type and return
 * the nullable type of the first argument (for expression stubs) or a generic nullable result.
 *
 * <p>Purpose: the compatibility report tests whether AE can handle queries end-to-end. With these
 * stubs registered, the validator no longer rejects calls like {@code percentile(age, 90)} or
 * {@code minute_of_hour(ts)} — the call passes validation and AE receives a resolved call tree
 * where AE can either handle it (success) or fail clearly (AE bucket in the report).
 *
 * <p>These stubs are INTENTIONALLY not correct implementations — they only register the name. Don't
 * ship this extension to production without replacing it with proper function implementations that
 * match OpenSearch SQL's semantics.
 */
public class OpenSearchStubFunctionExtension implements LanguageSpec.LanguageExtension {

  /**
   * OpenSearch-specific / MySQL-dialect functions we stub. Each is registered as a single
   * permissive UDF (variadic, type-agnostic). The list was derived from the
   * analyticsSqlCompatibilityReport's "No match found for function signature X" buckets.
   */
  private static final String[] STUB_NAMES = {
    // Aggregate extensions
    "percentile",
    // Null/boolean
    "isnull",
    // Relevance (takes a map of named args)
    "terms",
    "multimatch",
    "nested",
    // Geo
    "geo_intersects",
    // Date parts (MySQL-style and OpenSearch-specific aliases)
    "minute_of_hour",
    "minute_of_day",
    "hour_of_day",
    "day_of_month",
    "day_of_week",
    "day_of_year",
    "week_of_year",
    "month_of_year",
    "second_of_minute",
    // MySQL SECOND/MINUTE/HOUR etc. are already in SqlLibrary.MYSQL but only accept TIME
    // arguments. OpenSearch SQL tests call them with character strings too (e.g.
    // "SECOND('2022-11-22 12:23:34')"). Our VARIADIC stubs absorb those shapes.
    "second",
    "minute",
    "hour",
    "year",
    "month",
    "week",
    // Date arithmetic
    "adddate",
    "subdate",
    "addtime",
    "subtime",
    "datediff",
    "timediff",
    "period_add",
    "period_diff",
    "date_add",
    "date_sub",
    // Date construction
    "makedate",
    "maketime",
    "from_days",
    "to_days",
    "from_unixtime",
    "to_seconds",
    "sec_to_time",
    "time_to_sec",
    "yearweek",
    // Date formatting
    "date_format",
    "time_format",
    "str_to_date",
    "weekday",
    "dayname",
    "monthname",
    "weekofyear",
    // "Now"-family
    "now",
    "sysdate",
    "curdate",
    "curtime",
    "utc_date",
    "utc_time",
    "utc_timestamp",
    "microsecond",
    // Math — note: MIN/MAX/AVG are handled by SqlStdOperatorTable; these are MySQL aliases
    "e",
    "expm1",
    "signum",
    "rint",
    "conv",
    "crc32",
    // ATAN — ANSI is 1-arg, OpenSearch SQL allows 2-arg atan(y, x) = atan2(y, x).
    // Our VARIADIC stub accepts the 2-arg form; the stdlib 1-arg ATAN handles 1-arg calls.
    "atan",
    // convert_tz, day, unix_timestamp — MySQL-dialect library ships these with specific
    // signatures that don't match OpenSearch SQL's calling shapes in some tests; permissive
    // stubs absorb the remainder.
    "convert_tz",
    "day",
    "unix_timestamp",
    // Arithmetic operator aliases
    "add",
    "subtract",
    "multiply",
    "divide",
    "modulus"
  };

  private static final SqlOperatorTable TABLE = buildTable();

  private static SqlOperatorTable buildTable() {
    List<SqlOperator> operators = new ArrayList<>();
    for (String name : STUB_NAMES) {
      boolean zeroArg =
          name.equalsIgnoreCase("NOW")
              || name.equalsIgnoreCase("SYSDATE")
              || name.equalsIgnoreCase("CURDATE")
              || name.equalsIgnoreCase("CURTIME")
              || name.equalsIgnoreCase("UTC_DATE")
              || name.equalsIgnoreCase("UTC_TIME")
              || name.equalsIgnoreCase("UTC_TIMESTAMP")
              || name.equalsIgnoreCase("E");
      operators.add(
          SqlBasicFunction.create(
              // Uppercase so the validator's case-insensitive lookup matches any-case source
              name.toUpperCase(),
              // Zero-arg functions need a fixed return type (VARCHAR fallback). Functions with
              // at least one arg echo ARG0's type. ARG0_NULLABLE_VARYING would assert on
              // non-varying input types; plain ARG0_NULLABLE is safe.
              zeroArg ? ReturnTypes.VARCHAR_NULLABLE : ReturnTypes.ARG0_NULLABLE,
              // Accept any number of arguments of any type.
              OperandTypes.VARIADIC,
              SqlFunctionCategory.USER_DEFINED_FUNCTION));
    }
    return SqlOperatorTables.of(operators);
  }

  @Override
  public SqlOperatorTable operators() {
    return TABLE;
  }
}
