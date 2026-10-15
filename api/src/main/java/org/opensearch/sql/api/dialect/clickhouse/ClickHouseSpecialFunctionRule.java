/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.dialect.clickhouse;

import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.sql.SqlBasicTypeNameSpec;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlCharStringLiteral;
import org.apache.calcite.sql.SqlDataTypeSpec;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.fun.SqlCase;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.util.SqlShuttle;

/**
 * ClickHouse functions that a static alias template cannot express: {@code dateTrunc('unit', t)}
 * (unit is a string literal), {@code multiIf(c1, v1, ..., else)} (variadic), and ClickHouse type
 * names in {@code CAST(x AS Int64)}.
 */
public class ClickHouseSpecialFunctionRule extends SqlShuttle {

  private static final Map<String, SqlTypeName> TYPES =
      Map.ofEntries(
          Map.entry("Int8", SqlTypeName.TINYINT),
          Map.entry("Int16", SqlTypeName.SMALLINT),
          Map.entry("Int32", SqlTypeName.INTEGER),
          Map.entry("Int64", SqlTypeName.BIGINT),
          Map.entry("UInt8", SqlTypeName.SMALLINT),
          Map.entry("UInt16", SqlTypeName.INTEGER),
          Map.entry("UInt32", SqlTypeName.BIGINT),
          Map.entry("UInt64", SqlTypeName.BIGINT),
          Map.entry("Float32", SqlTypeName.REAL),
          Map.entry("Float64", SqlTypeName.DOUBLE),
          Map.entry("String", SqlTypeName.VARCHAR),
          Map.entry("Bool", SqlTypeName.BOOLEAN),
          Map.entry("Date", SqlTypeName.DATE),
          Map.entry("DateTime", SqlTypeName.TIMESTAMP));

  /** ClickHouse strings are variable-length: avoid CHAR(n) padding of literal CASE branches. */
  private static SqlNode varying(SqlNode n, SqlParserPos pos) {
    if (n instanceof SqlCharStringLiteral) {
      return SqlStdOperatorTable.CAST.createCall(
          pos, n, new SqlDataTypeSpec(new SqlBasicTypeNameSpec(SqlTypeName.VARCHAR, pos), pos));
    }
    return n;
  }

  @Override
  public SqlNode visit(SqlDataTypeSpec type) {
    SqlTypeName std = TYPES.get(type.getTypeName().getSimple());
    if (std != null && type.getTypeNameSpec().getTypeName().names.size() == 1) {
      return new SqlDataTypeSpec(
          new SqlBasicTypeNameSpec(std, type.getParserPosition()), type.getParserPosition());
    }
    return type;
  }

  @Override
  public SqlNode visit(SqlCall call) {
    SqlCall visited = (SqlCall) super.visit(call);
    if (visited == null
        || !(visited.getOperator() instanceof org.apache.calcite.sql.SqlFunction fn)) {
      return visited;
    }
    SqlParserPos pos = visited.getParserPosition();
    String name = fn.getName();
    if ((name.equals("dateTrunc") || name.equals("DATE_TRUNC") || name.equals("date_trunc"))
        && visited.operandCount() == 2
        && visited.operand(0) instanceof SqlCharStringLiteral unit) {
      TimeUnit tu = TimeUnit.valueOf(unit.toValue().toUpperCase(Locale.ROOT));
      return SqlStdOperatorTable.FLOOR.createCall(
          pos, visited.operand(1), new SqlIntervalQualifier(tu, null, pos));
    }
    if (name.equals("multiIf") && visited.operandCount() >= 3 && visited.operandCount() % 2 == 1) {
      List<SqlNode> whens = new ArrayList<>();
      List<SqlNode> thens = new ArrayList<>();
      for (int i = 0; i + 1 < visited.operandCount(); i += 2) {
        whens.add(visited.operand(i));
        thens.add(varying(visited.operand(i + 1), pos));
      }
      return new SqlCase(
          pos,
          null,
          SqlNodeList.of(pos, whens),
          SqlNodeList.of(pos, thens),
          varying(visited.operand(visited.operandCount() - 1), pos));
    }
    return visited;
  }
}
