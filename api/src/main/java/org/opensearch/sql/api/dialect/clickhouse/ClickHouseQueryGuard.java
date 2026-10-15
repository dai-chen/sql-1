/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.dialect.clickhouse;

import java.util.EnumSet;
import java.util.List;
import java.util.Set;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlDynamicParam;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlPivot;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlUnpivot;
import org.apache.calcite.sql.util.SqlShuttle;

/**
 * DQL-only guard for the ClickHouse dialect, applied to the parse tree <b>before</b> validation, so
 * nothing is resolved against the OpenSearch schema (no index metadata is loaded) for a rejected
 * query. The planner separately requires the root to be a query ({@code SqlKind.QUERY}); the
 * Calcite parser accepts exactly one statement.
 */
public class ClickHouseQueryGuard extends SqlShuttle {

  /** Statement kinds that must never appear anywhere in a dialect query. */
  private static final Set<SqlKind> FORBIDDEN =
      EnumSet.of(
          SqlKind.INSERT,
          SqlKind.DELETE,
          SqlKind.UPDATE,
          SqlKind.MERGE,
          SqlKind.SET_OPTION,
          SqlKind.EXPLAIN,
          SqlKind.DESCRIBE_SCHEMA,
          SqlKind.DESCRIBE_TABLE,
          SqlKind.COLLECTION_TABLE,
          SqlKind.SNAPSHOT,
          SqlKind.CURSOR,
          SqlKind.PROCEDURE_CALL);

  /** Default OpenSearch catalog name registered by the SQL plugin. */
  static final String CATALOG = "opensearch";

  @Override
  public SqlNode visit(SqlCall call) {
    SqlKind kind = call.getKind();
    if (FORBIDDEN.contains(kind) || SqlKind.DDL.contains(kind)) {
      throw reject("statement or clause " + kind + " is not allowed; only queries are supported");
    }
    if (call instanceof SqlSelect select) {
      checkFrom(select.getFrom());
    }
    return super.visit(call);
  }

  @Override
  public SqlNode visit(SqlDynamicParam param) {
    throw reject("positional parameters are not supported");
  }

  private void checkFrom(SqlNode from) {
    if (from == null) {
      return;
    }
    if (from instanceof SqlIdentifier id) {
      checkTableName(id.names);
    } else if (from instanceof SqlJoin join) {
      checkFrom(join.getLeft());
      checkFrom(join.getRight());
    } else if (from instanceof SqlPivot pivot) {
      checkFrom(pivot.query);
    } else if (from instanceof SqlUnpivot unpivot) {
      checkFrom(unpivot.query);
    } else if (from instanceof SqlBasicCall call) {
      switch (call.getKind()) {
        case AS, LATERAL, TABLESAMPLE -> checkFrom(call.operand(0));
        case UNNEST, VALUES -> {}
        default -> {
          if (FORBIDDEN.contains(call.getKind())) {
            throw reject("table function or relation " + call.getKind() + " is not allowed");
          }
        }
      }
    }
    // SqlSelect / set operations (derived tables) are visited recursively by the shuttle.
  }

  /** Allows only plain OpenSearch index names in the default catalog. */
  static void checkTableName(List<String> names) {
    if (names.size() == 2 && !CATALOG.equals(names.get(0))) {
      throw reject("catalog [" + names.get(0) + "] is not allowed");
    }
    if (names.isEmpty() || names.size() > 2) {
      throw reject("table reference " + String.join(".", names) + " is not allowed");
    }
    String table = names.get(names.size() - 1);
    if (table.isEmpty() || table.startsWith(".") || table.startsWith("_")) {
      throw reject("system or hidden index [" + table + "] is not allowed");
    }
    for (char c : new char[] {':', '*', ',', '/', '\\', '?', '"', '<', '>', '|', ' '}) {
      if (table.indexOf(c) >= 0) {
        throw reject(
            "index name ["
                + table
                + "] may not contain '"
                + c
                + "' (remote, pattern or path access)");
      }
    }
  }

  static IllegalArgumentException reject(String message) {
    return new IllegalArgumentException("dialect=clickhouse rejected the query: " + message);
  }
}
