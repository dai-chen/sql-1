/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.dialect.clickhouse;

import java.io.InputStream;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlFunctionCategory;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlUnresolvedFunction;
import org.apache.calcite.sql.fun.SqlLibrary;
import org.apache.calcite.sql.fun.SqlLibraryOperators;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParseException;
import org.apache.calcite.sql.parser.SqlParser;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlShuttle;
import org.json.JSONArray;
import org.json.JSONObject;

/**
 * Data-driven ClickHouse function alias adapter: renames a ClickHouse function call to an existing
 * Calcite standard or library operator, with optional argument reordering or constant arguments.
 * The table lives in {@code clickhouse/function-aliases.json} on the classpath.
 */
public class ClickHouseFunctionAliasRule extends SqlShuttle {

  /** One alias: target operator name and operand templates ("$i" or a SQL expression). */
  record Alias(String target, List<String> args, SqlOperator operator, SqlNode template) {}

  static final Map<String, Alias> ALIASES = new HashMap<>();
  static final Set<SqlLibrary> LIBRARIES = EnumSet.noneOf(SqlLibrary.class);

  static {
    try (InputStream in =
        ClickHouseFunctionAliasRule.class.getResourceAsStream(
            "/clickhouse/function-aliases.json")) {
      if (in != null) {
        JSONArray entries =
            new JSONArray(new String(in.readAllBytes(), java.nio.charset.StandardCharsets.UTF_8));
        for (int k = 0; k < entries.length(); k++) {
          JSONObject e = entries.getJSONObject(k);
          List<String> args = new ArrayList<>();
          JSONArray a = e.optJSONArray("args");
          for (int j = 0; a != null && j < a.length(); j++) {
            args.add(a.getString(j));
          }
          String template = e.optString("template", "");
          ALIASES.put(
              e.getString("ch"),
              new Alias(
                  e.optString("target", ""),
                  args,
                  resolveField(e.optString("field", "")),
                  template.isEmpty()
                      ? null
                      : parseFragment(template.replaceAll("\\$(\\d+)", "`__ch_arg_$1`"))));
          String lib = e.optString("library", "");
          for (String l : lib.split(",")) {
            if (!l.isBlank()) {
              LIBRARIES.add(SqlLibrary.valueOf(l.trim()));
            }
          }
        }
      }
    } catch (Exception e) {
      throw new IllegalStateException("Failed to load ClickHouse function aliases", e);
    }
  }

  @Override
  public SqlNode visit(SqlCall call) {
    SqlCall visited = (SqlCall) super.visit(call);
    if (visited == null || !(visited.getOperator() instanceof SqlUnresolvedFunction fn)) {
      return visited;
    }
    Alias alias = ALIASES.get(fn.getName());
    if (alias == null) {
      return visited;
    }
    SqlParserPos pos = visited.getParserPosition();
    if (alias.template() != null) {
      // Template alias: substitute `__ch_arg_i` identifiers with the call's operands.
      final SqlCall original = visited;
      return alias
          .template()
          .accept(
              new SqlShuttle() {
                @Override
                public SqlNode visit(SqlIdentifier id) {
                  if (id.isSimple() && id.getSimple().startsWith("__ch_arg_")) {
                    int i = Integer.parseInt(id.getSimple().substring("__ch_arg_".length()));
                    if (i >= original.operandCount()) {
                      throw ClickHouseQueryGuard.reject(
                          "function [" + fn.getName() + "] expects " + (i + 1) + " arguments");
                    }
                    return original.operand(i);
                  }
                  return id;
                }
              });
    }
    List<SqlNode> operands = new ArrayList<>();
    List<String> templates = alias.args().isEmpty() ? defaultTemplates(visited) : alias.args();
    for (String t : templates) {
      if (t.matches("\\$\\d+")) {
        int i = Integer.parseInt(t.substring(1));
        if (i >= visited.operandCount()) {
          // Optional trailing ClickHouse argument not supplied: stop here and let the target
          // operator apply its own default (validation reports a genuine arity mismatch).
          break;
        }
        operands.add(visited.operand(i));
      } else {
        operands.add(parseFragment(t));
      }
    }
    SqlUnresolvedFunction target =
        new SqlUnresolvedFunction(
            new SqlIdentifier(alias.target(), pos),
            null,
            null,
            null,
            null,
            SqlFunctionCategory.USER_DEFINED_FUNCTION);
    return target.createCall(
        visited.getFunctionQuantifier(), pos, operands.toArray(new SqlNode[0]));
  }

  /** Binds the exact Calcite operator by its Java field when several operators share a name. */
  private static SqlOperator resolveField(String field) {
    if (field.isEmpty()) {
      return null;
    }
    for (Class<?> c :
        new Class<?>[] {
          SqlLibraryOperators.class,
          SqlStdOperatorTable.class,
          org.opensearch.sql.expression.function.PPLBuiltinOperators.class
        }) {
      try {
        return (SqlOperator) c.getField(field).get(null);
      } catch (ReflectiveOperationException ignored) {
        // try the next class
      }
    }
    throw new IllegalStateException("Unknown Calcite operator field: " + field);
  }

  /**
   * Non-library operators bound by alias entries (e.g. OpenSearch PPL UDFs), for the operator
   * table.
   */
  static List<SqlOperator> boundOperators() {
    List<SqlOperator> ops = new ArrayList<>();
    for (Alias a : ALIASES.values()) {
      if (a.operator() != null && !ops.contains(a.operator())) {
        ops.add(a.operator());
      }
    }
    return ops;
  }

  private static List<String> defaultTemplates(SqlCall call) {
    List<String> t = new ArrayList<>();
    for (int i = 0; i < call.operandCount(); i++) {
      t.add("$" + i);
    }
    return t;
  }

  private static SqlNode parseFragment(String sql) {
    try {
      return SqlParser.create(sql, ClickHouseSqlSpec.create().parserConfig()).parseExpression();
    } catch (SqlParseException e) {
      throw new IllegalStateException("Bad alias template fragment: " + sql, e);
    }
  }
}
