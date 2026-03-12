/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.calcite.parser;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.jdbc.JavaTypeFactoryImpl;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.schema.SchemaPlus;
import org.apache.calcite.schema.Table;
import org.apache.calcite.sql.SqlBasicCall;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlJoin;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.util.SqlShuttle;

/**
 * A SqlShuttle that expands {@link SqlStarExceptReplace} nodes into explicit column lists before
 * Calcite validation. This rewriter resolves table schemas from the provided {@link SchemaPlus} to
 * determine available columns, then applies EXCEPT (column exclusion) and REPLACE (column
 * substitution) transformations.
 */
public class StarExceptReplaceRewriter extends SqlShuttle {

  private final SchemaPlus defaultSchema;
  private final RelDataTypeFactory typeFactory = new JavaTypeFactoryImpl();

  public StarExceptReplaceRewriter(SchemaPlus defaultSchema) {
    this.defaultSchema = defaultSchema;
  }

  public SqlNode rewrite(SqlNode node) {
    return node.accept(this);
  }

  @Override
  public SqlNode visit(SqlCall call) {
    if (call instanceof SqlSelect select) {
      rewriteSelectList(select);
    }
    return super.visit(call);
  }

  private void rewriteSelectList(SqlSelect select) {
    SqlNodeList selectList = select.getSelectList();
    if (selectList == null) {
      return;
    }

    boolean hasStarExceptReplace = false;
    for (SqlNode node : selectList) {
      if (node instanceof SqlStarExceptReplace) {
        hasStarExceptReplace = true;
        break;
      }
    }
    if (!hasStarExceptReplace) {
      return;
    }

    // Collect tables from FROM clause: alias/name -> tableName
    Map<String, String> tableMap = new LinkedHashMap<>();
    collectTables(select.getFrom(), tableMap);

    List<SqlNode> newSelectItems = new ArrayList<>();
    for (SqlNode node : selectList) {
      if (node instanceof SqlStarExceptReplace ser) {
        expandStarExceptReplace(ser, tableMap, newSelectItems);
      } else {
        newSelectItems.add(node);
      }
    }

    select.setSelectList(
        new SqlNodeList(newSelectItems, selectList.getParserPosition()));
  }

  private void collectTables(SqlNode from, Map<String, String> tableMap) {
    if (from == null) {
      return;
    }
    if (from instanceof SqlIdentifier id) {
      String name = id.getSimple();
      tableMap.put(name, name);
    } else if (from instanceof SqlBasicCall call
        && call.getOperator() == SqlStdOperatorTable.AS) {
      SqlNode tableNode = call.operand(0);
      SqlIdentifier alias = call.operand(1);
      if (tableNode instanceof SqlIdentifier tableId) {
        tableMap.put(alias.getSimple(), tableId.getSimple());
      } else if (tableNode instanceof SqlSelect subSelect) {
        // Trace through subquery to find the underlying table
        String resolved = resolveSubqueryTable(subSelect);
        if (resolved != null) {
          tableMap.put(alias.getSimple(), resolved);
        }
      }
    } else if (from instanceof SqlJoin join) {
      collectTables(join.getLeft(), tableMap);
      collectTables(join.getRight(), tableMap);
    }
  }

  /** Recursively resolve the underlying table name from a subquery chain. */
  private String resolveSubqueryTable(SqlSelect select) {
    SqlNode from = select.getFrom();
    if (from instanceof SqlIdentifier id) {
      return id.getSimple();
    }
    if (from instanceof SqlBasicCall call
        && call.getOperator() == SqlStdOperatorTable.AS) {
      SqlNode tableNode = call.operand(0);
      if (tableNode instanceof SqlIdentifier tableId) {
        return tableId.getSimple();
      }
      if (tableNode instanceof SqlSelect subSelect) {
        return resolveSubqueryTable(subSelect);
      }
    }
    return null;
  }

  private void expandStarExceptReplace(
      SqlStarExceptReplace ser,
      Map<String, String> tableMap,
      List<SqlNode> result) {
    SqlIdentifier star = (SqlIdentifier) ser.getStar();
    SqlNodeList except = ser.getExcept();
    SqlNodeList replace = ser.getReplace();
    SqlParserPos pos = ser.getParserPosition();

    // Determine which tables to expand
    String qualifier = null;
    if (star.names.size() > 1) {
      // Qualified star like t.* — qualifier is everything before the last "*"
      qualifier = star.names.get(star.names.size() - 2);
    }

    // Collect tables to expand
    Map<String, String> targetTables;
    if (qualifier != null) {
      // Only expand the matching table/alias
      targetTables = new LinkedHashMap<>();
      for (Map.Entry<String, String> entry : tableMap.entrySet()) {
        if (entry.getKey().equalsIgnoreCase(qualifier)) {
          targetTables.put(entry.getKey(), entry.getValue());
          break;
        }
      }
      if (targetTables.isEmpty()) {
        // No matching table found — leave as-is for validation to report error
        result.add(ser);
        return;
      }
    } else {
      targetTables = tableMap;
    }

    if (targetTables.isEmpty()) {
      // FROM is a subquery or unresolvable — leave as-is
      result.add(ser);
      return;
    }

    for (Map.Entry<String, String> entry : targetTables.entrySet()) {
      String alias = entry.getKey();
      String tableName = entry.getValue();
      Table table = findTable(tableName);
      if (table == null) {
        // Table not found in schema — leave as-is
        result.add(ser);
        return;
      }

      RelDataType rowType = table.getRowType(typeFactory);

      // Collect table column names for validation
      Set<String> tableColumns = new HashSet<>();
      for (RelDataTypeField f : rowType.getFieldList()) {
        tableColumns.add(f.getName().toUpperCase());
      }

      // Validate EXCEPT columns exist and check for duplicates
      if (except != null) {
        Set<String> seenExcept = new HashSet<>();
        for (SqlNode excNode : except) {
          if (excNode instanceof SqlIdentifier excId) {
            String excCol = excId.getSimple().toUpperCase();
            if (!seenExcept.add(excCol)) {
              throw new IllegalArgumentException(
                  "Duplicate column '" + excId.getSimple() + "' in EXCEPT list");
            }
            if (!tableColumns.contains(excCol)) {
              throw new IllegalArgumentException(
                  "EXCEPT column '" + excId.getSimple() + "' not found in table '" + tableName + "'");
            }
          }
        }
      }

      // Validate REPLACE columns exist
      if (replace != null) {
        for (SqlNode repNode : replace) {
          if (repNode instanceof SqlBasicCall repCall
              && repCall.getOperator() == SqlStdOperatorTable.AS) {
            SqlIdentifier repCol = repCall.operand(1);
            if (!tableColumns.contains(repCol.getSimple().toUpperCase())) {
              throw new IllegalArgumentException(
                  "REPLACE column '" + repCol.getSimple() + "' not found in table '" + tableName + "'");
            }
          }
        }
      }

      // Check EXCEPT/REPLACE overlap
      if (except != null && replace != null) {
        Set<String> exceptCols = new HashSet<>();
        for (SqlNode excNode : except) {
          if (excNode instanceof SqlIdentifier excId) {
            exceptCols.add(excId.getSimple().toUpperCase());
          }
        }
        for (SqlNode repNode : replace) {
          if (repNode instanceof SqlBasicCall repCall
              && repCall.getOperator() == SqlStdOperatorTable.AS) {
            SqlIdentifier repCol = repCall.operand(1);
            if (exceptCols.contains(repCol.getSimple().toUpperCase())) {
              throw new IllegalArgumentException(
                  "Column '" + repCol.getSimple() + "' cannot appear in both EXCEPT and REPLACE");
            }
          }
        }
      }

      int resultSizeBefore = result.size();
      for (RelDataTypeField field : rowType.getFieldList()) {
        String colName = field.getName();

        // Check if column is in EXCEPT list
        if (isExcepted(colName, except)) {
          continue;
        }

        // Check if column has a REPLACE expression
        SqlNode replaceExpr = findReplace(colName, replace);
        if (replaceExpr != null) {
          result.add(replaceExpr);
        } else if (qualifier != null) {
          // Qualified: emit alias.colName
          result.add(
              new SqlIdentifier(List.of(alias, colName), pos));
        } else if (tableMap.size() > 1) {
          // Multiple tables, unqualified star: qualify with alias
          result.add(
              new SqlIdentifier(List.of(alias, colName), pos));
        } else {
          // Single table, unqualified star: emit plain colName
          result.add(new SqlIdentifier(colName, pos));
        }
      }
      if (result.size() - resultSizeBefore == 0) {
        throw new IllegalArgumentException(
            "EXCEPT removes all columns from table '" + tableName + "'");
      }
    }
  }

  /** Case-insensitive table lookup since parser casing may differ from schema casing. */
  private Table findTable(String tableName) {
    Table table = defaultSchema.getTable(tableName);
    if (table != null) {
      return table;
    }
    for (String name : defaultSchema.getTableNames()) {
      if (name.equalsIgnoreCase(tableName)) {
        return defaultSchema.getTable(name);
      }
    }
    return null;
  }

  private boolean isExcepted(String colName, SqlNodeList except) {
    if (except == null) {
      return false;
    }
    for (SqlNode node : except) {
      if (node instanceof SqlIdentifier id
          && id.getSimple().equalsIgnoreCase(colName)) {
        return true;
      }
    }
    return false;
  }

  private SqlNode findReplace(String colName, SqlNodeList replace) {
    if (replace == null) {
      return null;
    }
    for (SqlNode node : replace) {
      if (node instanceof SqlBasicCall call
          && call.getOperator() == SqlStdOperatorTable.AS) {
        SqlIdentifier replaceCol = call.operand(1);
        if (replaceCol.getSimple().equalsIgnoreCase(colName)) {
          return call;
        }
      }
    }
    return null;
  }
}
