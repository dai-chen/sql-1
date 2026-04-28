/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.spec.search;

import java.util.function.UnaryOperator;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.SqlWriterConfig;
import org.apache.calcite.sql.dialect.AnsiSqlDialect;
import org.apache.calcite.sql.util.SqlShuttle;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.checkerframework.checker.nullness.qual.Nullable;

/**
 * Pre-validation rewriter that preserves unnamed SELECT-list items' original text as the column
 * name. Without it, Calcite's {@code SqlToRelConverter} synthesizes {@code EXPR$0}, {@code EXPR$1},
 * etc. for anything that is not a simple column reference or an explicit {@code AS} alias.
 *
 * <p>Identical expressions produce identical aliases; Calcite's {@code SqlValidatorUtil.uniquify()}
 * disambiguates them during validation.
 */
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public final class SelectItemAliasRewriter extends SqlShuttle {

  public static final SelectItemAliasRewriter INSTANCE = new SelectItemAliasRewriter();

  /** Unparse config: bare identifiers (e.g., {@code SUM(a)} not {@code SUM(`a`)}). */
  private static final UnaryOperator<SqlWriterConfig> UNPARSE_CONFIG =
      c ->
          c.withDialect(AnsiSqlDialect.DEFAULT)
              .withQuoteAllIdentifiers(false)
              .withAlwaysUseParentheses(false)
              .withSelectListItemsOnSeparateLines(false)
              .withUpdateSetListNewline(false)
              .withIndentation(0);

  @Override
  public @Nullable SqlNode visit(SqlCall call) {
    SqlCall visited = (SqlCall) super.visit(call);
    if (visited instanceof SqlSelect select) {
      rewriteSelectList(select);
    }
    return visited;
  }

  private static void rewriteSelectList(SqlSelect select) {
    SqlNodeList old = select.getSelectList();
    SqlNodeList rewritten = new SqlNodeList(old.getParserPosition());
    for (SqlNode item : old) {
      rewritten.add(aliasIfNeeded(item));
    }
    select.setSelectList(rewritten);
  }

  /** Returns the item wrapped in an AS alias if it needs one, otherwise returns it as-is. */
  private static SqlNode aliasIfNeeded(SqlNode item) {
    if (item.getKind() == SqlKind.AS || item instanceof SqlIdentifier) {
      return item;
    }
    return SqlValidatorUtil.addAlias(item, item.toSqlString(UNPARSE_CONFIG).getSql());
  }
}
