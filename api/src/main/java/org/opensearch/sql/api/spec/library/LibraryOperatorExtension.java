/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.spec.library;

import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlLibrary;
import org.apache.calcite.sql.fun.SqlLibraryOperatorTableFactory;
import org.opensearch.sql.api.spec.LanguageSpec;

/**
 * Registers Calcite's MYSQL and BIG_QUERY library operator tables as a blanket extension. This
 * unblocks validation for a broad tail of MySQL-style function names that OpenSearch SQL
 * historically accepts — including DATETIME, CONCAT_WS, RTRIM, LTRIM, IF, IFNULL, LENGTH, LPAD,
 * RPAD, NVL, and others — without per-function declarations.
 *
 * <p>Addresses compatibility report buckets: DATETIME (#5, #22, #23), CONCAT_WS (#24), RTRIM (#25).
 *
 * <p>Note: registration makes these functions validatable but does not add local execution.
 * Pushdown and runtime evaluation is the analytics-engine's responsibility.
 */
public class LibraryOperatorExtension implements LanguageSpec.LanguageExtension {

  @Override
  public SqlOperatorTable operators() {
    return SqlLibraryOperatorTableFactory.INSTANCE.getOperatorTable(
        SqlLibrary.MYSQL, SqlLibrary.BIG_QUERY);
  }
}
