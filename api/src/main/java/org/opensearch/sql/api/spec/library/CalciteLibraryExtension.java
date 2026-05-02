/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.spec.library;

import java.util.EnumSet;
import org.apache.calcite.sql.SqlOperatorTable;
import org.apache.calcite.sql.fun.SqlLibrary;
import org.apache.calcite.sql.fun.SqlLibraryOperatorTableFactory;
import org.opensearch.sql.api.spec.LanguageSpec;

/**
 * Language extension that registers Calcite's library-function tables with the SQL validator.
 * Beyond {@link org.apache.calcite.sql.fun.SqlStdOperatorTable} (which covers only ANSI SQL),
 * Calcite ships {@link SqlLibraryOperators} that contain dialect-specific functions — e.g. MySQL
 * has {@code IFNULL}, {@code IF}, {@code SUBSTR}, {@code DATE_FORMAT}, {@code STR_TO_DATE}, lots of
 * date/time arithmetic; BigQuery has {@code CONCAT_WS}, {@code DATETIME}, etc.
 *
 * <p>OpenSearch SQL historically mirrors MySQL's function surface, so registering MySQL + BigQuery
 * + Oracle + PostgreSQL + STANDARD libraries unblocks a large fraction of the "No match found for
 * function signature X" failures in the compatibility report without needing per- function UDF
 * code.
 *
 * <p>For functions NOT in any Calcite library (e.g., OpenSearch-specific {@code percentile}, {@code
 * nested}, {@code terms}, {@code multimatch}, {@code geo_intersects}, {@code minute_of_day}, {@code
 * weekday}, etc.), the validator will still reject them — those need either a dedicated rewriter or
 * UDF registration (out of scope for this extension).
 */
public class CalciteLibraryExtension implements LanguageSpec.LanguageExtension {

  /**
   * Libraries to enable. Order doesn't matter — the factory builds a chained table. We pick the
   * most common dialects so the validator has the widest function surface available.
   *
   * <p>Note: we DELIBERATELY do NOT include {@link SqlLibrary#STANDARD} because it contains
   * overrides of ANSI aggregates ({@code MIN}, {@code MAX}, {@code AVG}) with signatures that clash
   * with {@link org.apache.calcite.sql.fun.SqlStdOperatorTable}, causing the validator to reject
   * correct-looking calls like {@code SELECT MAX(age) FROM t} with "No match found for function
   * signature". {@link SqlStdOperatorTable} is always chained in first by {@link
   * LanguageSpec#operatorTable()}, so ANSI aggregates are covered there.
   */
  private static final EnumSet<SqlLibrary> LIBRARIES =
      EnumSet.of(SqlLibrary.MYSQL, SqlLibrary.BIG_QUERY, SqlLibrary.POSTGRESQL, SqlLibrary.ORACLE);

  private static final SqlOperatorTable TABLE =
      SqlLibraryOperatorTableFactory.INSTANCE.getOperatorTable(LIBRARIES);

  @Override
  public SqlOperatorTable operators() {
    return TABLE;
  }
}
