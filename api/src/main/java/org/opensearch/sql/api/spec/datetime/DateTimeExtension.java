/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.spec.datetime;

import java.util.List;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.util.SqlVisitor;
import org.opensearch.sql.api.spec.LanguageSpec;

/**
 * Datetime Extension: rewrites OpenSearch SQL's legacy function-style date/time literals (e.g.,
 * {@code DATE('2020-09-16')}) into ANSI typed literals before validation. Contributes no operators
 * — purely a pre-validation AST rewrite.
 */
public class DateTimeExtension implements LanguageSpec.LanguageExtension {

  @Override
  public List<SqlVisitor<SqlNode>> postParseRules() {
    // Order matters:
    // 1. DateTimeLiteralRewriter — converts DATE('str')/TIME('str')/TIMESTAMP('str') to ANSI
    //    typed literals. Must run first so the downstream rewriters see typed literals.
    // 2. MultiArgDateTimeRewriter — collapses CONVERT_TZ(s,t,t) / DATETIME(s,tz) to TIMESTAMP
    //    literals.
    // 3. CrossTypeTemporalCompareRewriter — wraps cross-type <DATE>/<TIME>/<TIMESTAMP>
    //    comparisons with CAST-to-VARCHAR so the validator accepts them.
    // 4. AvgTemporalRewriter — wraps AVG(<temporal>) in CAST(... AS BIGINT) since ANSI AVG
    //    rejects non-numeric types.
    return List.of(
        DateTimeLiteralRewriter.INSTANCE,
        MultiArgDateTimeRewriter.INSTANCE,
        CrossTypeTemporalCompareRewriter.INSTANCE,
        AvgTemporalRewriter.INSTANCE);
  }
}
