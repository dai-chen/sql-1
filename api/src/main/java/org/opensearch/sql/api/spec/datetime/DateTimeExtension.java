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
 * {@code DATE('2020-09-16')}) into ANSI typed literals before validation, then inserts explicit
 * {@code CAST} operators into comparisons between mismatched temporal types (e.g., DATE vs
 * TIMESTAMP) so Calcite's validator accepts them. Contributes no operators — purely pre-validation
 * AST rewrites.
 *
 * @see DateTimeLiteralRewriter
 * @see DateTimeComparisonRewriter
 */
public class DateTimeExtension implements LanguageSpec.LanguageExtension {

  @Override
  public List<SqlVisitor<SqlNode>> postParseRules() {
    return List.of(
        DateTimeLiteralRewriter.INSTANCE,
        ConvertTzRewriter.INSTANCE,
        DateTimeComparisonRewriter.INSTANCE);
  }
}
