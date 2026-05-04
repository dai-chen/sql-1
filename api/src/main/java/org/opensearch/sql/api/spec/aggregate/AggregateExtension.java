/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.spec.aggregate;

import java.util.List;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.util.SqlVisitor;
import org.opensearch.sql.api.spec.LanguageSpec;

/**
 * Aggregate Extension: rewrites aggregate function calls that Calcite's validator would reject due
 * to unsupported operand types. Currently handles {@code AVG} over temporal types by rewriting the
 * operand to {@code CAST(... AS BIGINT)}.
 *
 * @see AvgTemporalCastRewriter
 */
public class AggregateExtension implements LanguageSpec.LanguageExtension {

  @Override
  public List<SqlVisitor<SqlNode>> postParseRules() {
    return List.of(AvgTemporalCastRewriter.INSTANCE);
  }
}
