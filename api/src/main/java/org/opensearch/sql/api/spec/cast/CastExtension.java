/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.spec.cast;

import java.util.List;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.util.SqlVisitor;
import org.opensearch.sql.api.spec.LanguageSpec;

/**
 * Cast Extension: rewrites OpenSearch SQL's non-standard type names ({@code STRING}, etc.) in
 * {@code CAST} expressions to ANSI equivalents so Calcite's validator accepts them.
 */
public class CastExtension implements LanguageSpec.LanguageExtension {

  @Override
  public List<SqlVisitor<SqlNode>> postParseRules() {
    return List.of(CastTypeNameRewriter.INSTANCE);
  }
}
