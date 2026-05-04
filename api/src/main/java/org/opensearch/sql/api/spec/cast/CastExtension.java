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
 * Cast Extension: rewrites non-standard type names in CAST expressions (e.g., {@code STRING →
 * VARCHAR}) before validation.
 *
 * @see StringTypeNameRewriter
 */
public class CastExtension implements LanguageSpec.LanguageExtension {

  @Override
  public List<SqlVisitor<SqlNode>> postParseRules() {
    return List.of(StringTypeNameRewriter.INSTANCE);
  }
}
