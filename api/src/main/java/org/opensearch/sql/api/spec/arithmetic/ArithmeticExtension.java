/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.api.spec.arithmetic;

import java.util.List;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.util.SqlVisitor;
import org.opensearch.sql.api.spec.LanguageSpec;

/**
 * Arithmetic Extension: rewrites OpenSearch SQL's function-style arithmetic calls ({@code add},
 * {@code subtract}, {@code multiply}, {@code divide}, {@code modulus}) into standard SQL binary
 * operators before validation.
 *
 * @see ArithmeticFunctionRewriter
 */
public class ArithmeticExtension implements LanguageSpec.LanguageExtension {

  @Override
  public List<SqlVisitor<SqlNode>> postParseRules() {
    return List.of(ArithmeticFunctionRewriter.INSTANCE);
  }
}
