/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */


package org.opensearch.sql.analysis;

import java.util.Arrays;
import java.util.Optional;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.analysis.symbol.Namespace;
import org.opensearch.sql.analysis.symbol.Symbol;
import org.opensearch.sql.ast.expression.QualifiedName;
import org.opensearch.sql.common.antlr.SyntaxCheckException;
import org.opensearch.sql.exception.SemanticCheckException;
import org.opensearch.sql.expression.DSL;
import org.opensearch.sql.expression.Expression;

/**
 * Analyzer that analyzes qualifier(s) in a full field name.
 */
@RequiredArgsConstructor
public class QualifierAnalyzer {

  private final AnalysisContext context;

  /**
   * For a full name "t.field", first find env with index symbol "t"
   * then look up "field" name inside it.
   */
  public Expression resolve(QualifiedName fullName) {
    TypeEnvironment env = context.peek();
    while (env != null) {
      try {
        env.resolve(new Symbol(Namespace.INDEX_NAME, fullName.first().get()));
      } catch (SemanticCheckException e) {
        // Not the right environment to look up, continue
        env = env.getParent();
        continue;
      }

      try {
        return DSL.ref(fullName.toString(),
            env.resolve(new Symbol(Namespace.FIELD_NAME, fullName.getSuffix())));
      } catch (SemanticCheckException e) {
        break;
      }
    }
    throw new SemanticCheckException("Cannot resolve full name: " + fullName);
  }

  public String unqualified(String... parts) {
    return unqualified(QualifiedName.of(Arrays.asList(parts)));
  }

  /**
   * Get unqualified name if its qualifier symbol found is in index namespace
   * on type environment. Unqualified name means name with qualifier removed.
   * For example, unqualified name of "accounts.age" or "acc.age" is "age".
   *
   * @return  unqualified name if criteria met above, otherwise original name
   */
  public String unqualified(QualifiedName fullName) {
    return isQualifierIndexOrAlias(fullName) ? fullName.rest().toString() : fullName.toString();
  }

  private boolean isQualifierIndexOrAlias(QualifiedName fullName) {
    Optional<String> qualifier = fullName.first();
    if (qualifier.isPresent()) {
      if (isFieldName(qualifier.get())) {
        return false;
      }
      resolveQualifierSymbol(fullName, qualifier.get());
      return true;
    }
    return false;
  }

  private boolean isFieldName(String qualifier) {
    try {
      // Resolve the qualifier in Namespace.FIELD_NAME
      context.peek().resolve(new Symbol(Namespace.FIELD_NAME, qualifier));
      return true;
    } catch (SemanticCheckException e2) {
      return false;
    }
  }

  private void resolveQualifierSymbol(QualifiedName fullName, String qualifier) {
    try {
      context.peek().resolve(new Symbol(Namespace.INDEX_NAME, qualifier));
    } catch (SemanticCheckException e) {
      // Throw syntax check intentionally to indicate fall back to old engine.
      // Need change to semantic check exception in future.
      throw new SyntaxCheckException(String.format(
          "The qualifier [%s] of qualified name [%s] must be an field name, index name or its "
              + "alias", qualifier, fullName));
    }
  }

}
