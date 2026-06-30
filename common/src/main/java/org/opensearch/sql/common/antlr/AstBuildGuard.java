/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.common.antlr;

import java.util.function.Supplier;
import lombok.RequiredArgsConstructor;
import org.opensearch.sql.common.utils.StringUtils;

/**
 * Guardrails applied while building the AST from the parse tree, protecting the node from
 * pathological queries. Currently enforces a maximum nesting depth so a deeply nested expression
 * (e.g. thousands of chained OR/AND terms) is rejected with a {@link SyntaxCheckException} instead
 * of crashing the node with a StackOverflowError during AST construction. More guardrails can be
 * added here in future.
 *
 * <p>Holds per-traversal state, so an instance must not be shared across threads; the AST builders
 * that use it are created per parse.
 */
@RequiredArgsConstructor
public final class AstBuildGuard {

  public static final int DEFAULT_MAX_DEPTH = 1000;

  private final int maxDepth;

  /** Live nesting depth of the in-progress traversal; resets to 0 between top-level visits. */
  private int depth = 0;

  public AstBuildGuard() {
    this(DEFAULT_MAX_DEPTH);
  }

  /**
   * Runs a single AST-build descent under the configured guardrails.
   *
   * @param visit the visitor descent to execute
   * @return the result of {@code visit}
   * @throws SyntaxCheckException if the nesting depth would exceed the configured maximum
   */
  public <T> T enforce(Supplier<T> visit) {
    if (depth >= maxDepth) {
      throw new SyntaxCheckException(
          StringUtils.format(
              "Expression nesting depth exceeds the maximum allowed [%d]; simplify the query or"
                  + " reduce the number of chained conditions.",
              maxDepth));
    }
    depth++;
    try {
      return visit.get();
    } finally {
      depth--;
    }
  }
}
