/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.executor;

import org.opensearch.sql.ast.tree.UnresolvedPlan;

/**
 * Rewrites an {@link UnresolvedPlan} before analysis, keeping {@code core} free of engine types.
 */
public interface PlanTimeRewriter {
  UnresolvedPlan rewrite(UnresolvedPlan plan);
}
