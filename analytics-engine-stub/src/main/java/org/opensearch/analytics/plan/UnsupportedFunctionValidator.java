/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.analytics.plan;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelVisitor;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexShuttle;
import org.apache.calcite.sql.SqlKind;
import org.opensearch.analytics.spi.EngineCapabilities;

/**
 * Walks a {@link RelNode} tree and throws {@link UnsupportedOperationException} on the first
 * function call not supported by the given {@link EngineCapabilities}.
 */
public final class UnsupportedFunctionValidator {

  private UnsupportedFunctionValidator() {}

  /** Validates all function calls in the plan against the given capabilities. */
  public static void validate(RelNode relNode, EngineCapabilities capabilities) {
    new RelVisitor() {
      @Override
      public void visit(RelNode node, int ordinal, RelNode parent) {
        checkExpressions(node, capabilities);
        super.visit(node, ordinal, parent);
      }
    }.go(relNode);
  }

  private static void checkExpressions(RelNode node, EngineCapabilities capabilities) {
    RexShuttle checker = new FunctionChecker(capabilities);
    if (node instanceof LogicalFilter) {
      ((LogicalFilter) node).getCondition().accept(checker);
    } else if (node instanceof LogicalProject) {
      for (RexNode expr : ((LogicalProject) node).getProjects()) {
        expr.accept(checker);
      }
    }
  }

  private static class FunctionChecker extends RexShuttle {
    private final EngineCapabilities capabilities;

    FunctionChecker(EngineCapabilities capabilities) {
      this.capabilities = capabilities;
    }

    @Override
    public RexNode visitCall(RexCall call) {
      if (call.getKind() == SqlKind.OTHER_FUNCTION) {
        String name = call.getOperator().getName();
        if (!capabilities.supportsFunction(name)) {
          throw new UnsupportedOperationException(
              "Function '" + name + "' is not supported for Parquet index queries");
        }
      }
      return super.visitCall(call);
    }
  }
}
