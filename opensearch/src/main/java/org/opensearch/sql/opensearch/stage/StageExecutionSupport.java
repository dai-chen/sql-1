/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.sql.opensearch.stage;

import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.MULTI_FIELDS_RELEVANCE_FUNCTION_SET;
import static org.opensearch.sql.calcite.utils.UserDefinedFunctionUtils.SINGLE_FIELD_RELEVANCE_FUNCTION_SET;

import java.util.BitSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Set;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.logical.LogicalCalc;
import org.apache.calcite.rel.logical.LogicalFilter;
import org.apache.calcite.rel.logical.LogicalProject;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexLambda;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeName;
import org.opensearch.sql.calcite.plan.rel.LogicalSystemLimit;
import org.opensearch.sql.calcite.type.AbstractExprRelDataType;
import org.opensearch.sql.calcite.utils.OpenSearchTypeFactory;
import org.opensearch.sql.data.type.ExprCoreType;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType;
import org.opensearch.sql.opensearch.data.type.OpenSearchDataType.MappingType;
import org.opensearch.sql.opensearch.data.type.OpenSearchDateType;
import org.opensearch.sql.opensearch.storage.scan.AbstractCalciteIndexScan;
import org.opensearch.sql.opensearch.storage.scan.context.PushDownType;

/** Defines the operators, mappings, and wire types executable by the staged runtime. */
final class StageExecutionSupport {

  private static final Set<String> SHARD_UNSUPPORTED_FUNCTIONS =
      Set.of("REX_EXTRACT", "REX_EXTRACT_MULTI", "PARSE", "PATTERN_PARSER", "STRFTIME");

  private StageExecutionSupport() {}

  static boolean supportsScan(AbstractCalciteIndexScan scan, boolean aggregateFragment) {
    return hasSupportedPushdown(scan)
        && hasSupportedIndexMapping(scan)
        && (aggregateFragment || hasSupportedWireTypes(scan));
  }

  static boolean supportsRequiredMappings(
      AbstractCalciteIndexScan scan, BitSet requiredInputFields) {
    Map<String, OpenSearchDataType> fieldTypes = fieldTypes(scan);
    List<RelDataTypeField> fields = scan.getRowType().getFieldList();
    for (int field = requiredInputFields.nextSetBit(0);
        field >= 0;
        field = requiredInputFields.nextSetBit(field + 1)) {
      OpenSearchDataType type = fieldTypes.get(fields.get(field).getName());
      if (type instanceof OpenSearchDateType dateType && dateType.hasFormats()) {
        return false;
      }
    }
    return true;
  }

  static boolean supportsCoordinator(RelNode node) {
    Package nodePackage = node.getClass().getPackage();
    boolean standardLogical =
        nodePackage != null && nodePackage.getName().equals("org.apache.calcite.rel.logical");
    if (!standardLogical && !(node instanceof LogicalSystemLimit)) {
      return false;
    }
    if (containsUnsupportedCoordinatorExpression(node)) {
      return false;
    }
    return node.getInputs().stream().allMatch(StageExecutionSupport::supportsCoordinator);
  }

  static boolean supportsShardExpressions(List<? extends RexNode> expressions) {
    return !containsUnsupportedExpression(expressions, true);
  }

  private static boolean hasSupportedWireTypes(AbstractCalciteIndexScan scan) {
    return scan.getRowType().getFieldList().stream()
        .noneMatch(field -> isWireSensitiveType(field.getType()));
  }

  private static boolean isWireSensitiveType(RelDataType type) {
    SqlTypeName typeName = type.getSqlTypeName();
    return isUnsupportedBinaryUdt(type)
        || typeName == SqlTypeName.ARRAY
        || typeName == SqlTypeName.MAP
        || typeName == SqlTypeName.MULTISET
        || typeName == SqlTypeName.ROW
        || typeName == SqlTypeName.GEOMETRY;
  }

  private static boolean isUnsupportedBinaryUdt(RelDataType type) {
    return type instanceof AbstractExprRelDataType<?> exprType
        && exprType.getExprType() == ExprCoreType.BINARY;
  }

  private static boolean hasSupportedIndexMapping(AbstractCalciteIndexScan scan) {
    if (scan.getOsIndex().getIndexMappings().size() != 1) {
      return false;
    }
    return fieldTypes(scan).values().stream()
        .noneMatch(type -> type.getMappingType() == MappingType.Nested);
  }

  private static Map<String, OpenSearchDataType> fieldTypes(AbstractCalciteIndexScan scan) {
    return OpenSearchDataType.traverseAndFlatten(
        scan.getOsIndex().getIndexMappings().values().iterator().next().getFieldMappings());
  }

  private static boolean hasSupportedPushdown(AbstractCalciteIndexScan scan) {
    var pushdown = scan.getPushDownContext();
    return !pushdown.isAggregatePushed()
        && pushdown.getAggSpec() == null
        && !pushdown.isScriptPushed()
        && pushdown.stream()
            .allMatch(
                operation ->
                    operation.type() == PushDownType.FILTER
                        || operation.type() == PushDownType.PROJECT);
  }

  private static boolean containsUnsupportedCoordinatorExpression(RelNode node) {
    if (node instanceof LogicalProject project) {
      return containsUnsupportedExpression(project.getProjects(), false);
    }
    if (node instanceof LogicalFilter filter) {
      return containsUnsupportedExpression(List.of(filter.getCondition()), false);
    }
    if (node instanceof LogicalCalc calc) {
      return containsUnsupportedExpression(calc.getProgram().getExprList(), false);
    }
    return false;
  }

  private static boolean containsUnsupportedExpression(
      List<? extends RexNode> expressions, boolean shard) {
    boolean[] found = {false};
    RexVisitorImpl<Void> visitor =
        new RexVisitorImpl<>(true) {
          @Override
          public Void visitCall(RexCall call) {
            String operatorName = call.getOperator().getName().toLowerCase(Locale.ROOT);
            if (call.getKind() == SqlKind.SEARCH
                || SINGLE_FIELD_RELEVANCE_FUNCTION_SET.contains(operatorName)
                || MULTI_FIELDS_RELEVANCE_FUNCTION_SET.contains(operatorName)
                || (shard
                    && SHARD_UNSUPPORTED_FUNCTIONS.contains(
                        call.getOperator().getName().toUpperCase(Locale.ROOT)))
                || isNegatedTemporalPredicate(call)
                || isTemporalArithmetic(call)
                || isUnsupportedBinaryUdtCall(call)
                || (shard && containsComplexType(call))) {
              found[0] = true;
              return null;
            }
            return super.visitCall(call);
          }

          @Override
          public Void visitLambda(RexLambda lambda) {
            found[0] = true;
            return null;
          }

          @Override
          public Void visitLiteral(RexLiteral literal) {
            if (shard && isUnsupportedBinaryUdt(literal.getType())) {
              found[0] = true;
            }
            return null;
          }

          @Override
          public Void visitSubQuery(RexSubQuery subQuery) {
            found[0] = true;
            return null;
          }
        };
    for (RexNode expression : expressions) {
      if (shard && containsComplexType(expression)) {
        return true;
      }
      expression.accept(visitor);
      if (found[0]) {
        return true;
      }
    }
    return false;
  }

  private static boolean isNegatedTemporalPredicate(RexCall call) {
    return (call.getKind() == SqlKind.NOT || call.getKind() == SqlKind.NOT_EQUALS)
        && containsTemporalType(call);
  }

  private static boolean containsTemporalType(RexNode expression) {
    if (isTemporalType(expression.getType())) {
      return true;
    }
    if (expression instanceof RexCall call) {
      String operatorName = call.getOperator().getName().toUpperCase(Locale.ROOT);
      return Set.of("DATE", "TIME", "TIMESTAMP").contains(operatorName)
          || call.getOperands().stream().anyMatch(StageExecutionSupport::containsTemporalType);
    }
    return false;
  }

  private static boolean isTemporalType(RelDataType type) {
    return OpenSearchTypeFactory.isTimeBasedType(type);
  }

  private static boolean isTemporalArithmetic(RexCall call) {
    return switch (call.getKind()) {
      case PLUS, MINUS, TIMES, DIVIDE ->
          call.getOperands().stream()
              .map(RexNode::getType)
              .anyMatch(StageExecutionSupport::isTemporalType);
      default -> false;
    };
  }

  private static boolean isUnsupportedBinaryUdtCall(RexCall call) {
    return isUnsupportedBinaryUdt(call.getType())
        || call.getOperands().stream()
            .map(RexNode::getType)
            .anyMatch(StageExecutionSupport::isUnsupportedBinaryUdt);
  }

  private static boolean containsComplexType(RexNode expression) {
    SqlTypeName typeName = expression.getType().getSqlTypeName();
    if (typeName == SqlTypeName.ARRAY
        || typeName == SqlTypeName.MAP
        || typeName == SqlTypeName.MULTISET
        || typeName == SqlTypeName.ROW
        || typeName == SqlTypeName.GEOMETRY) {
      return true;
    }
    if (expression instanceof RexCall call) {
      return call.getOperands().stream().anyMatch(StageExecutionSupport::containsComplexType);
    }
    if (expression instanceof RexFieldAccess fieldAccess) {
      return containsComplexType(fieldAccess.getReferenceExpr());
    }
    return false;
  }
}
