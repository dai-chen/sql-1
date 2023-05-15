/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping.valuelist

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.analysis.UnresolvedAttribute
import org.apache.spark.sql.catalyst.expressions.{ArrayContains, Attribute, AttributeReference, EqualTo, Expression, Literal, SubqueryExpression}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateFunction, CollectSet}
import org.opensearch.flint.spark.skipping.FlintSparkSkippingIndexProvider

case class ValueListSketch(colName: String) extends FlintSparkSkippingIndexProvider {

  override def outputSchema: Seq[String] = Seq(colName)

  override def getAggregators: Seq[AggregateFunction] = {
    Seq(CollectSet(new Column(colName).expr))
  }

  override def rewritePredicate(predicate: Expression): Option[Expression] = {
    Option(predicate).collect {
      case EqualTo(column: Attribute, value: Literal) =>
        // Assume column name in skipping index for value list
        // is the same as original
        /*
        ArrayContains(
          AttributeReference(colName, ArrayType(IntegerType, containsNull = false))(),
          value)
        */
        EqualTo(UnresolvedAttribute(colName), value)
    }
  }

}

/*
trait ExpressionExtractor {
  def unapply(e: Expression): Option[Expression]
}

case class EqualToExtractor(left: ExpressionExtractor, right: ExpressionExtractor) {
  def unapply(p: Expression): Option[(Expression, Expression)] =
    p match {
      case EqualTo(left(l), right(r)) => Some((l, r))
      case EqualTo(right(r), left(l)) => Some((l, r))
      case _ => None
    }
}

case class NormalizedExprExtractor(expr: Expression, nameMap: Map[ExprId, String])
  extends ExpressionExtractor {
  def unapply(e: Expression): Option[Expression] = {
    val renamed = e.transformUp { case a: Attribute => a.withName(nameMap(a.exprId)) }
    val normalized = ExpressionUtils.normalize(renamed)
    if (expr == normalized) Some(expr) else None
  }
}

case class AttrValueExtractor(attrMap: Map[Attribute, Expression]) extends ExpressionExtractor {
  override def unapply(e: Expression): Option[Expression] = {
    if (canTransform(e)) Some(transform(e)) else None
  }

  private def canTransform(e: Expression): Boolean = {
    e.deterministic &&
      e.references.forall(attrMap.contains) &&
      !SubqueryExpression.hasSubquery(e)
  }

  private def transform(e: Expression): Expression = {
    e.transform { case a: Attribute => attrMap(a) }
  }
}
*/
