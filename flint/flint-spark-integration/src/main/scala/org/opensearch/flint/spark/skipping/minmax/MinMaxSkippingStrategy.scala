/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping.minmax

import org.opensearch.flint.spark.skipping.FlintSparkSkippingStrategy

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.{And, AttributeReference, EqualTo, GreaterThanOrEqual, LessThanOrEqual, Literal, Predicate}
import org.apache.spark.sql.catalyst.expressions.aggregate.{AggregateFunction, Max, Min}

/**
 * Skipping strategy based on min-max boundary of column values.
 */
class MinMaxSkippingStrategy(
    override val kind: String = "minmax",
    override val columnName: String,
    override val columnType: String)
    extends FlintSparkSkippingStrategy {

  override def outputSchema(): Map[String, String] =
    Map(minColName -> columnType, maxColName -> columnType)

  override def getAggregators: Seq[AggregateFunction] =
    Seq(Min(new Column(columnName).expr), Max(new Column(columnName).expr))

  override def rewritePredicate(predicate: Predicate): Option[Predicate] =
    predicate.collect { case EqualTo(AttributeReference(`columnName`, _, _, _), value: Literal) =>
      And(
        LessThanOrEqual(new Column(minColName).expr, value),
        GreaterThanOrEqual(new Column(maxColName).expr, value))
    }.headOption

  private def minColName = columnName + "_min"
  private def maxColName = columnName + "_max"
}
