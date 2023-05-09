/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping

import org.apache.spark.sql.catalyst.expressions.Expression
import org.apache.spark.sql.catalyst.expressions.aggregate.AggregateFunction

trait FlintSparkSkippingIndexProvider {

  def outputSchema: Seq[String]

  def getAggregators: Seq[AggregateFunction]

  def rewritePredicate(predicate: Expression): Option[Expression]

}
