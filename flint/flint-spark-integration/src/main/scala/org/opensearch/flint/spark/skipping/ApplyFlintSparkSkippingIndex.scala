/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.plans.logical.{Filter, LogicalPlan}
import org.apache.spark.sql.catalyst.rules.Rule
import org.apache.spark.sql.execution.datasources.{HadoopFsRelation, LogicalRelation}
import org.opensearch.flint.spark.FlintSpark
import org.opensearch.flint.spark.skipping.valuelist.ValueListSketch

class ApplyFlintSparkSkippingIndex(flint: FlintSpark) extends Rule[LogicalPlan] {

  // TODO: parse out of FlintMetadata
  val kind = "SkippingIndex"
  val indexedColumns = Seq(ValueListSketch("elb_status_code"))

  override def apply(plan: LogicalPlan): LogicalPlan = plan transform {
    case filter @ Filter(condition, LogicalRelation(baseRelation, _, Some(table), false)) =>

      val indexName = FlintSparkSkippingIndex.onTable(table.qualifiedName)
      val index = flint.describeIndex(indexName)
      if (index.isDefined) {

        val rewrittenPredicate = indexedColumns.head.rewritePredicate(condition) // TODO: handle complex expression and multiple indexed columns

        if (rewrittenPredicate.isDefined) {

          // TODO
          // It's hard to construct FileStatus by index data only
          // Maybe not a problem if StreamingRelation only list files in Batch?
          // But for direct query, a list operation always occur with S3?

          val selectedFiles = flint.queryIndex(indexName)
            .filter(new Column(rewrittenPredicate.get))
            .select("file_path")
            .collect
            .map(_.getString(0))
            .toSet

          val indexRelation = baseRelation match {
            case relation: HadoopFsRelation =>
              val fileIndex = new FlintSparkSkippingFileIndex(relation.location, selectedFiles)
              relation.copy(location = fileIndex)(relation.sparkSession)
            case _ => throw new UnsupportedOperationException("Unsupported relation")
          }

          filter.copy(condition, LogicalRelation(indexRelation, table))
        } else {
          filter
        }
      } else {
        filter
      }
  }

}
