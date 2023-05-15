/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping

import org.apache.spark.sql.Column
import org.apache.spark.sql.catalyst.expressions.AttributeReference
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
    case filter @ Filter(condition,
      relation @ LogicalRelation(
        baseRelation @ HadoopFsRelation(location, _, _, _, _, _),
        _, Some(table), false)) =>

      // Spark optimize recursively
      if (location.isInstanceOf[FlintSparkSkippingFileIndex]) {
        return filter
      }

      val indexName = FlintSparkSkippingIndex.onTable(table.identifier.table)
      val index = flint.describeIndex(indexName)

      println(indexName)
      println(index)

      if (index.isDefined) {

        val rewrittenPredicate = indexedColumns.head.rewritePredicate(condition) // TODO: handle complex expression and multiple indexed columns

        println(rewrittenPredicate)

        if (rewrittenPredicate.isDefined) {

          println("Found rewrittenPredicate")

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

          println(selectedFiles)

         /*
          val indexRelation = baseRelation match {
            case relation: HadoopFsRelation =>
              val fileIndex = new FlintSparkSkippingFileIndex(relation.location, selectedFiles)
              relation.copy(location = fileIndex)(relation.sparkSession)
            case _ => throw new UnsupportedOperationException("Unsupported relation")
          }
          */
          val fileIndex = new FlintSparkSkippingFileIndex(baseRelation.location, selectedFiles)
          val indexRelation = baseRelation.copy(location = fileIndex)(baseRelation.sparkSession)

          //filter.copy(condition, LogicalRelation(indexRelation, table))
          filter.copy(condition, relation.copy(indexRelation))
        } else {
          filter
        }
      } else {
        filter
      }
  }

}
