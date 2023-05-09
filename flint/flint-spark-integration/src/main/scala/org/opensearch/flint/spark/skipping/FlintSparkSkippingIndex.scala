/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping

import org.apache.spark.sql.catalyst.dsl.expressions.DslExpression
import org.apache.spark.sql.functions.{column, input_file_name}
import org.apache.spark.sql.{Column, DataFrame, SparkSession}
import org.opensearch.flint.core.FlintMetadata
import org.opensearch.flint.spark.FlintSparkIndex
import org.opensearch.flint.spark.skipping.FlintSparkSkippingIndex.onTable

class FlintSparkSkippingIndex(
  tableName: String, // TODO: should be full table name to avoid conflicts in OS namespace
  indexColumns: Seq[FlintSparkSkippingIndexProvider],
  filterPredicate: Option[String] = Option.empty
) extends FlintSparkIndex {

  override def indexName(): String = onTable(tableName)

  override def metadata(): FlintMetadata =
    FlintMetadata(
      refreshJob(SparkSession.active).schema // TODO: rely on spark???
        .fields.map(field => field.name -> field.dataType.simpleString)
        .toMap,
      s"""
        | {
        |   "version": "0.1",
        |   "derivedDataset": {
        |     "kind": "SkippingIndex",
        |     "indexedColumns": ""
        |   },
        |   "content": {
        |     "location": "$indexName"
        |   },
        |   "source": {
        |     "tableName": "$tableName"
        |   },
        |   "state": "active",
        |   "enabled": true
        | }
        |""".stripMargin)

  override def refreshJob(spark: SparkSession): DataFrame = {
    val aggFuncs = getAggregateFunctions

    val partitionCols = spark.catalog
      .listColumns(tableName)
      .filter(col => col.isPartition)
      .collect()
      .map(c => column(c.name))
      .toSeq

    val groupByCols = partitionCols :+ input_file_name().as("file_path")

    spark.readStream
      .table(tableName)
      // .filter(filterPredicate) TODO: optional
      .groupBy(groupByCols: _*)
      .agg(aggFuncs.head, aggFuncs.tail: _*)
  }

  private def getAggregateFunctions: Seq[Column] = {
    indexColumns.flatMap(provider =>
      (provider.getAggregators, provider.outputSchema).zipped.map {
        case (agg, name) =>
          new Column(agg.toAggregateExpression().as(name))
      })
  }
}

object FlintSparkSkippingIndex {

  def apply(tableName: String, indexedCols: Seq[FlintSparkSkippingIndexProvider]): FlintSparkSkippingIndex =
    new FlintSparkSkippingIndex(tableName, indexedCols)

  def onTable(tableName: String): String = tableName + "_skipping_index"
}
