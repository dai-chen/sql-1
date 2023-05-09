/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.opensearch.flint.core.{FlintClient, FlintMetadata}
import org.opensearch.flint.core.opensearch.FlintOpenSearchClient

/**
 * Flint Spark integration API.
 */
class FlintSpark(spark: SparkSession) {

  val flintClient: FlintClient = {
    // TODO: Initialize client according to spark.conf
    // such as flint.storage and flint.opensearch.location
    new FlintOpenSearchClient
  }

  def createIndex(index: FlintSparkIndex): Unit = {
    val indexName = index.indexName()
    flintClient.createIndex(indexName, index.metadata())

    /*
    index.refreshJob(spark)
      .writeStream
      .outputMode("append")
      // .format("flint") //TODO
      .format("opensearch")
      .option("checkpointLocation", s"/tmp/$indexName") // Hardcoding for now
      .option("opensearch.resource", indexName)
      .option("opensearch.spark.sql.streaming.sink.log.enabled", false)
      .start()
     */
  }

  def describeIndex(indexName: String): Option[FlintMetadata] = {
    if (flintClient.exists(indexName)) {
      Option.empty
    } else {
      Some(flintClient.getIndexMetadata(indexName))
    }
  }

  def queryIndex(indexName: String): DataFrame = {
    spark.read
      .format("opensearch")
      .load(indexName)
  }
}
