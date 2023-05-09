/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import org.apache.spark.sql.{DataFrame, SparkSession}
import org.opensearch.flint.core.FlintMetadata

trait FlintSparkIndex {

  def indexName(): String

  def metadata(): FlintMetadata

  def refreshJob(spark: SparkSession): DataFrame

}
