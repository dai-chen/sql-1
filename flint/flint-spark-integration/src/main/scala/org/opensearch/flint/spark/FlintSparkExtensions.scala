/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark

import org.apache.spark.sql.SparkSessionExtensions
import org.opensearch.flint.spark.skipping.ApplyFlintSparkSkippingIndex

class FlintSparkExtensions extends (SparkSessionExtensions => Unit) {

  override def apply(extensions: SparkSessionExtensions): Unit = {
    extensions.injectOptimizerRule { spark =>
      new ApplyFlintSparkSkippingIndex(new FlintSpark(spark))
    }
  }
}
