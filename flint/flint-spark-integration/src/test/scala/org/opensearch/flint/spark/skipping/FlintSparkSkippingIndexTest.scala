/*
 * Copyright OpenSearch Contributors
 * SPDX-License-Identifier: Apache-2.0
 */

package org.opensearch.flint.spark.skipping

import org.apache.spark.SparkFunSuite
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, dayofmonth, month, year}
import org.opensearch.flint.spark.skipping.valuelist.ValueListSketch
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll}

class FlintSparkSkippingIndexTest extends SparkFunSuite
  with BeforeAndAfterAll with BeforeAndAfter {

  lazy val spark: SparkSession = SparkSession
    .builder()
    .master(s"local[1]")
    .config("spark.driver.bindAddress", "127.0.0.1")
    // .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
    .config("spark.ui.enabled", "false")
    .config("spark.ui.showConsoleProgress", "false")
    .appName("Flint Test")
    .getOrCreate()

  override def afterAll(): Unit = spark.stop()

  test("Test") {
    SparkSession.setActiveSession(spark)

    // spark.sql("create temporary view alb_logs (time timestamp, elb_status_code int) using text partitioned by (year INT, month INT, day INT)")
    // Create a DataFrame
    val data = Seq(
      ("2022-01-01T00:00:00Z", 200, "192.168.1.1"),
      ("2022-01-01T00:01:00Z", 404, "192.168.1.2"),
      ("2022-01-01T00:02:00Z", 200, "192.168.1.3"),
      ("2022-01-02T00:00:00Z", 200, "192.168.1.4"),
      ("2022-01-02T00:01:00Z", 200, "192.168.1.5"),
      ("2022-01-02T00:02:00Z", 500, "192.168.1.6")
    )
    val df = spark.createDataFrame(data).toDF("time", "elb_status_code", "client_ip")
      .withColumn("year", year(col("time")))
      .withColumn("month", month(col("time")))
      .withColumn("day", dayofmonth(col("time")))

    // Register the DataFrame as a partitioned temporary table in text format
    df.write.format("json").partitionBy("year", "month", "day")
    // df.createOrReplaceTempView("alb_logs")
    df.registerTempTable("alb_logs")
    //spark.sql("CREATE TEMPORARY VIEW alb_logs_partitioned USING text OPTIONS ('path'='alb_logs') PARTITIONED BY (year, month, day)")

    val index = FlintSparkSkippingIndex("alb_logs", Seq(ValueListSketch("elb_status")))

    assert(index.indexName() == "alb_logs-skipping-index")
    println(index.metadata())
  }

}
