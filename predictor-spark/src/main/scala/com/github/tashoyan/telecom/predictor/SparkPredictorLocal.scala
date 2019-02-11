package com.github.tashoyan.telecom.predictor

import com.github.tashoyan.telecom.event.Event._
import com.github.tashoyan.telecom.event.SparkEventAdapter.EventDataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode

//scalastyle:off
object SparkPredictorLocal {
  private val port: Long = 9999

  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession.builder()
      .getOrCreate()
    spark.sparkContext
      .setLogLevel("WARN")

    val input = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", port)
      .option("includeTimestamp", value = true)
      .load()

    val events = input
      .withColumn(siteIdColumn, lit(1L))
      .withColumn(severityColumn, lit("MAJOR"))
      .withColumn(infoColumn, lit("On site ") + col(siteIdColumn))
      .asEvents

    //    def dummyFunction: (Long, Iterator[Event], GroupState[ProblemState]) => Iterator[Alarm] = {
    //    (siteId, events, state) =>
    //}

    val query = events.writeStream
      .outputMode(OutputMode.Append())
      .format("console")
      .option("truncate", value = false)
      .start()
    query.awaitTermination()
  }

}
