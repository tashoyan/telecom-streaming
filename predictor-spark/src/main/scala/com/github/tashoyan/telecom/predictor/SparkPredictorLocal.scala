package com.github.tashoyan.telecom.predictor

import java.util.concurrent.TimeUnit

import com.github.tashoyan.telecom.event.Event._
import com.github.tashoyan.telecom.event.SparkEventAdapter.EventDataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.{GroupStateTimeout, OutputMode}
import org.apache.spark.sql.types.LongType

object SparkPredictorLocal {
  private val port: Long = 9999
  private val watermarkIntervalSec = 20L
  private val problemTimeoutSec = 10L

  def main(args: Array[String]): Unit = {
    implicit val spark: SparkSession = SparkSession.builder()
      .getOrCreate()
    spark.sparkContext
      .setLogLevel("WARN")
    import spark.implicits._

    val input = spark.readStream
      .format("socket")
      .option("host", "localhost")
      .option("port", port)
      .option("includeTimestamp", value = true)
      .load()

    val infoCol = when(
      col("input_words")(1).contains("2"),
      concat_ws(" ", lit("Smoke on site"), col(siteIdColumn))
    )
      .otherwise(
        concat_ws(" ", lit("Heat on site"), col(siteIdColumn))
      )

    val events = input
      .withColumn("input_words", split(col("value"), "\\s+"))
      .withColumn(siteIdColumn, col("input_words")(0) cast LongType)
      .withColumn(severityColumn, lit("MAJOR"))
      .withColumn(infoColumn, infoCol)
      .asEvents

    val alarmStateFunction = new FireAlarmStateFunction(TimeUnit.SECONDS.toMillis(problemTimeoutSec))
    val alarms = events
      .withWatermark(timestampColumn, s"$watermarkIntervalSec seconds")
      .groupByKey(_.siteId)
      .flatMapGroupsWithState(OutputMode.Update(), GroupStateTimeout.EventTimeTimeout())(alarmStateFunction.updateAlarmState)

    val query = alarms.writeStream
      .outputMode(OutputMode.Update())
      .format("console")
      .option("truncate", value = false)
      .start()
    query.awaitTermination()
  }

}
