package com.github.tashoyan.telecom.generator

import java.sql.Timestamp
import java.util.concurrent.TimeUnit

import com.github.tashoyan.telecom.event.Event._
import com.github.tashoyan.telecom.event.SparkEventAdapter.EventDataFrame
import com.github.tashoyan.telecom.event.{Event, KafkaStreamingSender}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object EventGeneratorMain extends EventGeneratorArgParser {

  def main(args: Array[String]): Unit = {
    parser.parse(args, EventGeneratorConfig()) match {
      case Some(config) => doMain(config)
      case None => sys.exit(1)
    }
  }

  private def doMain(config: EventGeneratorConfig): Unit = {
    println(config)

    implicit val spark: SparkSession = SparkSession.builder()
      .getOrCreate()
    spark.sparkContext
      .setLogLevel("WARN")

    val schema = spark.read
      .parquet(config.schemaFile)
      .schema

    val inputEvents = spark.readStream
      .schema(schema)
      .parquet(config.inputDir)

    val eventTimestampUdf = udf { (currentTimeSec: Long, dummyTimestamp: Timestamp) =>
      val timestampMillis: Long = TimeUnit.SECONDS.toMillis(currentTimeSec) - dummyTimestamp.getTime
      new Timestamp(timestampMillis)
    }
    val currentTimeSecColumn = "current_time_sec"
    val events = inputEvents
      .withColumn(currentTimeSecColumn, unix_timestamp())
      .withColumn(timestampColumn, eventTimestampUdf(col(currentTimeSecColumn), col(timestampColumn)))
      .asEvents

    val eventSender = new KafkaStreamingSender[Event](
      config.kafkaBrokers,
      config.kafkaTopic,
      siteIdColumn,
      config.checkpointDir
    )
    val query = eventSender.sendingQuery(events)
    query.awaitTermination()
  }

}
