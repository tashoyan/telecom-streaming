package com.github.tashoyan.telecom.generator

import java.sql.Timestamp
import java.util.concurrent.TimeUnit

import com.github.tashoyan.telecom.event.Event
import com.github.tashoyan.telecom.spark.KafkaStreamingSender
import com.github.tashoyan.telecom.spark.SparkEvent._
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode
import org.apache.spark.sql.{Dataset, SparkSession}

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
    import spark.implicits._

    val schema = spark.read
      .parquet(config.schemaFile)
      .schema

    val inputEvents = spark.readStream
      .schema(schema)
      .parquet(config.inputDir)

    val eventTimestampUdf = udf { (currentTimeSec: Long, dummyTimestamp: Long) =>
      val timestampMillis: Long = TimeUnit.SECONDS.toMillis(currentTimeSec) - dummyTimestamp
      new Timestamp(timestampMillis)
    }
    val currentTimeSecColumn = "current_time_sec"
    val events: Dataset[Event] = inputEvents
      .withColumn(currentTimeSecColumn, unix_timestamp())
      .withColumn(
        timestampColumn,
        eventTimestampUdf(col(currentTimeSecColumn), col(timestampColumn))
      )
      .asSparkEvents
      .map(_.toEvent)

    val eventSender = new KafkaStreamingSender[Event](
      config.kafkaBrokers,
      config.kafkaTopic,
      siteIdColumn,
      config.checkpointDir,
      OutputMode.Append()
    )
    val query = eventSender.sendingQuery(events)
    query.awaitTermination()
  }

}
