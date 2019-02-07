package com.github.tashoyan.telecom.writer

import java.sql.Timestamp

import com.github.tashoyan.telecom.event.Event._
import com.github.tashoyan.telecom.event._
import com.github.tashoyan.telecom.util.Timestamps._
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.OutputMode

object EventWriterMain extends EventWriterArgParser {
  private implicit val spark: SparkSession = SparkSession.builder()
    .getOrCreate()
  spark.sparkContext
    .setLogLevel("WARN")

  def main(args: Array[String]): Unit = {
    parser.parse(args, EventWriterConfig()) match {
      case Some(config) => doMain(config)
      case None => sys.exit(1)
    }
  }

  private def doMain(config: EventWriterConfig): Unit = {
    println(config)

    val eventReceiver = new KafkaEventReceiver(config.kafkaBrokers, config.kafkaTopic)
    val eventDeduplicator = new DefaultEventDeduplicator(config.watermarkIntervalSec)
    val kafkaEvents = eventReceiver.receiveEvents()
    val events = eventDeduplicator.deduplicateEvents(kafkaEvents)

    val yearMonthColumn = "year_month"
    val yearMonthUdf = udf { origEventTime: Timestamp => yearMonth(origEventTime) }
    val outputEvents = events
      .repartition(col(siteIdColumn))
      .withColumn(yearMonthColumn, yearMonthUdf(col(timestampColumn)))

    val query = outputEvents
      .writeStream
      .outputMode(OutputMode.Append())
      .queryName(getClass.getSimpleName)
      .partitionBy(siteIdColumn, yearMonthColumn)
      .format("parquet")
      .option("path", config.outputDir)
      .option("checkpointLocation", config.checkpointDir)
      .start()
    query.awaitTermination()
  }

}
